const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const ArrayList = std.ArrayListUnmanaged;

const native_endian = @import("builtin").target.cpu.arch.endian();

const header = @import("./header.zig");
const chunk = @import("./chunk.zig");
const schema = @import("./schema.zig");

pub const Error = error{
    UnsupportedTypeForMinMax,
    OutOfMemory,
    DictArrayNotSupported,
    RunEndEncodedArrayNotSupported,
    BinaryViewArrayNotSupported,
    ListViewArrayNotSupported,
    DataSectionOverflow,
    NonBinaryArrayWithDict,
    Lz4CompressionFail,
    ZstdCompressionFail,
};

/// Swap bytes of integer if target is big endian
fn maybe_byte_swap(val: anytype) @TypeOf(val) {
    return switch (native_endian) {
        .big => @byteSwap(val),
        .little => val,
    };
}

pub const Write = struct {
    /// Input data and schema.
    chunk: *const chunk.Chunk,
    /// Allocator that is used for allocating any dynamic memory relating to outputted header.
    /// Lifetime of the header is tied to this allocator after creation.
    header_alloc: Allocator,
    /// Allocator for allocating temporary memory used for constructing the output
    scratch_alloc: Allocator,
    /// For outputting the buffers
    data_section: []u8,
    /// Targeted page size in kilobytes
    page_size_kb: ?u32,
};

pub fn write(params: Write) Error!header.Header {
    var data_section_size: u32 = 0;

    const num_dicts = params.chunk.schema.dicts.len;
    const dicts = try params.header_alloc.alloc(header.Dict, num_dicts);
    const tables = try params.header_alloc.alloc(header.Table, params.chunk.data.len);

    const dict_elems = try params.scratch_alloc.alloc([]const []const u8, num_dicts);

    for (params.chunk.schema.dicts, 0..) |dict, dict_idx| {
        var num_elems: usize = 0;

        for (dict.members) |member| {
            num_elems += count_array_to_dict(&params.chunk.data[member.table_index].field_values[member.field_index]);
        }

        var elems = try params.scratch_alloc.alloc([]const u8, num_elems);

        var write_idx: usize = 0;
        for (dict.members) |member| {
            const array = &params.chunk.data[member.table_index].field_values[member.field_index];
            write_idx = try push_array_to_dict(array, write_idx, elems);
        }

        elems = sort_and_dedup(dict_elems[0..write_idx]);
        dict_elems[dict_idx] = elems;

        const dict_array = try arrow.builder.BinaryBuilder.from_slice(elems, false, params.scratch_alloc);

        const filter = if (dict.has_filter)
            try header.Filter.construct(elems, params.scratch_alloc, params.header_alloc)
        else
            null;

        dicts[dict_idx] = header.Dict{
            .data = try write_binary_array(.i32, params, &dict_array, &data_section_size),
            .filter = filter,
        };
    }

    return .{
        .dicts = dicts,
        .tables = tables,
        .data_section_size = data_section_size,
    };
}

fn write_binary_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericBinaryArray(index_t), data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        const buffers = try params.header_alloc.alloc(header.Buffer, 2);
        buffers[0] = header.Buffer{ .row_index_ends = &.{}, .minmax = &.{}, .pages = &.{}, .compression = .no_compression };
        buffers[1] = header.Buffer{ .row_index_ends = &.{}, .minmax = &.{}, .pages = &.{}, .compression = .no_compression };

        return header.Array{
            .buffers = buffers,
            .children = &.{},
        };
    }
    const target_page_size: usize = if (params.page_size_kb) |ps| ps << 10 else std.math.maxInt(usize);

    var data_compression: ?header.Compression = null;
    var offsets_compression: ?header.Compression = null;

    var data_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var offsets_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var row_index_ends = try ArrayList(u32).initCapacity(params.scratch_alloc, 128);
    var data_minmax = try ArrayList(header.MinMax).initCapacity(params.scratch_alloc, 128);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) {
        var min: [header.MinMaxLen]u8 = undefined;
        @memset(min, std.math.maxInt(u8));
        var max: [header.MinMaxLen]u8 = undefined;
        @memset(max, 0);

        var data_page_size: usize = 0;
        var page_elem_count: usize = 0;

        while (data_page_size < target_page_size and idx < array.len + array.offset) : (idx += 1) {
            const elem = arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, array.validity.ptr);

            page_elem_count += 1;

            if (elem) |e| {
                data_page_size += e.len;

                const minmax_val: [header.MinMaxLen]u8 = undefined;
                @memset(minmax_val, 0);
                @memcpy(minmax_val, e[0..@min(e.len, header.MinMaxLen)]);

                if (std.mem.order(u8, min, minmax_val) == .gt) {
                    min = minmax_val;
                }
                if (std.mem.order(u8, max, minmax_val) == .lt) {
                    max = minmax_val;
                }
            }
        }

        if (page_elem_count == 0) {
            continue;
        }

        const start = array.offsets[idx - page_elem_count];
        const end = array.offsets[idx];
        const data_page = array.data[start..end];
        std.debug.assert(data_page.len == data_page_size);

        // Write data page
        const data_page_offset = data_section_size.*;
        const data_page_compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = data_page,
            .data_section_size = data_section_size,
            .compression = &data_compression,
        });

        // Write offsets page
        const offsets_page_offset = data_section_size.*;
        const offsets_page_compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = @ptrCast(array.offsets[idx - page_elem_count .. idx]),
            .data_section_size = data_section_size,
            .compression = &offsets_compression,
        });

        // Write header info to arrays
        try data_pages.append(params.scratch_alloc, header.Page{
            .uncompressed_size = data_page_size,
            .compressed_size = data_page_compressed_size,
            .offset = data_page_offset,
        });
        try offsets_pages.append(params.scratch_alloc, header.Page{
            .uncompressed_size = @sizeOf(index_t.to_type()) * page_elem_count + 1,
            .compressed_size = offsets_page_compressed_size,
            .offset = offsets_page_offset,
        });
        try row_index_ends.append(params.scratch_alloc, idx);
        try data_minmax.append(params.scratch_alloc, binary_minmax(index_t, arrow.slice.slice_binary(index_t, array, idx - page_elem_count, page_elem_count)));
    }

    std.debug.assert(data_pages.len == offsets_pages.len and data_pages.len == row_index_ends.len and data_pages.len == data_minmax.len);

    const data_buffer = header.Buffer{
        .pages = try params.header_alloc.alloc(header.Page, data_pages.items.len),
        .compression = data_compression orelse .no_compression,
        .minmax = try params.header_alloc.alloc(header.MinMax, data_minmax.items.len),
        .row_index_ends = try params.header_alloc.alloc(u32, row_index_ends.items.len),
    };
    @memcpy(&data_buffer.pages, data_pages.items);
    @memcpy(&data_buffer.row_index_ends, row_index_ends.items);
    @memcpy(&data_buffer.minmax, data_minmax.items);

    const offsets_buffer = header.Buffer{
        .pages = try params.header_alloc.alloc(header.Page, offsets_pages.items.len),
        .compression = offsets_compression orelse .no_compression,
        .minmax = null,
        .row_index_ends = data_buffer.row_index_ends,
    };
    @memcpy(&offsets_buffer.pages, offsets_pages.items);

    const buffers = try params.header_alloc.alloc(header.Buffer, 2);
    buffers[0] = offsets_buffer;
    buffers[1] = data_buffer;

    return header.Array{
        .buffers = buffers,
        .children = &.{},
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn binary_minmax(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) ?header.MinMax {
    var idx: u32 = array.offset;
    var found_valid = false;
    var min: [header.MinMaxLen]u8 = undefined;
    @memset(min, std.math.maxInt(u8));
    var max: [header.MinMaxLen]u8 = undefined;
    @memset(max, 0);
    while (idx < array.len + array.offset) : (idx += 1) {
        if (arrow.get.get_binary_opt(array.data.ptr, array.offsets.ptr, array.validity.ptr, idx)) |elem| {
            const minmax_val: [header.MinMaxLen]u8 = undefined;
            @memset(minmax_val, 0);
            @memcpy(minmax_val, elem[0..@min(elem.len, header.MinMaxLen)]);
            if (std.mem.order(u8, min, minmax_val) == .gt) {
                min = minmax_val;
            }
            if (std.mem.order(u8, max, minmax_val) == .lt) {
                max = minmax_val;
            }
            found_valid = true;
        }
    }

    return if (found_valid)
        .{ .min = min, .max = max }
    else
        null;
}

const WritePage = struct {
    data_section: []u8,
    page: []const u8,
    data_section_size: *u32,
    compression: *?header.Compression,
};

fn write_page(params: WritePage) Error!void {
    const ds_size = params.data_section_size.*;
    const compr_bound = compress_bound(params.page.len);
    if (params.data_section.len < ds_size + compr_bound) {
        return Error.DataSectionOverflow;
    }
    const compress_dst = params.data_section[ds_size .. ds_size + compr_bound];
    const compressed_size = compress(params.page, compress_dst, params.compression);
    params.data_section_size.* = ds_size + compressed_size;
}

fn compress(src: []const u8, dst: []u8, compression: *?header.Compression) usize {
    if (src.len == 0) {
        return 0;
    }

    const algo: header.Compression = if (compression.*) |alg| 
        alg
    else alg_calc: {
        // Numbers taken from https://github.com/lz4/lz4?tab=readme-ov-file#benchmarks
        //
        // decompress speed (MB/s) divided by compressed_size gives us 

    };

    if (algo) |a| {
        switch (a) {
            .lz4 => {
                const res = LZ4_compress_default(src.ptr, dst.ptr, src.len, dst.len);
                if (res != 0) {
                    return res;
                } else {
                    return Error.Lz4CompressionFail;
                }
            },
            .zstd => {
                const res = ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, 8);
                if (ZSTD_isError(res) == 0) {
                    return res;
                } else {
                    return Error.ZstdCompressionFail;
                }
            },
            .no_compression => {
                return src.len;
            },
        }
    } else {
        // try all algo and decide
    }
}

// fn create_dict_filter(params: Write, elem: []const []const u8) Error!header.Array {

// }

/// Ascending sort elements and deduplicate
fn sort_and_dedup(data: [][]const u8) [][]const u8 {
    std.mem.sort([]const u8, data, {}, std.sort.asc([]const u8));
    var write_idx: usize = 0;

    for (data[1..]) |s| {
        if (!std.mem.eql(u8, s, data[write_idx])) {
            write_idx += 1;
            data[write_idx] = s;
        }
    }

    return data[0 .. write_idx + 1];
}

fn count_array_to_dict(array: *const arr.Array) usize {
    switch (array.*) {
        .binary => |*a| {
            return a.len - a.null_count;
        },
        .large_binary => |*a| {
            return a.len - a.null_count;
        },
        .binary_view => |*a| {
            return a.len - a.null_count;
        },
        .fixed_size_binary => |*a| {
            return a.len - a.null_count;
        },
        .utf8 => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        .large_utf8 => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        .utf8_view => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn push_array_to_dict(array: *const arr.Array, write_idx: usize, elems: [][]const u8) Error!usize {
    switch (array.*) {
        .binary => |*a| {
            return push_binary_to_dict(.i32, a, write_idx, elems);
        },
        .large_binary => |*a| {
            return push_binary_to_dict(.i64, a, write_idx, elems);
        },
        .binary_view => |*a| {
            return push_binary_view_to_dict(a, write_idx, elems);
        },
        .fixed_size_binary => |*a| {
            return push_fixed_size_binary_to_dict(a, write_idx, elems);
        },
        .utf8 => |*a| {
            return push_binary_to_dict(.i32, &a.inner, write_idx, elems);
        },
        .large_utf8 => |*a| {
            return push_binary_to_dict(.i64, &a.inner, write_idx, elems);
        },
        .utf8_view => |*a| {
            return push_binary_view_to_dict(&a.inner, write_idx, elems);
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn push_binary_to_dict(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t), write_idx: usize, out: [][]const u8) usize {
    var wi = write_idx;

    var item: u32 = array.offset;
    while (item < array.offset + array.len) : (item += 1) {
        if (arrow.get.get_binary_opt(index_t, array.data, array.offsets, array.validity, item)) |s| {
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_binary_view_to_dict(array: *const arr.BinaryViewArray, write_idx: usize, out: [][]const u8) usize {
    var wi = write_idx;

    var item: u32 = array.offset;
    while (item < array.offset + array.len) : (item += 1) {
        if (arrow.get.get_binary_view_opt(array.buffers, array.views, array.validity, item)) |s| {
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_fixed_size_binary_to_dict(array: *const arr.FixedSizeBinaryArray, write_idx: usize, out: [][]const u8) usize {
    var wi = write_idx;

    var item: u32 = array.offset;
    while (item < array.offset + array.len) : (item += 1) {
        if (arrow.get.get_fixed_size_binary_opt(array.data, array.byte_width, array.validity, item)) |s| {
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}
