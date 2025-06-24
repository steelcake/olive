const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;

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
    // Construct dictionaries
    //
    // Construct Pages/Buffers
    //
    //
    //

    var data_section_size: u32 = 0;

    const num_dicts = params.chunk.schema.dict_has_filter.len;
    const dicts = try params.header_alloc.alloc(header.Dict, num_dicts);
    const tables = try params.header_alloc.alloc(header.Table, params.chunk.data.len);

    var dict_elems: [][]const u8 = &.{};
    for (0..num_dicts) |dict_idx| {
        var num_elems: usize = 0;

        for (params.chunk.schema.tables, params.chunk.data) |sc, arrays| {
            for (sc.dict_indices, 0..) |field_dict_idx, field_idx| {
                if (field_dict_idx) |fdi| {
                    if (fdi == dict_idx) {
                        num_elems += arrow.length.length(arrays.field_values[field_idx]);
                    }
                }
            }
        }

        dict_elems = if (num_elems > dict_elems) try params.scratch_alloc.realloc(dict_elems, num_elems) else dict_elems;

        var write_idx: usize = 0;
        for (params.chunk.schema.tables, params.chunk.data) |sc, arrays| {
            for (sc.dict_indices, 0..) |field_dict_idx, field_idx| {
                if (field_dict_idx) |fdi| {
                    if (fdi == dict_idx) {
                        switch (arrays.field_values[field_idx]) {
                            .binary => |*a| {
                                write_idx = push_binary_to_dict(.i32, a, write_idx, dict_elems);
                            },
                            .large_binary => |*a| {
                                write_idx = push_binary_to_dict(.i64, a, write_idx, dict_elems);
                            },
                            .binary_view => |*a| {
                                write_idx = push_binary_view_to_dict(a, write_idx, dict_elems);
                            },
                            .fixed_size_binary => |*a| {
                                write_idx = push_fixed_size_binary_to_dict(a, write_idx, dict_elems);
                            },
                            .utf8 => |*a| {
                                write_idx = push_binary_to_dict(.i32, &a.inner, write_idx, dict_elems);
                            },
                            .large_utf8 => |*a| {
                                write_idx = push_binary_to_dict(.i64, &a.inner, write_idx, dict_elems);
                            },
                            .utf8_view => |*a| {
                                write_idx = push_binary_view_to_dict(&a.inner, write_idx, dict_elems);
                            },
                            else => return Error.NonBinaryArrayWithDict,
                        }
                    }
                }
            }
        }

        const dict_data = sort_and_dedup(dict_elems[0..write_idx]);

        dicts[dict_idx] = .{};
    }

    return .{
        .dicts = dicts,
        .tables = tables,
        .data_section_size = data_section_size,
    };
}

// fn write_dict_as_array(data: []const []const u8, data_section: )

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
