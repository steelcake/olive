const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const ArrayList = std.ArrayListUnmanaged;
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const header = @import("./header.zig");
const schema = @import("./schema.zig");
const compression = @import("./compression.zig");
const dict_impl = @import("./dict.zig");

pub const Compression = compression.Compression;

pub const Error = error{
    OutOfMemory,
    DataSectionOverflow,
    NonBinaryArrayWithDict,
    CompressFail,
};

pub const Write = struct {
    schema: *const schema.DatasetSchema,
    tables: []const []const arr.Array,
    /// Allocator that is used for allocating any dynamic memory relating to outputted header.
    /// Lifetime of the header is tied to this allocator after creation.
    header_alloc: Allocator,
    /// Allocator for allocating temporary memory used for constructing the output
    scratch_alloc: Allocator,
    /// For outputting the buffers
    data_section: []u8,
    /// Targeted page size in kilobytes
    page_size_kb: ?u32,
    /// Compression to use for individual pages.
    /// Compression will be disabled for buffers that don't compress enough to be worth it
    compression: Compression,
};

pub fn write(params: Write) Error!header.Header {
    var data_section_size: u32 = 0;

    const num_dicts = params.schema.dicts.len;
    const dicts = try params.header_alloc.alloc(?header.Dict, num_dicts);
    const tables = try params.header_alloc.alloc(header.Table, params.tables.len);

    const dict_arrays = try params.scratch_alloc.alloc(arr.BinaryArray, num_dicts);

    for (params.schema.dicts, 0..) |dict, dict_idx| {
        var num_elems: usize = 0;

        for (dict.members) |member| {
            num_elems += try dict_impl.count_array_to_dict(&params.tables[member.table_index][member.field_index]);
        }

        var elems = try params.scratch_alloc.alloc([]const u8, num_elems);

        var write_idx: usize = 0;
        for (dict.members) |member| {
            const array = &params.tables[member.table_index][member.field_index];
            write_idx = try dict_impl.push_array_to_dict(array, write_idx, elems);
        }

        elems = dict_impl.sort_and_dedup(elems[0..write_idx]);

        const dict_array = arrow.builder.BinaryBuilder.from_slice(elems, false, params.scratch_alloc) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
        };
        const data = try write_binary_array(.i32, params, &dict_array, &data_section_size, true);

        dict_arrays[dict_idx] = dict_array;

        if (num_elems > 0) {
            const filter = if (dict.has_filter)
                header.Filter.construct(elems, params.scratch_alloc, params.header_alloc) catch |e| {
                    if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
                }
            else
                null;

            dicts[dict_idx] = header.Dict{
                .data = data,
                .filter = filter,
            };
        } else {
            dicts[dict_idx] = null;
        }
    }

    for (params.tables, 0..) |table, table_idx| {
        const fields = try params.header_alloc.alloc(header.Array, table.len);

        for (table, 0..) |*array, field_idx| {
            if (dict_impl.find_dict(params.schema.dicts, dict_arrays, table_idx, field_idx)) |dict_arr| {
                const dicted_array = try dict_impl.apply_dict(dict_arr, array, params.scratch_alloc);
                fields[field_idx] = try write_primitive_array(u32, params, &dicted_array, &data_section_size, true);
            } else {
                fields[field_idx] = try write_array(params, array, &data_section_size, params.schema.tables[table_idx].has_minmax_index);
            }
        }

        tables[table_idx] = .{
            .fields = fields,
        };
    }

    return .{
        .dicts = dicts,
        .tables = tables,
        .data_section_size = data_section_size,
    };
}

fn write_array(params: Write, array: *const arr.Array, data_section_size: *u32, has_minmax_index: bool) Error!header.Array {
    switch (array.*) {
        .null => |*a| return .{ .null = .{ .len = a.len } },
        .i8 => |*a| return .{ .i8 = try write_primitive_array(i8, params, a, data_section_size, has_minmax_index) },
        .i16 => |*a| return .{ .i16 = try write_primitive_array(i16, params, a, data_section_size, has_minmax_index) },
        .i32 => |*a| return .{ .i32 = try write_primitive_array(i32, params, a, data_section_size, has_minmax_index) },
        .i64 => |*a| return .{ .i64 = try write_primitive_array(i64, params, a, data_section_size, has_minmax_index) },
        .u8 => |*a| return .{ .u8 = try write_primitive_array(u8, params, a, data_section_size, has_minmax_index) },
        .u16 => |*a| return .{ .u16 = try write_primitive_array(u16, params, a, data_section_size, has_minmax_index) },
        .u32 => |*a| return .{ .u32 = try write_primitive_array(u32, params, a, data_section_size, has_minmax_index) },
        .u64 => |*a| return .{ .u64 = try write_primitive_array(u64, params, a, data_section_size, has_minmax_index) },
        .f16 => |*a| return .{ .f16 = try write_primitive_array(f16, params, a, data_section_size, has_minmax_index) },
        .f32 => |*a| return .{ .f32 = try write_primitive_array(f32, params, a, data_section_size, has_minmax_index) },
        .f64 => |*a| return .{ .f64 = try write_primitive_array(f64, params, a, data_section_size, has_minmax_index) },
        .binary => |*a| return .{ .binary = try write_binary_array(.i32, params, a, data_section_size, has_minmax_index) },
        .utf8 => |*a| return .{ .binary = try write_binary_array(.i32, params, &a.inner, data_section_size, has_minmax_index) },
        .bool => |*a| return .{ .bool = try write_bool_array(params, a, data_section_size) },
        .decimal32 => |*a| return .{ .i32 = try write_primitive_array(i32, params, &a.inner, data_section_size, has_minmax_index) },
        .decimal64 => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .decimal128 => |*a| return .{ .i128 = try write_primitive_array(i128, params, &a.inner, data_section_size, has_minmax_index) },
        .decimal256 => |*a| return .{ .i256 = try write_primitive_array(i256, params, &a.inner, data_section_size, has_minmax_index) },
        .date32 => |*a| return .{ .i32 = try write_primitive_array(i32, params, &a.inner, data_section_size, has_minmax_index) },
        .date64 => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .time32 => |*a| return .{ .i32 = try write_primitive_array(i32, params, &a.inner, data_section_size, has_minmax_index) },
        .time64 => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .timestamp => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .interval_year_month => |*a| return .{ .i32 = try write_primitive_array(i32, params, &a.inner, data_section_size, has_minmax_index) },
        .interval_day_time => |*a| return .{ .interval_day_time = try write_primitive_array([2]i32, params, &a.inner, data_section_size, has_minmax_index) },
        .interval_month_day_nano => |*a| return .{ .interval_month_day_nano = try write_primitive_array(arr.MonthDayNano, params, &a.inner, data_section_size, has_minmax_index) },
        .list => |*a| return .{ .list = try write_list_array(.i32, params, a, data_section_size) },
        .struct_ => |*a| return .{ .struct_ = try write_struct_array(params, a, data_section_size) },
        .dense_union => |*a| return .{ .dense_union = try write_dense_union_array(params, a, data_section_size) },
        .sparse_union => |*a| return .{ .sparse_union = try write_sparse_union_array(params, a, data_section_size) },
        .fixed_size_binary => |*a| return .{ .fixed_size_binary = try write_fixed_size_binary_array(params, a, data_section_size, has_minmax_index) },
        .fixed_size_list => |*a| return .{ .fixed_size_list = try write_fixed_size_list_array(params, a, data_section_size) },
        // .map => |*a| return .{ .map = try write_map_array(params, a, data_section_size) },
        .duration => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .large_binary => |*a| return .{ .binary = try write_binary_array(.i64, params, a, data_section_size, has_minmax_index) },
        .large_utf8 => |*a| return .{ .binary = try write_binary_array(.i64, params, &a.inner, data_section_size, has_minmax_index) },
        .large_list => |*a| return .{ .list = try write_list_array(.i64, params, a, data_section_size) },
        // .run_end_encoded => |*a| return .{ .run_end_encoded = try write_run_end_encoded_array(params, a, data_section_size) },
        .binary_view => |*a| return .{ .binary = try write_binary_view_array(params, a, data_section_size, has_minmax_index) },
        .utf8_view => |*a| return .{ .binary = try write_binary_view_array(params, &a.inner, data_section_size, has_minmax_index) },
        // .list_view => |*a| return .{ .list = try write_list_view_array(.i32, params, a, data_section_size) },
        // .large_list_view => |*a| return .{ .list = try write_list_view_array(.i64, params, a, data_section_size) },
        // .dict => |*a| return .{ .dict = try write_dict_array(params, a, data_section_size) },
    }
}

fn write_sparse_union_array(params: Write, array: *const arr.SparseUnionArray, data_section_size: *u32) Error!header.Array {
    if (array.inner.len == 0) {
        return empty_array();
    }

    const buffers = try params.header_alloc.alloc(header.Buffer, 1);
    buffers[0] = try write_buffer(i8, params, array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len], data_section_size);

    const children = try params.header_alloc.alloc(header.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*c, idx| {
        const sliced = arrow.slice.slice(c, array.inner.offset, array.inner.len);
        children[idx] = try write_array(params, &sliced, data_section_size);
    }

    return .{
        .buffers = buffers,
        .children = children,
        .len = array.inner.len,
        .null_count = 0,
    };
}

fn write_dense_union_array(params: Write, array: *const arr.DenseUnionArray, data_section_size: *u32) Error!header.Array {
    if (array.inner.len == 0) {
        return empty_array();
    }

    // TODO: Can minimize the data written more if the union array itself has an offset. But it is not trivial to do it

    const buffers = try params.header_alloc.alloc(header.Buffer, 2);
    buffers[0] = try write_buffer(i32, params, array.offsets[array.inner.offset .. array.inner.offset + array.inner.len], data_section_size);
    buffers[1] = try write_buffer(i8, params, array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len], data_section_size);

    const children = try params.header_alloc.alloc(header.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*c, idx| {
        children[idx] = try write_array(params, c, data_section_size);
    }

    return .{
        .buffers = buffers,
        .children = children,
        .len = array.inner.len,
        .null_count = 0,
    };
}

fn write_struct_array(params: Write, array: *const arr.StructArray, data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }

    const buffers = try params.header_alloc.alloc(header.Buffer, 1);
    buffers[0] = try write_validity(params, array.offset, array.len, array.validity, data_section_size);

    const children = try params.header_alloc.alloc(header.Array, array.field_values.len);

    for (array.field_values, 0..) |*field, idx| {
        const sliced = arrow.slice.slice(field, array.offset, array.len);
        children[idx] = try write_array(params, &sliced, data_section_size);
    }

    return .{
        .buffers = buffers,
        .children = children,
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn write_fixed_size_list_array(params: Write, array: *const arr.FixedSizeListArray, data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }

    var builder = arrow.builder.ListBuilder.with_capacity(array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.validity) |validity| {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.bitmap.get(validity.ptr, idx)) {
                builder.append_item(array.item_width) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    }

    const l_array = builder.finish(array.inner) catch unreachable;

    return try write_list_array(.i32, params, &l_array, data_section_size);
}

fn write_fixed_size_binary_array(params: Write, array: *const arr.FixedSizeBinaryArray, data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }

    const byte_width: u32 = @intCast(array.byte_width);
    var builder = arrow.builder.BinaryBuilder.with_capacity(byte_width * (array.len - array.null_count), array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_option(arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, idx)) catch unreachable;
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_value(arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, idx)) catch unreachable;
        }
    }

    const bin_array = builder.finish() catch unreachable;

    return try write_binary_array(.i32, params, &bin_array, data_section_size);
}

fn write_list_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericListArray(index_t), data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }

    const buffers = try params.header_alloc.alloc(header.Buffer, 2);
    buffers[0] = try write_validity(params, array.offset, array.len, array.validity, data_section_size);
    const offsets = try normalize_offsets(index_t, array.offsets[array.offset .. array.offset + array.len + 1], params.scratch_alloc);
    buffers[1] = try write_buffer(index_t.to_type(), params, offsets, data_section_size);

    const start_offset = array.offsets[array.offset];
    const end_offset = array.offsets[array.offset + array.len];
    const child_len = end_offset - start_offset;
    const inner = arrow.slice.slice(array.inner, @intCast(start_offset), @intCast(child_len));

    const children = try params.header_alloc.alloc(header.Array, 1);
    children[0] = try write_array(params, &inner, data_section_size);

    return .{
        .buffers = buffers,
        .children = children,
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn write_bool_array(params: Write, array: *const arr.BoolArray, data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }

    const buffers = try params.header_alloc.alloc(header.Buffer, 2);
    buffers[0] = try write_validity(params, array.offset, array.len, array.validity, data_section_size);
    buffers[1] = try write_validity(params, array.offset, array.len, array.values, data_section_size);

    return .{
        .buffers = buffers,
        .children = &.{},
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn write_binary_view_array(params: Write, array: *const arr.BinaryViewArray, data_section_size: *u32) Error!header.Array {
    var total_size: u32 = 0;

    var idx: u32 = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        total_size += @as(u32, @bitCast(array.views[idx].length));
    }

    var builder = arrow.builder.BinaryBuilder.with_capacity(total_size, array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_option(arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, idx)) catch unreachable;
        }
    } else {
        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_value(arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, idx)) catch unreachable;
        }
    }

    const bin_array = builder.finish() catch unreachable;

    return try write_binary_array(.i32, params, &bin_array, data_section_size);
}

fn empty_buffer() header.Buffer {
    return .{
        .row_index_ends = &.{},
        .pages = &.{},
        .compression = .no_compresion,
    };
}

fn write_primitive_array(comptime T: type, params: Write, array: *const arr.PrimitiveArray(T), data_section_size: *u32) Error!header.PrimitiveArray(T) {
    if (array.len == 0) {
        return .{
            .values = empty_buffer(),
            .validity = null,
            .len = 0,
            .minmax = null,
        };
    }

    const buffers = try params.header_alloc.alloc(header.Buffer, 2);
    buffers[0] = try write_validity(params, array.offset, array.len, array.validity, data_section_size);
    buffers[1] = try write_buffer(T, params, array.values[array.offset .. array.offset + array.len], data_section_size);

    return .{
        .buffers = buffers,
        .children = &.{},
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn write_validity(params: Write, offset: u32, len: u32, validity_opt: ?[]const u8, data_section_size: *u32) Error!header.Buffer {
    const validity = validity_opt orelse return empty_buffer();

    const v = if (offset % 8 != 0) non_aligned: {
        const x = try params.scratch_alloc.alloc(u8, (len + 7) / 8);
        @memset(x, 0);

        var i: u32 = offset;
        while (i < offset + len) : (i += 1) {
            if (arrow.bitmap.get(validity.ptr, i)) {
                arrow.bitmap.set(x.ptr, i);
            }
        }

        break :non_aligned x;
    } else validity[(offset / 8)..(offset / 8 + (len + 7) / 8)];

    return try write_buffer(u8, params, v, data_section_size);
}

fn write_buffer(comptime T: type, params: Write, buffer: []const T, data_section_size: *u32) Error!header.Buffer {
    if (buffer.len == 0) {
        return empty_buffer();
    }
    const max_page_len: usize = if (params.page_size_kb) |ps| ((ps << 10) + @sizeOf(T) - 1) / @sizeOf(T) else std.math.maxInt(usize);
    std.debug.assert(max_page_len > 0);

    var compr: ?Compression = null;

    var pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var row_index_ends = try ArrayList(u32).initCapacity(params.scratch_alloc, 128);

    var buffer_offset: u32 = 0;
    while (buffer_offset < buffer.len) {
        const page_len = @min(max_page_len, buffer.len);
        const page: []const u8 = @ptrCast(buffer[buffer_offset .. buffer_offset + page_len]);

        const page_offset = data_section_size.*;
        const compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = page,
            .data_section_size = data_section_size,
            .compression = &compr,
            .compression_cfg = params.compression,
        });

        // Write header info to arrays
        try pages.append(params.scratch_alloc, header.Page{
            .uncompressed_size = @intCast(page_len * @sizeOf(T)),
            .compressed_size = @intCast(compressed_size),
            .offset = page_offset,
        });
        try row_index_ends.append(params.scratch_alloc, buffer_offset + page_len);

        buffer_offset += page_len;
    }

    const out_pages = try params.header_alloc.alloc(header.Page, pages.items.len);
    const out_row_index_ends = try params.header_alloc.alloc(u32, row_index_ends.items.len);
    @memcpy(out_pages, pages.items);
    @memcpy(out_row_index_ends, row_index_ends.items);

    return header.Buffer{
        .compression = compr orelse .no_compression,
        .pages = out_pages,
        .row_index_ends = out_row_index_ends,
    };
}

fn write_binary_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericBinaryArray(index_t), data_section_size: *u32, has_minmax_index: bool) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }
    const target_page_size: usize = if (params.page_size_kb) |ps| ps << 10 else std.math.maxInt(usize);

    var data_compression: ?Compression = null;
    var offsets_compression: ?Compression = null;

    var data_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var offsets_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var row_index_ends = try ArrayList(u32).initCapacity(params.scratch_alloc, 128);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) {
        var data_page_size: usize = 0;
        var page_elem_count: u32 = 0;

        while (data_page_size < target_page_size and idx < array.len + array.offset) : (idx += 1) {
            const elem: ?[]const u8 = if (array.null_count > 0)
                arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, (array.validity orelse unreachable).ptr, idx)
            else
                arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, idx);

            page_elem_count += 1;

            if (elem) |e| {
                data_page_size += e.len;
            }
        }

        if (page_elem_count == 0) {
            continue;
        }

        const start: usize = @intCast(array.offsets[idx - page_elem_count]);
        const end: usize = @intCast(array.offsets[idx]);
        const data_page = array.data[start..end];
        std.debug.assert(data_page.len == data_page_size);

        // Write data page
        const data_page_offset = data_section_size.*;
        const data_page_compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = data_page,
            .data_section_size = data_section_size,
            .compression = &data_compression,
            .compression_cfg = params.compression,
        });

        // Write offsets page
        const offsets_page_offset = data_section_size.*;
        const offsets_page_compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = @ptrCast(try normalize_offsets(index_t, array.offsets[idx - page_elem_count .. idx], params.scratch_alloc)),
            .data_section_size = data_section_size,
            .compression = &offsets_compression,
            .compression_cfg = params.compression,
        });

        // Write header info to arrays
        try data_pages.append(params.scratch_alloc, header.Page{
            .uncompressed_size = @intCast(data_page_size),
            .compressed_size = @intCast(data_page_compressed_size),
            .offset = data_page_offset,
        });
        try offsets_pages.append(params.scratch_alloc, header.Page{
            .uncompressed_size = @intCast(@sizeOf(index_t.to_type()) * (page_elem_count + 1)),
            .compressed_size = @intCast(offsets_page_compressed_size),
            .offset = offsets_page_offset,
        });
        try row_index_ends.append(params.scratch_alloc, idx - page_elem_count);
    }

    std.debug.assert(data_pages.items.len == offsets_pages.items.len and data_pages.items.len == row_index_ends.items.len);

    const data_buffer_pages = try params.header_alloc.alloc(header.Page, data_pages.items.len);
    const data_buffer_row_index_ends = try params.header_alloc.alloc(u32, row_index_ends.items.len);
    @memcpy(data_buffer_pages, data_pages.items);
    @memcpy(data_buffer_row_index_ends, row_index_ends.items);

    var minmax: ?arrow.array.Array = null;
    if (has_minmax_index) {
        var start_idx = 0;

        var mm = arrow.builder.BinaryBuilder.with_capacity();

        for (row_index_ends, 0..) |end_idx, w_idx| {
            const sliced = arrow.slice.slice_binary(i32, array, start_idx, end_idx - start_idx);
            const min = arrow.minmax.min_binary(index_t, &sliced);
            const max = arrow.minmax.max_binary(index_t, &sliced);
            start_idx = end_idx;
        }
    }

    // const minmax = try params.header_alloc.alloc([]const u8, )

    const data_buffer = header.Buffer{
        .pages = data_buffer_pages,
        .minmax = minmax,
        .row_index_ends = data_buffer_row_index_ends,
        .compression = data_compression orelse .no_compression,
    };

    const offsets_buffer_pages = try params.header_alloc.alloc(header.Page, offsets_pages.items.len);
    @memcpy(offsets_buffer_pages, offsets_pages.items);

    const offsets_buffer = header.Buffer{
        .pages = offsets_buffer_pages,
        .minmax = null,
        .row_index_ends = data_buffer_row_index_ends,
        .compression = offsets_compression orelse .no_compression,
    };

    const buffers = try params.header_alloc.alloc(header.Buffer, 3);
    buffers[0] = try write_validity(params, array.offset, array.len, array.validity, data_section_size);
    buffers[1] = offsets_buffer;
    buffers[2] = data_buffer;

    return header.Array{
        .buffers = buffers,
        .children = &.{},
        .len = array.len,
        .null_count = array.null_count,
    };
}

fn normalize_offsets(comptime index_t: arr.IndexType, offsets: []const index_t.to_type(), scratch_alloc: Allocator) Error![]const index_t.to_type() {
    if (offsets.len == 0) {
        return &.{};
    }

    const base = offsets[0];

    if (base == 0) {
        return offsets;
    }

    const normalized = try scratch_alloc.alloc(index_t.to_type(), offsets.len);

    for (0..offsets.len) |idx| {
        normalized[idx] = offsets[idx] - base;
    }

    return normalized;
}

const WritePage = struct {
    data_section: []u8,
    page: []const u8,
    data_section_size: *u32,
    compression: *?Compression,
    compression_cfg: Compression,
};

fn write_page(params: WritePage) Error!usize {
    const ds_size = params.data_section_size.*;
    const compr_bound = compression.compress_bound(params.page.len);
    if (params.data_section.len < ds_size + compr_bound) {
        return Error.DataSectionOverflow;
    }
    const compress_dst = params.data_section[ds_size .. ds_size + compr_bound];
    const compressed_size = try compress(params.page, compress_dst, params.compression, params.compression_cfg);
    params.data_section_size.* = ds_size + @as(u32, @intCast(compressed_size));

    return compressed_size;
}

fn compress(src: []const u8, dst: []u8, compr: *?Compression, compr_cfg: Compression) Error!usize {
    if (src.len == 0) {
        return 0;
    }

    if (compr.*) |algo| {
        return try compression.compress(src, dst, algo);
    }

    const compressed_size = try compression.compress(src, dst, compr_cfg);

    // Apply compression only if compression factor is over 1.5
    if (compressed_size * 3 <= src.len * 2) {
        compr.* = compr_cfg;
        return compressed_size;
    } else {
        compr.* = .no_compression;
        return try compression.compress(src, dst, .no_compression);
    }
}

fn run_test_impl(arrays: []const arr.Array, id: usize) !void {
    var tables: [3][]const arr.Array = undefined;

    for (0..3) |idx| {
        const base = (id + idx) * 3;
        const arr0 = arrays[base % arrays.len];
        const arr1 = arrays[(base + 1) % arrays.len];
        const arr2 = arrays[(base + 2) % arrays.len];

        tables[idx] = &.{
            arr0, arr1, arr2,
        };
    }

    const table_names = &.{ "a", "b", "c" };

    const field_names = &.{ "q", "w", "e" };

    var dt_arena = ArenaAllocator.init(testing.allocator);
    defer dt_arena.deinit();
    const dt_alloc = dt_arena.allocator();

    var data_types: [3][]const arrow.data_type.DataType = undefined;

    for (0..3) |idx| {
        data_types[idx] = &.{
            try arrow.data_type.get_data_type(&tables[idx][0], dt_alloc),
            try arrow.data_type.get_data_type(&tables[idx][1], dt_alloc),
            try arrow.data_type.get_data_type(&tables[idx][2], dt_alloc),
        };
    }

    var table_schemas: [3]schema.TableSchema = undefined;
    for (0..3) |idx| {
        table_schemas[idx] = .{
            .field_names = field_names,
            .data_types = data_types[idx],
        };
    }

    var dict_members: [9]schema.DictMember = undefined;
    var num_dict_members: usize = 0;

    for (0..3) |table_idx| {
        for (0..3) |field_idx| {
            switch (tables[table_idx][field_idx]) {
                .fixed_size_binary, .binary, .large_binary, .utf8, .large_utf8, .binary_view, .utf8_view => {
                    dict_members[num_dict_members] = .{
                        .table_index = @intCast(table_idx),
                        .field_index = @intCast(field_idx),
                    };
                    num_dict_members += 1;
                },
                else => {},
            }
        }
    }

    const dicts = if (num_dict_members > 0)
        &.{schema.DictSchema{ .members = dict_members[0..num_dict_members], .has_filter = true }}
    else
        &.{};

    const data_section = try testing.allocator.alloc(u8, 1 << 22);
    defer testing.allocator.free(data_section);

    var scratch_arena = ArenaAllocator.init(testing.allocator);
    defer scratch_arena.deinit();

    var header_arena = ArenaAllocator.init(testing.allocator);
    defer header_arena.deinit();

    const sch = schema.DatasetSchema{
        .table_names = table_names,
        .dicts = dicts,
        .tables = &table_schemas,
    };

    try sch.validate();
    try testing.expect(sch.check(&tables));

    const head = try write(.{
        .data_section = data_section,
        .compression = .{ .lz4_hc = 9 },
        .page_size_kb = 1 << 20,
        .header_alloc = header_arena.allocator(),
        .scratch_alloc = scratch_arena.allocator(),
        .tables = &tables,
        .schema = &sch,
    });

    _ = head;
}

fn run_test(arrays: []const arr.Array, id: usize) !void {
    return run_test_impl(arrays, id) catch |e| err: {
        std.log.err("failed test id: {}", .{id});
        break :err e;
    };
}

test "smoke test write" {
    var array_arena = ArenaAllocator.init(testing.allocator);
    defer array_arena.deinit();
    const array_alloc = array_arena.allocator();

    var len: usize = 0;
    var arrays: [arrow.test_array.NUM_ARRAYS]arr.Array = undefined;

    for (0..arrow.test_array.NUM_ARRAYS) |idx| {
        arrays[len] = try arrow.test_array.make_array(@intCast(idx), array_alloc);

        switch (arrays[len]) {
            .dict, .run_end_encoded, .list_view, .large_list_view, .map => {},
            else => {
                len += 1;
            },
        }
    }

    for (0..arrow.test_array.NUM_ARRAYS) |i| {
        try run_test(arrays[0..len], i);
    }
}
