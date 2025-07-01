const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const ArrayList = std.ArrayListUnmanaged;
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const native_endian = @import("builtin").target.cpu.arch.endian();

comptime {
    if (native_endian != .little) {
        @compileError("olive only supports little-endian architectures.");
    }
}

const header = @import("./header.zig");
const chunk = @import("./chunk.zig");
const schema = @import("./schema.zig");
const compression = @import("./compression.zig");

pub const Compression = compression.Compression;

pub const Error = error{
    OutOfMemory,
    DictArrayNotSupported,
    RunEndEncodedArrayNotSupported,
    ListViewArrayNotSupported,
    MapArrayNotSupported,
    DataSectionOverflow,
    NonBinaryArrayWithDict,
    CompressFail,
};

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
    /// Compression to use for individual pages.
    /// Compression will be disabled for buffers that don't compress enough to be worth it
    compression: Compression,
};

pub fn write(params: Write) Error!header.Header {
    var data_section_size: u32 = 0;

    const num_dicts = params.chunk.schema.dicts.len;
    const dicts = try params.header_alloc.alloc(header.Dict, num_dicts);
    const tables = try params.header_alloc.alloc(header.Table, params.chunk.tables.len);

    const dict_arrays = try params.scratch_alloc.alloc(arr.BinaryArray, num_dicts);

    for (params.chunk.schema.dicts, 0..) |dict, dict_idx| {
        var num_elems: usize = 0;

        for (dict.members) |member| {
            num_elems += try count_array_to_dict(&params.chunk.tables[member.table_index][member.field_index]);
        }

        var elems = try params.scratch_alloc.alloc([]const u8, num_elems);

        var write_idx: usize = 0;
        for (dict.members) |member| {
            const array = &params.chunk.tables[member.table_index][member.field_index];
            write_idx = try push_array_to_dict(array, write_idx, elems);
        }

        elems = sort_and_dedup(elems[0..write_idx]);

        const dict_array = arrow.builder.BinaryBuilder.from_slice(elems, false, params.scratch_alloc) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
        };
        const data = try write_binary_array(.i32, params, &dict_array, &data_section_size);

        dict_arrays[dict_idx] = dict_array;

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
    }

    for (params.chunk.tables, 0..) |table, table_idx| {
        const fields = try params.header_alloc.alloc(header.Array, table.len);

        for (table, 0..) |*array, field_idx| {
            if (find_dict(params.chunk.schema.dicts, dict_arrays, table_idx, field_idx)) |dict_arr| {
                const dicted_array = try apply_dict(dict_arr, array, params.scratch_alloc);
                fields[field_idx] = try write_primitive_array(u32, params, &dicted_array, &data_section_size);
            } else {
                fields[field_idx] = try write_array(params, array, &data_section_size);
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

fn apply_dict(dict_array: *const arr.BinaryArray, array: *const arr.Array, scratch_alloc: Allocator) Error!arr.UInt32Array {
    switch (array.*) {
        .binary => |*a| {
            return try apply_dict_to_binary_array(.i32, dict_array, a, scratch_alloc);
        },
        .large_binary => |*a| {
            return try apply_dict_to_binary_array(.i64, dict_array, a, scratch_alloc);
        },
        .binary_view => |*a| {
            return try apply_dict_to_binary_view_array(dict_array, a, scratch_alloc);
        },
        .fixed_size_binary => |*a| {
            return try apply_dict_to_fixed_size_binary_array(dict_array, a, scratch_alloc);
        },
        .utf8 => |*a| {
            return try apply_dict_to_binary_array(.i32, dict_array, &a.inner, scratch_alloc);
        },
        .large_utf8 => |*a| {
            return try apply_dict_to_binary_array(.i64, dict_array, &a.inner, scratch_alloc);
        },
        .utf8_view => |*a| {
            return try apply_dict_to_binary_view_array(dict_array, &a.inner, scratch_alloc);
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn apply_dict_to_binary_view_array(dict_array: *const arr.BinaryArray, array: *const arr.BinaryViewArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_fixed_size_binary_array(dict_array: *const arr.BinaryArray, array: *const arr.FixedSizeBinaryArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_binary_array(comptime index_t: arr.IndexType, dict_array: *const arr.BinaryArray, array: *const arr.GenericBinaryArray(index_t), scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn find_dict_idx(dict_array: *const arr.BinaryArray, val: []const u8) ?u32 {
    var idx: u32 = dict_array.offset;
    while (idx < dict_array.offset + dict_array.len) : (idx += 1) {
        const dict_elem = arrow.get.get_binary(.i32, dict_array.data.ptr, dict_array.offsets.ptr, idx);

        if (std.mem.eql(u8, val, dict_elem)) {
            return idx;
        }
    }

    return null;
}

fn find_dict(dicts: []const schema.DictSchema, dict_elements: []const arr.BinaryArray, table_index: usize, field_index: usize) ?*const arr.BinaryArray {
    for (dicts, 0..) |dict, dict_idx| {
        for (dict.members) |member| {
            if (member.table_index == table_index and member.field_index == field_index) {
                return &dict_elements[dict_idx];
            }
        }
    }

    return null;
}

fn write_array(params: Write, array: *const arr.Array, data_section_size: *u32) Error!header.Array {
    switch (array.*) {
        .null => |*a| return write_null_array(a),
        .i8 => |*a| return try write_primitive_array(i8, params, a, data_section_size),
        .i16 => |*a| return try write_primitive_array(i16, params, a, data_section_size),
        .i32 => |*a| return try write_primitive_array(i32, params, a, data_section_size),
        .i64 => |*a| return try write_primitive_array(i64, params, a, data_section_size),
        .u8 => |*a| return try write_primitive_array(u8, params, a, data_section_size),
        .u16 => |*a| return try write_primitive_array(u16, params, a, data_section_size),
        .u32 => |*a| return try write_primitive_array(u32, params, a, data_section_size),
        .u64 => |*a| return try write_primitive_array(u64, params, a, data_section_size),
        .f16 => |*a| return try write_primitive_array(f16, params, a, data_section_size),
        .f32 => |*a| return try write_primitive_array(f32, params, a, data_section_size),
        .f64 => |*a| return try write_primitive_array(f64, params, a, data_section_size),
        .binary => |*a| return try write_binary_array(.i32, params, a, data_section_size),
        .utf8 => |*a| return try write_binary_array(.i32, params, &a.inner, data_section_size),
        .bool => |*a| return try write_bool_array(params, a, data_section_size),
        .decimal32 => |*a| return try write_primitive_array(i32, params, &a.inner, data_section_size),
        .decimal64 => |*a| return try write_primitive_array(i64, params, &a.inner, data_section_size),
        .decimal128 => |*a| return try write_primitive_array(i128, params, &a.inner, data_section_size),
        .decimal256 => |*a| return try write_primitive_array(i256, params, &a.inner, data_section_size),
        .date32 => |*a| return try write_primitive_array(i32, params, &a.inner, data_section_size),
        .date64 => |*a| return try write_primitive_array(i64, params, &a.inner, data_section_size),
        .time32 => |*a| return try write_primitive_array(i32, params, &a.inner, data_section_size),
        .time64 => |*a| return try write_primitive_array(i64, params, &a.inner, data_section_size),
        .timestamp => |*a| return try write_primitive_array(i64, params, &a.inner, data_section_size),
        .interval_year_month => |*a| return try write_primitive_array(i32, params, &a.inner, data_section_size),
        .interval_day_time => |*a| return try write_primitive_array([2]i32, params, &a.inner, data_section_size),
        .interval_month_day_nano => |*a| return try write_primitive_array(arr.MonthDayNano, params, &a.inner, data_section_size),
        .list => |*a| return try write_list_array(.i32, params, a, data_section_size),
        .struct_ => |*a| return try write_struct_array(params, a, data_section_size),
        .dense_union => |*a| return try write_dense_union_array(params, a, data_section_size),
        .sparse_union => |*a| return try write_sparse_union_array(params, a, data_section_size),
        .fixed_size_binary => |*a| return try write_fixed_size_binary_array(params, a, data_section_size),
        .fixed_size_list => |*a| return try write_fixed_size_list_array(params, a, data_section_size),
        .map => return Error.MapArrayNotSupported,
        .duration => |*a| return try write_primitive_array(i64, params, &a.inner, data_section_size),
        .large_binary => |*a| return try write_binary_array(.i64, params, a, data_section_size),
        .large_utf8 => |*a| return try write_binary_array(.i64, params, &a.inner, data_section_size),
        .large_list => |*a| return try write_list_array(.i64, params, a, data_section_size),
        // TODO: not added because it isn't tested for ffi in arrow-zig yet
        .run_end_encoded => return Error.RunEndEncodedArrayNotSupported,
        .binary_view => |*a| return try write_binary_view_array(params, a, data_section_size),
        .utf8_view => |*a| return try write_binary_view_array(params, &a.inner, data_section_size),
        // TODO: probably need concat implemented in arrow-zig in order to implement this in a way that makes sense
        .list_view => return Error.ListViewArrayNotSupported,
        .large_list_view => return Error.ListViewArrayNotSupported,
        // TODO: not added because it isn't tested for ffi in arrow-zig yet
        .dict => return Error.DictArrayNotSupported,
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

fn write_null_array(array: *const arr.NullArray) header.Array {
    return header.Array{
        .buffers = &.{},
        .children = &.{},
        .len = array.len,
        .null_count = 0,
    };
}

fn write_primitive_array(comptime T: type, params: Write, array: *const arr.PrimitiveArray(T), data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
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

fn empty_array() header.Array {
    return header.Array{
        .buffers = &.{},
        .children = &.{},
        .len = 0,
        .null_count = 0,
    };
}

fn max_of(comptime T: type) T {
    return comptime switch (@typeInfo(T)) {
        .float => std.math.floatMax(T),
        .int => std.math.maxInt(T),
        else => unreachable,
    };
}

fn min_of(comptime T: type) T {
    return comptime switch (@typeInfo(T)) {
        .float => std.math.floatMax(T),
        .int => std.math.maxInt(T),
        else => unreachable,
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

    var compr: ?Compression = null;

    const page_offset = data_section_size.*;
    const compressed_size = try write_page(.{
        .data_section = params.data_section,
        .page = v,
        .data_section_size = data_section_size,
        .compression = &compr,
        .compression_cfg = params.compression,
    });

    const pages = try params.header_alloc.alloc(header.Page, 1);
    pages[0] = header.Page{
        .offset = page_offset,
        .compressed_size = @intCast(compressed_size),
        .uncompressed_size = @intCast(v.len),
    };

    const row_index_ends = try params.header_alloc.alloc(u32, 1);
    row_index_ends[0] = len;

    return .{
        .pages = pages,
        .minmax = null,
        .row_index_ends = row_index_ends,
        .compression = compr orelse .no_compression,
    };
}

fn empty_buffer() header.Buffer {
    return header.Buffer{
        .minmax = null,
        .pages = &.{},
        .compression = .no_compression,
        .row_index_ends = &.{},
    };
}

fn minmax_val_to_binary(comptime T: type, val: T) [header.MinMaxLen]u8 {
    var out: [header.MinMaxLen]u8 = undefined;
    @memset(&out, 0);

    const bytes: [@sizeOf(T)]u8 = @bitCast(val);
    @memcpy(out[0..@sizeOf(T)], &bytes);

    return out;
}

fn int_or_float(comptime T: type) bool {
    return comptime switch (@typeInfo(T)) {
        .int => true,
        .float => true,
        else => false,
    };
}

fn write_buffer(comptime T: type, params: Write, buffer: []const T, data_section_size: *u32) Error!header.Buffer {
    const is_int_or_float = comptime int_or_float(T);

    if (buffer.len == 0) {
        return empty_buffer();
    }
    const target_page_size: usize = if (params.page_size_kb) |ps| ps << 10 else std.math.maxInt(usize);

    var compr: ?Compression = null;

    var pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var row_index_ends = try ArrayList(u32).initCapacity(params.scratch_alloc, 128);
    var minmax: ArrayList(header.MinMax) = if (is_int_or_float) try ArrayList(header.MinMax).initCapacity(params.scratch_alloc, 128) else undefined;

    var idx: u32 = 0;
    while (idx < buffer.len) {
        var min: T = comptime if (is_int_or_float) min_of(T) else undefined;
        var max: T = comptime if (is_int_or_float) max_of(T) else undefined;

        var page_size: usize = 0;
        var page_elem_count: u32 = 0;

        while (page_size < target_page_size and idx < buffer.len) : (idx += 1) {
            const elem = buffer[idx];

            page_elem_count += 1;
            page_size += @sizeOf(T);

            if (is_int_or_float) {
                min = @min(min, elem);
                max = @max(max, elem);
            }
        }

        if (page_elem_count == 0) {
            continue;
        }

        const page: []const u8 = @ptrCast(buffer[idx - page_elem_count .. idx]);
        std.debug.assert(page.len == page_elem_count * @sizeOf(T));

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
            .uncompressed_size = @intCast(page_size),
            .compressed_size = @intCast(compressed_size),
            .offset = page_offset,
        });
        try row_index_ends.append(params.scratch_alloc, idx - page_elem_count);
        if (is_int_or_float) {
            try minmax.append(params.scratch_alloc, header.MinMax{ .min = minmax_val_to_binary(T, min), .max = minmax_val_to_binary(T, max) });
        }
    }

    const out_pages = try params.header_alloc.alloc(header.Page, pages.items.len);
    const out_minmax = if (is_int_or_float) try params.header_alloc.alloc(header.MinMax, minmax.items.len) else undefined;
    const out_row_index_ends = try params.header_alloc.alloc(u32, row_index_ends.items.len);
    @memcpy(out_pages, pages.items);
    @memcpy(out_row_index_ends, row_index_ends.items);
    if (is_int_or_float) {
        @memcpy(out_minmax, minmax.items);
    }

    return header.Buffer{
        .compression = compr orelse .no_compression,
        .pages = out_pages,
        .minmax = if (is_int_or_float) out_minmax else null,
        .row_index_ends = out_row_index_ends,
    };
}

fn write_binary_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericBinaryArray(index_t), data_section_size: *u32) Error!header.Array {
    if (array.len == 0) {
        return empty_array();
    }
    const target_page_size: usize = if (params.page_size_kb) |ps| ps << 10 else std.math.maxInt(usize);

    var data_compression: ?Compression = null;
    var offsets_compression: ?Compression = null;

    var data_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var offsets_pages = try ArrayList(header.Page).initCapacity(params.scratch_alloc, 128);
    var row_index_ends = try ArrayList(u32).initCapacity(params.scratch_alloc, 128);
    var data_minmax = try ArrayList(header.MinMax).initCapacity(params.scratch_alloc, 128);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) {
        var min: [header.MinMaxLen]u8 = undefined;
        @memset(&min, std.math.maxInt(u8));
        var max: [header.MinMaxLen]u8 = undefined;
        @memset(&max, 0);

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

                var minmax_val: [header.MinMaxLen]u8 = undefined;
                @memset(&minmax_val, 0);
                const copy_len = @min(e.len, header.MinMaxLen);
                @memcpy(minmax_val[0..copy_len], e[0..copy_len]);

                if (std.mem.order(u8, &min, &minmax_val) == .gt) {
                    min = minmax_val;
                }
                if (std.mem.order(u8, &max, &minmax_val) == .lt) {
                    max = minmax_val;
                }
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
        try data_minmax.append(params.scratch_alloc, header.MinMax{ .min = min, .max = max });
    }

    std.debug.assert(data_pages.items.len == offsets_pages.items.len and data_pages.items.len == row_index_ends.items.len and data_pages.items.len == data_minmax.items.len);

    const data_buffer_pages = try params.header_alloc.alloc(header.Page, data_pages.items.len);
    const data_buffer_minmax = try params.header_alloc.alloc(header.MinMax, data_minmax.items.len);
    const data_buffer_row_index_ends = try params.header_alloc.alloc(u32, row_index_ends.items.len);
    @memcpy(data_buffer_pages, data_pages.items);
    @memcpy(data_buffer_row_index_ends, row_index_ends.items);
    @memcpy(data_buffer_minmax, data_minmax.items);

    const data_buffer = header.Buffer{
        .pages = data_buffer_pages,
        .minmax = data_buffer_minmax,
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

fn stringLessThan(_: void, l: []const u8, r: []const u8) bool {
    return std.mem.order(u8, l, r) == .lt;
}

/// Ascending sort elements and deduplicate
fn sort_and_dedup(data: [][]const u8) [][]const u8 {
    std.mem.sortUnstable([]const u8, data, {}, stringLessThan);
    var write_idx: usize = 0;

    for (data[1..]) |s| {
        if (!std.mem.eql(u8, s, data[write_idx])) {
            write_idx += 1;
            data[write_idx] = s;
        }
    }

    return data[0 .. write_idx + 1];
}

fn count_array_to_dict(array: *const arr.Array) Error!usize {
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
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_binary_view_to_dict(array: *const arr.BinaryViewArray, write_idx: usize, out: [][]const u8) usize {
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_fixed_size_binary_to_dict(array: *const arr.FixedSizeBinaryArray, write_idx: usize, out: [][]const u8) usize {
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
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

    const data = chunk.Chunk{
        .tables = &tables,
        .schema = schema.DatasetSchema{
            .table_names = table_names,
            .dicts = &.{},
            .tables = &table_schemas,
        },
    };

    const data_section = try testing.allocator.alloc(u8, 1 << 22);
    defer testing.allocator.free(data_section);

    var scratch_arena = ArenaAllocator.init(testing.allocator);
    defer scratch_arena.deinit();

    var header_arena = ArenaAllocator.init(testing.allocator);
    defer header_arena.deinit();

    try data.schema.validate();
    try testing.expect(data.schema.check(data.tables));

    const head = try write(.{
        .data_section = data_section,
        .compression = .{ .lz4_hc = 9 },
        .page_size_kb = 1 << 20,
        .header_alloc = header_arena.allocator(),
        .scratch_alloc = scratch_arena.allocator(),
        .chunk = &data,
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
