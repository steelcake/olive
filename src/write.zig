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
const chunk = @import("./chunk.zig");

pub const Compression = compression.Compression;

pub const Error = error{
    OutOfMemory,
    DataSectionOverflow,
    NonBinaryArrayWithDict,
    CompressFail,
};

pub const Write = struct {
    schema: *const schema.DatasetSchema,
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

    const dicts = try params.header_alloc.alloc(?header.Dict, params.chunk.dicts.len);

    for (params.chunk.dicts, params.schema.dicts, 0..) |*dict, dict_schema, dict_idx| {
        const dict_array = try write_binary_array(.i32, params, dict, &data_section_size, true);

        if (dict.len > 0) {
            const filter = if (dict_schema.has_filter)
                header.Filter.construct(dict, params.scratch_alloc, params.header_alloc) catch |e| {
                    if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
                }
            else
                null;

            dicts[dict_idx] = header.Dict{
                .data = dict_array,
                .filter = filter,
            };
        } else {
            dicts[dict_idx] = null;
        }
    }

    const tables = try params.header_alloc.alloc(header.Table, params.chunk.tables.len);

    for (params.chunk.tables, 0..) |table, table_idx| {
        const fields = try params.header_alloc.alloc(header.Array, table.fields.len);

        for (table.fields, 0..) |*array, field_idx| {
            fields[field_idx] = try write_array(params, array, &data_section_size, params.schema.tables[table_idx].has_minmax_index[field_idx]);
        }

        tables[table_idx] = .{
            .fields = fields,
            .num_rows = table.num_rows,
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
        .interval_year_month => |*a| return .{ .interval_year_month = try write_interval_array(i32, params, &a.inner, data_section_size) },
        .interval_day_time => |*a| return .{ .interval_day_time = try write_interval_array([2]i32, params, &a.inner, data_section_size) },
        .interval_month_day_nano => |*a| return .{ .interval_month_day_nano = try write_interval_array(arr.MonthDayNano, params, &a.inner, data_section_size) },
        .list => |*a| return .{ .list = try write_list_array(.i32, params, a, data_section_size) },
        .struct_ => |*a| return .{ .struct_ = try write_struct_array(params, a, data_section_size) },
        .dense_union => |*a| return .{ .dense_union = try write_dense_union_array(params, a, data_section_size) },
        .sparse_union => |*a| return .{ .sparse_union = try write_sparse_union_array(params, a, data_section_size) },
        .fixed_size_binary => |*a| return .{ .fixed_size_binary = try write_fixed_size_binary_array(params, a, data_section_size, has_minmax_index) },
        .fixed_size_list => |*a| return .{ .fixed_size_list = try write_fixed_size_list_array(params, a, data_section_size) },
        .map => |*a| return .{ .map = try write_map_array(params, a, data_section_size) },
        .duration => |*a| return .{ .i64 = try write_primitive_array(i64, params, &a.inner, data_section_size, has_minmax_index) },
        .large_binary => |*a| return .{ .binary = try write_binary_array(.i64, params, a, data_section_size, has_minmax_index) },
        .large_utf8 => |*a| return .{ .binary = try write_binary_array(.i64, params, &a.inner, data_section_size, has_minmax_index) },
        .large_list => |*a| return .{ .list = try write_list_array(.i64, params, a, data_section_size) },
        .run_end_encoded => |*a| return .{ .run_end_encoded = try write_run_end_encoded_array(params, a, data_section_size) },
        .binary_view => |*a| return .{ .binary = try write_binary_view_array(params, a, data_section_size, has_minmax_index) },
        .utf8_view => |*a| return .{ .binary = try write_binary_view_array(params, &a.inner, data_section_size, has_minmax_index) },
        .list_view => |*a| return .{ .list = try write_list_view_array(.i32, params, a, data_section_size) },
        .large_list_view => |*a| return .{ .list = try write_list_view_array(.i64, params, a, data_section_size) },
        .dict => |*a| return .{ .dict = try write_dict_array(params, a, data_section_size) },
    }
}

fn write_list_view_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericListViewArray(index_t), data_section_size: *u32) Error!header.ListArray {
    var builder = arrow.builder.GenericListBuilder(index_t).with_capacity(array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    const inner_arrays_builder = try params.scratch_alloc.alloc(arr.Array, array.len);
    var num_inner_arrays: u32 = 0;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.bitmap.get(validity, idx)) {
                builder.append_item(array.sizes.ptr[idx]) catch unreachable;

                inner_arrays_builder[num_inner_arrays] = arrow.slice.slice(array.inner, @intCast(array.offsets[idx]), @intCast(array.sizes[idx]));
                num_inner_arrays += 1;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_item(array.sizes.ptr[idx]) catch unreachable;
            inner_arrays_builder[num_inner_arrays] = arrow.slice.slice(array.inner, @intCast(array.offsets[idx]), @intCast(array.sizes[idx]));
            num_inner_arrays += 1;
        }
    }

    const inner_arrays = inner_arrays_builder[0..num_inner_arrays];

    const inner_dt = arrow.data_type.get_data_type(array.inner, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };
    const inner = try params.scratch_alloc.create(arr.Array);
    inner.* = try arrow.concat.concat(inner_dt, inner_arrays, params.scratch_alloc, params.scratch_alloc);

    const list_array = builder.finish(inner) catch unreachable;

    return try write_list_array(index_t, params, &list_array, data_section_size);
}

fn scalar_to_u32(scalar: arrow.scalar.Scalar) u32 {
    return switch (scalar) {
        .u8 => |x| @intCast(x),
        .u16 => |x| @intCast(x),
        .u32 => |x| @intCast(x),
        .u64 => |x| @intCast(x),
        .i8 => |x| @intCast(x),
        .i16 => |x| @intCast(x),
        .i32 => |x| @intCast(x),
        .i64 => |x| @intCast(x),
        else => unreachable,
    };
}

fn normalize_dict_array_keys_impl(comptime T: type, base: T, offsets: []const T, scratch_alloc: Allocator) Error![]const T {
    if (offsets.len == 0) {
        return &.{};
    }

    if (base == 0) {
        return offsets;
    }

    const normalized = try scratch_alloc.alloc(T, offsets.len);

    for (0..offsets.len) |idx| {
        normalized[idx] = offsets[idx] - base;
    }

    return normalized;
}

fn normalize_dict_array_keys(comptime T: type, base_key: arrow.scalar.Scalar, keys: *const arr.PrimitiveArray(T), scratch_alloc: Allocator) Error!arr.PrimitiveArray(T) {
    const base = @field(base_key, @typeName(T));
    const values = try normalize_dict_array_keys_impl(T, base, keys.values[keys.offset .. keys.offset + keys.len], scratch_alloc);

    std.debug.assert(keys.null_count == 0);
    std.debug.assert(values.len == keys.len);

    return arr.PrimitiveArray(T){
        .len = keys.len,
        .values = values,
        .offset = 0,
        .validity = null,
        .null_count = 0,
    };
}

fn write_dict_array(params: Write, array: *const arr.DictArray, data_section_size: *u32) Error!header.DictArray {
    if (array.len == 0) {
        return .{
            .keys = null,
            .values = null,
            .is_ordered = false,
            .len = 0,
        };
    }

    const sliced_keys = arrow.slice.slice(array.keys, array.offset, array.len);

    const base_key = (arrow.minmax.min(&sliced_keys) catch unreachable) orelse unreachable;
    const min_key = scalar_to_u32(base_key);
    const max_key = scalar_to_u32((arrow.minmax.max(&sliced_keys) catch unreachable) orelse unreachable);

    const sliced_values = arrow.slice.slice(array.values, min_key, max_key - min_key + 1);

    const values = try params.header_alloc.create(header.Array);
    values.* = try write_array(params, &sliced_values, data_section_size, false);

    const keys_arr: arr.Array = switch (sliced_keys) {
        .i8 => |*a| .{ .i8 = try normalize_dict_array_keys(i8, base_key, a, params.scratch_alloc) },
        .i16 => |*a| .{ .i16 = try normalize_dict_array_keys(i16, base_key, a, params.scratch_alloc) },
        .i32 => |*a| .{ .i32 = try normalize_dict_array_keys(i32, base_key, a, params.scratch_alloc) },
        .i64 => |*a| .{ .i64 = try normalize_dict_array_keys(i64, base_key, a, params.scratch_alloc) },
        .u8 => |*a| .{ .u8 = try normalize_dict_array_keys(u8, base_key, a, params.scratch_alloc) },
        .u16 => |*a| .{ .u16 = try normalize_dict_array_keys(u16, base_key, a, params.scratch_alloc) },
        .u32 => |*a| .{ .u32 = try normalize_dict_array_keys(u32, base_key, a, params.scratch_alloc) },
        .u64 => |*a| .{ .u64 = try normalize_dict_array_keys(u64, base_key, a, params.scratch_alloc) },
        else => unreachable,
    };

    const keys = try params.header_alloc.create(header.Array);
    keys.* = try write_array(params, &keys_arr, data_section_size, false);

    return .{
        .keys = keys,
        .values = values,
        .is_ordered = array.is_ordered,
        .len = array.len,
    };
}

fn write_run_end_encoded_array(params: Write, array: *const arr.RunEndArray, data_section_size: *u32) Error!header.RunEndArray {
    if (array.len == 0) {
        return .{
            .run_ends = null,
            .values = null,
            .len = 0,
        };
    }

    const run_ends = try params.header_alloc.create(header.Array);
    run_ends.* = try write_array(params, &arrow.slice.slice(array.run_ends, array.offset, array.len), data_section_size, false);
    const values = try params.header_alloc.create(header.Array);
    values.* = try write_array(params, &arrow.slice.slice(array.values, array.offset, array.len), data_section_size, false);

    return .{
        .run_ends = run_ends,
        .values = values,
        .len = array.len,
    };
}

fn write_map_array(params: Write, array: *const arr.MapArray, data_section_size: *u32) Error!header.MapArray {
    if (array.len == 0) {
        return .{
            .entries = null,
            .offsets = empty_buffer(),
            .validity = null,
            .len = 0,
            .keys_are_sorted = false,
        };
    }

    const entries = slice_entries: {
        const start: u32 = @intCast(array.offsets[array.offset]);
        const end: u32 = @intCast(array.offsets[array.offset + array.len]);

        const sliced = arrow.slice.slice_struct(array.entries, start, end - start);

        const entries = try params.header_alloc.create(header.StructArray);
        entries.* = try write_struct_array(params, &sliced, data_section_size);

        break :slice_entries entries;
    };

    const normalized_offsets = try normalize_offsets(i32, array.offsets[array.offset .. array.offset + array.len + 1], params.scratch_alloc);
    const offsets = try write_buffer(params, @ptrCast(normalized_offsets[0..array.len]), @sizeOf(i32), data_section_size);

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .entries = entries,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
        .keys_are_sorted = array.keys_are_sorted,
    };
}

fn write_sparse_union_array(params: Write, array: *const arr.SparseUnionArray, data_section_size: *u32) Error!header.SparseUnionArray {
    if (array.inner.len == 0) {
        return .{
            .inner = .{
                .type_ids = empty_buffer(),
                .children = &.{},
                .len = 0,
            },
        };
    }

    const type_ids = try write_buffer(params, @ptrCast(array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len]), @sizeOf(i8), data_section_size);

    const children = try params.header_alloc.alloc(header.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*c, idx| {
        const sliced = arrow.slice.slice(c, array.inner.offset, array.inner.len);
        children[idx] = try write_array(params, &sliced, data_section_size, false);
    }

    return .{
        .inner = .{
            .children = children,
            .type_ids = type_ids,
            .len = array.inner.len,
        },
    };
}

fn write_dense_union_array(params: Write, array: *const arr.DenseUnionArray, data_section_size: *u32) Error!header.DenseUnionArray {
    // Do a validation here since this function does some complicated operations while assuming the array is valid
    arrow.validate.validate_dense_union(array) catch unreachable;

    if (array.inner.len == 0) {
        return .{
            .inner = .{
                .type_ids = empty_buffer(),
                .children = &.{},
                .len = 0,
            },
            .offsets = empty_buffer(),
        };
    }

    const tids = array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len];
    const type_ids = try write_buffer(params, @ptrCast(tids), @sizeOf(i8), data_section_size);
    const input_offsets = array.offsets[array.inner.offset .. array.inner.offset + array.inner.len];
    const normalized_offsets = try params.scratch_alloc.alloc(i32, input_offsets.len);
    const sliced_children = try params.scratch_alloc.alloc(arrow.array.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*child, child_idx| {
        const child_tid = array.inner.type_id_set[child_idx];

        var mm = for (input_offsets, tids) |offset, tid| {
            if (child_tid == tid) {
                break header.MinMax(i32){ .min = offset, .max = offset };
            }
        } else {
            continue;
        };

        for (input_offsets[1..], tids[1..]) |offset, tid| {
            if (child_tid == tid) {
                mm = .{
                    .min = @min(mm.min, offset),
                    .max = @max(mm.max, offset),
                };
            }
        }

        sliced_children[child_idx] = arrow.slice.slice(child, @intCast(mm.min), @intCast(mm.max - mm.min + 1));

        for (input_offsets, tids, 0..) |offset, tid, idx| {
            if (child_tid == tid) {
                normalized_offsets[idx] = offset - mm.min;
            }
        }
    }

    const offsets = try write_buffer(params, @ptrCast(normalized_offsets), @sizeOf(i32), data_section_size);

    const children = try params.header_alloc.alloc(header.Array, array.inner.children.len);
    for (sliced_children, 0..) |*c, child_idx| {
        children[child_idx] = try write_array(params, c, data_section_size, false);
    }

    return .{
        .offsets = offsets,
        .inner = .{
            .type_ids = type_ids,
            .children = children,
            .len = array.inner.len,
        },
    };
}

fn write_struct_array(params: Write, array: *const arr.StructArray, data_section_size: *u32) Error!header.StructArray {
    if (array.len == 0) {
        return .{
            .field_values = &.{},
            .validity = null,
            .len = 0,
        };
    }

    const field_values = try params.header_alloc.alloc(header.Array, array.field_values.len);

    for (array.field_values, 0..) |*field, idx| {
        const sliced = arrow.slice.slice(field, array.offset, array.len);
        field_values[idx] = try write_array(params, &sliced, data_section_size, false);
    }

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .field_values = field_values,
        .len = array.len,
        .validity = validity,
    };
}

fn write_fixed_size_list_array(params: Write, array: *const arr.FixedSizeListArray, data_section_size: *u32) Error!header.FixedSizeListArray {
    if (array.len == 0) {
        return .{
            .inner = null,
            .validity = null,
            .len = 0,
        };
    }

    const item_width: u32 = @intCast(array.item_width);

    const start = array.offset * item_width;
    const end = start + array.len * item_width;

    const inner = try params.header_alloc.create(header.Array);
    inner.* = try write_array(params, &arrow.slice.slice(array.inner, start, end - start), data_section_size, false);

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .inner = inner,
        .validity = validity,
        .len = array.len,
    };
}

fn write_fixed_size_binary_array(params: Write, array: *const arr.FixedSizeBinaryArray, data_section_size: *u32, has_minmax_index: bool) Error!header.FixedSizeBinaryArray {
    if (array.len == 0) {
        return .{
            .data = empty_buffer(),
            .validity = null,
            .len = 0,
            .minmax = null,
        };
    }

    const byte_width: u32 = @intCast(array.byte_width);

    const start = array.offset * byte_width;
    const end = start + array.len * byte_width;
    const data = try write_buffer(params, array.data[start..end], @intCast(byte_width), data_section_size);

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    var minmax: ?[]const header.MinMax([]const u8) = null;
    if (has_minmax_index) {
        const mm = try params.header_alloc.alloc(header.MinMax([]const u8), data.row_index_ends.len);

        var page_start: u32 = 0;
        for (data.row_index_ends, 0..) |page_end, page_idx| {
            const page_data = arrow.slice.slice_fixed_size_binary(array, page_start, page_end - page_start);
            const min = try copy_str(arrow.minmax.minmax_fixed_size_binary(.min, &page_data) orelse unreachable, params.header_alloc);
            const max = try copy_str(arrow.minmax.minmax_fixed_size_binary(.max, &page_data) orelse unreachable, params.header_alloc);
            mm[page_idx] = .{ .min = min, .max = max };
            page_start = page_end;
        }
        minmax = mm;
    }

    return .{
        .data = data,
        .validity = validity,
        .len = array.len,
        .minmax = minmax,
    };
}

fn write_list_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericListArray(index_t), data_section_size: *u32) Error!header.ListArray {
    const I = index_t.to_type();

    if (array.len == 0) {
        return .{
            .inner = null,
            .offsets = empty_buffer(),
            .validity = null,
            .len = 0,
        };
    }

    const inner = slice_inner: {
        const start: u32 = @intCast(array.offsets[array.offset]);
        const end: u32 = @intCast(array.offsets[array.offset + array.len]);

        const sliced_inner = arrow.slice.slice(array.inner, start, end - start);

        const out = try params.header_alloc.create(header.Array);
        out.* = try write_array(params, &sliced_inner, data_section_size, false);
        break :slice_inner out;
    };

    const normalized_offsets = try normalize_offsets(index_t.to_type(), array.offsets[array.offset .. array.offset + array.len + 1], params.scratch_alloc);
    const offsets = try write_buffer(params, @ptrCast(normalized_offsets[0..array.len]), @sizeOf(I), data_section_size);

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .inner = inner,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
    };
}

fn write_bool_array(params: Write, array: *const arr.BoolArray, data_section_size: *u32) Error!header.BoolArray {
    if (array.len == 0) {
        return .{
            .values = empty_buffer(),
            .validity = null,
            .len = 0,
        };
    }

    const aligned_values = try maybe_align_bitmap(array.values, array.offset, array.len, params.scratch_alloc);
    const values = try write_buffer(params, aligned_values, @sizeOf(u8), data_section_size);
    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .values = values,
        .validity = validity,
        .len = array.len,
    };
}

fn write_binary_view_array(params: Write, array: *const arr.BinaryViewArray, data_section_size: *u32, has_minmax_index: bool) Error!header.BinaryArray {
    var total_size: u32 = 0;

    var idx: u32 = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        total_size += @as(u32, @bitCast(array.views[idx].length));
    }

    var builder = arrow.builder.LargeBinaryBuilder.with_capacity(total_size, array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
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

    return try write_binary_array(.i64, params, &bin_array, data_section_size, has_minmax_index);
}

fn empty_buffer() header.Buffer {
    return .{
        .row_index_ends = &.{},
        .pages = &.{},
        .compression = .no_compression,
    };
}

fn write_interval_array(comptime T: type, params: Write, array: *const arr.PrimitiveArray(T), data_section_size: *u32) Error!header.IntervalArray {
    if (array.len == 0) {
        return .{
            .values = empty_buffer(),
            .validity = null,
            .len = 0,
        };
    }

    const values = try write_buffer(params, @ptrCast(array.values[array.offset .. array.offset + array.len]), @sizeOf(T), data_section_size);
    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    return .{
        .values = values,
        .validity = validity,
        .len = array.len,
    };
}

fn write_primitive_array(comptime T: type, params: Write, array: *const arr.PrimitiveArray(T), data_section_size: *u32, has_minmax_index: bool) Error!header.PrimitiveArray(T) {
    if (array.len == 0) {
        return .{
            .values = empty_buffer(),
            .validity = null,
            .len = 0,
            .minmax = null,
        };
    }

    const values = try write_buffer(params, @ptrCast(array.values[array.offset .. array.offset + array.len]), @sizeOf(T), data_section_size);
    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    var minmax: ?[]const header.MinMax(T) = null;
    if (has_minmax_index) {
        const mm = try params.header_alloc.alloc(header.MinMax(T), values.row_index_ends.len);

        var start: u32 = 0;
        for (values.row_index_ends, 0..) |end, page_idx| {
            const page_data = arrow.slice.slice_primitive(T, array, start, end - start);

            const min = arrow.minmax.minmax_primitive(.min, T, &page_data) orelse unreachable;
            const max = arrow.minmax.minmax_primitive(.max, T, &page_data) orelse unreachable;

            mm[page_idx] = .{ .min = min, .max = max };

            start = end;
        }

        minmax = mm;
    }

    return .{
        .values = values,
        .validity = validity,
        .len = array.len,
        .minmax = minmax,
    };
}

// Aligns the given bitmap by allocating new memory using alloc and copying over the bits if bitmap isn't already aligned (offset is multiple of 8)
fn maybe_align_bitmap(bitmap: []const u8, offset: u32, len: u32, alloc: Allocator) Error![]const u8 {
    std.debug.assert(bitmap.len * 8 >= offset + len);

    if (offset % 8 == 0) {
        return bitmap[(offset / 8)..(offset / 8 + (len + 7) / 8)];
    }

    const x = try alloc.alloc(u8, (len + 7) / 8);
    @memset(x, 0);

    var i: u32 = offset;
    while (i < offset + len) : (i += 1) {
        if (arrow.bitmap.get(bitmap.ptr, i)) {
            arrow.bitmap.set(x.ptr, i);
        }
    }

    return x;
}

fn write_validity(params: Write, offset: u32, len: u32, null_count: u32, validity_opt: ?[]const u8, data_section_size: *u32) Error!?header.Buffer {
    if (null_count == 0) {
        return null;
    }

    const validity = validity_opt orelse return null;

    const v = try maybe_align_bitmap(validity, offset, len, params.scratch_alloc);

    return try write_buffer(params, v, @sizeOf(u8), data_section_size);
}

/// Similar to write_buffer but takes a list of offsets to break up the logical rows in the buffer to pages.
/// Intended to be used for writing data section of binary arrays
fn write_buffer_with_offsets(comptime I: type, params: Write, buffer: []const u8, offsets: []const I, data_section_size: *u32) Error!header.Buffer {
    if (offsets.len <= 1) {
        return empty_buffer();
    }

    const max_p_size: u32 = if (params.page_size_kb) |ps| ps << 10 else std.math.maxInt(u32);
    const max_page_size: I = @intCast(max_p_size);

    std.debug.assert(max_page_size > 0);

    var compr: ?Compression = null;

    const pages_builder = try params.scratch_alloc.alloc(header.Page, offsets.len);
    const row_index_ends_builder = try params.scratch_alloc.alloc(u32, offsets.len);
    var num_pages: u32 = 0;

    var offset_idx: u32 = 1;
    var page_start: I = offsets[0];
    while (offset_idx < offsets.len) : (offset_idx += 1) {
        const page_end = offsets[offset_idx];
        const page_size = page_end - page_start;

        if (page_size >= max_page_size or offset_idx == offsets.len - 1) {
            const p_start: u32 = @intCast(page_start);
            const p_end: u32 = @intCast(page_end);
            const page = buffer[p_start..p_end];

            const page_offset = data_section_size.*;
            const compressed_size = try write_page(.{
                .data_section = params.data_section,
                .page = page,
                .data_section_size = data_section_size,
                .compression = &compr,
                .compression_cfg = params.compression,
            });

            // Write header info to arrays
            pages_builder[num_pages] = header.Page{
                .uncompressed_size = @intCast(page_size),
                .compressed_size = @intCast(compressed_size),
                .offset = page_offset,
            };
            row_index_ends_builder[num_pages] = offset_idx;
            num_pages += 1;
            page_start = page_end;
        }
    }

    const pages = try params.header_alloc.alloc(header.Page, num_pages);
    const row_index_ends = try params.header_alloc.alloc(u32, num_pages);
    @memcpy(pages, pages_builder[0..num_pages]);
    @memcpy(row_index_ends, row_index_ends_builder[0..num_pages]);

    return header.Buffer{
        .compression = compr orelse .no_compression,
        .pages = pages,
        .row_index_ends = row_index_ends,
    };
}

fn write_buffer(params: Write, buffer: []const u8, elem_size: u8, data_section_size: *u32) Error!header.Buffer {
    std.debug.assert(buffer.len % elem_size == 0);
    const buffer_len = buffer.len / elem_size;

    if (buffer_len == 0) {
        return empty_buffer();
    }
    const max_page_len: u32 = if (params.page_size_kb) |ps| ((ps << 10) + elem_size - 1) / elem_size else std.math.maxInt(u32);
    std.debug.assert(max_page_len > 0);

    var compr: ?Compression = null;

    const num_pages = (buffer_len + max_page_len - 1) / max_page_len;
    const pages = try params.header_alloc.alloc(header.Page, num_pages);
    const row_index_ends = try params.header_alloc.alloc(u32, num_pages);

    var buffer_offset: u32 = 0;
    var page_idx: usize = 0;
    while (buffer_offset < buffer_len) {
        const page_len = @min(max_page_len, buffer_len);
        const page: []const u8 = buffer[buffer_offset .. buffer_offset + page_len];

        const page_offset = data_section_size.*;
        const compressed_size = try write_page(.{
            .data_section = params.data_section,
            .page = page,
            .data_section_size = data_section_size,
            .compression = &compr,
            .compression_cfg = params.compression,
        });

        // Write header info to arrays
        pages[page_idx] = header.Page{
            .uncompressed_size = @intCast(page_len * elem_size),
            .compressed_size = @intCast(compressed_size),
            .offset = page_offset,
        };
        row_index_ends[page_idx] = buffer_offset + page_len;

        buffer_offset += page_len;
        page_idx += 1;
    }
    std.debug.assert(page_idx == num_pages);

    return header.Buffer{
        .compression = compr orelse .no_compression,
        .pages = pages,
        .row_index_ends = row_index_ends,
    };
}

fn write_binary_array(comptime index_t: arr.IndexType, params: Write, array: *const arr.GenericBinaryArray(index_t), data_section_size: *u32, has_minmax_index: bool) Error!header.BinaryArray {
    const I = index_t.to_type();

    if (array.len == 0) {
        return .{
            .data = empty_buffer(),
            .offsets = empty_buffer(),
            .validity = null,
            .len = 0,
            .minmax = null,
        };
    }

    const data = slice_data: {
        const start: usize = @intCast(array.offsets[array.offset]);
        const end: usize = @intCast(array.offsets[array.offset + array.len]);
        break :slice_data try write_buffer_with_offsets(I, params, @ptrCast(array.data[start..end]), array.offsets[array.offset .. array.offset + array.len], data_section_size);
    };

    const normalized_offsets = try normalize_offsets(index_t.to_type(), array.offsets[array.offset .. array.offset + array.len + 1], params.scratch_alloc);
    const offsets = try write_buffer(params, @ptrCast(normalized_offsets[0..array.len]), @sizeOf(I), data_section_size);

    const validity = try write_validity(params, array.offset, array.len, array.null_count, array.validity, data_section_size);

    var minmax: ?[]const header.MinMax([]const u8) = null;
    if (has_minmax_index) {
        const mm = try params.header_alloc.alloc(header.MinMax([]const u8), offsets.row_index_ends.len);

        var page_start: u32 = 0;
        for (offsets.row_index_ends, 0..) |page_end, page_idx| {
            const page_data = arrow.slice.slice_binary(index_t, array, page_start, page_end - page_start);
            const min = try copy_str(arrow.minmax.minmax_binary(.min, index_t, &page_data) orelse unreachable, params.header_alloc);
            const max = try copy_str(arrow.minmax.minmax_binary(.max, index_t, &page_data) orelse unreachable, params.header_alloc);
            mm[page_idx] = .{ .min = min, .max = max };
            page_start = page_end;
        }
        minmax = mm;
    }

    return .{
        .data = data,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
        .minmax = minmax,
    };
}

fn copy_str(str: []const u8, alloc: Allocator) Error![]const u8 {
    const out = try alloc.alloc(u8, str.len);
    @memcpy(out, str);
    return out;
}

fn normalize_offsets(comptime T: type, offsets: []const T, scratch_alloc: Allocator) Error![]const T {
    if (offsets.len == 0) {
        return &.{};
    }

    const base = offsets[0];

    if (base == 0) {
        return offsets;
    }

    const normalized = try scratch_alloc.alloc(T, offsets.len);

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
        const arr0 = &arrays[base % arrays.len];
        const arr1 = &arrays[(base + 1) % arrays.len];
        const arr2 = &arrays[(base + 2) % arrays.len];

        const num_rows = @min(
            arrow.length.length(arr0),
            arrow.length.length(arr1),
            arrow.length.length(arr2),
        );

        tables[idx] = &.{
            arrow.slice.slice(arr0, 0, num_rows),
            arrow.slice.slice(arr1, 0, num_rows),
            arrow.slice.slice(arr2, 0, num_rows),
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

    var has_minmax_index: [3][3]bool = undefined;
    for (0..3) |table_idx| {
        for (0..3) |field_idx| {
            has_minmax_index[table_idx][field_idx] = switch (data_types[table_idx][field_idx]) {
                .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .decimal32, .decimal64, .decimal128, .decimal256, .f16, .f32, .f64, .binary, .utf8, .large_binary, .large_utf8, .binary_view, .utf8_view, .fixed_size_binary => true,
                else => false,
            };
        }
    }

    var table_schemas: [3]schema.TableSchema = undefined;
    for (0..3) |idx| {
        table_schemas[idx] = .{
            .field_names = field_names,
            .data_types = data_types[idx],
            .has_minmax_index = &has_minmax_index[idx],
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

    const chunk_data = try chunk.Chunk.from_arrow(&sch, &tables, header_arena.allocator(), scratch_arena.allocator());

    const head = try write(.{
        .data_section = data_section,
        .compression = .{ .lz4_hc = 9 },
        .page_size_kb = 1 << 20,
        .header_alloc = header_arena.allocator(),
        .scratch_alloc = scratch_arena.allocator(),
        .chunk = &chunk_data,
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
