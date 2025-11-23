const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;

const header = @import("./header.zig");
const schema = @import("./schema.zig");
const compression = @import("./compression.zig");
const dict_impl = @import("./dict.zig");
const chunk = @import("./chunk.zig");

const Compression = compression.Compression;
const Compressor = compression.Compressor;

pub const Error = error{
    OutOfMemory,
    DataSectionOverflow,
};

const Context = struct {
    header_alloc: Allocator,
    scratch_alloc: Allocator,
    data_section: []u8,
    data_section_size: *u32,
    page_size: u32,
    compressor: *Compressor,
    compr_bias: CompressionBias,
};

pub const CompressionBias = enum {
    balanced,
    read_optimized,
};

pub fn write(params: struct {
    chunk: *const chunk.Chunk,
    /// Allocator that is used for allocating any dynamic memory relating to outputted header.
    /// Lifetime of the header is tied to this allocator after creation.
    header_alloc: Allocator,
    /// Allocator for allocating temporary memory used for constructing the output
    scratch_alloc: Allocator,
    /// For outputting the buffers
    data_section: []u8,
    /// Targeted page size in bytes
    page_size: ?u32 = null,
    /// What to optimize the compression algorihm for.
    ///
    /// `balanced` is balanced.
    ///
    /// `read_optimized` heavily optimizes for read speed at the cost of write speed.
    compression_bias: CompressionBias = .balanced,
}) Error!header.Header {
    var compressor = try compression.Compressor.init(params.scratch_alloc);
    defer compressor.deinit(params.scratch_alloc);

    var data_section_size: u32 = 0;

    const ctx = Context{
        .header_alloc = params.header_alloc,
        .scratch_alloc = params.scratch_alloc,
        .data_section = params.data_section,
        .data_section_size = &data_section_size,
        .page_size = params.page_size orelse 1 << 30,
        .compressor = &compressor,
        .compr_bias = params.compression_bias,
    };
    std.debug.assert(ctx.page_size > 0);

    const dict_ctx = header.DictContext{
        .dict20 = try write_dict(20, ctx, params.chunk.dict_ctx.dict20),
        .dict32 = try write_dict(32, ctx, params.chunk.dict_ctx.dict32),
    };

    const tables = try params.header_alloc.alloc(header.Table, params.chunk.tables.len);

    for (params.chunk.tables, 0..) |table, table_idx| {
        const fields = try params.header_alloc.alloc(header.Array, table.len);

        const num_rows = arrow.length.length(&table[0]);

        for (table, 0..) |*array, field_idx| {
            std.debug.assert(num_rows == arrow.length.length(array));

            fields[field_idx] = try write_array(
                ctx,
                array,
            );
        }

        tables[table_idx] = header.Table{
            .fields = fields,
            .num_rows = num_rows,
        };
    }

    return header.Header{
        .dict_ctx = dict_ctx,
        .tables = tables,
        .data_section_size = data_section_size,
    };
}

fn write_dict(comptime W: comptime_int, ctx: Context, dict: []const [W]u8) Error!header.Dict {
    const page_offset = ctx.data_section_size.*;
    const page_size = try write_page(ctx, .no_compression, @ptrCast(dict));
    std.debug.assert(page_size == dict.len * W);

    return header.Dict{
        .offset = page_offset,
        .size = @intCast(page_size),
    };
}

fn write_array(
    ctx: Context,
    array: *const arr.Array,
) Error!header.Array {
    switch (array.*) {
        .null => |*a| return .{ .null = .{ .len = a.len } },
        .i8 => |*a| return .{ .primitive = try write_primitive_array(
            i8,
            ctx,
            a,
        ) },
        .i16 => |*a| return .{ .primitive = try write_primitive_array(i16, ctx, a) },
        .i32 => |*a| return .{ .primitive = try write_primitive_array(i32, ctx, a) },
        .i64 => |*a| return .{ .primitive = try write_primitive_array(i64, ctx, a) },
        .u8 => |*a| return .{ .primitive = try write_primitive_array(u8, ctx, a) },
        .u16 => |*a| return .{ .primitive = try write_primitive_array(u16, ctx, a) },
        .u32 => |*a| return .{ .primitive = try write_primitive_array(u32, ctx, a) },
        .u64 => |*a| return .{ .primitive = try write_primitive_array(u64, ctx, a) },
        .f16 => |*a| return .{ .primitive = try write_primitive_array(f16, ctx, a) },
        .f32 => |*a| return .{ .primitive = try write_primitive_array(f32, ctx, a) },
        .f64 => |*a| return .{ .primitive = try write_primitive_array(f64, ctx, a) },
        .binary => |*a| return .{ .binary = try write_binary_array(.i32, ctx, a) },
        .utf8 => |*a| return .{ .binary = try write_binary_array(.i32, ctx, &a.inner) },
        .bool => |*a| return .{ .bool = try write_bool_array(ctx, a) },
        .decimal32 => |*a| return .{ .primitive = try write_primitive_array(
            i32,
            ctx,
            &a.inner,
        ) },
        .decimal64 => |*a| return .{ .primitive = try write_primitive_array(
            i64,
            ctx,
            &a.inner,
        ) },
        .decimal128 => |*a| return .{ .primitive = try write_primitive_array(
            i128,
            ctx,
            &a.inner,
        ) },
        .decimal256 => |*a| return .{ .primitive = try write_primitive_array(
            i256,
            ctx,
            &a.inner,
        ) },
        .date32 => |*a| return .{ .primitive = try write_primitive_array(
            i32,
            ctx,
            &a.inner,
        ) },
        .date64 => |*a| return .{ .primitive = try write_primitive_array(
            i64,
            ctx,
            &a.inner,
        ) },
        .time32 => |*a| return .{ .primitive = try write_primitive_array(
            i32,
            ctx,
            &a.inner,
        ) },
        .time64 => |*a| return .{ .primitive = try write_primitive_array(
            i64,
            ctx,
            &a.inner,
        ) },
        .timestamp => |*a| return .{ .primitive = try write_primitive_array(
            i64,
            ctx,
            &a.inner,
        ) },
        .interval_year_month => |*a| return .{ .primitive = try write_primitive_array(
            i32,
            ctx,
            &a.inner,
        ) },
        .interval_day_time => |*a| return .{ .primitive = try write_primitive_array(
            [2]i32,
            ctx,
            &a.inner,
        ) },
        .interval_month_day_nano => |*a| return .{ .primitive = try write_primitive_array(
            arr.MonthDayNano,
            ctx,
            &a.inner,
        ) },
        .list => |*a| return .{ .list = try write_list_array(.i32, ctx, a) },
        .struct_ => |*a| return .{ .struct_ = try write_struct_array(ctx, a) },
        .dense_union => |*a| return .{ .dense_union = try write_dense_union_array(ctx, a) },
        .sparse_union => |*a| return .{ .sparse_union = try write_sparse_union_array(ctx, a) },
        .fixed_size_binary => |*a| return .{ .fixed_size_binary = try write_fixed_size_binary_array(
            ctx,
            a,
        ) },
        .fixed_size_list => |*a| return .{ .fixed_size_list = try write_fixed_size_list_array(
            ctx,
            a,
        ) },
        .map => |*a| return .{ .map = try write_map_array(ctx, a) },
        .duration => |*a| return .{ .primitive = try write_primitive_array(
            i64,
            ctx,
            &a.inner,
        ) },
        .large_binary => |*a| return .{ .binary = try write_binary_array(.i64, ctx, a) },
        .large_utf8 => |*a| return .{ .binary = try write_binary_array(.i64, ctx, &a.inner) },
        .large_list => |*a| return .{ .list = try write_list_array(.i64, ctx, a) },
        .run_end_encoded => |*a| return .{ .run_end_encoded = try write_run_end_encoded_array(ctx, a) },
        .binary_view => |*a| return .{ .binary = try write_binary_view_array(
            ctx,
            a,
        ) },
        .utf8_view => |*a| return .{ .binary = try write_binary_view_array(
            ctx,
            &a.inner,
        ) },
        .list_view => |*a| return .{ .list = try write_list_view_array(.i32, ctx, a) },
        .large_list_view => |*a| return .{ .list = try write_list_view_array(.i64, ctx, a) },
        .dict => |*a| return .{ .dict = try write_dict_array(ctx, a) },
    }
}

fn write_list_view_array(
    comptime index_t: arr.IndexType,
    ctx: Context,
    array: *const arr.GenericListViewArray(index_t),
) Error!header.ListArray {
    const I = index_t.to_type();

    const offsets = try ctx.scratch_alloc.alloc(I, array.len + 1);
    offsets[0] = 0;

    const inner_arrays = try ctx.scratch_alloc.alloc(arr.Array, array.len);

    var idx: u32 = 0;
    var current_offset: I = 0;
    while (idx < array.len) : (idx += 1) {
        const size = array.sizes[idx + array.offset];
        const offset = array.offsets[idx + array.offset];
        inner_arrays[idx] = arrow.slice.slice(array.inner, @intCast(offset), @intCast(size));
        current_offset += size;
        offsets[idx + 1] = current_offset;
    }

    const inner_dt = arrow.data_type.get_data_type(array.inner, ctx.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };
    const inner = try ctx.scratch_alloc.create(arr.Array);
    inner.* = try arrow.concat.concat(inner_dt, inner_arrays, ctx.scratch_alloc, ctx.scratch_alloc);

    const validity = if (array.null_count > 0)
        try maybe_align_bitmap(
            array.validity orelse unreachable,
            array.offset,
            array.len,
            ctx.scratch_alloc,
        )
    else
        null;

    const list_array = arr.GenericListArray(index_t){
        .inner = inner,
        .offset = 0,
        .len = array.len,
        .offsets = offsets,
        .validity = validity,
        .null_count = array.null_count,
    };

    return try write_list_array(index_t, ctx, &list_array);
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

fn normalize_dict_array_keys_impl(
    comptime T: type,
    base: T,
    offsets: []const T,
    scratch_alloc: Allocator,
) Error![]const T {
    if (base == 0) {
        return offsets;
    }

    const normalized = try scratch_alloc.alloc(T, offsets.len);

    for (0..offsets.len) |idx| {
        // allow overflow here since these might be garbage values (we don't check for nulls)
        normalized[idx] = offsets[idx] -% base;
    }

    return normalized;
}

fn normalize_dict_array_keys(
    comptime T: type,
    base_key: arrow.scalar.Scalar,
    keys: *const arr.PrimitiveArray(T),
    scratch_alloc: Allocator,
) Error!arr.PrimitiveArray(T) {
    const base = @field(base_key, @typeName(T));
    const values = try normalize_dict_array_keys_impl(
        T,
        base,
        keys.values[keys.offset .. keys.offset + keys.len],
        scratch_alloc,
    );
    std.debug.assert(values.len == keys.len);

    const validity = if (keys.null_count > 0)
        try maybe_align_bitmap(
            keys.validity orelse unreachable,
            keys.offset,
            keys.len,
            scratch_alloc,
        )
    else
        null;

    return arr.PrimitiveArray(T){
        .len = keys.len,
        .values = values,
        .offset = 0,
        .validity = validity,
        .null_count = keys.null_count,
    };
}

fn write_dict_array(
    ctx: Context,
    array: *const arr.DictArray,
) Error!header.DictArray {
    if (array.len == 0) {
        const keys = try ctx.header_alloc.create(header.Array);
        const values = try ctx.header_alloc.create(header.Array);

        keys.* = try write_array(ctx, &arrow.slice.slice(array.keys, 0, 0));
        values.* = try write_array(ctx, &arrow.slice.slice(array.values, 0, 0));

        return .{
            .keys = keys,
            .values = values,
            .is_ordered = false,
            .len = 0,
        };
    }

    const sliced_keys = arrow.slice.slice(array.keys, array.offset, array.len);

    const base_key = (arrow.minmax.min(&sliced_keys) catch unreachable) orelse unreachable;
    const min_key = scalar_to_u32(base_key);
    const max_key = scalar_to_u32((arrow.minmax.max(&sliced_keys) catch unreachable) orelse unreachable);

    const sliced_values = arrow.slice.slice(array.values, min_key, max_key - min_key + 1);

    const values = try ctx.header_alloc.create(header.Array);
    values.* = try write_array(ctx, &sliced_values);

    const keys_arr: arr.Array = switch (sliced_keys) {
        .i8 => |*a| .{ .i8 = try normalize_dict_array_keys(i8, base_key, a, ctx.scratch_alloc) },
        .i16 => |*a| .{ .i16 = try normalize_dict_array_keys(i16, base_key, a, ctx.scratch_alloc) },
        .i32 => |*a| .{ .i32 = try normalize_dict_array_keys(i32, base_key, a, ctx.scratch_alloc) },
        .i64 => |*a| .{ .i64 = try normalize_dict_array_keys(i64, base_key, a, ctx.scratch_alloc) },
        .u8 => |*a| .{ .u8 = try normalize_dict_array_keys(u8, base_key, a, ctx.scratch_alloc) },
        .u16 => |*a| .{ .u16 = try normalize_dict_array_keys(u16, base_key, a, ctx.scratch_alloc) },
        .u32 => |*a| .{ .u32 = try normalize_dict_array_keys(u32, base_key, a, ctx.scratch_alloc) },
        .u64 => |*a| .{ .u64 = try normalize_dict_array_keys(u64, base_key, a, ctx.scratch_alloc) },
        else => unreachable,
    };

    const keys = try ctx.header_alloc.create(header.Array);
    keys.* = try write_array(ctx, &keys_arr);

    return .{
        .keys = keys,
        .values = values,
        .is_ordered = array.is_ordered,
        .len = array.len,
    };
}

fn write_run_end_encoded_array(
    ctx: Context,
    array: *const arr.RunEndArray,
) Error!header.RunEndArray {
    const normalized = try arrow.slice.normalize_run_end_encoded(array, 0, ctx.scratch_alloc);

    const run_ends = try ctx.header_alloc.create(header.Array);
    run_ends.* = try write_array(ctx, normalized.run_ends);
    const values = try ctx.header_alloc.create(header.Array);
    values.* = try write_array(ctx, normalized.values);

    return .{
        .run_ends = run_ends,
        .values = values,
        .len = array.len,
    };
}

fn write_map_array(
    ctx: Context,
    array: *const arr.MapArray,
) Error!header.MapArray {
    const entries = slice_entries: {
        const start: u32 = @intCast(array.offsets[array.offset]);
        const end: u32 = @intCast(array.offsets[array.offset + array.len]);

        const sliced = arrow.slice.slice_struct(array.entries, start, end - start);

        const entries = try ctx.header_alloc.create(header.StructArray);
        entries.* = try write_struct_array(ctx, &sliced);

        break :slice_entries entries;
    };

    const normalized_offsets = try normalize_offsets(
        i32,
        array.offsets[array.offset .. array.offset + array.len + 1],
        ctx.scratch_alloc,
    );
    const offsets = try write_buffer(ctx, .lz4, @ptrCast(normalized_offsets), @sizeOf(i32));

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .entries = entries,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
        .keys_are_sorted = array.keys_are_sorted,
    };
}

fn write_sparse_union_array(
    ctx: Context,
    array: *const arr.SparseUnionArray,
) Error!header.SparseUnionArray {
    const type_ids = try write_buffer(
        ctx,
        .lz4,
        @ptrCast(array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len]),
        @sizeOf(i8),
    );

    const children = try ctx.header_alloc.alloc(header.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*c, idx| {
        const sliced = arrow.slice.slice(c, array.inner.offset, array.inner.len);
        children[idx] = try write_array(ctx, &sliced);
    }

    return .{
        .inner = .{
            .children = children,
            .type_ids = type_ids,
            .len = array.inner.len,
        },
    };
}

fn write_dense_union_array(
    ctx: Context,
    array: *const arr.DenseUnionArray,
) Error!header.DenseUnionArray {
    // Do a validation here since this function does some complicated operations
    //  while assuming the array is valid
    arrow.validate.validate_dense_union_array(array) catch unreachable;

    const tids = array.inner.type_ids[array.inner.offset .. array.inner.offset + array.inner.len];
    const type_ids = try write_buffer(ctx, .lz4, @ptrCast(tids), @sizeOf(i8));
    const input_offsets = array.offsets[array.inner.offset .. array.inner.offset + array.inner.len];
    const normalized_offsets = try ctx.scratch_alloc.alloc(i32, input_offsets.len);
    const sliced_children = try ctx.scratch_alloc.alloc(arrow.array.Array, array.inner.children.len);

    for (array.inner.children, 0..) |*child, child_idx| {
        const child_tid = array.inner.type_id_set[child_idx];

        var mm = for (input_offsets, tids) |offset, tid| {
            if (child_tid == tid) {
                break .{ .min = offset, .max = offset };
            }
        } else {
            sliced_children[child_idx] = arrow.slice.slice(child, 0, 0);
            continue;
        };

        for (input_offsets, tids) |offset, tid| {
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

    const offsets = try write_buffer(ctx, .lz4, @ptrCast(normalized_offsets), @sizeOf(i32));

    const children = try ctx.header_alloc.alloc(header.Array, array.inner.children.len);
    for (sliced_children, 0..) |*c, child_idx| {
        children[child_idx] = try write_array(ctx, c);
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

fn write_struct_array(
    ctx: Context,
    array: *const arr.StructArray,
) Error!header.StructArray {
    const field_values = try ctx.header_alloc.alloc(header.Array, array.field_values.len);

    for (array.field_values, 0..) |*field, idx| {
        const sliced = arrow.slice.slice(field, array.offset, array.len);
        field_values[idx] = try write_array(ctx, &sliced);
    }

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .field_values = field_values,
        .len = array.len,
        .validity = validity,
    };
}

fn write_fixed_size_list_array(
    ctx: Context,
    array: *const arr.FixedSizeListArray,
) Error!header.FixedSizeListArray {
    const item_width: u32 = @intCast(array.item_width);

    const start = array.offset * item_width;
    const end = start + array.len * item_width;

    const inner = try ctx.header_alloc.create(header.Array);
    inner.* = try write_array(ctx, &arrow.slice.slice(array.inner, start, end - start));

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .inner = inner,
        .validity = validity,
        .len = array.len,
    };
}

fn write_list_array(
    comptime index_t: arr.IndexType,
    ctx: Context,
    array: *const arr.GenericListArray(index_t),
) Error!header.ListArray {
    const I = index_t.to_type();

    const inner = slice_inner: {
        const start: u32 = @intCast(array.offsets[array.offset]);
        const end: u32 = @intCast(array.offsets[array.offset + array.len]);

        const sliced_inner = arrow.slice.slice(array.inner, start, end - start);

        const out = try ctx.header_alloc.create(header.Array);
        out.* = try write_array(ctx, &sliced_inner);
        break :slice_inner out;
    };

    const normalized_offsets = try normalize_offsets(
        index_t.to_type(),
        array.offsets[array.offset .. array.offset + array.len + 1],
        ctx.scratch_alloc,
    );
    const offsets = try write_buffer(
        ctx,
        .lz4,
        @ptrCast(normalized_offsets[0 .. array.len + 1]),
        @sizeOf(I),
    );

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .inner = inner,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
    };
}

fn write_bool_array(
    ctx: Context,
    array: *const arr.BoolArray,
) Error!header.BoolArray {
    const aligned_values = try maybe_align_bitmap(
        array.values,
        array.offset,
        array.len,
        ctx.scratch_alloc,
    );
    const values = try write_buffer(ctx, .lz4, aligned_values, @sizeOf(u8));
    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .values = values,
        .validity = validity,
        .len = array.len,
    };
}

fn write_fixed_size_binary_array(
    ctx: Context,
    array: *const arr.FixedSizeBinaryArray,
) Error!header.FixedSizeBinaryArray {
    const byte_width: u32 = @intCast(array.byte_width);

    const start = array.offset * byte_width;
    const end = start + array.len * byte_width;
    const data = try write_buffer(ctx, .lz4, array.data[start..end], @intCast(byte_width));

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .data = data,
        .validity = validity,
        .len = array.len,
    };
}

fn write_binary_view_array(
    ctx: Context,
    array: *const arr.BinaryViewArray,
) Error!header.BinaryArray {
    var total_size: i64 = 0;

    if (array.null_count > 0) {
        const validity = array.validity orelse unreachable;

        const Closure = struct {
            a: *const arr.BinaryViewArray,
            tsize: *i64,

            fn process(self: @This(), idx: u32) void {
                self.tsize.* += @as(i64, @intCast(self.a.views[idx].length));
            }
        };

        arrow.bitmap.for_each(
            Closure,
            Closure.process,
            Closure{
                .a = array,
                .tsize = &total_size,
            },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            total_size += @as(i64, @intCast(array.views[idx].length));
        }
    }

    const data = try ctx.scratch_alloc.alloc(u8, @intCast(total_size));
    const offsets = try ctx.scratch_alloc.alloc(i64, array.len + 1);
    offsets[0] = 0;

    if (array.null_count > 0) {
        const validity = array.validity orelse unreachable;

        const Closure = struct {
            a: *const arr.BinaryViewArray,
            of: []i64,
            d: []u8,

            fn process(self: @This(), idx: u32) void {
                const s = arrow.get.get_binary_view(
                    self.a.buffers,
                    self.a.views,
                    idx,
                );
                const start_offset = self.of[idx - self.a.offset];
                const end_offset = start_offset + @as(i64, @intCast(s.len));
                @memcpy(self.d[@intCast(start_offset)..@intCast(end_offset)], s);
                self.of[idx - self.a.offset + 1] = end_offset;
            }
        };

        arrow.bitmap.for_each(
            Closure,
            Closure.process,
            Closure{
                .a = array,
                .of = offsets,
                .d = data,
            },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const s = arrow.get.get_binary_view(
                array.buffers,
                array.views,
                idx,
            );
            const start_offset = offsets[idx - array.offset];
            const end_offset = start_offset + @as(i64, @intCast(s.len));
            @memcpy(data[@intCast(start_offset)..@intCast(end_offset)], s);
            offsets[idx - array.offset + 1] = end_offset;
        }
    }

    const validity = if (array.null_count > 0)
        try maybe_align_bitmap(
            array.validity orelse unreachable,
            array.offset,
            array.len,
            ctx.scratch_alloc,
        )
    else
        null;

    return try write_binary_array(
        .i64,
        ctx,
        &arr.LargeBinaryArray{
            .len = array.len,
            .data = data,
            .offset = 0,
            .offsets = offsets,
            .validity = validity,
            .null_count = array.null_count,
        },
    );
}

fn empty_buffer() header.Buffer {
    return .{
        .row_index_ends = &.{},
        .pages = &.{},
        .compression = .no_compression,
    };
}

fn write_primitive_array(
    comptime T: type,
    ctx: Context,
    array: *const arr.PrimitiveArray(T),
) Error!header.PrimitiveArray {
    if (array.len == 0) {
        return .{
            .values = empty_buffer(),
            .validity = null,
            .len = 0,
        };
    }

    const values = try write_buffer(
        ctx,
        .lz4,
        @ptrCast(array.values[array.offset .. array.offset + array.len]),
        @sizeOf(T),
    );
    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .values = values,
        .validity = validity,
        .len = array.len,
    };
}

fn write_binary_array(
    comptime index_t: arr.IndexType,
    ctx: Context,
    array: *const arr.GenericBinaryArray(index_t),
) Error!header.BinaryArray {
    const I = index_t.to_type();

    const data = try write_buffer_with_offsets(
        I,
        ctx,
        switch (ctx.compr_bias) {
            .balanced => .zstd,
            .read_optimized => .lz4_hc,
        },
        array.data,
        array.offsets[array.offset .. array.offset + array.len + 1],
    );

    const normalized_offsets = try normalize_offsets(
        index_t.to_type(),
        array.offsets[array.offset .. array.offset + array.len + 1],
        ctx.scratch_alloc,
    );
    const offsets = try write_buffer(
        ctx,
        .lz4,
        @ptrCast(normalized_offsets[0 .. array.len + 1]),
        @sizeOf(I),
    );

    const validity = try write_validity(.{
        .ctx = ctx,
        .offset = array.offset,
        .len = array.len,
        .null_count = array.null_count,
        .validity_opt = array.validity,
    });

    return .{
        .data = data,
        .offsets = offsets,
        .len = array.len,
        .validity = validity,
    };
}

fn write_validity(params: struct {
    ctx: Context,
    offset: u32,
    len: u32,
    null_count: u32,
    validity_opt: ?[]const u8,
}) Error!?header.Buffer {
    if (params.null_count == 0) {
        return null;
    }

    const validity = params.validity_opt orelse return null;

    const v = try maybe_align_bitmap(validity, params.offset, params.len, params.ctx.scratch_alloc);

    return try write_buffer(params.ctx, .lz4, v, @sizeOf(u8));
}

/// Similar to write_buffer but takes a list of offsets to break up the logical rows in the buffer to pages.
/// Intended to be used for writing data section of binary arrays
fn write_buffer_with_offsets(
    comptime I: type,
    ctx: Context,
    compr: Compression,
    buffer: []const u8,
    offsets: []const I,
) Error!header.Buffer {
    if (offsets.len <= 1) {
        return empty_buffer();
    }

    const max_p_size: u32 = ctx.page_size;
    const max_page_size: I = @intCast(max_p_size);

    std.debug.assert(max_page_size > 0);

    const pages_builder = try ctx.scratch_alloc.alloc(header.Page, offsets.len);
    const row_index_ends_builder = try ctx.scratch_alloc.alloc(u32, offsets.len);
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

            const page_offset = ctx.data_section_size.*;
            const compressed_size = try write_page(ctx, compr, page);

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

    const pages = try ctx.header_alloc.alloc(header.Page, num_pages);
    const row_index_ends = try ctx.header_alloc.alloc(u32, num_pages);
    @memcpy(pages, pages_builder[0..num_pages]);
    @memcpy(row_index_ends, row_index_ends_builder[0..num_pages]);

    return header.Buffer{
        .compression = compr,
        .pages = pages,
        .row_index_ends = row_index_ends,
    };
}

fn write_buffer(ctx: Context, compr: Compression, buffer: []const u8, elem_size: u8) Error!header.Buffer {
    std.debug.assert(buffer.len % elem_size == 0);
    const buffer_len = buffer.len / elem_size;

    if (buffer_len == 0) {
        return empty_buffer();
    }
    const max_page_len: u32 = (ctx.page_size + elem_size - 1) / elem_size;
    std.debug.assert(max_page_len > 0);

    const num_pages = (buffer_len + max_page_len - 1) / max_page_len;
    const pages = try ctx.header_alloc.alloc(header.Page, num_pages);
    const row_index_ends = try ctx.header_alloc.alloc(u32, num_pages);

    var buffer_offset: u32 = 0;
    var page_idx: usize = 0;
    while (buffer_offset < buffer_len) {
        const page_len = @min(max_page_len, buffer_len - buffer_offset);
        const page: []const u8 = buffer[elem_size * buffer_offset .. elem_size * (buffer_offset + page_len)];

        const page_offset = ctx.data_section_size.*;
        const compressed_size = try write_page(ctx, compr, page);

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
        .compression = compr,
        .pages = pages,
        .row_index_ends = row_index_ends,
    };
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

fn write_page(ctx: Context, compr: Compression, page: []const u8) Error!usize {
    const ds_size = ctx.data_section_size.*;
    const compr_bound = compression.compress_bound(page.len);
    if (ctx.data_section.len < ds_size + compr_bound) {
        return Error.DataSectionOverflow;
    }
    const compress_dst = ctx.data_section[ds_size .. ds_size + compr_bound];
    const compressed_size = ctx.compressor.compress(page, compress_dst, compr) catch unreachable;
    ctx.data_section_size.* = ds_size + @as(u32, @intCast(compressed_size));

    return compressed_size;
}

// Aligns the given bitmap by allocating new memory using alloc and copying over the bits
//  if bitmap isn't already aligned (offset is multiple of 8)
fn maybe_align_bitmap(bitmap: []const u8, offset: u32, len: u32, alloc: Allocator) Error![]const u8 {
    std.debug.assert(bitmap.len * 8 >= offset + len);

    if (offset % 8 == 0) {
        return bitmap[(offset / 8)..(offset / 8 + (len + 7) / 8)];
    }

    const x = try alloc.alloc(u8, (len + 7) / 8);
    arrow.bitmap.copy(len, x, 0, bitmap, offset);

    return x;
}
