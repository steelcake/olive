const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;
const StructType = arrow.data_type.StructType;
const UnionType = arrow.data_type.UnionType;
const FixedSizeListType = arrow.data_type.FixedSizeListType;
const RunEndEncodedType = arrow.data_type.RunEndEncodedType;
const MapType = arrow.data_type.MapType;
const DictType = arrow.data_type.DictType;
const std = @import("std");
const Allocator = std.mem.Allocator;

const header = @import("./header.zig");
const schema = @import("./schema.zig");
const compression = @import("./compression.zig");
const chunk = @import("./chunk.zig");
const dict_impl = @import("./dict.zig");

const Compression = compression.Compression;
const Decompressor = compression.Decompressor;

pub const Error = error{
    OutOfMemory,
    DataSectionTooSmall,
    DecompressFail,
    UnexpectedArrayType,
    LengthMismatch,
    BufferTooBig,
    ValidationError,
    InvalidBufferLen,
};

const Context = struct {
    alloc: Allocator,
    scratch_alloc: Allocator,
    data_section: []const u8,
    decompressor: *Decompressor,
    dict_ctx: dict_impl.DictContext,
};

pub fn read(params: struct {
    header: *const header.Header,
    schema: *const schema.Schema,
    alloc: Allocator,
    scratch_alloc: Allocator,
    data_section: []const u8,
}) Error!chunk.Chunk {
    var decompressor = try Decompressor.init(params.scratch_alloc);
    defer decompressor.deinit(params.scratch_alloc);

    if (params.header.data_section_size != params.data_section.len) {
        return Error.ValidationError;
    }

    const ctx = Context{
        .alloc = params.alloc,
        .scratch_alloc = params.scratch_alloc,
        .data_section = params.data_section,
        .decompressor = &decompressor,
        .dict_ctx = dict_impl.DictContext{
            .dict20 = try read_dict_data(
                20,
                params.data_section,
                params.header.dict_ctx.dict20,
                params.alloc,
            ),
            .dict32 = try read_dict_data(
                32,
                params.data_section,
                params.header.dict_ctx.dict32,
                params.alloc,
            ),
        },
    };

    if (params.schema.table_schemas.len != params.header.tables.len) {
        return Error.ValidationError;
    }
    const tables = try params.alloc.alloc([]const arr.Array, params.schema.table_schemas.len);

    for (params.schema.table_schemas, params.header.tables, 0..) |table_schema, table_header, table_idx| {
        if (table_schema.field_types.len != table_header.fields.len) {
            return Error.ValidationError;
        }

        const fields = try params.alloc.alloc(arr.Array, table_header.fields.len);

        for (table_schema.field_types, table_header.fields, 0..) |field_type, *field_header, field_idx| {
            fields[field_idx] = try read_array(ctx, field_type, field_header);
        }

        tables[table_idx] = fields;
    }

    return .{
        .tables = tables,
        .dict_ctx = ctx.dict_ctx,
        .schema = params.schema,
    };
}

fn read_dict_data(
    comptime W: comptime_int,
    data_section: []const u8,
    dict_header: header.Dict,
    alloc: Allocator,
) Error![]const [W]u8 {
    if (dict_header.size % W != 0) {
        return Error.ValidationError;
    }

    const end, const overflow_bit = @addWithOverflow(dict_header.offset, dict_header.size);
    if (overflow_bit > 0) {
        return Error.ValidationError;
    }

    if (end > data_section.len) {
        return Error.DataSectionTooSmall;
    }

    const out = try alloc.alloc(u8, dict_header.size);
    @memcpy(out, data_section[dict_header.offset..end]);

    return @ptrCast(out);
}

fn check_field_type(field_type: DataType, field_header: *const header.Array) Error!void {
    const expected: header.ArrayTag = switch (field_type) {
        .null => .null,
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .f16,
        .f32,
        .f64,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .date32,
        .date64,
        .time32,
        .time64,
        .timestamp,
        .interval_year_month,
        .interval_day_time,
        .interval_month_day_nano,
        .duration,
        => .primitive,
        .binary => .binary,
        .utf8 => .binary,
        .bool => .bool,
        .list => .list,
        .struct_ => .struct_,
        .dense_union => .dense_union,
        .sparse_union => .sparse_union,
        .fixed_size_binary => |bw| switch (bw) {
            20, 32 => .primitive,
            else => .fixed_size_binary,
        },
        .fixed_size_list => .fixed_size_list,
        .map => .map,
        .large_binary => .binary,
        .large_utf8 => .binary,
        .large_list => .list,
        .run_end_encoded => .run_end_encoded,
        .binary_view => .binary,
        .utf8_view => .binary,
        .list_view => .list,
        .large_list_view => .list,
        .dict => .dict,
    };

    if (@intFromEnum(expected) != @intFromEnum(field_header.*)) {
        return Error.UnexpectedArrayType;
    }
}

fn read_array(ctx: Context, field_type: DataType, field_header: *const header.Array) Error!arr.Array {
    try check_field_type(field_type, field_header);
    const array: arr.Array = switch (field_type) {
        .null => .{ .null = .{ .len = field_header.null.len } },
        .i8 => .{ .i8 = try read_primitive(i8, ctx, &field_header.primitive) },
        .i16 => .{ .i16 = try read_primitive(i16, ctx, &field_header.primitive) },
        .i32 => .{ .i32 = try read_primitive(i32, ctx, &field_header.primitive) },
        .i64 => .{ .i64 = try read_primitive(i64, ctx, &field_header.primitive) },
        .u8 => .{ .u8 = try read_primitive(u8, ctx, &field_header.primitive) },
        .u16 => .{ .u16 = try read_primitive(u16, ctx, &field_header.primitive) },
        .u32 => .{ .u32 = try read_primitive(u32, ctx, &field_header.primitive) },
        .u64 => .{ .u64 = try read_primitive(u64, ctx, &field_header.primitive) },
        .f16 => .{ .f16 = try read_primitive(f16, ctx, &field_header.primitive) },
        .f32 => .{ .f32 = try read_primitive(f32, ctx, &field_header.primitive) },
        .f64 => .{ .f64 = try read_primitive(f64, ctx, &field_header.primitive) },
        .binary => .{ .binary = try read_binary(.i32, ctx, &field_header.binary) },
        .utf8 => .{ .utf8 = .{ .inner = try read_binary(.i32, ctx, &field_header.binary) } },
        .bool => .{ .bool = try read_bool(ctx, &field_header.bool) },
        .decimal32 => |dec_params| .{ .decimal32 = .{
            .params = dec_params,
            .inner = try read_primitive(i32, ctx, &field_header.primitive),
        } },
        .decimal64 => |dec_params| .{ .decimal64 = .{
            .params = dec_params,
            .inner = try read_primitive(i64, ctx, &field_header.primitive),
        } },
        .decimal128 => |dec_params| .{ .decimal128 = .{
            .params = dec_params,
            .inner = try read_primitive(i128, ctx, &field_header.primitive),
        } },
        .decimal256 => |dec_params| .{ .decimal256 = .{
            .params = dec_params,
            .inner = try read_primitive(i256, ctx, &field_header.primitive),
        } },
        .date32 => .{ .date32 = .{ .inner = try read_primitive(i32, ctx, &field_header.primitive) } },
        .date64 => .{ .date64 = .{ .inner = try read_primitive(i64, ctx, &field_header.primitive) } },
        .time32 => |unit| .{ .time32 = .{
            .unit = unit,
            .inner = try read_primitive(i32, ctx, &field_header.primitive),
        } },
        .time64 => |unit| .{ .time64 = .{
            .unit = unit,
            .inner = try read_primitive(i64, ctx, &field_header.primitive),
        } },
        .timestamp => |ts| .{ .timestamp = .{
            .ts = ts,
            .inner = try read_primitive(i64, ctx, &field_header.primitive),
        } },
        .interval_year_month => .{
            .interval_year_month = try read_interval(.year_month, ctx, &field_header.primitive),
        },
        .interval_day_time => .{
            .interval_day_time = try read_interval(.day_time, ctx, &field_header.primitive),
        },
        .interval_month_day_nano => .{
            .interval_month_day_nano = try read_interval(.month_day_nano, ctx, &field_header.primitive),
        },
        .list => |inner_t| .{
            .list = try read_list(.i32, ctx, inner_t.*, &field_header.list),
        },
        .struct_ => |struct_t| .{
            .struct_ = try read_struct(ctx, struct_t.*, &field_header.struct_),
        },
        .dense_union => |union_t| .{
            .dense_union = try read_dense_union(ctx, union_t.*, &field_header.dense_union),
        },
        .sparse_union => |union_t| .{
            .sparse_union = try read_sparse_union(ctx, union_t.*, &field_header.sparse_union),
        },
        .fixed_size_binary => |byte_width| make_array: {
            break :make_array switch (byte_width) {
                20 => .{
                    .u32 = try read_dict_indices(ctx, @intCast(ctx.dict_ctx.dict20.len), &field_header.primitive),
                },
                32 => .{
                    .u32 = try read_dict_indices(ctx, @intCast(ctx.dict_ctx.dict32.len), &field_header.primitive),
                },
                else => .{
                    .fixed_size_binary = try read_fixed_size_binary(ctx, byte_width, &field_header.fixed_size_binary),
                },
            };
        },
        .fixed_size_list => |fsl_t| .{
            .fixed_size_list = try read_fixed_size_list(ctx, fsl_t.*, &field_header.fixed_size_list),
        },
        .map => |map_t| .{ .map = try read_map(ctx, map_t.*, &field_header.map) },
        .duration => |unit| .{ .duration = .{
            .unit = unit,
            .inner = try read_primitive(i64, ctx, &field_header.primitive),
        } },
        .large_binary => .{ .large_binary = try read_binary(.i64, ctx, &field_header.binary) },
        .large_utf8 => .{ .large_utf8 = .{ .inner = try read_binary(.i64, ctx, &field_header.binary) } },
        .large_list => |inner_t| .{
            .large_list = try read_list(.i64, ctx, inner_t.*, &field_header.list),
        },
        .run_end_encoded => |ree_t| .{
            .run_end_encoded = try read_run_end_encoded(ctx, ree_t.*, &field_header.run_end_encoded),
        },
        .binary_view => .{ .binary_view = try read_binary_view(ctx, &field_header.binary) },
        .utf8_view => .{ .utf8_view = .{
            .inner = try read_binary_view(ctx, &field_header.binary),
        } },
        .list_view => |inner_t| .{
            .list_view = try read_list_view(.i32, ctx, inner_t.*, &field_header.list),
        },
        .large_list_view => |inner_t| .{
            .large_list_view = try read_list_view(.i64, ctx, inner_t.*, &field_header.list),
        },
        .dict => |dict_t| .{ .dict = try read_dict(ctx, dict_t.*, &field_header.dict) },
    };

    arrow.validate.validate_array(&array) catch {
        return Error.ValidationError;
    };

    return array;
}

fn read_dict(ctx: Context, dict_t: DictType, field_header: *const header.DictArray) Error!arr.DictArray {
    const values = try ctx.alloc.create(arr.Array);
    values.* = try read_array(ctx, dict_t.value, field_header.values);

    const keys = try ctx.alloc.create(arr.Array);
    keys.* = try read_array(ctx, dict_t.key.to_data_type(), field_header.keys);

    return arr.DictArray{
        .len = field_header.len,
        .offset = 0,
        .is_ordered = field_header.is_ordered,
        .values = values,
        .keys = keys,
    };
}

fn read_list_view(
    comptime index_t: arr.IndexType,
    ctx: Context,
    inner_t: DataType,
    field_header: *const header.ListArray,
) Error!arr.GenericListViewArray(index_t) {
    const array = try read_list(index_t, ctx, inner_t, field_header);

    // validate to avoid unsafety when handling the array
    arrow.validate.validate_list_array(index_t, &array) catch {
        return Error.ValidationError;
    };

    const I = index_t.to_type();

    const offsets = try ctx.alloc.alloc(I, array.len);
    const sizes = try ctx.alloc.alloc(I, array.len);

    var idx = array.offset;
    var start = array.offsets[array.offset];
    while (idx < array.offset + array.len) : (idx += 1) {
        const end = array.offsets[idx + 1];
        offsets[idx - array.offset] = start;
        sizes[idx - array.offset] = end - start;
        start = end;
    }

    const validity = if (array.null_count > 0)
        try maybe_align_bitmap(
            array.validity orelse unreachable,
            array.offset,
            array.len,
            ctx.alloc,
        )
    else
        null;

    return arr.GenericListViewArray(index_t){
        .validity = validity,
        .offset = 0,
        .len = array.len,
        .inner = array.inner,
        .offsets = offsets,
        .sizes = sizes,
        .null_count = array.null_count,
    };
}

fn read_binary_view(ctx: Context, field_header: *const header.BinaryArray) Error!arr.BinaryViewArray {
    const array = try read_binary(.i64, ctx, field_header);

    // validate to avoid unsafety when handling the array
    arrow.validate.validate_binary_array(.i64, &array) catch {
        return Error.ValidationError;
    };

    if (array.data.len > std.math.maxInt(i32)) {
        return Error.BufferTooBig;
    }

    const views = try ctx.alloc.alloc(arr.BinaryView, array.len);

    var idx = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        const start = array.offsets[idx];
        const end = array.offsets[idx + 1];
        const val = array.data[@intCast(start)..@intCast(end)];
        if (val.len <= 12) {
            var data: [12]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
            for (0..val.len) |i| {
                data[i] = val[i];
            }
            const datas: [3]i32 = @bitCast(data);
            views[idx - array.offset] = arr.BinaryView{
                .length = @intCast(end - start),
                .prefix = datas[0],
                .buffer_idx = datas[1],
                .offset = datas[2],
            };
        } else {
            const prefix: [4]u8 = .{ val.ptr[0], val.ptr[1], val.ptr[2], val.ptr[3] };
            views[idx - array.offset] = arr.BinaryView{
                .length = @intCast(end - start),
                .prefix = @bitCast(prefix),
                .buffer_idx = 0,
                .offset = @intCast(start),
            };
        }
    }

    const validity = if (array.null_count > 0)
        try maybe_align_bitmap(
            array.validity orelse unreachable,
            array.offset,
            array.len,
            ctx.alloc,
        )
    else
        null;

    const buffers = try ctx.alloc.alloc([]const u8, 1);
    buffers[0] = array.data;

    return arr.BinaryViewArray{
        .len = array.len,
        .offset = 0,
        .validity = validity,
        .views = views,
        .buffers = buffers,
        .null_count = array.null_count,
    };
}

fn read_run_end_encoded(
    ctx: Context,
    ree_t: RunEndEncodedType,
    field_header: *const header.RunEndArray,
) Error!arr.RunEndArray {
    const values = try ctx.alloc.create(arr.Array);
    values.* = try read_array(ctx, ree_t.value, field_header.values);

    const run_ends = try ctx.alloc.create(arr.Array);
    run_ends.* = try read_array(ctx, ree_t.run_end.to_data_type(), field_header.run_ends);

    return arr.RunEndArray{
        .offset = 0,
        .len = field_header.len,
        .values = values,
        .run_ends = run_ends,
    };
}

fn read_map(ctx: Context, map_type: MapType, field_header: *const header.MapArray) Error!arr.MapArray {
    const len = field_header.len;

    const offsets = try read_buffer(i32, ctx, field_header.offsets);

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    const entries = try ctx.alloc.create(arr.StructArray);

    const field_types = try ctx.alloc.alloc(DataType, 2);
    field_types[0] = map_type.key.to_data_type();
    field_types[1] = map_type.value;

    const entries_t = StructType{
        .field_names = &.{ "keys", "values" },
        .field_types = field_types,
    };
    entries.* = try read_struct(ctx, entries_t, field_header.entries);

    return arr.MapArray{
        .len = len,
        .offset = 0,
        .offsets = offsets,
        .validity = validity,
        .null_count = null_count,
        .entries = entries,
        .keys_are_sorted = field_header.keys_are_sorted,
    };
}

fn read_fixed_size_list(
    ctx: Context,
    fsl_type: FixedSizeListType,
    field_header: *const header.FixedSizeListArray,
) Error!arr.FixedSizeListArray {
    const len = field_header.len;

    const inner = try ctx.alloc.create(arr.Array);
    inner.* = try read_array(ctx, fsl_type.inner, field_header.inner);

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return arr.FixedSizeListArray{
        .len = len,
        .offset = 0,
        .validity = validity,
        .null_count = null_count,
        .item_width = fsl_type.item_width,
        .inner = inner,
    };
}

fn read_fixed_size_binary(
    ctx: Context,
    byte_width: i32,
    field_header: *const header.FixedSizeBinaryArray,
) Error!arr.FixedSizeBinaryArray {
    const len = field_header.len;

    const data = try read_buffer(u8, ctx, field_header.data);

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return arr.FixedSizeBinaryArray{
        .len = len,
        .offset = 0,
        .validity = validity,
        .null_count = null_count,
        .byte_width = byte_width,
        .data = data,
    };
}

fn read_dense_union(
    ctx: Context,
    union_type: UnionType,
    field_header: *const header.DenseUnionArray,
) Error!arr.DenseUnionArray {
    const len = field_header.inner.len;

    if (field_header.inner.children.len != union_type.field_types.len) {
        return Error.LengthMismatch;
    }

    const children = try ctx.alloc.alloc(arr.Array, field_header.inner.children.len);
    for (field_header.inner.children, union_type.field_types, 0..) |*child_header, child_type, child_idx| {
        children[child_idx] = try read_array(ctx, child_type, child_header);
    }

    const type_ids = try read_buffer(i8, ctx, field_header.inner.type_ids);
    const offsets = try read_buffer(i32, ctx, field_header.offsets);

    if (type_ids.len != len or offsets.len != len) {
        return Error.LengthMismatch;
    }

    return arr.DenseUnionArray{
        .inner = .{
            .offset = 0,
            .len = len,
            .field_names = union_type.field_names,
            .children = children,
            .type_ids = type_ids,
            .type_id_set = union_type.type_id_set,
        },
        .offsets = offsets,
    };
}

fn read_sparse_union(
    ctx: Context,
    union_type: UnionType,
    field_header: *const header.SparseUnionArray,
) Error!arr.SparseUnionArray {
    const len = field_header.inner.len;

    if (field_header.inner.children.len != union_type.field_types.len) {
        return Error.LengthMismatch;
    }

    const children = try ctx.alloc.alloc(arr.Array, field_header.inner.children.len);
    for (field_header.inner.children, union_type.field_types, 0..) |*child_header, child_type, child_idx| {
        children[child_idx] = try read_array(ctx, child_type, child_header);
    }

    const type_ids = try read_buffer(i8, ctx, field_header.inner.type_ids);
    if (type_ids.len != len) {
        return Error.LengthMismatch;
    }

    return arr.SparseUnionArray{
        .inner = .{
            .offset = 0,
            .len = len,
            .field_names = union_type.field_names,
            .children = children,
            .type_ids = type_ids,
            .type_id_set = union_type.type_id_set,
        },
    };
}

fn read_struct(
    ctx: Context,
    struct_type: StructType,
    field_header: *const header.StructArray,
) Error!arr.StructArray {
    const len = field_header.len;

    if (struct_type.field_types.len != field_header.field_values.len) {
        return Error.ValidationError;
    }

    const field_values = try ctx.alloc.alloc(arr.Array, struct_type.field_types.len);
    for (struct_type.field_types, field_header.field_values, 0..) |field_type, *field_h, field_idx| {
        field_values[field_idx] = try read_array(ctx, field_type, field_h);
    }

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return arr.StructArray{
        .field_names = struct_type.field_names,
        .validity = validity,
        .null_count = null_count,
        .len = len,
        .offset = 0,
        .field_values = field_values,
    };
}

fn read_list(
    comptime index_t: arr.IndexType,
    ctx: Context,
    inner_type: DataType,
    field_header: *const header.ListArray,
) Error!arr.GenericListArray(index_t) {
    const I = index_t.to_type();

    const len = field_header.len;

    const inner = try ctx.alloc.create(arr.Array);
    inner.* = try read_array(ctx, inner_type, field_header.inner);
    const offsets = try read_buffer(I, ctx, field_header.offsets);

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return .{
        .null_count = null_count,
        .validity = validity,
        .inner = inner,
        .offsets = offsets,
        .len = len,
        .offset = 0,
    };
}

fn read_interval(
    comptime interval_t: arr.IntervalType,
    ctx: Context,
    field_header: *const header.PrimitiveArray,
) Error!arr.IntervalArray(interval_t) {
    const T = interval_t.to_type();

    const len = field_header.len;

    const values = try read_buffer(T, ctx, field_header.values);
    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    const inner: arr.PrimitiveArray(T) = .{
        .null_count = null_count,
        .validity = validity,
        .values = values,
        .len = len,
        .offset = 0,
    };

    return .{ .inner = inner };
}

fn read_bool(ctx: Context, field_header: *const header.BoolArray) Error!arr.BoolArray {
    const len = field_header.len;

    const values = try read_buffer(u8, ctx, field_header.values);
    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return .{
        .null_count = null_count,
        .validity = validity,
        .values = values,
        .len = len,
        .offset = 0,
    };
}

fn read_binary(
    comptime index_t: arr.IndexType,
    ctx: Context,
    field_header: *const header.BinaryArray,
) Error!arr.GenericBinaryArray(index_t) {
    const I = index_t.to_type();

    const len = field_header.len;

    if (len > std.math.maxInt(u32) / 2) {
        return Error.InvalidBufferLen;
    }

    const data = try read_buffer(u8, ctx, field_header.data);
    const offsets = try read_buffer(I, ctx, field_header.offsets);

    if (offsets.len != len + 1) {
        return Error.InvalidBufferLen;
    }

    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return .{
        .null_count = null_count,
        .validity = validity,
        .data = data,
        .offsets = offsets,
        .len = len,
        .offset = 0,
    };
}

fn read_dict_indices(
    ctx: Context,
    dict_len: u32,
    field_header: *const header.PrimitiveArray,
) Error!arr.UInt32Array {
    const array = try read_primitive(u32, ctx, field_header);

    if (dict_len == 0) {
        if (array.len > array.null_count) {
            return Error.ValidationError;
        }
    } else {
        const end, const overflow_bit = @addWithOverflow(array.offset, array.len);

        if (overflow_bit > 0) {
            return Error.ValidationError;
        }

        const vals = array.values[array.offset..end];
        if (vals.len > 0) {
            const max_idx = std.mem.max(u32, vals);
            if (max_idx >= dict_len) {
                return Error.ValidationError;
            }
        }
    }

    return array;
}

fn read_primitive(
    comptime T: type,
    ctx: Context,
    field_header: *const header.PrimitiveArray,
) Error!arr.PrimitiveArray(T) {
    const len = field_header.len;

    const values = try read_buffer(T, ctx, field_header.values);
    const validity = try read_validity(ctx, field_header.validity, len);

    const null_count = if (validity) |v|
        arrow.bitmap.count_unset_bits(v, 0, len)
    else
        0;

    return .{
        .null_count = null_count,
        .validity = validity,
        .values = values,
        .len = len,
        .offset = 0,
    };
}

fn read_validity(ctx: Context, validity: ?header.Buffer, expected_len: u32) Error!?[]const u8 {
    if (expected_len > std.math.maxInt(u32) / 2) {
        return Error.InvalidBufferLen;
    }

    const v = if (validity) |x| x else return null;
    const buf = try read_buffer(u8, ctx, v);
    const expected_num_bytes = (expected_len + 7) / 8;
    if (buf.len != expected_num_bytes) {
        return Error.InvalidBufferLen;
    }

    return buf;
}

fn read_buffer(comptime T: type, ctx: Context, buffer: header.Buffer) Error![]const T {
    var total_size: u64 = 0;
    for (buffer.pages) |page| {
        total_size += page.uncompressed_size;
    }

    if (total_size > std.math.maxInt(u32) / 4) {
        return Error.InvalidBufferLen;
    }
    if (total_size % @sizeOf(T) != 0) {
        return Error.InvalidBufferLen;
    }

    const len = total_size / @sizeOf(T);

    const out = try ctx.alloc.alloc(T, len);
    const out_raw: []u8 = @ptrCast(out);

    var out_offset: u32 = 0;
    for (buffer.pages) |page| {
        // check for possible overflow
        const page_end: u64 = @as(u64, page.offset) + @as(u64, page.compressed_size);
        if (page_end > std.math.maxInt(u32)) {
            return Error.InvalidBufferLen;
        }
        if (@as(u64, out_offset) + @as(u64, page.uncompressed_size) > std.math.maxInt(u32)) {
            return Error.InvalidBufferLen;
        }

        if (ctx.data_section.len < page_end) {
            return Error.DataSectionTooSmall;
        }

        if (buffer.compression == .no_compression and page.compressed_size != page.uncompressed_size) {
            return Error.InvalidBufferLen;
        }

        try ctx.decompressor.decompress(
            ctx.data_section[page.offset .. page.offset + page.compressed_size],
            out_raw[out_offset .. out_offset + page.uncompressed_size],
            buffer.compression,
        );
        out_offset += page.uncompressed_size;
    }

    return out;
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
