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
const Compression = compression.Compression;
const chunk = @import("./chunk.zig");

const Error = error{
    OutOfMemory,
    DataSectionTooSmall,
    DecompressFail,
    UnexpectedArrayType,
    LengthMismatch,
    BufferTooBig,
    ValidationError,
};

pub const Read = struct {
    header: *const header.Header,
    schema: *const schema.DatasetSchema,
    alloc: Allocator,
    scratch_alloc: Allocator,
    data_section: []const u8,
};

pub fn read(params: Read) Error!chunk.Chunk {
    const tables = try params.alloc.alloc(chunk.Table, params.schema.tables.len);
    const dicts = try params.alloc.alloc(arr.FixedSizeBinaryArray, params.schema.dicts.len);

    for (params.schema.tables, params.header.tables, 0..) |table_schema, table_header, table_idx| {
        const fields = try params.alloc.alloc(arr.Array, table_header.fields.len);

        for (table_schema.data_types, table_header.fields, 0..) |field_type, *field_header, field_idx| {
            fields[field_idx] = try read_array(params, field_type, field_header);
        }

        tables[table_idx] = .{
            .fields = fields,
            .num_rows = table_header.num_rows,
        };
    }

    return .{
        .tables = tables,
        .dicts = dicts,
        .schema = params.schema,
    };
}

fn check_field_type(field_type: DataType, field_header: *const header.Array) Error!void {
    const expected: @typeInfo(header.Array).@"union".tag_type.? = switch (field_type) {
        .null => .null,
        .i8 => .i8,
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
        .u8 => .u8,
        .u16 => .u16,
        .u32 => .u32,
        .u64 => .u64,
        .f16 => .f16,
        .f32 => .f32,
        .f64 => .f64,
        .binary => .binary,
        .utf8 => .binary,
        .bool => .bool,
        .decimal32 => .i32,
        .decimal64 => .i64,
        .decimal128 => .i128,
        .decimal256 => .i256,
        .date32 => .i32,
        .date64 => .i64,
        .time32 => .i32,
        .time64 => .i64,
        .timestamp => .i64,
        .interval_year_month => .interval_year_month,
        .interval_day_time => .interval_day_time,
        .interval_month_day_nano => .interval_month_day_nano,
        .list => .list,
        .struct_ => .struct_,
        .dense_union => .dense_union,
        .sparse_union => .sparse_union,
        .fixed_size_binary => .fixed_size_binary,
        .fixed_size_list => .fixed_size_list,
        .map => .map,
        .duration => .i64,
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

    if (expected != field_header.*) {
        return Error.UnexpectedArrayType;
    }
}

fn read_array(params: Read, field_type: DataType, field_header: *const header.Array) Error!arr.Array {
    try check_field_type(field_type, field_header);
    return switch (field_type) {
        .null => .{ .null = .{ .len = field_header.null.len } },
        .i8 => .{ .i8 = try read_primitive(i8, params, &field_header.i8) },
        .i16 => .{ .i16 = try read_primitive(i16, params, &field_header.i16) },
        .i32 => .{ .i32 = try read_primitive(i32, params, &field_header.i32) },
        .i64 => .{ .i64 = try read_primitive(i64, params, &field_header.i64) },
        .u8 => .{ .u8 = try read_primitive(u8, params, &field_header.u8) },
        .u16 => .{ .u16 = try read_primitive(u16, params, &field_header.u16) },
        .u32 => .{ .u32 = try read_primitive(u32, params, &field_header.u32) },
        .u64 => .{ .u64 = try read_primitive(u64, params, &field_header.u64) },
        .f16 => .{ .f16 = try read_primitive(f16, params, &field_header.f16) },
        .f32 => .{ .f32 = try read_primitive(f32, params, &field_header.f32) },
        .f64 => .{ .f64 = try read_primitive(f64, params, &field_header.f64) },
        .binary => .{ .binary = try read_binary(.i32, params, &field_header.binary) },
        .utf8 => .{ .utf8 = .{ .inner = try read_binary(.i32, params, &field_header.binary) } },
        .bool => .{ .bool = try read_bool(params, &field_header.bool) },
        .decimal32 => |dec_params| .{ .decimal32 = .{ .params = dec_params, .inner = try read_primitive(i32, params, &field_header.i32) } },
        .decimal64 => |dec_params| .{ .decimal64 = .{ .params = dec_params, .inner = try read_primitive(i64, params, &field_header.i64) } },
        .decimal128 => |dec_params| .{ .decimal128 = .{ .params = dec_params, .inner = try read_primitive(i128, params, &field_header.i128) } },
        .decimal256 => |dec_params| .{ .decimal256 = .{ .params = dec_params, .inner = try read_primitive(i256, params, &field_header.i256) } },
        .date32 => .{ .date32 = .{ .inner = try read_primitive(i32, params, &field_header.i32) } },
        .date64 => .{ .date64 = .{ .inner = try read_primitive(i64, params, &field_header.i64) } },
        .time32 => |unit| .{ .time32 = .{ .unit = unit, .inner = try read_primitive(i32, params, &field_header.i32) } },
        .time64 => |unit| .{ .time64 = .{ .unit = unit, .inner = try read_primitive(i64, params, &field_header.i64) } },
        .timestamp => |ts| .{ .timestamp = .{ .ts = ts, .inner = try read_primitive(i64, params, &field_header.i64) } },
        .interval_year_month => .{ .interval_year_month = try read_interval(.year_month, params, &field_header.interval_year_month) },
        .interval_day_time => .{ .interval_day_time = try read_interval(.day_time, params, &field_header.interval_day_time) },
        .interval_month_day_nano => .{ .interval_month_day_nano = try read_interval(.month_day_nano, params, &field_header.interval_month_day_nano) },
        .list => |inner_t| .{ .list = try read_list(.i32, params, inner_t.*, &field_header.list) },
        .struct_ => |struct_t| .{ .struct_ = try read_struct(params, struct_t.*, &field_header.struct_) },
        .dense_union => |union_t| .{ .dense_union = try read_dense_union(params, union_t.*, &field_header.dense_union) },
        .sparse_union => |union_t| .{ .sparse_union = try read_sparse_union(params, union_t.*, &field_header.sparse_union) },
        .fixed_size_binary => |byte_width| .{ .fixed_size_binary = try read_fixed_size_binary(params, byte_width, &field_header.fixed_size_binary) },
        .fixed_size_list => |fsl_t| .{ .fixed_size_list = try read_fixed_size_list(params, fsl_t.*, &field_header.fixed_size_list) },
        .map => |map_t| .{ .map = try read_map(params, map_t.*, &field_header.map) },
        .duration => |unit| .{ .duration = .{ .unit = unit, .inner = try read_primitive(i64, params, &field_header.i64) } },
        .large_binary => .{ .large_binary = try read_binary(.i64, params, &field_header.binary) },
        .large_utf8 => .{ .large_utf8 = .{ .inner = try read_binary(.i64, params, &field_header.binary) } },
        .large_list => |inner_t| .{ .large_list = try read_list(.i64, params, inner_t.*, &field_header.list) },
        .run_end_encoded => |ree_t| .{ .run_end_encoded = try read_run_end_encoded(params, ree_t.*, &field_header.run_end_encoded) },
        .binary_view => .{ .binary_view = try read_binary_view(params, &field_header.binary) },
        .utf8_view => .{ .utf8_view = .{ .inner = try read_binary_view(params, &field_header.binary) } },
        .list_view => |inner_t| .{ .list_view = try read_list_view(.i32, params, inner_t.*, &field_header.list) },
        .large_list_view => |inner_t| .{ .large_list_view = try read_list_view(.i64, params, inner_t.*, &field_header.list) },
        .dict => |dict_t| .{ .dict = try read_dict(params, dict_t.*, &field_header.dict) },
    };
}

fn read_dict(params: Read, dict_t: DictType, field_header: *const header.DictArray) Error!arr.DictArray {
    const values = try params.alloc.create(arr.Array);
    values.* = try read_array(params, dict_t.value, field_header.values);

    const keys = try params.alloc.create(arr.Array);
    keys.* = try read_array(params, dict_t.key.to_data_type(), field_header.values);

    return arr.DictArray{
        .len = field_header.len,
        .offset = 0,
        .is_ordered = field_header.is_ordered,
        .values = values,
        .keys = keys,
    };
}

fn read_list_view(comptime index_t: arr.IndexType, params: Read, inner_t: DataType, field_header: *const header.ListArray) Error!arr.GenericListViewArray(index_t) {
    const array = try read_list(index_t, params, inner_t, field_header);

    // validate to avoid unsafety when handling the array
    arrow.validate.validate_list(index_t, &array) catch {
        return Error.ValidationError;
    };

    var builder = arrow.builder.GenericListViewBuilder(index_t).with_capacity(array.len, array.null_count > 0, params.scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.bitmap.get(validity, idx)) {
                const start = array.offsets.ptr[idx];
                const end = array.offsets.ptr[idx + 1];
                const len = end - start;
                builder.append_item(start, len) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const start = array.offsets.ptr[idx];
            const end = array.offsets.ptr[idx + 1];
            const len = end - start;
            builder.append_item(start, len) catch unreachable;
        }
    }

    return builder.finish(array.inner) catch unreachable;
}

fn read_binary_view(params: Read, field_header: *const header.BinaryArray) Error!arr.BinaryViewArray {
    const array = try read_binary(.i64, Read{
        .schema = params.schema,
        .alloc = params.scratch_alloc,
        .scratch_alloc = params.scratch_alloc,
        .data_section = params.data_section,
        .header = params.header,
    }, field_header);

    // validate to avoid unsafety when handling the array
    arrow.validate.validate_binary(.i64, &array) catch {
        return Error.ValidationError;
    };

    if (array.data.len > std.math.maxInt(u32)) {
        return Error.BufferTooBig;
    }

    var buffer_len: u64 = 0;
    var idx: u32 = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        const start = array.offsets.ptr[idx];
        const end = array.offsets.ptr[idx + 1];
        const len: u64 = @as(u64, @bitCast(end -% start));

        if (len > 12) {
            buffer_len +%= len;
        }
    }

    var builder = arrow.builder.BinaryViewBuilder.with_capacity(@intCast(buffer_len), array.len, array.null_count > 0, params.alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_option(arrow.get.get_binary_opt(.i64, array.data.ptr, array.offsets.ptr, validity, idx)) catch unreachable;
        }
    } else {
        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_value(arrow.get.get_binary(.i64, array.data.ptr, array.offsets.ptr, idx)) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn read_run_end_encoded(params: Read, ree_t: RunEndEncodedType, field_header: *const header.RunEndArray) Error!arr.RunEndArray {
    const values = try params.alloc.create(arr.Array);
    values.* = try read_array(params, ree_t.value, field_header.values);

    const run_ends = try params.alloc.create(arr.Array);
    run_ends.* = try read_array(params, ree_t.run_end.to_data_type(), field_header.run_ends);

    return arr.RunEndArray{
        .offset = 0,
        .len = field_header.len,
        .values = values,
        .run_ends = run_ends,
    };
}

fn read_map(params: Read, map_type: MapType, field_header: *const header.MapArray) Error!arr.MapArray {
    const len = field_header.len;

    const offsets = try read_buffer(i32, params, field_header.offsets);

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
    else
        0;

    const entries = try params.alloc.create(arr.StructArray);

    const field_types = try params.alloc.alloc(DataType, 2);
    field_types[0] = map_type.key.to_data_type();
    field_types[1] = map_type.value;

    const entries_t = StructType{
        .field_names = &.{ "keys", "values" },
        .field_types = field_types,
    };
    entries.* = try read_struct(params, entries_t, field_header.entries);

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

fn read_fixed_size_list(params: Read, fsl_type: FixedSizeListType, field_header: *const header.FixedSizeListArray) Error!arr.FixedSizeListArray {
    const len = field_header.len;

    const inner = try params.alloc.create(arr.Array);
    inner.* = try read_array(params, fsl_type.inner, field_header.inner);

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_fixed_size_binary(params: Read, byte_width: i32, field_header: *const header.FixedSizeBinaryArray) Error!arr.FixedSizeBinaryArray {
    const len = field_header.len;

    const data = try read_buffer(u8, params, field_header.data);

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_dense_union(params: Read, union_type: UnionType, field_header: *const header.DenseUnionArray) Error!arr.DenseUnionArray {
    const len = field_header.inner.len;

    if (field_header.inner.children.len != union_type.field_types.len) {
        return Error.LengthMismatch;
    }

    const children = try params.alloc.alloc(arr.Array, field_header.inner.children.len);
    for (field_header.inner.children, union_type.field_types, 0..) |*child_header, child_type, child_idx| {
        children[child_idx] = try read_array(params, child_type, child_header);
    }

    const type_ids = try read_buffer(i8, params, field_header.inner.type_ids);
    const offsets = try read_buffer(i32, params, field_header.offsets);

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

fn read_sparse_union(params: Read, union_type: UnionType, field_header: *const header.SparseUnionArray) Error!arr.SparseUnionArray {
    const len = field_header.inner.len;

    if (field_header.inner.children.len != union_type.field_types.len) {
        return Error.LengthMismatch;
    }

    const children = try params.alloc.alloc(arr.Array, field_header.inner.children.len);
    for (field_header.inner.children, union_type.field_types, 0..) |*child_header, child_type, child_idx| {
        children[child_idx] = try read_array(params, child_type, child_header);
    }

    const type_ids = try read_buffer(i8, params, field_header.inner.type_ids);
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

fn read_struct(params: Read, struct_type: StructType, field_header: *const header.StructArray) Error!arr.StructArray {
    const len = field_header.len;

    const field_values = try params.alloc.alloc(arr.Array, struct_type.field_types.len);
    for (struct_type.field_types, field_header.field_values, 0..) |field_type, *field_h, field_idx| {
        field_values[field_idx] = try read_array(params, field_type, field_h);
    }

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_list(comptime index_t: arr.IndexType, params: Read, inner_type: DataType, field_header: *const header.ListArray) Error!arr.GenericListArray(index_t) {
    const I = index_t.to_type();

    const len = field_header.len;

    const inner = try params.alloc.create(arr.Array);
    inner.* = try read_array(params, inner_type, field_header.inner);
    const offsets = try read_buffer(I, params, field_header.offsets);

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_interval(comptime interval_t: arr.IntervalType, params: Read, field_header: *const header.IntervalArray) Error!arr.IntervalArray(interval_t) {
    const T = interval_t.to_type();

    const len = field_header.len;

    const values = try read_buffer(T, params, field_header.values);
    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_bool(params: Read, field_header: *const header.BoolArray) Error!arr.BoolArray {
    const len = field_header.len;

    const values = try read_buffer(u8, params, field_header.values);
    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_binary(comptime index_t: arr.IndexType, params: Read, field_header: *const header.BinaryArray) Error!arr.GenericBinaryArray(index_t) {
    const I = index_t.to_type();

    const len = field_header.len;

    const data = try read_buffer(u8, params, field_header.data);
    const offsets = try read_buffer(I, params, field_header.offsets);

    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_primitive(comptime T: type, params: Read, field_header: *const header.PrimitiveArray(T)) Error!arr.PrimitiveArray(T) {
    const len = field_header.len;

    const values = try read_buffer(T, params, field_header.values);
    const validity = try read_validity(params, field_header.validity);

    const null_count = if (validity) |v|
        arrow.bitmap.count_nulls(v, 0, len)
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

fn read_buffer(comptime T: type, params: Read, buffer: header.Buffer) Error![]const T {
    var total_size: u32 = 0;
    for (buffer.pages) |page| {
        total_size += page.uncompressed_size;
    }

    std.debug.assert(total_size % @sizeOf(T) == 0);
    const len = total_size / @sizeOf(T);

    const out = try params.alloc.alloc(T, len);
    const out_raw: []u8 = @ptrCast(out);

    var out_offset: u32 = 0;
    for (buffer.pages) |page| {
        if (params.data_section.len < page.offset + page.compressed_size) {
            return Error.DataSectionTooSmall;
        }
        try compression.decompress(params.data_section[page.offset .. page.offset + page.compressed_size], out_raw[out_offset .. out_offset + page.uncompressed_size], buffer.compression);
        out_offset += page.uncompressed_size;
    }

    return out;
}

fn read_validity(params: Read, validity: ?header.Buffer) Error!?[]const u8 {
    const v = if (validity) |x| x else return null;
    return try read_buffer(u8, params, v);
}
