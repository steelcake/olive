const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;
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
};

pub const Read = struct {
    header: *const header.Header,
    schema: *const schema.DatasetSchema,
    alloc: Allocator,
    scratch_alloc: Allocator,
    data_section: []const u8,
};

pub fn read(params: Read) Error!chunk.Chunk {
    const tables = try params.alloc.alloc(params.schema.tables.len);
    const dicts = try params.alloc.alloc(params.schema.dicts.len);

    for (params.schema.tables, params.header.tables, 0..) |table_schema, table_header, table_idx| {
        const fields = try params.alloc.alloc(table_header.fields.len);

        for (table_schema.data_types, table_header.fields, 0..) |field_type, field_header, field_idx| {
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

fn read_array(params: Read, field_type: DataType, field_header: *const header.Array) Error!arr.Array {
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
        .interval_year_month => .{ .interval_month_day_nano = try read_interval(.month_day_nano, params, &field_header.interval_month_day_nano) },
        .list => .{ .list = try read_list(.i32, params, &field_header.list) },
    };
}

fn read_list(comptime index_t: arr.IndexType, params: Read, inner_type: DataType, field_header: *const header.ListArray) Error!arr.GenericListArray(index_t) {
    const I = index_t.to_type();
    const len = field_header.len;
    const inner = try read(params, inner_type, field_header.inner);
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

fn read_binary(comptime index_t: arr.IndexType, params: Read, field_header: *const header.BinaryArray) Error!arr.BinaryArray(index_t) {
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

    var out_offset: u32 = 0;
    for (buffer.pages) |page| {
        if (params.data_section.len < page.offset + page.compressed_size) {
            return Error.DataSectionTooSmall;
        }
        try compression.decompress(params.data_section[page.offset .. page.offset + page.compressed_size], out[out_offset .. out_offset + page.uncompressed_size], buffer.compression);
        out_offset += page.uncompressed_size;
    }

    return out;
}

fn read_validity(params: Read, validity: ?header.Buffer) Error!?[]const u8 {
    const v = if (validity) |x| x else return null;
    return try read_buffer(u8, params, v);
}
