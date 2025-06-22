const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const native_endian = @import("builtin").target.cpu.arch.endian();

const chunk = @import("./chunk.zig");
const header = @import("./header.zig");

pub const Error = error{
    UnsupportedTypeForMinMax,
    OutOfMemory,
};

/// Swap bytes of integer if target is big endian
fn maybe_byte_swap(val: anytype) @TypeOf(val) {
    return switch (native_endian) {
        .big => @byteSwap(val),
        .little => val,
    };
}

pub const CreateHeader = struct {
    chunk: *const chunk.Chunk,
    alloc: Allocator,
    scratch_alloc: Allocator,
    page_size_kb: ?u32 = null,
    dict_filters: bool = false,
};

/// Calculate `header.Array` for flat arrays with fixed size elements e.g. integers, fixed_sized_binary.
fn create_array_header_for_fixed_size(comptime bytes_per_element: comptime_int, has_validity: bool, array_len: u32, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const num_pages = if (page_size_kb) |ps| ps_calc: {
        std.debug.assert(ps != 0);

        const ps_bytes = ps * 1024;

        const n_pages = ((array_len * bytes_per_element) + ps_bytes - 1) / ps_bytes;

        break :ps_calc n_pages;
    } else 1;

    const pages = try alloc.alloc(header.Page, num_pages);
    const row_ranges = try alloc.alloc(header.RowRange, num_pages);

    for (0..num_pages) |page_index| {
        const page_size = (array_len * bytes_per_element + num_pages - 1) / num_pages;

        pages[page_index] = .{
            .offset = 0,
            .size = page_size,
            .compressed_size = null,
        };

        const elements_per_page = (array_len + num_pages - 1) / num_pages;

        const from_idx = page_index * elements_per_page;
        const to_idx = @min(array_len, from_idx + elements_per_page);

        row_ranges[page_index] = .{
            .from_idx = from_idx,
            .to_idx = to_idx,
            .min = .{},
            .max = .{},
        };
    }

    const buffers = try alloc.alloc(header.Buffer, 2);
    buffers[1] = .{
        .pages = pages,
        .row_ranges = row_ranges,
        .compression = .no_compression,
    };

    if (has_validity) {
        const validity_pages = try alloc.alloc(header.Page, 1);
        const validity_row_ranges = try alloc.alloc(header.RowRange, 1);

        buffers[0] = .{
            .pages = validity_pages,
            .row_ranges = validity_row_ranges,
            .compression = .no_compression,
        };
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }

    return .{
        .children = &.{},
        .buffers = buffers,
    };
}

/// Calculate `header.Array` based on given arrow array.
/// Splits into pages and calculates maximum size of pages.
fn create_array_header(array: *const arr.Array, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {}

pub fn create_header(params: CreateHeader) Error!header.Header {
    const tables = try params.alloc.alloc(header.Table, params.chunk.tables.len);

    std.debug.assert(params.chunk.tables.len == params.chunk.table_schemas.len);
    for (params.chunk.tables, params.chunk.table_schemas, 0..) |table, schema, table_index| {
        const fields = try params.alloc.alloc(header.Array, table.field_names.len);
        const dict_indices = try params.alloc.alloc(?u8, table.field_names.len);

        for (table.field_values, schema.dict_indices, 0..) |*array, dict_index, field_index| {
            dict_indices[field_index] = dict_index;

            if (dict_index) |_| {
                fields[field_index] = try create_array_header_for_fixed_size(@sizeOf(u32), arrow.length.length(array), params.alloc, params.page_size_kb);
            } else {
                fields[field_index] = try create_array_header(array, params.alloc, params.page_size_kb);
            }
        }

        tables[table_index] = .{
            .fields = fields,
            .dict_indices = dict_indices,
        };
    }

    return .{
        .dicts = &.{},
        .tables = tables,
    };
}

pub const Write = struct {
    chunk: *const chunk.Chunk,
    header: *header.Header,
    buffer: []u8,
    filter_alloc: Allocator,
};

pub fn write(_: Write) Error!void {}

/// Convert the element at index into a binary blob value allocated using the given alloc
fn array_elem_as_bytes_primitive(comptime T: type, array: *const arr.PrimitiveArray(T), index: u32, alloc: Allocator) Error![]u8 {
    std.debug.assert(arrow.length.length(array) > index);

    const out = try alloc.alloc(T, 1);
    if (arrow.get.get_primitive_opt(T, array.values.ptr, array.validity.ptr, index)) |elem| {
        out[0] = maybe_byte_swap(elem);
    } else {
        out[0] = 0;
    }

    return @ptrCast(out);
}

/// Allocate a new buffer and fill it with given data
fn alloc_in(data: []const u8, alloc: Allocator) Error![]u8 {
    const out = try alloc.alloc(u8, data.len);
    @memcpy(out, data);
    return out;
}

/// Convert the element at index into a binary blob value allocated using the given alloc
/// Intended to be used for generating min/max values
fn array_elem_as_bytes(array: *const arr.Array, index: u32, alloc: Allocator) Error![]u8 {
    std.debug.assert(arrow.length.length(array) > index);

    switch (array.*) {
        .u8 => |*a| return try array_elem_as_bytes_primitive(u8, a, index, alloc),
        .u16 => |*a| return try array_elem_as_bytes_primitive(u16, a, index, alloc),
        .u32 => |*a| return try array_elem_as_bytes_primitive(u32, a, index, alloc),
        .u64 => |*a| return try array_elem_as_bytes_primitive(u64, a, index, alloc),
        .i8 => |*a| return try array_elem_as_bytes_primitive(i8, a, index, alloc),
        .i16 => |*a| return try array_elem_as_bytes_primitive(i16, a, index, alloc),
        .i32 => |*a| return try array_elem_as_bytes_primitive(i32, a, index, alloc),
        .i64 => |*a| return try array_elem_as_bytes_primitive(i64, a, index, alloc),
        .f16 => |*a| return try array_elem_as_bytes_primitive(f16, a, index, alloc),
        .f32 => |*a| return try array_elem_as_bytes_primitive(f32, a, index, alloc),
        .f64 => |*a| return try array_elem_as_bytes_primitive(f64, a, index, alloc),
        .decimal32 => |*a| return try array_elem_as_bytes_primitive(i32, a, index, alloc),
        .decimal64 => |*a| return try array_elem_as_bytes_primitive(i64, a, index, alloc),
        .decimal128 => |*a| return try array_elem_as_bytes_primitive(i128, a, index, alloc),
        .decimal256 => |*a| return try array_elem_as_bytes_primitive(i256, a, index, alloc),
        .binary => |*a| {
            const data = arrow.get.get_binary_opt(.i32, &a.data, &a.offsets, &a.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .large_binary => |*a| {
            const data = arrow.get.get_binary_opt(.i64, &a.data, &a.offsets, &a.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .utf8 => |*a| {
            const data = arrow.get.get_binary_opt(.i32, &a.inner.data, &a.inner.offsets, &a.inner.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .large_utf8 => |*a| {
            const data = arrow.get.get_binary_opt(.i64, &a.inner.data, &a.inner.offsets, &a.inner.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .binary_view => |*a| {
            const data = arrow.get.get_binary_view_opt(&a.buffers, &a.views, &a.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .utf8_view => |*a| {
            const data = arrow.get.get_binary_view_opt(&a.inner.buffers, &a.inner.views, &a.inner.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        .fixed_size_binary => |*a| {
            const data = arrow.get.get_fixed_size_binary_opt(&a.data, a.byte_width, &a.validity, index) orelse &.{};
            return try alloc_in(data, alloc);
        },
        else => return Error.UnsupportedTypeForMinMax,
    }
}
