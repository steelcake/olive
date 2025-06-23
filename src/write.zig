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
    DictArrayNotSupported,
    RunEndEncodedArrayNotSupported,
    BinaryViewArrayNotSupported,
    ListViewArrayNotSupported,
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
};

/// Create buffer header for variable sized buffer e.g. values buffer of BinaryArray
fn create_var_buffer_header(comptime index_t: arr.IndexType, offsets: []const index_t.to_type(), array_len: u32, alloc: Allocator, page_size_kb: ?u32) Error!header.Buffer {
    std.debug.assert(offsets.len == array_len + 1);

    const num_pages = if (page_size_kb) |ps| ps_calc: {
        var num_pages: u32 = 0;
        var current_page_len: u32 = 0;
        for (0..array_len) |i| {
            const start = offsets[i];
            const end = offsets[i + 1];
            const len = end - start;
            current_page_len += len;

            if (current_page_len >= ps) {
                num_pages += 1;
                current_page_len = 0;
            }
        }

        if (current_page_len > 0) {
            num_pages += 1;
        }

        break :ps_calc num_pages;
    } else 1;

    const pages = try alloc.alloc(header.Page, num_pages);
    const row_ranges = try alloc.alloc(header.RowRange, num_pages);

    if (page_size_kb) |ps| {
        var current_page_len: u32 = 0;
        var current_page_from_idx: u32 = 0;
        var current_page_idx: u32 = 0;
        for (0..array_len) |i| {
            const start = offsets[i];
            const end = offsets[i + 1];
            const len = end - start;
            current_page_len += len;

            if (current_page_len >= ps) {
                pages[current_page_idx] = .{
                    .offset = 0,
                    .size = current_page_len,
                    .compressed_size = 0,
                };
                row_ranges[current_page_idx] = .{
                    .from_idx = current_page_from_idx,
                    .to_idx = current_page_idx + 1,
                    .min = &.{},
                    .max = &.{},
                };

                current_page_len = 0;
                current_page_from_idx = current_page_idx + 1;
                current_page_idx += 1;
            }
        }

        if (current_page_len > 0) {
            pages[current_page_idx] = .{
                .offset = 0,
                .size = current_page_len,
                .compressed_size = 0,
            };
            row_ranges[current_page_idx] = .{
                .from_idx = current_page_from_idx,
                .to_idx = current_page_idx + 1,
                .min = &.{},
                .max = &.{},
            };
        }
    } else {
        pages[0] = .{
            .offset = 0,
            .size = offsets[array_len],
            .compressed_size = 0,
        };
        row_ranges[0] = .{
            .from_idx = 0,
            .to_idx = array_len,
            .min = &.{},
            .max = &.{},
        };
    }

    return .{
        .pages = pages,
        .row_ranges = row_ranges,
        .compression = .no_compression,
    };
}

fn create_buffer_header(bytes_per_element: u32, buffer_len: u32, alloc: Allocator, page_size_kb: ?u32) Error!header.Buffer {
    const num_pages = if (page_size_kb) |ps| ps_calc: {
        std.debug.assert(ps != 0);

        const ps_bytes = ps * 1024;

        const n_pages = (bytes_per_element * buffer_len + ps_bytes - 1) / ps_bytes;

        break :ps_calc n_pages;
    } else 1;

    const pages = try alloc.alloc(header.Page, num_pages);
    const row_ranges = try alloc.alloc(header.RowRange, num_pages);

    for (0..num_pages) |page_index| {
        const page_size = (bytes_per_element * buffer_len + num_pages - 1) / num_pages;

        pages[page_index] = .{
            .offset = 0,
            .size = page_size,
            .compressed_size = 0,
        };

        const elements_per_page = (buffer_len + num_pages - 1) / num_pages;

        const from_idx = page_index * elements_per_page;
        const to_idx = @min(buffer_len, from_idx + elements_per_page);

        row_ranges[page_index] = .{
            .from_idx = from_idx,
            .to_idx = to_idx,
            .min = .{},
            .max = .{},
        };
    }

    return .{
        .pages = pages,
        .row_ranges = row_ranges,
        .compression = .no_compression,
    };
}

fn create_buffer_header_for_validity(array_len: u32, alloc: Allocator, page_size_kb: ?u32) Error!header.Buffer {
    return create_buffer_header(1, (array_len + 8 - 1) / 8, alloc, page_size_kb);
}

/// Calculate `header.Array` for flat arrays with fixed size elements e.g. integers, fixed_sized_binary.
fn create_array_header_for_fixed_size(bytes_per_element: u32, has_validity: bool, array_len: u32, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 2);
    if (has_validity) {
        buffers[0] = try create_buffer_header_for_validity(array_len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }
    buffers[1] = try create_buffer_header(bytes_per_element, array_len, alloc, page_size_kb);

    return .{
        .children = &.{},
        .buffers = buffers,
    };
}

fn create_binary_array_header(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t), alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 3);
    if (array.validity != null) {
        buffers[0] = try create_buffer_header_for_validity(array.len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }
    const bytes_per_element = switch (index_t) {
        .i32 => @sizeOf(i32),
        .i64 => @sizeOf(i64),
    };
    buffers[1] = try create_buffer_header(bytes_per_element, array.len, alloc, page_size_kb);
    buffers[2] = try create_var_buffer_header(index_t, array.offsets[array.offset .. array.offset + array.len + 1], array.len, alloc, page_size_kb);

    return .{
        .children = &.{},
        .buffers = buffers,
    };
}

fn create_list_array_header(comptime index_t: arr.IndexType, array: *const arr.GenericListArray(index_t), alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 2);
    if (array.validity != null) {
        buffers[0] = try create_buffer_header_for_validity(array.len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }
    const bytes_per_element = switch (index_t) {
        .i32 => @sizeOf(i32),
        .i64 => @sizeOf(i64),
    };
    buffers[1] = try create_buffer_header(bytes_per_element, array.len, alloc, page_size_kb);

    const children = try alloc.alloc(header.Array, 1);
    children[0] = try create_array_header(&array.inner, alloc, page_size_kb);

    return .{
        .children = children,
        .buffers = buffers,
    };
}

fn create_struct_array_header(array: *const arr.StructArray, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 1);
    if (array.validity != null) {
        buffers[0] = try create_buffer_header_for_validity(array.len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }

    const children = try alloc.alloc(header.Array, array.field_names.len);

    for (0..array.field_names.len) |i| {
        children[i] = try create_array_header(&array.field_values[i], alloc, page_size_kb);
    }

    return .{
        .children = children,
        .buffers = buffers,
    };
}

fn create_dense_union_array_header(array: *const arr.DenseUnionArray, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 2);
    // types buffer
    buffers[0] = try create_buffer_header(@sizeOf(i8), array.inner.len, alloc, page_size_kb);
    // offsets buffer
    buffers[1] = try create_buffer_header(@sizeOf(i32), array.inner.len, alloc, page_size_kb);

    const children = try alloc.alloc(header.Array, array.inner.children.len);

    for (0..array.inner.children.len) |i| {
        children[i] = try create_array_header(&array.inner.children[i], alloc, page_size_kb);
    }

    return .{
        .children = children,
        .buffers = buffers,
    };
}

fn create_sparse_union_array_header(array: *const arr.SparseUnionArray, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 1);
    // types buffer
    buffers[0] = try create_buffer_header(@sizeOf(i8), array.inner.len, alloc, page_size_kb);

    const children = try alloc.alloc(header.Array, array.inner.children.len);

    for (0..array.inner.children.len) |i| {
        children[i] = try create_array_header(&array.inner.children[i], alloc, page_size_kb);
    }

    return .{
        .children = children,
        .buffers = buffers,
    };
}

fn create_fixed_size_list_array_header(array: *const arr.FixedSizeListArray, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 1);
    if (array.validity != null) {
        buffers[0] = try create_buffer_header_for_validity(array.len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }

    const children = try alloc.alloc(header.Array, 1);
    children[0] = try create_array_header(&array.inner, alloc, page_size_kb);

    return .{
        .children = children,
        .buffers = buffers,
    };
}

fn create_map_array_header(array: *const arr.MapArray, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    const buffers = try alloc.alloc(header.Buffer, 2);
    if (array.validity != null) {
        buffers[0] = try create_buffer_header_for_validity(array.len, alloc, page_size_kb);
    } else {
        buffers[0] = .{
            .pages = &.{},
            .row_ranges = &.{},
            .compression = .no_compression,
        };
    }
    buffers[1] = try create_buffer_header(@sizeOf(i32), array.len, alloc, page_size_kb);

    const children = try alloc.alloc(header.Array, 1);
    children[0] = try create_array_header(array.entries, alloc, page_size_kb);

    return .{
        .children = children,
        .buffers = buffers,
    };
}

/// Calculate `header.Array` based on given arrow array.
/// Splits into pages and calculates maximum size of pages.
fn create_array_header(array: *const arr.Array, alloc: Allocator, page_size_kb: ?u32) Error!header.Array {
    switch (array.*) {
        .null => |_| return .{ .children = &.{}, .buffers = &.{} },
        .i8 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i8), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .i16 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i16), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .i32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .i64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .u8 => |*a| return try create_array_header_for_fixed_size(@sizeOf(u8), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .u16 => |*a| return try create_array_header_for_fixed_size(@sizeOf(u16), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .u32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(u32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .u64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(u64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .f16 => |*a| return try create_array_header_for_fixed_size(@sizeOf(f16), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .f32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(f32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .f64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(f64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .binary => |*a| return try create_binary_array_header(.i32, a, alloc, page_size_kb),
        .utf8 => |*a| return try create_binary_array_header(.i32, &a.inner, alloc, page_size_kb),
        .bool => |*a| return try create_array_header_for_fixed_size(1, a.validity != null, (arrow.length.length(array) + 8 - 1) / 8, alloc, page_size_kb),
        .decimal32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .decimal64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .decimal128 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i128), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .decimal256 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i256), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .date32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .date64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .time32 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .time64 => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .timestamp => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .interval_year_month => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .interval_day_time => |*a| return try create_array_header_for_fixed_size(@sizeOf(i32) * 2, a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .interval_month_day_nano => |*a| return try create_array_header_for_fixed_size(@sizeOf(arr.MonthDayNano), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .list => |*a| return try create_list_array_header(.i32, a, alloc, page_size_kb),
        .struct_ => |*a| return try create_struct_array_header(a, alloc, page_size_kb),
        .dense_union => |*a| return try create_dense_union_array_header(a, alloc, page_size_kb),
        .sparse_union => |*a| return try create_sparse_union_array_header(a, alloc, page_size_kb),
        .fixed_size_binary => |*a| return try create_array_header_for_fixed_size(a.byte_width, a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .fixed_size_list => |*a| return try create_fixed_size_list_array_header(a, alloc, page_size_kb),
        .map => |*a| return try create_map_array_header(a, alloc, page_size_kb),
        .duration => |*a| return try create_array_header_for_fixed_size(@sizeOf(i64), a.validity != null, arrow.length.length(array), alloc, page_size_kb),
        .large_binary => |*a| return try create_binary_array_header(.i64, a, alloc, page_size_kb),
        .large_utf8 => |*a| return try create_binary_array_header(.i64, &a.inner, alloc, page_size_kb),
        .large_list => |*a| return try create_list_array_header(.i64, a, alloc, page_size_kb),
        .run_end_encoded => return Error.RunEndEncodedArrayNotSupported,
        .binary_view => return Error.BinaryViewArrayNotSupported,
        .utf8_view => return Error.BinaryViewArrayNotSupported,
        .list_view => return Error.ListViewArrayNotSupported,
        .large_list_view => return Error.ListViewArrayNotSupported,
        .dict => return Error.DictArrayNotSupported,
    }
}

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
    /// Pass null if constructing xor filters based on dictionaries is not wanted.
    filter_alloc: ?Allocator,
    scratch_alloc: Allocator,
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
