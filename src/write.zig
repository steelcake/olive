const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;

const chunk = @import("./chunk.zig");
const header = @import("./header.zig");

pub const CreateHeader = struct {
    chunk: *const chunk.Chunk,
    alloc: Allocator,
    scratch_alloc: Allocator,
    page_size_kb: ?u32 = null,
    dict_filters: bool = false,
};

/// Calculate header.Array based on given arrow array.
/// Splits into pages and calculates maximum size of pages.
fn create_array_header(array: *const arr.Array, alloc: Allocator, page_size_kb: ?u32) error.OutOfMemory!header.Array {}

/// Similar to create_array_header but:
/// we calculate the array header for dict_keys for an array that has a dictionary.
/// The amount of space that this array adds to the dictionary itself should be accounted seperately.
fn create_array_header_for_dict_keys(array_len: u32, alloc: Allocator, page_size_kb: ?u32) error.OutOfMemory!header.Array {
    // since dict keys are always u32
    const bytes_per_element = 4;

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

    const buffers = try alloc.alloc(header.Buffer, 1);
    buffers[0] = .{
        .pages = pages,
        .row_ranges = row_ranges,
        .compression = .no_compression,
    };

    return .{
        .children = &.{},
        .buffers = buffers,
    };
}

pub fn create_header(params: CreateHeader) error.OutOfMemory!header.Header {
    const tables = try params.alloc.alloc(header.Table, params.chunk.tables.len);

    std.debug.assert(params.chunk.tables.len == params.chunk.table_schemas.len);
    for (params.chunk.tables, params.chunk.table_schemas, 0..) |table, schema, table_index| {
        const fields = try params.alloc.alloc(header.Array, table.field_names.len);
        const dict_indices = try params.alloc.alloc(?u8, table.field_names.len);

        for (table.field_values, schema.dict_indices, 0..) |*array, dict_index, field_index| {
            dict_indices[field_index] = dict_index;

            if (dict_index) |_| {
                fields[field_index] = try create_array_header_for_dict_keys(arrow.length.length(array), params.alloc, params.page_size_kb);
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

pub fn write(_: Write) error.OutOfMemory!void {}

fn array_elem_as_bytes_primitive(comptime T: type, array: *const arr.PrimitiveArray(T), index: u32, alloc: Allocator) error.OutOfMemory![]u8 {
    std.debug.assert(arrow.length.length(array) > index);

    const out = try alloc.alloc(T, 1);
    if (arrow.get.get_primitive_opt(T, array.values.ptr, array.validity.ptr, index)) |elem| {
        out[0] = elem;
    } else {
        out[0] = 0;
    }

    return @ptrCast(out);
}

fn array_elem_as_bytes(array: *const arr.Array, index: u32, alloc: Allocator) error.OutOfMemory![]u8 {
    std.debug.assert(arrow.length.length(array) > index);

    switch(array.*) {
        .u8 => |*a| return try array_elem_as_bytes_primitive(u8, a, index, alloc),
        else => unreachable,
    }
}
