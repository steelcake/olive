const std = @import("std");
const Allocator = std.mem.Allocator;

const chunk = @import("./chunk.zig");
const header = @import("./header.zig");

const CreateHeaderError = error{
    OutOfMemory,
};

pub const CreateHeader = struct {
    data: *const chunk.Chunk,
    alloc: Allocator,
    scratch_alloc: Allocator,
    page_size_kb: ?u32 = null,
    dict_filters: bool = false,
};

pub fn create_header(params: CreateHeader) CreateHeaderError!header.Header {
    var data_section_size: u32 = 0;

    const tables = try params.alloc.alloc(header.Table, params.data.tables.len);
    const dicts = try params.alloc.alloc(header.Dict, params.data.dicts.len);

    return .{
        .data_section_size = data_section_size,
        .dicts = dicts,
        .tables = tables,
    };
}

pub fn write(data: *const chunk.Chunk, head: *header.Header, buf: []u8, allocator: Allocator) void {}
