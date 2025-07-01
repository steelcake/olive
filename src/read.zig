const arrow = @import("arrow");
const arr = arrow.array;
const std = @import("std");
const Allocator = std.mem.Allocator;

const header = @import("./header.zig");
const schema = @import("./schema.zig");
const compression = @import("./compression.zig");
const Compression = compression.Compression;

const Error = error{
    OutOfMemory,
    DataTooShort,
};

// Reads given data into arrow tables based on the given header and schema.
// It is safe to use on untrusted input, will error instead of doing out of bounds access on given `data` and will limit recursion.
// pub fn read(head: *const header.Header, data: []const u8, sch: schema.DatasetSchema, alloc: Allocator) Error![]const []const arr.Array {}

// Reads the given page into an arrow array
// pub fn read_page(page: *const header.Page, data: []const u8, compression: Compression, data_type: *const arrow.data_type.DataType) Error!arr.Array {}
