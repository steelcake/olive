const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const arr = arrow.array;

const Schema = @import("./schema.zig").Schema;

const dict = @import("./dict.zig");

const Error = error{
    OutOfMemory,
};

pub const Chunk = struct {
    tables: []const []const arr.Array,
    schema: *const Schema,
    dict_ctx: dict.DictContext,

    pub fn to_arrow(self: *const Chunk, alloc: Allocator) Error![]const []const arr.Array {
        return try dict.decode_chunk(self.dict_ctx, self.schema, self.tables, alloc);
    }

    pub fn from_arrow(
        schema: *const Schema,
        tables: []const []const arr.Array,
        alloc: Allocator,
        scratch_alloc: Allocator,
    ) Error!Chunk {
        const dict_o = try dict.encode_chunk(tables, alloc, scratch_alloc);

        return .{
            .tables = dict_o.tables,
            .dict_ctx = dict_o.context,
            .schema = schema,
        };
    }
};
