const schema = @import("./schema.zig");
const arrow = @import("arrow");

pub const Chunk = struct {
    schema: schema.DatasetSchema,
    tables: []const []const arrow.array.Array,
};
