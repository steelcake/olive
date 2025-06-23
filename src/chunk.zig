const schema = @import("./schema.zig");
const arrow = @import("arrow");

const Error = error{
    SchemaMismatch,
};

pub const Chunk = struct {
    schema: schema.DatasetSchema,
    data: []const arrow.array.StructArray,

    pub fn init_checked(sch: schema.DatasetSchema, data: []const arrow.array.StructArray) Error!Chunk {
        if (!sch.check(data)) {
            return Error.SchemaMismatch;
        }

        return .{
            .schema = sch,
            .data = data,
        };
    }
};
