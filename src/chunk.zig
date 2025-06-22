const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;

pub const Schema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    dict_indices: []const ?u8,
};

pub const Chunk = struct {
    table_names: []const [:0]const u8,
    table_schemas: []const Schema,
    tables: []const arr.StructArray,
    dicts: []const arr.Array,
};
