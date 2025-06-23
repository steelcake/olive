const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;

pub const Schema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    /// Index of the dictionary the field corresponds to if it is dictionary encoded.
    /// Multiple fields can be included in a single dictionary.
    dict_indices: []const ?u8,
    /// Whether the field has a minmax index or not
    min_max: []const bool,
};

pub const Chunk = struct {
    table_names: []const [:0]const u8,
    table_schemas: []const Schema,
    tables: []const arr.StructArray,
    dicts: []const arr.Array,
};
