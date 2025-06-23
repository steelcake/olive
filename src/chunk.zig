const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;
const xorf = @import("filterz").xorf;

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    /// Index of the dictionary the field corresponds to if it is dictionary encoded.
    /// Multiple fields can be included in a single dictionary.
    dict_indices: []const ?u8,
    /// Whether the field has a minmax index or not
    min_max: []const bool,
    /// Whether the dictionary at index i has a xor filter
    dict_has_filter: []const bool,
};

pub const Filter = struct {
    header: xorf.Header,
    fingerprints: []const u16,
};

pub const Dict = struct {
    data: *const arr.Array,
    filter: ?Filter,
};

pub const Chunk = struct {
    table_names: []const [:0]const u8,
    table_schemas: []const TableSchema,
    tables: []const arr.StructArray,
    dicts: []const Dict,
};
