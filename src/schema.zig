const std = @import("std");
const arrow = @import("arrow");
const DataType = arrow.data_type.DataType;

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,

    pub fn check(self: *const TableSchema, data: *const arrow.array.StructArray) bool {
        std.debug.assert(self.field_names.len == self.data_types.len);

        if (self.field_names.len != data.field_names.len or self.data_types.len != data.field_values.len) {
            return false;
        }

        for (self.field_names, data.field_names) |sfn, dfn| {
            if (!std.mem.eql(u8, sfn, dfn)) {
                return false;
            }
        }

        for (self.data_types, data.field_values) |*sdt, *dfv| {
            if (!arrow.data_type.check_data_type(dfv, sdt)) {
                return false;
            }
        }

        return true;
    }
};

pub const DictMember = struct {
    table_index: u8,
    field_index: u8,
};

pub const DictSchema = struct {
    /// (TableIndex, ColumnIndex) of fields included in this dictionary
    /// All fields have to be some kind of binary array e.g. FixedSizeBinary/LargeBinary/BinaryView/Utf8
    members: []const DictMember,
    /// Whether this dictionary has an accompanying xor filter in the file header
    has_filter: bool,
};

pub const ValidationError = error{};

pub const DatasetSchema = struct {
    table_names: []const []const u8,
    tables: []const TableSchema,
    dicts: []const DictSchema,

    /// Validate this schema for any inconsistencies
    pub fn validate(_: *const DatasetSchema) ValidationError!void {}

    /// Check if given data matches with the schema
    pub fn check(self: *const DatasetSchema, data: []const arrow.array.StructArray) bool {
        std.debug.assert(self.table_names.len == self.tables.len);

        if (self.tables.len != data.len) {
            return false;
        }

        for (self.tables, data) |*tbl, *d| {
            if (!tbl.check(d)) {
                return false;
            }
        }

        return true;
    }
};
