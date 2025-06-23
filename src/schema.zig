const std = @import("std");
const arrow = @import("arrow");
const DataType = arrow.data_type.DataType;

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    /// Index of the dictionary the field corresponds to if it is dictionary encoded.
    /// Multiple fields can be included in a single dictionary.
    dict_indices: []const ?u8,
    /// Whether the field at index i has a minmax index or not
    min_max: []const bool,

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

pub const DatasetSchema = struct {
    table_names: []const []const u8,
    tables: []const TableSchema,
    /// Whether the dictionary at index i has a xor filter
    /// The length of this slice should be the same as the number of dictionaries that this dataset has
    dict_has_filter: []const bool,

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
