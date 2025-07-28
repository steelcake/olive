const std = @import("std");
const arrow = @import("arrow");
const DataType = arrow.data_type.DataType;
const Compression = @import("./compression.zig");

/// Finds the dictionary the corresponds to given field (table_index/field_index)
pub fn find_dict_idx(dicts: []const DictSchema, table_index: usize, field_index: usize) ?usize {
    for (dicts, 0..) |dict, dict_idx| {
        for (dict.members) |member| {
            if (member.table_index == table_index and member.field_index == field_index) {
                return dict_idx;
            }
        }
    }

    return null;
}

pub fn can_have_minmax_index(data_type: DataType) bool {
    return switch (data_type) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .f16, .f32, .f64, .binary, .utf8, .decimal32, .decimal64, .decimal128, .decimal256, .large_binary, .large_utf8, .binary_view, .utf8_view, .fixed_size_binary => true,
        else => false,
    };
}

pub fn can_be_dict_member(data_type: DataType) bool {
    return switch (data_type) {
        .binary, .large_binary, .fixed_size_binary, .utf8, .large_utf8, .utf8_view, .binary_view => true,
        else => false,
    };
}

const Error = error{Invalid};

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    /// Minmax index is only supported for primitive and binary types
    has_minmax_index: []const bool,

    pub fn validate(self: *const TableSchema) Error!void {
        if (self.field_names.len != self.data_types.len) {
            return Error.Invalid;
        }
        if (self.field_names.len != self.has_minmax_index.len) {
            return Error.Invalid;
        }
        for (self.data_types, self.has_minmax_index) |dt, has_mm| {
            if (has_mm and !can_have_minmax_index(dt)) {
                return Error.Invalid;
            }
        }
    }

    pub fn check(self: *const TableSchema, table: []const arrow.array.Array) Error!void {
        std.debug.assert(self.field_names.len == self.data_types.len);
        std.debug.assert(self.field_names.len == self.has_minmax_index.len);

        if (self.data_types.len != table.len) {
            return Error.Invalid;
        }

        for (self.data_types, table) |*sdt, *dfv| {
            if (!arrow.data_type.check_data_type(dfv, sdt)) {
                return Error.Invalid;
            }
        }
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
    /// length of each string in the dictionary, will be used for constructing arrow.FixedSizeBinaryArray
    byte_width: i32,
};

pub const DatasetSchema = struct {
    table_names: []const []const u8,
    tables: []const TableSchema,
    dicts: []const DictSchema,

    /// Validate this schema for any inconsistencies
    pub fn validate(self: *const DatasetSchema) Error!void {
        if (self.table_names.len != self.tables.len) {
            return Error.Invalid;
        }

        for (self.tables) |table_schema| {
            try table_schema.validate();
        }

        for (self.dicts, 0..) |dict, dict_idx| {
            for (dict.members) |member| {
                if (member.table_index >= self.tables.len) {
                    return Error.Invalid;
                }

                if (member.field_index >= self.tables[member.table_index].field_names.len) {
                    return Error.Invalid;
                }

                if (!can_be_dict_member(self.tables[member.table_index].data_types[member.field_index])) {
                    return Error.Invalid;
                }

                const dict_idx_of_member = find_dict_idx(self.dicts, member.table_index, member.field_index) orelse unreachable;
                if (dict_idx_of_member != dict_idx) {
                    // Multiple dicts have the same member
                    return Error.Invalid;
                }
            }

            if (dict.byte_width <= 0) {
                return Error.Invalid;
            }
        }
    }

    /// Check if given data matches with the schema
    pub fn check(self: *const DatasetSchema, tables: []const []const arrow.array.Array) Error!void {
        std.debug.assert(self.table_names.len == self.tables.len);

        if (self.tables.len != tables.len) {
            return Error.Invalid;
        }

        for (self.tables, tables) |*table_sch, table| {
            try table_sch.check(table);
        }
    }
};
