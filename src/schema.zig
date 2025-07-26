const std = @import("std");
const arrow = @import("arrow");
const DataType = arrow.data_type.DataType;
const Compression = @import("./compression.zig");

pub fn can_have_minmax_index(data_type: DataType) bool {
    return switch (data_type) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .f16, .f32, .f64, .binary, .utf8, .decimal32, .decimal64, .decimal128, .decimal256, .large_binary, .large_utf8, .binary_view, .utf8_view, .fixed_size_binary => true,
        else => false,
    };
}

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    data_types: []const DataType,
    /// Minmax index is only supported for primitive and binary types
    has_minmax_index: []const bool,

    pub fn check(self: *const TableSchema, table: []const arrow.array.Array) bool {
        std.debug.assert(self.field_names.len == self.data_types.len);

        if (self.data_types.len != table.len) {
            return false;
        }

        for (self.data_types, table) |*sdt, *dfv| {
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
    /// length of each string in the dictionary, will be used for constructing arrow.FixedSizeBinaryArray
    byte_width: i32,
};

pub const ValidationError = error{};

pub const DatasetSchema = struct {
    table_names: []const []const u8,
    tables: []const TableSchema,
    dicts: []const DictSchema,

    /// Validate this schema for any inconsistencies
    pub fn validate(_: *const DatasetSchema) ValidationError!void {}

    /// Check if given data matches with the schema
    pub fn check(self: *const DatasetSchema, tables: []const []const arrow.array.Array) bool {
        std.debug.assert(self.table_names.len == self.tables.len);

        if (self.tables.len != tables.len) {
            return false;
        }

        for (self.tables, tables) |*tbl, d| {
            if (!tbl.check(d)) {
                return false;
            }
        }

        return true;
    }
};
