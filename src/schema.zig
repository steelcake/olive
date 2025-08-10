const std = @import("std");
const Allocator = std.mem.Allocator;

const borsh = @import("borsh");
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

fn validate_names(names: []const []const u8) Error!void {
    for (names, 0..) |name, field_idx| {
        if (name.len == 0) {
            return Error.Invalid;
        }

        for (names[0..field_idx]) |other_name| {
            if (std.mem.eql(u8, name, other_name)) {
                return Error.Invalid;
            }
        }
    }
}

pub const TableSchema = struct {
    field_names: []const []const u8,
    data_types: []const DataType,
    /// Minmax index is only supported for primitive and binary types
    has_minmax_index: []const bool,

    pub fn eql(self: *const TableSchema, other: *const TableSchema) bool {
        if (self.field_names.len != other.field_names.len) {
            return false;
        }
        for (self.field_names, other.field_names) |sfn, ofn| {
            if (!std.mem.eql(u8, sfn, ofn)) {
                return false;
            }
        }

        if (self.data_types.len != other.data_types.len) {
            return false;
        }
        for (self.data_types, other.data_types) |*sdt, *odt| {
            if (!sdt.eql(odt)) {
                return false;
            }
        }

        if (!std.mem.eql(bool, self.has_minmax_index, other.has_minmax_index)) {
            return false;
        }

        return true;
    }

    pub fn validate(self: *const TableSchema) Error!void {
        if (self.field_names.len == 0) {
            return Error.Invalid;
        }

        try validate_names(self.field_names);

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
        if (self.field_names.len != self.data_types.len) {
            return Error.Invalid;
        }
        if (self.field_names.len != self.has_minmax_index.len) {
            return Error.Invalid;
        }

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

    pub fn eql(self: *const DictSchema, other: *const DictSchema) bool {
        if (self.members.len != other.members.len) {
            return false;
        }
        for (self.members, other.members) |sm, om| {
            if (sm.table_index != om.table_index or sm.field_index != om.table_index) {
                return false;
            }
        }

        if (self.has_filter != other.has_filter) {
            return false;
        }

        if (self.byte_width != other.byte_width) {
            return false;
        }

        return true;
    }
};

const SerdeError = error{
    BorshError,
    OutOfMemory,
    /// Buffer isn't big enough to hold the output
    BufferTooSmall,
};

pub const Schema = struct {
    table_names: []const []const u8,
    tables: []const TableSchema,
    dicts: []const DictSchema,

    pub fn deserialize(input: []const u8, alloc: Allocator, max_recursion_depth: u8) SerdeError!Schema {
        return borsh.serde.deserialize(Schema, input, alloc, max_recursion_depth) catch |e| {
            if (e == error.OutOfMemory) return SerdeError.OutOfMemory else return SerdeError.BorshError;
        };
    }

    pub fn serialize(self: *const Schema, output: []u8, max_recursion_depth: u8) SerdeError!usize {
        return borsh.serde.serialize(Schema, self, output, max_recursion_depth) catch |e| {
            if (e == error.BufferTooSmall) return SerdeError.BufferTooSmall else return SerdeError.BorshError;
        };
    }

    pub fn eql(self: *const Schema, other: *const Schema) bool {
        if (self.table_names.len != other.table_names.len) {
            return false;
        }
        for (self.table_names, other.table_names) |st, ot| {
            if (!std.mem.eql(u8, st, ot)) {
                return false;
            }
        }

        if (self.tables.len != other.tables.len) {
            return false;
        }
        for (self.tables, other.tables) |*st, *ot| {
            if (!st.eql(ot)) {
                return false;
            }
        }

        if (self.dicts.len != other.dicts.len) {
            return false;
        }
        for (self.dicts, other.dicts) |*sd, *od| {
            if (!sd.eql(od)) {
                return false;
            }
        }

        return true;
    }

    /// Validate this schema for any inconsistencies
    pub fn validate(self: *const Schema) Error!void {
        if (self.table_names.len == 0) {
            return Error.Invalid;
        }

        try validate_names(self.table_names);

        if (self.table_names.len != self.tables.len) {
            return Error.Invalid;
        }

        for (self.tables) |table_schema| {
            try table_schema.validate();
        }

        for (self.dicts, 0..) |dict, dict_idx| {
            if (dict.members.len == 0) {
                return Error.Invalid;
            }

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
    pub fn check(self: *const Schema, tables: []const []const arrow.array.Array) Error!void {
        if (self.table_names.len != self.tables.len) {
            return Error.Invalid;
        }

        if (self.tables.len != tables.len) {
            return Error.Invalid;
        }

        for (self.tables, tables) |*table_sch, table| {
            try table_sch.check(table);
        }
    }
};
