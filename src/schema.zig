const std = @import("std");

const arrow = @import("arrow");
const DataType = arrow.data_type.DataType;
const Array = arrow.array.Array;

const Error = error{Invalid};

pub const TableSchema = struct {
    field_names: []const [:0]const u8,
    field_types: []const DataType,

    pub fn validate(self: *const TableSchema) Error!void {
        if (self.field_names.len == 0) {
            return Error.Invalid;
        }

        if (self.field_names.len != self.field_types.len) {
            return Error.Invalid;
        }

        try validate_names(self.field_names);

        for (self.field_types) |dt| {
            try arrow.validate.validate_data_type(&dt);
        }
    }

    pub fn check(self: *const TableSchema, fields: []const Array) Error!void {
        if (self.field_types.len != fields.len) {
            return Error.Invalid;
        }

        for (self.field_types, fields) |*dt, *field| {
            arrow.data_type.check_data_type(field, dt) catch {
                return Error.Invalid;
            };
        }
    }
};

pub const Schema = struct {
    table_names: []const [:0]const u8,
    table_schemas: []const TableSchema,

    /// Validate this schema for any inconsistencies
    pub fn validate(self: *const Schema) Error!void {
        if (self.table_names.len == 0) {
            return Error.Invalid;
        }

        try validate_names(self.table_names);

        if (self.table_names.len != self.table_schemas.len) {
            return Error.Invalid;
        }

        for (self.table_schemas) |table_schema| {
            try table_schema.validate();
        }
    }

    /// Check if given data matches with the schema
    pub fn check(self: *const Schema, tables: []const []const Array) Error!void {
        if (self.table_schemas.len != tables.len) {
            return Error.Invalid;
        }

        for (self.table_schemas, tables) |*table_schema, table| {
            try table_schema.check(table);
        }
    }
};

fn validate_names(names: []const [:0]const u8) Error!void {
    for (names, 0..) |name, name_idx| {
        for (name) |c| {
            if (c == 0) {
                return Error.Invalid;
            }
        }

        if (names.len == 0) {
            return Error.Invalid;
        }

        for (names[0..name_idx]) |other_name| {
            if (std.mem.eql(u8, name, other_name)) {
                return Error.Invalid;
            }
        }
    }
}
