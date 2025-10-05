const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const arr = arrow.array;

const schema_impl = @import("./schema.zig");
const Schema = schema_impl.Schema;
const dict_impl = @import("./dict.zig");

const Error = error{
    OutOfMemory,
    NonBinaryArrayWithDict,
    DictElemInvalidLen,
};

pub const Table = struct {
    fields: []const arr.Array,
    num_rows: u32,
};

pub const Chunk = struct {
    tables: []const Table,
    dicts: []const arr.FixedSizeBinaryArray,
    schema: *const schema_impl.Schema,

    pub fn to_arrow(self: *const Chunk, alloc: Allocator) Error![]const []const arr.Array {
        const out = try alloc.alloc([]const arr.Array, self.tables.len);

        for (self.tables, 0..) |table, table_idx| {
            const fields = try alloc.alloc(arr.Array, table.fields.len);

            for (table.fields, 0..) |*field, field_idx| {
                if (schema_impl.find_dict_idx(self.schema.dicts, table_idx, field_idx)) |dict_idx| {
                    fields[field_idx] = try dict_impl.unpack_dict(&self.dicts[dict_idx], &field.u32, self.schema.tables[table_idx].data_types[field_idx], alloc);
                } else {
                    fields[field_idx] = field.*;
                }
            }

            out[table_idx] = fields;
        }

        return out;
    }

    pub fn from_arrow(schema: *const Schema, tables: []const []const arr.Array, alloc: Allocator, scratch_alloc: Allocator) Error!Chunk {
        const out = try alloc.alloc(Table, tables.len);

        const num_dicts = schema.dicts.len;
        const dict_arrays = try alloc.alloc(arr.FixedSizeBinaryArray, num_dicts);

        for (schema.dicts, 0..) |dict, dict_idx| {
            var num_elems: usize = 0;

            for (dict.members) |member| {
                num_elems += try dict_impl.count_array_to_dict(&tables[member.table_index][member.field_index]);
            }

            var elems = std.StringHashMapUnmanaged(void){};
            try elems.ensureTotalCapacity(scratch_alloc, @intCast(num_elems));

            for (dict.members) |member| {
                const array = &tables[member.table_index][member.field_index];
                try dict_impl.push_array_to_dict(array, &elems);
            }

            const keys = try scratch_alloc.alloc([]const u8, elems.size);
            var key_it = elems.keyIterator();
            var idx: usize = 0;
            while (key_it.next()) |k| {
                keys[idx] = k.*;
                idx += 1;
            }

            std.mem.sortUnstable([]const u8, keys, {}, stringLessThan);

            std.debug.assert(dict.byte_width > 0);

            const dict_array = arrow.builder.FixedSizeBinaryBuilder.from_slice(dict.byte_width, keys, false, alloc) catch |e| {
                switch (e) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.InvalidSliceLength => return error.DictElemInvalidLen,
                    else => unreachable,
                }
            };

            dict_arrays[dict_idx] = dict_array;
        }

        for (tables, 0..) |table, table_idx| {
            const num_rows = for (table) |*field| {
                break arrow.length.length(field);
            } else 0;

            for (table) |*field| {
                std.debug.assert(arrow.length.length(field) == num_rows);
            }

            const fields = try alloc.alloc(arr.Array, table.len);

            for (table, 0..) |*array, field_idx| {
                if (schema_impl.find_dict_idx(schema.dicts, table_idx, field_idx)) |dict_idx| {
                    fields[field_idx] = .{ .u32 = try dict_impl.apply_dict(&dict_arrays[dict_idx], array, alloc) };
                } else {
                    fields[field_idx] = array.*;
                }
            }

            out[table_idx] = .{
                .fields = fields,
                .num_rows = num_rows,
            };
        }

        return .{
            .tables = out,
            .dicts = dict_arrays,
            .schema = schema,
        };
    }
};

fn stringLessThan(_: void, l: []const u8, r: []const u8) bool {
    return std.mem.order(u8, l, r) == .lt;
}
