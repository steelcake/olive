const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const arr = arrow.array;

const schema_impl = @import("./schema.zig");
const DatasetSchema = schema_impl.DatasetSchema;
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

    pub fn from_arrow(schema: *const DatasetSchema, tables: []const []const arr.Array, alloc: Allocator, scratch_alloc: Allocator) Error!Chunk {
        const out = try alloc.alloc(Table, tables.len);

        const num_dicts = schema.dicts.len;
        const dict_arrays = try alloc.alloc(arr.FixedSizeBinaryArray, num_dicts);

        for (schema.dicts, 0..) |dict, dict_idx| {
            var num_elems: usize = 0;

            for (dict.members) |member| {
                num_elems += try dict_impl.count_array_to_dict(&tables[member.table_index][member.field_index]);
            }

            var elems = try scratch_alloc.alloc([]const u8, num_elems);

            var write_idx: usize = 0;
            for (dict.members) |member| {
                const array = &tables[member.table_index][member.field_index];
                write_idx = try dict_impl.push_array_to_dict(array, write_idx, elems);
            }

            elems = dict_impl.sort_and_dedup(elems[0..write_idx]);

            std.debug.assert(dict.byte_width > 0);

            const dict_array = arrow.builder.FixedSizeBinaryBuilder.from_slice(dict.byte_width, elems, false, alloc) catch |e| {
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
                if (dict_impl.find_dict_idx(schema.dicts, table_idx, field_idx)) |dict_idx| {
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
        };
    }
};
