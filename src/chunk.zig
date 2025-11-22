const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const arr = arrow.array;

const Schema = @import("./schema.zig").Schema;

const dict_mod = @import("./dict.zig");
const DictFn32 = dict_mod.DictFn32;
const DictFn20 = dict_mod.DictFn20;

const Error = error{
    OutOfMemory,
};

pub const Table = struct {
    fields: []const arr.Array,
    num_rows: u32,
};

pub const Chunk = struct {
    tables: []const Table,
    schema: *const Schema,
    dict_32: []const [32]u8,
    dict_20: []const [20]u8,

    pub fn to_arrow(self: *const Chunk, alloc: Allocator) Error![]const []const arr.Array {
        const out = try alloc.alloc([]const arr.Array, self.tables.len);

        for (self.tables, 0..) |table, table_idx| {
            const fields = try alloc.alloc(arr.Array, table.fields.len);

            var t = timer();
            for (table.fields, 0..) |*field, field_idx| {
                if (schema_impl.find_dict_idx(self.schema.dicts, table_idx, field_idx)) |dict_idx| {
                    fields[field_idx] = try dict_impl.unpack_dict(
                        &self.dicts[dict_idx],
                        &field.u32,
                        self.schema.tables[table_idx].data_types[field_idx],
                        alloc,
                    );
                } else {
                    fields[field_idx] = field.*;
                }
                std.debug.print("\t\t FFFFFFIELD in {}us\n", .{t.lap() / 1000});
            }

            out[table_idx] = fields;
        }

        return out;
    }

    fn timer() std.time.Timer {
        return std.time.Timer.start() catch unreachable;
    }

    pub fn from_arrow(
        schema: *const Schema,
        tables: []const []const arr.Array,
        alloc: Allocator,
        scratch_alloc: Allocator,
    ) Error!Chunk {
        const out = try alloc.alloc(Table, tables.len);

        const num_dicts = schema.dicts.len;
        const dict_arrays = try alloc.alloc(arr.FixedSizeBinaryArray, num_dicts);
        const dict_lookup = try scratch_alloc.alloc(std.StringHashMapUnmanaged(u32), num_dicts);

        var t = timer();

        for (schema.dicts, 0..) |dict, dict_idx| {
            var num_elems: usize = 0;

            for (dict.members) |member| {
                num_elems += try dict_impl.count_array_to_dict(&tables[member.table_index][member.field_index]);
            }

            std.debug.print("\t\t COUNT in {}us\n", .{t.lap() / 1000});

            var elems = std.StringHashMapUnmanaged(u32){};
            try elems.ensureTotalCapacity(scratch_alloc, @intCast(num_elems));

            for (dict.members) |member| {
                const array = &tables[member.table_index][member.field_index];
                try dict_impl.push_array_to_dict(array, &elems);
            }

            std.debug.print("\t\t PUSH in {}us\n", .{t.lap() / 1000});

            const elems_list = try alloc.alloc([20]u8, elems.count());
            var elems_it = elems.keyIterator();
            var idx: u32 = 0;
            while (elems_it.next()) |key| {
                @memcpy(&elems_list[idx], key.*);
                idx += 1;
            }

            std.debug.print("\t\t COPY in {}us\n", .{t.lap() / 1000});

            std.sort.pdq([20]u8, elems_list, {}, stringLessThan);

            std.debug.print("\t\t SORT in {}us\n", .{t.lap() / 1000});

            idx = 0;
            while (idx < elems_list.len) : (idx += 1) {
                elems.putAssumeCapacity(&elems_list[idx], idx);
            }

            std.debug.print("\t\t REINDEX in {}us\n", .{t.lap() / 1000});

            dict_lookup[dict_idx] = elems;

            std.debug.assert(dict.byte_width > 0);

            const dict_array = arrow.array.FixedSizeBinaryArray{
                .byte_width = dict.byte_width,
                .len = @intCast(elems_list.len),
                .data = @ptrCast(elems_list),
                .offset = 0,
                .validity = null,
                .null_count = 0,
            };

            // const dict_array = arrow.builder.FixedSizeBinaryBuilder.from_slice(
            //     dict.byte_width,
            //     elems_list,
            //     false,
            //     alloc,
            // ) catch |e| {
            //     switch (e) {
            //         error.OutOfMemory => return error.OutOfMemory,
            //         error.InvalidSliceLength => return error.DictElemInvalidLen,
            //         else => unreachable,
            //     }
            // };

            std.debug.print("\t\t BUILD in {}us\n", .{t.lap() / 1000});

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

            _ = t.lap();
            for (table, 0..) |*array, field_idx| {
                if (schema_impl.find_dict_idx(schema.dicts, table_idx, field_idx)) |dict_idx| {
                    fields[field_idx] = .{ .u32 = try dict_impl.apply_dict(&dict_lookup[dict_idx], array, alloc) };
                } else {
                    fields[field_idx] = array.*;
                }

                std.debug.print("\t\t FIELD in {}us\n", .{t.lap() / 1000});
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
