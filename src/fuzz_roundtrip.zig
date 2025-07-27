const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arrow = @import("arrow");
const validate = arrow.validate.validate;
const arr = arrow.array;
const FuzzInput = arrow.fuzz_input.FuzzInput;
const data_type = arrow.data_type;

const chunk_mod = @import("./chunk.zig");
const Chunk = chunk_mod.Chunk;
const write = @import("./write.zig");
const read = @import("./read.zig");
const schema_mod = @import("./schema.zig");
const DatasetSchema = schema_mod.DatasetSchema;
const TableSchema = schema_mod.TableSchema;
const dict_impl = @import("./dict.zig");

fn make_dicted_array(input: *FuzzInput, len: u32, dict_array: *const arr.FixedSizeBinaryArray, alloc: Allocator) !arr.Array {
    const keys = make_keys: {
        const keys_raw = try input.primitive_array(u32, len, alloc);

        const keys = try alloc.alloc(u32, keys_raw.offset + keys_raw.len);

        var idx: u32 = 0;
        while (idx < keys_raw.offset + keys_raw.len) : (idx += 1) {
            keys.ptr[idx] = keys_raw.values.ptr[idx] % dict_array.len;
        }

        break :make_keys arr.UInt32Array{
            .values = keys,
            .validity = keys_raw.validity,
            .offset = keys_raw.offset,
            .len = keys_raw.len,
            .null_count = keys_raw.null_count,
        };
    };

    const dt: data_type.DataType = switch ((try input.int(u8)) % 7) {
        0 => .{ .binary = {} },
        1 => .{ .large_binary = {} },
        2 => .{ .binary_view = {} },
        3 => .{ .fixed_size_binary = dict_array.byte_width },
        4 => .{ .utf8 = {} },
        5 => .{ .large_utf8 = {} },
        6 => .{ .utf8_view = {} },
        else => unreachable,
    };

    return try dict_impl.unpack_dict(dict_array, &keys, dt, alloc);
}

fn roundtrip_test(input: *FuzzInput, alloc: Allocator) !void {
    const num_tables = (try input.int(u8)) % 10 + 1;
    const num_dicts = (try input.int(u8)) % 4;

    var chunk_arena = ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();
    const chunk_alloc = chunk_arena.allocator();

    const dicts = try chunk_alloc.alloc(arr.FixedSizeBinaryArray, num_dicts);
    for (0..num_dicts) |dict_idx| {
        dicts[dict_idx] = try input.fixed_size_binary_array(try input.int(u8), chunk_alloc);
    }

    const table_num_fields = try chunk_alloc.alloc(u8, num_tables);
    for (0..num_tables) |table_idx| {
        table_num_fields[table_idx] = (try input.int(u8)) % 12 + 1;
    }

    const tables = try chunk_alloc.alloc([]const arr.Array, num_tables);
    const dict_schemas = try chunk_alloc.alloc(schema_mod.DictSchema, num_dicts);
    for (0..num_dicts) |dict_idx| {
        const num_members = (try input.int(u8)) % 10;
        const members = try chunk_alloc.alloc(schema_mod.DictMember, num_members);

        const table_index = (try input.int(u8)) % num_tables;
        const field_index = (try input.int(u8)) % table_num_fields[table_index];

        for (0..num_members) |member_idx| {
            members[member_idx] = schema_mod.DictMember{
                .table_index = table_index,
                .field_index = field_index,
            };
        }

        dict_schemas[dict_idx] = schema_mod.DictSchema{
            .members = members,
            .has_filter = true,
            .byte_width = dicts[dict_idx].byte_width,
        };
    }

    for (0..num_tables) |table_idx| {
        const num_fields = table_num_fields[table_idx];
        const num_rows = try input.int(u8);

        const fields = try chunk_alloc.alloc(arr.Array, num_fields);
        for (0..num_fields) |field_idx| {
            if (dict_impl.find_dict_idx(dict_schemas, table_idx, field_idx)) |dict_idx| {
                fields[field_idx] = try make_dicted_array(input, num_rows, &dicts[dict_idx], chunk_alloc);
            } else {
                fields[field_idx] = try input.make_array(num_rows, chunk_alloc);
            }
        }

        tables[table_idx] = fields;
    }

    var prng = try input.make_prng();
    const rand = prng.random();

    const table_schemas = try chunk_alloc.alloc(TableSchema, num_tables);
    const table_names = try chunk_alloc.alloc([:0]const u8, num_tables);
    for (0..num_tables) |table_idx| {
        const fields = tables[table_idx];
        const num_fields = fields.len;

        const has_minmax_index = try chunk_alloc.alloc(bool, num_fields);
        const data_types = try chunk_alloc.alloc(data_type.DataType, num_fields);
        const field_names = try chunk_alloc.alloc([:0]const u8, num_fields);

        for (0..num_fields) |field_idx| {
            const dt = try data_type.get_data_type(&fields[field_idx], chunk_alloc);

            data_types[field_idx] = dt;
            has_minmax_index[field_idx] = schema_mod.can_have_minmax_index(dt) and (try input.boolean());
            field_names[field_idx] = try make_name(rand, chunk_alloc);
        }

        table_names[table_idx] = try make_name(rand, chunk_alloc);
        table_schemas[table_idx] = TableSchema{
            .has_minmax_index = has_minmax_index,
            .data_types = data_types,
            .field_names = field_names,
        };
    }

    const schema = DatasetSchema{
        .tables = table_schemas,
        .table_names = table_names,
        .dicts = dict_schemas,
    };

    const chunk = make_chunk: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();
        break :make_chunk try Chunk.from_arrow(&schema, tables, chunk_alloc, scratch_alloc);
    };

    var header_arena = ArenaAllocator.init(alloc);
    defer header_arena.deinit();
    const header_alloc = header_arena.allocator();

    var filter_arena = ArenaAllocator.init(alloc);
    defer filter_arena.deinit();
    const filter_alloc = filter_arena.allocator();

    // max should be 16 MB since the number is in KB and min should be 1 KB
    const page_size_kb = (try input.int(u32)) % (1 << 14) + 1;

    const data_section = try alloc.alloc(u8, 1 << 28);
    defer alloc.free(data_section);

    const header = write: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :write try write.write(.{
            .chunk = &chunk,
            .compression = .lz4,
            .header_alloc = header_alloc,
            .filter_alloc = filter_alloc,
            .scratch_alloc = scratch_alloc,
            .data_section = data_section,
            .page_size_kb = page_size_kb,
        });
    };

    var out_arena = ArenaAllocator.init(alloc);
    defer out_arena.deinit();
    const out_alloc = out_arena.allocator();

    const out_chunk = read: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :read try read.read(.{
            .data_section = data_section[0..header.data_section_size],
            .scratch_alloc = scratch_alloc,
            .schema = chunk.schema,
            .alloc = out_alloc,
            .header = &header,
        });
    };

    std.debug.assert(out_chunk.tables.len == chunk.tables.len);
    for (out_chunk.tables, chunk.tables) |out_table, chunk_table| {
        std.debug.assert(out_table.num_rows == chunk_table.num_rows);

        for (out_table.fields, chunk_table.fields) |*out_field, *chunk_field| {
            arrow.equals.equals(out_field, chunk_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(out_alloc);

    std.debug.assert(out_tables.len == tables.len);
    for (out_tables, tables) |out_table, table| {
        std.debug.assert(out_table.len == table.len);

        for (out_table, table) |*out_field, *field| {
            arrow.equals.equals(out_field, field);
        }
    }
}

fn to_fuzz(data: []const u8) !void {
    var general_purpose_allocator: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const gpa = general_purpose_allocator.allocator();
    defer {
        switch (general_purpose_allocator.deinit()) {
            .ok => {},
            .leak => |l| {
                std.debug.panic("LEAK: {any}", .{l});
            },
        }
    }

    var input = FuzzInput{ .data = data };

    try roundtrip_test(&input, gpa);
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}

fn rand_bytes_zero_sentinel(rand: std.Random, out: []u8) void {
    rand.bytes(out);

    for (0..out.len) |i| {
        if (out.ptr[i] == 0) {
            out.ptr[i] = 1;
        }
    }
}

fn make_name(rand: std.Random, alloc: Allocator) ![:0]const u8 {
    const name_len = rand.int(u8) % 30;
    const name = try alloc.allocSentinel(u8, name_len, 0);
    rand_bytes_zero_sentinel(rand, name);
    return name;
}
