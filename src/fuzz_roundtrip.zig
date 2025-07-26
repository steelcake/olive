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

fn roundtrip_test(input: *FuzzInput, alloc: Allocator) !void {
    const num_tables = (try input.int(u8)) % 10;

    var chunk_arena = ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();
    const chunk_alloc = chunk_arena.allocator();

    const tables = try chunk_alloc.alloc(chunk_mod.Table, num_tables);
    for (0..num_tables) |table_idx| {
        const num_fields = (try input.int(u8)) % 10;
        const num_rows = try input.int(u8);

        const fields = try chunk_alloc.alloc(arr.Array, num_fields);
        for (0..num_fields) |field_idx| {
            fields[field_idx] = try input.make_array(num_rows, chunk_alloc);
        }

        tables[table_idx] = .{
            .num_rows = num_rows,
            .fields = fields,
        };
    }

    var prng = try input.make_prng();
    const rand = prng.random();

    const table_schemas = try chunk_alloc.alloc(TableSchema, num_tables);
    const table_names = try chunk_alloc.alloc([:0]const u8, num_tables);
    for (0..num_tables) |table_idx| {
        const num_fields = tables[table_idx].fields.len;

        const has_minmax_index = try chunk_alloc.alloc(bool, num_fields);
        const data_types = try chunk_alloc.alloc(data_type.DataType, num_fields);
        const field_names = try chunk_alloc.alloc([:0]const u8, num_fields);

        for (0..num_fields) |field_idx| {
            const dt = try data_type.get_data_type(&tables[table_idx].fields[field_idx], chunk_alloc);

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
        .dicts = &.{},
    };

    const chunk = Chunk{
        .tables = tables,
        .schema = &schema,
        .dicts = &.{},
    };

    var header_arena = ArenaAllocator.init(alloc);
    defer header_arena.deinit();
    const header_alloc = header_arena.allocator();

    var filter_arena = ArenaAllocator.init(alloc);
    defer filter_arena.deinit();
    const filter_alloc = filter_arena.allocator();

    const page_size_kb = (try input.int(u32)) % (1 << 24);

    const data_section = try alloc.alloc(u8, 1 << 28);

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
            .data_section = data_section,
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
