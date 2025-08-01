const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;

const borsh = @import("borsh");

const arrow = @import("arrow");
const validate = arrow.validate.validate;

const chunk_mod = @import("./chunk.zig");
const Chunk = chunk_mod.Chunk;
const write = @import("./write.zig");
const read = @import("./read.zig");
const schema_mod = @import("./schema.zig");
const DatasetSchema = schema_mod.DatasetSchema;
const TableSchema = schema_mod.TableSchema;
const dict_impl = @import("./dict.zig");
const header_mod = @import("./header.zig");
const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn roundtrip_test(input: *FuzzInput, alloc: Allocator) !void {
    const chunk_buf = try alloc.alloc(u8, 1 << 24);
    defer alloc.free(chunk_buf);
    var chunk_fb_alloc = FixedBufferAllocator.init(chunk_buf);
    const chunk_alloc = chunk_fb_alloc.allocator();

    const chunk = make_chunk: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();
        break :make_chunk try input.make_chunk(chunk_alloc, scratch_alloc);
    };

    var header_arena = ArenaAllocator.init(alloc);
    defer header_arena.deinit();
    const header_alloc = header_arena.allocator();

    var filter_arena = ArenaAllocator.init(alloc);
    defer filter_arena.deinit();
    const filter_alloc = filter_arena.allocator();

    // max should be 16 MB since the number is in KB and min should be 1 KB
    const page_size_kb = (try input.inner.int(u32)) % (1 << 14) + 1;

    const data_section = try alloc.alloc(u8, 1 << 28);
    defer alloc.free(data_section);

    const input_header = write: {
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

    const header_bytes_buf = try alloc.alloc(u8, 1 << 14);
    defer alloc.free(header_bytes_buf);
    const header_bytes_len = try borsh.serde.serialize(header_mod.Header, &input_header, header_bytes_buf, 20);
    const header_bytes = header_bytes_buf[0..header_bytes_len];

    var out_arena = ArenaAllocator.init(alloc);
    defer out_arena.deinit();
    const out_alloc = out_arena.allocator();

    var out_header_fb_alloc = std.heap.FixedBufferAllocator.init(try out_alloc.alloc(u8, 1 << 14));
    const out_header_alloc = out_header_fb_alloc.allocator();
    const out_header = try borsh.serde.deserialize(header_mod.Header, header_bytes, out_header_alloc, 20);

    const out_chunk = read: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :read try read.read(.{
            .data_section = data_section[0..out_header.data_section_size],
            .scratch_alloc = scratch_alloc,
            .schema = chunk.schema,
            .alloc = out_alloc,
            .header = &out_header,
        });
    };

    std.debug.assert(out_chunk.tables.len == chunk.tables.len);
    for (out_chunk.tables, chunk.tables) |out_table, chunk_table| {
        std.debug.assert(out_table.num_rows == chunk_table.num_rows);

        for (out_table.fields, chunk_table.fields) |*out_field, *chunk_field| {
            try validate(out_field);
            try validate(chunk_field);

            arrow.equals.equals(out_field, chunk_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(out_alloc);
    const tables = try chunk.to_arrow(out_alloc);

    std.debug.assert(out_tables.len == tables.len);
    for (out_tables, tables) |out_table, table| {
        std.debug.assert(out_table.len == table.len);

        for (out_table, table) |*out_field, *field| {
            try validate(out_field);
            try validate(field);

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

    var input = FuzzInput.init(data);

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
