const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const olive = @import("olive");
const arrow = @import("arrow");

fn fuzz_schema_de(data: []const u8, gpa: Allocator) !void {
    const de_buf = try gpa.alloc(u8, 1 << 14);
    defer gpa.free(de_buf);

    var fb_alloc = std.heap.FixedBufferAllocator.init(de_buf);
    const alloc = fb_alloc.allocator();

    const sch = olive.schema.Schema.deserialize(data, alloc, 30) catch return;
    sch.validate() catch return;
}

fn fuzz_read(data: []const u8, gpa: Allocator) !void {
    var input = olive.fuzz_input.FuzzInput.init(data);

    const alloc_buf = try gpa.alloc(u8, 1 << 28);
    defer gpa.free(alloc_buf);

    var fba = std.heap.FixedBufferAllocator.init(alloc_buf);
    const alloc = fba.allocator();

    const head = try input.make_header(alloc);

    const schema = make_schema: {
        var scratch_arena = std.heap.ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();
        break :make_schema try input.make_schema(alloc, scratch_alloc);
    };

    const out_chunk = read: {
        var scratch_arena = std.heap.ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        var h = head;

        h.data_section_size = @intCast(@min(input.inner.data.len, std.math.maxInt(u32)));

        break :read olive.read.read(.{
            .alloc = alloc,
            .header = &h,
            .schema = &schema,
            .scratch_alloc = scratch_alloc,
            .data_section = input.inner.data[0..h.data_section_size],
        }) catch return;
    };

    for (out_chunk.tables) |out_table| {
        for (out_table.fields) |*out_field| {
            try arrow.validate.validate(out_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(alloc);

    for (out_tables) |out_table| {
        for (out_table) |*out_field| {
            try arrow.validate.validate(out_field);
        }
    }
}

fn fuzz_header_de(data: []const u8, gpa: Allocator) !void {
    const de_buf = try gpa.alloc(u8, 1 << 14);
    defer gpa.free(de_buf);

    var fb_alloc = std.heap.FixedBufferAllocator.init(de_buf);
    const alloc = fb_alloc.allocator();

    _ = olive.header.Header.deserialize(data, alloc, 30) catch {};
}

fn fuzz_roundtrip(data: []const u8, alloc: Allocator) !void {
    var input = olive.fuzz_input.FuzzInput.init(data);

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

        break :write try olive.write.write(.{
            .chunk = &chunk,
            .compression = .lz4,
            .header_alloc = header_alloc,
            .filter_alloc = filter_alloc,
            .scratch_alloc = scratch_alloc,
            .data_section = data_section,
            .page_size_kb = page_size_kb,
        });
    };

    const schema_bytes_buf = try alloc.alloc(u8, 1 << 19);
    defer alloc.free(schema_bytes_buf);
    const schema_bytes_len = try chunk.schema.serialize(schema_bytes_buf, 40);
    const schema_bytes = schema_bytes_buf[0..schema_bytes_len];

    const header_bytes_buf = try alloc.alloc(u8, 1 << 19);
    defer alloc.free(header_bytes_buf);
    const header_bytes_len = try input_header.serialize(header_bytes_buf, 40);
    const header_bytes = header_bytes_buf[0..header_bytes_len];

    var out_arena = ArenaAllocator.init(alloc);
    defer out_arena.deinit();
    const out_alloc = out_arena.allocator();

    var out_header_fb_alloc = std.heap.FixedBufferAllocator.init(try out_alloc.alloc(u8, 1 << 19));
    const out_header_alloc = out_header_fb_alloc.allocator();
    const out_header = try olive.header.Header.deserialize(header_bytes, out_header_alloc, 40);

    var out_schema_fb_alloc = std.heap.FixedBufferAllocator.init(try out_alloc.alloc(u8, 1 << 19));
    const out_schema_alloc = out_schema_fb_alloc.allocator();
    const out_schema = try olive.schema.Schema.deserialize(schema_bytes, out_schema_alloc, 40);
    std.debug.assert(chunk.schema.eql(&out_schema));

    const out_chunk = read: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :read try olive.read.read(.{
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
            try arrow.validate.validate(out_field);
            try arrow.validate.validate(chunk_field);
            arrow.equals.equals(out_field, chunk_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(out_alloc);
    const tables = try chunk.to_arrow(out_alloc);

    std.debug.assert(out_tables.len == tables.len);
    for (out_tables, tables) |out_table, table| {
        std.debug.assert(out_table.len == table.len);

        for (out_table, table) |*out_field, *field| {
            try arrow.validate.validate(out_field);
            try arrow.validate.validate(field);
            arrow.equals.equals(out_field, field);
        }
    }
}

const FuzzContext = struct {
    fb_alloc: *FixedBufferAllocator,
};

fn run_fuzz_test(comptime fuzz_one: fn (data: []const u8, gpa: Allocator) anyerror!void, data: []const u8, fb_alloc: *FixedBufferAllocator) anyerror!void {
    fb_alloc.reset();

    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{
        .backing_allocator_zeroes = false,
    }){
        .backing_allocator = fb_alloc.allocator(),
    };
    const gpa = general_purpose_allocator.allocator();
    defer {
        switch (general_purpose_allocator.deinit()) {
            .ok => {},
            .leak => |l| {
                std.debug.panic("LEAK: {any}", .{l});
            },
        }
    }

    fuzz_one(data, gpa) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

fn to_fuzz_wrap(ctx: FuzzContext, data: []const u8) anyerror!void {
    try run_fuzz_test(fuzz_roundtrip, data, ctx.fb_alloc);
    try run_fuzz_test(fuzz_header_de, data, ctx.fb_alloc);
    try run_fuzz_test(fuzz_read, data, ctx.fb_alloc);
    try run_fuzz_test(fuzz_schema_de, data, ctx.fb_alloc);
}

test "fuzz" {
    var fb_alloc = FixedBufferAllocator.init(std.heap.page_allocator.alloc(u8, 1 << 30) catch unreachable);
    try std.testing.fuzz(FuzzContext{
        .fb_alloc = &fb_alloc,
    }, to_fuzz_wrap, .{});
}
