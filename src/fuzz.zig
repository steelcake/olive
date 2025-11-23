const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const olive = @import("olive");
const arrow = @import("arrow");
const fuzzin = @import("fuzzin");

fn fuzz_read(data: []const u8, gpa: Allocator) !void {
    var input = olive.fuzz_input.FuzzInput.init(data);

    var arena = ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

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

test "fuzz read" {
    try FuzzWrap(fuzz_read, 1 << 25).run();
}

fn fuzz_roundtrip(data: []const u8, alloc: Allocator) !void {
    const StaticBufs = struct {
        var data_section: ?[]u8 = null;
        var header_bytes_buf: ?[]u8 = null;
        var schema_bytes_buf: ?[]u8 = null;

        fn get_data_section() []u8 {
            if (data_section) |ds| {
                return ds;
            } else {
                const ds = std.heap.page_allocator.alloc(u8, 1 << 28) catch unreachable;
                data_section = ds;
                return ds;
            }
        }

        fn get_header_bytes_buf() []u8 {
            if (header_bytes_buf) |b| {
                return b;
            } else {
                const b = std.heap.page_allocator.alloc(u8, 1 << 20) catch unreachable;
                header_bytes_buf = b;
                return b;
            }
        }

        fn get_schema_bytes_buf() []u8 {
            if (schema_bytes_buf) |b| {
                return b;
            } else {
                const b = std.heap.page_allocator.alloc(u8, 1 << 20) catch unreachable;
                schema_bytes_buf = b;
                return b;
            }
        }
    };

    var input = olive.fuzz_input.FuzzInput.init(data);

    var chunk_arena = ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();
    const chunk_alloc = chunk_arena.allocator();

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

    // min 1KB max 1 GB
    const page_size = ((try input.inner.int(u32)) % (1 << 30)) + (1 << 10);

    const data_section = StaticBufs.get_data_section();

    const input_header = write: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :write try olive.write.write(.{
            .chunk = &chunk,
            .compression = try input.make_chunk_compression(chunk.schema, chunk_alloc),
            .header_alloc = header_alloc,
            .filter_alloc = filter_alloc,
            .scratch_alloc = scratch_alloc,
            .data_section = data_section,
            .page_size = page_size,
        });
    };

    const schema_bytes_buf = StaticBufs.get_schema_bytes_buf();
    const schema_bytes_len = try chunk.schema.serialize(schema_bytes_buf, 40);
    const schema_bytes = schema_bytes_buf[0..schema_bytes_len];

    const header_bytes_buf = StaticBufs.get_header_bytes_buf();
    const header_bytes_len = try input_header.serialize(header_bytes_buf, 40);
    const header_bytes = header_bytes_buf[0..header_bytes_len];

    var out_arena = ArenaAllocator.init(alloc);
    defer out_arena.deinit();
    const out_alloc = out_arena.allocator();

    const out_header = try olive.header.Header.deserialize(header_bytes, out_alloc, 40);
    const out_schema = try olive.schema.Schema.deserialize(schema_bytes, out_alloc, 40);
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

test "fuzz roundtrip" {
    try FuzzWrap(fuzz_roundtrip, 1 << 25).run();
}

fn FuzzWrap(comptime fuzz_one: fn (data: []const u8, gpa: Allocator) anyerror!void, comptime alloc_size: comptime_int) type {
    const FuzzContext = struct {
        fb_alloc: *FixedBufferAllocator,
    };

    return struct {
        fn run_one(ctx: FuzzContext, data: []const u8) anyerror!void {
            ctx.fb_alloc.reset();

            var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{
                .backing_allocator_zeroes = false,
            }){
                .backing_allocator = ctx.fb_alloc.allocator(),
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

        fn run() !void {
            var fb_alloc = FixedBufferAllocator.init(std.heap.page_allocator.alloc(u8, alloc_size) catch unreachable);
            try std.testing.fuzz(FuzzContext{
                .fb_alloc = &fb_alloc,
            }, run_one, .{});
        }
    };
}
