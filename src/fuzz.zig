const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const olive = @import("olive");
const arrow = @import("arrow");
const fuzzin = @import("fuzzin");

const FuzzInput = fuzzin.FuzzInput;

fn fuzz_read(ctx: void, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = ctx;

    var arena = ArenaAllocator.init(dbg_alloc);
    defer arena.deinit();
    var limited_alloc = fuzzin.LimitedAllocator.init(arena.allocator(), 1 << 20);
    const alloc = limited_alloc.allocator();

    const schema = try olive.fuzz_input.schema(input, alloc);
    var head = try input.auto(olive.header.Header, alloc, 12);
    const data_section = input.all_bytes();
    head.data_section_size = @intCast(data_section.len);

    const out_chunk = read: {
        var scratch_arena = std.heap.ArenaAllocator.init(dbg_alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :read olive.read.read(.{
            .alloc = arena.allocator(),
            .header = &head,
            .schema = &schema,
            .scratch_alloc = scratch_alloc,
            .data_section = data_section,
        }) catch return;
    };

    for (out_chunk.tables) |out_table| {
        for (out_table) |*out_field| {
            arrow.validate.validate_array(out_field) catch unreachable;
        }
    }

    const out_tables = try out_chunk.to_arrow(arena.allocator());

    for (out_tables) |out_table| {
        for (out_table) |*out_field| {
            arrow.validate.validate_array(out_field) catch unreachable;
        }
    }
}

test fuzz_read {
    fuzzin.fuzz_test(
        void,
        {},
        fuzz_read,
        1 << 26,
    );
}

fn fuzz_roundtrip(
    data_section_buf: []u8,
    input: *FuzzInput,
    dbg_alloc: Allocator,
) fuzzin.Error!void {
    var arena = ArenaAllocator.init(dbg_alloc);
    defer arena.deinit();
    var limited_alloc_v = fuzzin.LimitedAllocator.init(arena.allocator(), 1 << 20);
    const limited_alloc = limited_alloc_v.allocator();

    const schema = try olive.fuzz_input.schema(input, limited_alloc);
    const arrow_chunk = try olive.fuzz_input.arrow_chunk(&schema, input, limited_alloc);

    const alloc = arena.allocator();

    const olive_chunk = make_chunk: {
        var scratch_arena = std.heap.ArenaAllocator.init(dbg_alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :make_chunk olive.chunk.Chunk.from_arrow(
            &schema,
            arrow_chunk,
            alloc,
            scratch_alloc,
        ) catch unreachable;
    };

    // min 1B max 1 GB
    const page_size = ((try input.int(u32)) % (1 << 30)) + 1;

    const compression_bias: olive.write.CompressionBias = if (try input.boolean())
        .balanced
    else
        .read_optimized;

    const header = write: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :write olive.write.write(.{
            .chunk = &olive_chunk,
            .header_alloc = alloc,
            .scratch_alloc = scratch_alloc,
            .data_section = data_section_buf,
            .page_size = page_size,
            .compression_bias = compression_bias,
        }) catch unreachable;
    };

    const data_section = data_section_buf[0..header.data_section_size];

    const out_chunk = read: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();

        break :read try olive.read.read(.{
            .data_section = data_section,
            .scratch_alloc = scratch_alloc,
            .schema = &schema,
            .alloc = alloc,
            .header = &header,
        });
    };

    std.debug.assert(out_chunk.tables.len == olive_chunk.tables.len);
    for (out_chunk.tables, olive_chunk.tables) |out_table, chunk_table| {
        for (out_table, chunk_table) |*out_field, *chunk_field| {
            try arrow.validate.validate_array(out_field);
            try arrow.validate.validate_array(chunk_field);
            arrow.equals.equals(out_field, chunk_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(alloc);
    const olive_tables = try olive_chunk.to_arrow(alloc);

    std.debug.assert(out_tables.len == olive_tables.len);
    for (out_tables, olive_tables) |out_table, table| {
        std.debug.assert(out_table.len == table.len);

        for (out_table, table) |*out_field, *field| {
            try arrow.validate.validate_array(out_field);
            try arrow.validate.validate_array(field);
            arrow.equals.equals(out_field, field);
        }
    }
}

test fuzz_roundtrip {
    const data_section_buf = try std.heap.page_allocator.alloc(u8, 1 << 26);
    fuzzin.fuzz_test(
        []u8,
        data_section_buf,
        fuzz_roundtrip,
        0,
    );
}
