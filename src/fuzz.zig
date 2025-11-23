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

// fn fuzz_roundtrip(data: []const u8, alloc: Allocator) !void {
//     const StaticBufs = struct {
//         var data_section: ?[]u8 = null;
//         var header_bytes_buf: ?[]u8 = null;
//         var schema_bytes_buf: ?[]u8 = null;

//         fn get_data_section() []u8 {
//             if (data_section) |ds| {
//                 return ds;
//             } else {
//                 const ds = std.heap.page_allocator.alloc(u8, 1 << 28) catch unreachable;
//                 data_section = ds;
//                 return ds;
//             }
//         }

//         fn get_header_bytes_buf() []u8 {
//             if (header_bytes_buf) |b| {
//                 return b;
//             } else {
//                 const b = std.heap.page_allocator.alloc(u8, 1 << 20) catch unreachable;
//                 header_bytes_buf = b;
//                 return b;
//             }
//         }

//         fn get_schema_bytes_buf() []u8 {
//             if (schema_bytes_buf) |b| {
//                 return b;
//             } else {
//                 const b = std.heap.page_allocator.alloc(u8, 1 << 20) catch unreachable;
//                 schema_bytes_buf = b;
//                 return b;
//             }
//         }
//     };

//     var input = olive.fuzz_input.FuzzInput.init(data);

//     var chunk_arena = ArenaAllocator.init(alloc);
//     defer chunk_arena.deinit();
//     const chunk_alloc = chunk_arena.allocator();

//     const chunk = make_chunk: {
//         var scratch_arena = ArenaAllocator.init(alloc);
//         defer scratch_arena.deinit();
//         const scratch_alloc = scratch_arena.allocator();
//         break :make_chunk try input.make_chunk(chunk_alloc, scratch_alloc);
//     };

//     var header_arena = ArenaAllocator.init(alloc);
//     defer header_arena.deinit();
//     const header_alloc = header_arena.allocator();

//     var filter_arena = ArenaAllocator.init(alloc);
//     defer filter_arena.deinit();
//     const filter_alloc = filter_arena.allocator();

//     // min 1KB max 1 GB
//     const page_size = ((try input.inner.int(u32)) % (1 << 30)) + (1 << 10);

//     const data_section = StaticBufs.get_data_section();

//     const input_header = write: {
//         var scratch_arena = ArenaAllocator.init(alloc);
//         defer scratch_arena.deinit();
//         const scratch_alloc = scratch_arena.allocator();

//         break :write try olive.write.write(.{
//             .chunk = &chunk,
//             .compression = try input.make_chunk_compression(chunk.schema, chunk_alloc),
//             .header_alloc = header_alloc,
//             .filter_alloc = filter_alloc,
//             .scratch_alloc = scratch_alloc,
//             .data_section = data_section,
//             .page_size = page_size,
//         });
//     };

//     const schema_bytes_buf = StaticBufs.get_schema_bytes_buf();
//     const schema_bytes_len = try chunk.schema.serialize(schema_bytes_buf, 40);
//     const schema_bytes = schema_bytes_buf[0..schema_bytes_len];

//     const header_bytes_buf = StaticBufs.get_header_bytes_buf();
//     const header_bytes_len = try input_header.serialize(header_bytes_buf, 40);
//     const header_bytes = header_bytes_buf[0..header_bytes_len];

//     var out_arena = ArenaAllocator.init(alloc);
//     defer out_arena.deinit();
//     const out_alloc = out_arena.allocator();

//     const out_header = try olive.header.Header.deserialize(header_bytes, out_alloc, 40);
//     const out_schema = try olive.schema.Schema.deserialize(schema_bytes, out_alloc, 40);
//     std.debug.assert(chunk.schema.eql(&out_schema));

//     const out_chunk = read: {
//         var scratch_arena = ArenaAllocator.init(alloc);
//         defer scratch_arena.deinit();
//         const scratch_alloc = scratch_arena.allocator();

//         break :read try olive.read.read(.{
//             .data_section = data_section[0..out_header.data_section_size],
//             .scratch_alloc = scratch_alloc,
//             .schema = chunk.schema,
//             .alloc = out_alloc,
//             .header = &out_header,
//         });
//     };

//     std.debug.assert(out_chunk.tables.len == chunk.tables.len);
//     for (out_chunk.tables, chunk.tables) |out_table, chunk_table| {
//         std.debug.assert(out_table.num_rows == chunk_table.num_rows);

//         for (out_table.fields, chunk_table.fields) |*out_field, *chunk_field| {
//             try arrow.validate.validate(out_field);
//             try arrow.validate.validate(chunk_field);
//             arrow.equals.equals(out_field, chunk_field);
//         }
//     }

//     const out_tables = try out_chunk.to_arrow(out_alloc);
//     const tables = try chunk.to_arrow(out_alloc);

//     std.debug.assert(out_tables.len == tables.len);
//     for (out_tables, tables) |out_table, table| {
//         std.debug.assert(out_table.len == table.len);

//         for (out_table, table) |*out_field, *field| {
//             try arrow.validate.validate(out_field);
//             try arrow.validate.validate(field);
//             arrow.equals.equals(out_field, field);
//         }
//     }
// }

// test "fuzz roundtrip" {
//     try FuzzWrap(fuzz_roundtrip, 1 << 25).run();
// }
