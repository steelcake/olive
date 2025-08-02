const std = @import("std");

const arrow = @import("arrow");
const validate = arrow.validate.validate;

const header = @import("./header.zig");
const FuzzInput = @import("./fuzz_input.zig").FuzzInput;
const read = @import("./read.zig");

fn to_fuzz(data: []const u8) anyerror!void {
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
        h.data_section_size = input.inner.data.len;
        break :read read.read(.{
            .alloc = alloc,
            .header = &h,
            .schema = &schema,
            .scratch_alloc = scratch_alloc,
            .data_section = input.inner.data,
        }) catch return;
    };

    for (out_chunk.tables) |out_table| {
        for (out_table.fields) |*out_field| {
            try validate(out_field);
        }
    }

    const out_tables = try out_chunk.to_arrow(alloc);

    for (out_tables) |out_table| {
        for (out_table) |*out_field| {
            try validate(out_field);
        }
    }
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
