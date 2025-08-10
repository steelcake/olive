const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const validate = arrow.validate.validate;

const schema = @import("./schema.zig");

fn to_fuzz(_: void, input: []const u8) anyerror!void {
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

    const de_buf = try gpa.alloc(u8, 1 << 14);
    defer gpa.free(de_buf);

    var fb_alloc = std.heap.FixedBufferAllocator.init(de_buf);
    const alloc = fb_alloc.allocator();

    _ = schema.Schema.deserialize(input, alloc, 30) catch {};
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz, .{});
}
