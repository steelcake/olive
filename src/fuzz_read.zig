// const std = @import("std");
// const Allocator = std.mem.Allocator;
// const ArenaAllocator = std.heap.ArenaAllocator;

// const arrow = @import("arrow");
// const validate = arrow.validate.validate;

// fn to_fuzz(_: void, data: []const u8) anyerror!void {
//     var general_purpose_allocator: std.heap.GeneralPurposeAllocator(.{}) = .init;
//     const gpa = general_purpose_allocator.allocator();
//     defer {
//         switch (general_purpose_allocator.deinit()) {
//             .ok => {},
//             .leak => |l| {
//                 std.debug.panic("LEAK: {any}", .{l});
//             },
//         }
//     }

//     var arena = ArenaAllocator.init(gpa);
//     defer arena.deinit();
//     const alloc = arena.allocator();

//     const chunk = {
//         var scratch_arena = ArenaAllocator.init(gpa);
//         defer scratch_arena.deinit();
//         const scratch_alloc = scratch_arena.allocator();
//     };
// }

// test "fuzz" {
//     try std.testing.fuzz({}, to_fuzz, .{});
// }
