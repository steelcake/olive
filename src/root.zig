const std = @import("std");
const xorf = @import("filterz").xorf;
const arrow = @import("arrow");

extern fn ZSTD_versionNumber() callconv(.C) c_uint;
extern fn LZ4_versionNumber() callconv(.C) c_int;

test "fa" {
    const alloc = std.testing.allocator;
    const keys: []const u64 = &.{ 1, 2, 3, 4, 5 };
    const arity = 3;
    const Fingerprint = u16;
    var header = xorf.calculate_header(arity, @intCast(keys.len));
    const fingerprints = try alloc.alloc(Fingerprint, header.array_length);
    defer alloc.free(fingerprints);
    try xorf.construct_fingerprints(Fingerprint, arity, fingerprints, alloc, keys, &header);

    std.log.warn("{any}", .{fingerprints});

    std.log.warn("{d}", .{ZSTD_versionNumber()});

    std.log.warn("{d}", .{LZ4_versionNumber()});
}
