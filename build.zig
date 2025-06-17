const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const filterz = b.dependency("filterz", .{
        .target = target,
        .optimize = optimize,
    });

    const zstd = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    const lz4 = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });

    const mod = b.addModule("olive", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });
    mod.addImport("filterz", filterz.module("filterz"));
    mod.linkLibrary(zstd.artifact("zstd"));
    mod.linkLibrary(lz4.artifact("lz4"));

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
}
