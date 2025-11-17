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

    const arrow = b.dependency("arrow", .{
        .target = target,
        .optimize = optimize,
    });

    const borsh = b.dependency("borsh", .{
        .target = target,
        .optimize = optimize,
    });

    const fuzzin = b.dependency("fuzzin", .{
        .target = target,
        .optimize = optimize,
    });

    const mod = b.addModule("olive", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("filterz", filterz.module("filterz"));
    mod.linkLibrary(zstd.artifact("zstd"));
    mod.linkLibrary(lz4.artifact("lz4"));
    mod.addImport("arrow", arrow.module("arrow"));
    mod.addImport("borsh", borsh.module("borsh"));
    mod.addImport("fuzzin", fuzzin.module("fuzzin"));

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const fuzz = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });
    fuzz.root_module.addImport("olive", mod);
    fuzz.root_module.addImport("arrow", arrow.module("arrow"));
    fuzz.root_module.addImport("fuzzin", fuzzin.module("fuzzin"));

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step("fuzz", "run fuzz tests");
    fuzz_step.dependOn(&run_fuzz.step);
}
