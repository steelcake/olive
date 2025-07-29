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

    const deps = Dependencies{
        .filterz = filterz,
        .zstd = zstd,
        .lz4 = lz4,
        .arrow = arrow,
        .borsh = borsh,
    };

    const mod = b.addModule("olive", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    deps.add_to_module(mod);

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // Have to have single fuzz entrypoint until this is solved: https://github.com/ziglang/zig/issues/23738
    // So create one target per fuzz target using this function and run the commands seperately.
    add_fuzz_target(b, deps, "fuzz_roundtrip", "src/fuzz_roundtrip.zig", "run fuzz tests for read/write roundtrip", target, optimize);
    add_fuzz_target(b, deps, "fuzz_read", "src/fuzz_read.zig", "run fuzz tests for read", target, optimize);
}

fn add_fuzz_target(b: *std.Build, deps: Dependencies, command_name: []const u8, root_source_file: []const u8, description: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const mod = b.createModule(.{
        .root_source_file = b.path(root_source_file),
        .target = target,
        .optimize = optimize,
    });
    deps.add_to_module(mod);
    const fuzz = b.addTest(.{
        .root_module = mod,
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step(command_name, description);
    fuzz_step.dependOn(&run_fuzz.step);
}

const Dependencies = struct {
    filterz: *std.Build.Dependency,
    arrow: *std.Build.Dependency,
    lz4: *std.Build.Dependency,
    zstd: *std.Build.Dependency,
    borsh: *std.Build.Dependency,

    fn add_to_module(deps: *const Dependencies, mod: *std.Build.Module) void {
        mod.addImport("filterz", deps.filterz.module("filterz"));
        mod.linkLibrary(deps.zstd.artifact("zstd"));
        mod.linkLibrary(deps.lz4.artifact("lz4"));
        mod.addImport("arrow", deps.arrow.module("arrow"));
        mod.addImport("borsh", deps.borsh.module("borsh"));
    }
};
