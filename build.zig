const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addModule("parzig", .{
        .root_source_file = b.path("src/parzig.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "parzig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("parzig", lib);
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const generate = b.addExecutable(.{
        .name = "generate",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generate.root_module.addImport("parzig", lib);
    b.installArtifact(generate);

    const generate_cmd = b.addRunArtifact(generate);
    if (b.args) |args| {
        generate_cmd.addArgs(args);
    }
    const generate_step = b.step("generate", "Generate Zig file from parquet.thrift");
    generate_step.dependOn(&generate_cmd.step);

    const lib_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parzig.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const exe_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    const parquet_testing = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parquet_testing.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    parquet_testing.root_module.addImport("parzig", lib);
    const run_parquet_testing = b.addRunArtifact(parquet_testing);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_exe_unit_tests.step);
    test_step.dependOn(&run_parquet_testing.step);

    const test_lldb_step = b.step("test-lldb", "Debug unit tests with LLDB");
    const lldb = b.addSystemCommand(&.{"lldb"});
    lldb.addArtifactArg(parquet_testing);
    test_lldb_step.dependOn(&lldb.step);
}
