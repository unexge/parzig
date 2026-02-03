const std = @import("std");
const parzig = @import("parzig");
const build_options = @import("build_options");

const File = parzig.parquet.File;
const testing = std.testing;
const io = testing.io;
const Io = std.Io;

const ci_tests = build_options.ci_tests;

fn readAllRowGroups(file: *File) !void {
    for (file.metadata.row_groups, 0..) |rg_metadata, rg_idx| {
        var rg = file.rowGroup(rg_idx);

        for (rg_metadata.columns, 0..) |_, col_idx| {
            _ = try rg.readColumnDynamic(col_idx);
        }
    }
}

// =============================================================================
// Small datasets - always run
// =============================================================================

test "nyc taxi: green tripdata 2025-10" {
    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/nyc-taxi/green_tripdata_2025-10.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(1, file.metadata.row_groups.len);
    try testing.expectEqual(49416, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // VendorID (i32)
    const vendor_ids = try rg.readColumn(i32, 0);
    try testing.expectEqualSlices(i32, &[_]i32{ 2, 2, 2 }, vendor_ids[0..3]);

    // PULocationID (i32)
    const pu_location_ids = try rg.readColumn(i32, 5);
    try testing.expectEqualSlices(i32, &[_]i32{ 247, 66, 244 }, pu_location_ids[0..3]);

    // DOLocationID (i32)
    const do_location_ids = try rg.readColumn(i32, 6);
    try testing.expectEqualSlices(i32, &[_]i32{ 69, 25, 244 }, do_location_ids[0..3]);

    // passenger_count (i64, nullable)
    const passenger_counts = try rg.readColumn(?i64, 7);
    try testing.expectEqualSlices(?i64, &[_]?i64{ 1, 1, 1 }, passenger_counts[0..3]);

    // trip_distance (f64)
    const trip_distances = try rg.readColumn(f64, 8);
    try testing.expectEqualSlices(f64, &[_]f64{ 0.7, 1.61, 0.0 }, trip_distances[0..3]);

    // fare_amount (f64)
    const fare_amounts = try rg.readColumn(f64, 9);
    try testing.expectEqualSlices(f64, &[_]f64{ 5.8, 11.4, 10.0 }, fare_amounts[0..3]);

    try readAllRowGroups(&file);
}

test "nyc taxi: fhv tripdata 2025-10" {
    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/nyc-taxi/fhv_tripdata_2025-10.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(3, file.metadata.row_groups.len);
    try testing.expectEqual(2446615, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // dispatching_base_num (string)
    const base_nums = try rg.readColumn([]const u8, 0);
    try testing.expectEqualDeep(@as([]const u8, "B00009"), base_nums[0]);

    // Affiliated_base_number (string)
    const affiliated_base_nums = try rg.readColumn(?[]const u8, 6);
    try testing.expectEqualDeep(@as(?[]const u8, "B00009"), affiliated_base_nums[0]);

    try readAllRowGroups(&file);
}

// =============================================================================
// Big datasets - CI only
// =============================================================================

test "nyc taxi: yellow tripdata 2025-10 (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/nyc-taxi/yellow_tripdata_2025-10.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expect(file.metadata.num_rows > 0);
    try testing.expect(file.metadata.row_groups.len > 0);

    try readAllRowGroups(&file);
}

test "nyc taxi: fhvhv tripdata 2025-10 (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/nyc-taxi/fhvhv_tripdata_2025-10.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expect(file.metadata.num_rows > 0);
    try testing.expect(file.metadata.row_groups.len > 0);

    // TODO: This causes OOM on the CI. We probably need to have a seperate arena for each row group and de-allocate it between.
    // try readAllRowGroups(&file);
}

// =============================================================================
// ClickBench Dataset - CI only
// Source: https://github.com/ClickHouse/ClickBench
// 105 columns, real web analytics data, diverse types
// =============================================================================

test "clickbench: hits (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    const files = [_][]const u8{
        "testdata/public-datasets/clickbench/hits_0.parquet",
        "testdata/public-datasets/clickbench/hits_1.parquet",
        "testdata/public-datasets/clickbench/hits_2.parquet",
    };

    for (files) |path| {
        var reader_buf: [4096]u8 = undefined;
        var file_reader = (try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only })).reader(io, &reader_buf);
        var file = try File.read(testing.allocator, &file_reader);
        defer file.deinit();

        try testing.expectEqual(105, file.metadata.schema.len - 1); // -1 for root schema element
        try testing.expect(file.metadata.num_rows > 0);
        try testing.expect(file.metadata.row_groups.len > 0);

        try readAllRowGroups(&file);
    }
}
