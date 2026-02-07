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
// TPC-H SF1 Dataset
// Generated using DuckDB's TPC-H extension
// 8 tables, diverse types including DECIMAL and DATE
// =============================================================================

test "tpch sf1: region" {
    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/region.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(1, file.metadata.row_groups.len);
    try testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const keys = try rg.readColumn(i32, 0);
    try testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 3, 4 }, keys);

    const names = try rg.readColumn([]const u8, 1);
    try testing.expectEqualDeep(@as([]const u8, "AFRICA"), names[0]);
    try testing.expectEqualDeep(@as([]const u8, "AMERICA"), names[1]);
    try testing.expectEqualDeep(@as([]const u8, "ASIA"), names[2]);
    try testing.expectEqualDeep(@as([]const u8, "EUROPE"), names[3]);
    try testing.expectEqualDeep(@as([]const u8, "MIDDLE EAST"), names[4]);

    try readAllRowGroups(&file);
}

test "tpch sf1: nation" {
    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/nation.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(1, file.metadata.row_groups.len);
    try testing.expectEqual(25, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const keys = try rg.readColumn(i32, 0);
    try testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 3, 4 }, keys[0..5]);

    const names = try rg.readColumn([]const u8, 1);
    try testing.expectEqualDeep(@as([]const u8, "ALGERIA"), names[0]);
    try testing.expectEqualDeep(@as([]const u8, "ARGENTINA"), names[1]);
    try testing.expectEqualDeep(@as([]const u8, "BRAZIL"), names[2]);
    try testing.expectEqualDeep(@as([]const u8, "CANADA"), names[3]);
    try testing.expectEqualDeep(@as([]const u8, "EGYPT"), names[4]);
    try testing.expectEqualDeep(@as([]const u8, "UNITED STATES"), names[24]);

    const region_keys = try rg.readColumn(i32, 2);
    try testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 1, 1, 4 }, region_keys[0..5]);

    try readAllRowGroups(&file);
}

test "tpch sf1: supplier" {
    const Decimal = parzig.parquet.Decimal;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/supplier.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(1, file.metadata.row_groups.len);
    try testing.expectEqual(10000, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // s_suppkey (i64)
    const suppkeys = try rg.readColumn(i64, 0);
    try testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, suppkeys[0..3]);

    // s_name (string)
    const names = try rg.readColumn([]const u8, 1);
    try testing.expectEqualDeep(@as([]const u8, "Supplier#000000001"), names[0]);
    try testing.expectEqualDeep(@as([]const u8, "Supplier#000000002"), names[1]);

    // s_nationkey (i32)
    const nationkeys = try rg.readColumn(i32, 3);
    try testing.expectEqualSlices(i32, &[_]i32{ 17, 5, 1 }, nationkeys[0..3]);

    // s_acctbal (decimal(15,2))
    const acctbals = try rg.readColumn(Decimal, 5);
    try testing.expectApproxEqAbs(5755.94, @as(f64, @floatCast(acctbals[0].value)), 0.01);
    try testing.expectApproxEqAbs(4032.68, @as(f64, @floatCast(acctbals[1].value)), 0.01);
    try testing.expectApproxEqAbs(4192.40, @as(f64, @floatCast(acctbals[2].value)), 0.01);

    try readAllRowGroups(&file);
}

test "tpch sf1: lineitem (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    const Date = parzig.parquet.Date;
    const Decimal = parzig.parquet.Decimal;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/lineitem.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(49, file.metadata.row_groups.len);
    try testing.expectEqual(6001215, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // l_orderkey (i64)
    const orderkeys = try rg.readColumn(i64, 0);
    try testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1 }, orderkeys[0..3]);

    // l_quantity (decimal(15,2))
    const quantities = try rg.readColumn(Decimal, 4);
    try testing.expectApproxEqAbs(17.0, @as(f64, @floatCast(quantities[0].value)), 0.01);
    try testing.expectApproxEqAbs(36.0, @as(f64, @floatCast(quantities[1].value)), 0.01);
    try testing.expectApproxEqAbs(8.0, @as(f64, @floatCast(quantities[2].value)), 0.01);

    // l_returnflag (string)
    const returnflags = try rg.readColumn([]const u8, 8);
    try testing.expectEqualDeep(@as([]const u8, "N"), returnflags[0]);

    // l_shipdate (date)
    const shipdates = try rg.readColumn(Date, 10);
    try testing.expectEqual(9568, shipdates[0].days_since_epoch); // 1996-03-13
    try testing.expectEqual(9598, shipdates[1].days_since_epoch); // 1996-04-12
    try testing.expectEqual(9524, shipdates[2].days_since_epoch); // 1996-01-29

    // l_shipmode (string)
    const shipmodes = try rg.readColumn([]const u8, 14);
    try testing.expectEqualDeep(@as([]const u8, "TRUCK"), shipmodes[0]);
    try testing.expectEqualDeep(@as([]const u8, "MAIL"), shipmodes[1]);
    try testing.expectEqualDeep(@as([]const u8, "REG AIR"), shipmodes[2]);

    try readAllRowGroups(&file);
}

test "tpch sf1: orders (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/orders.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(13, file.metadata.row_groups.len);
    try testing.expectEqual(1500000, file.metadata.num_rows);

    try readAllRowGroups(&file);
}

test "tpch sf1: part (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    const Decimal = parzig.parquet.Decimal;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/part.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(2, file.metadata.row_groups.len);
    try testing.expectEqual(200000, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // p_partkey (i64)
    const partkeys = try rg.readColumn(i64, 0);
    try testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, partkeys[0..3]);

    // p_name (string)
    const names = try rg.readColumn([]const u8, 1);
    try testing.expectEqualDeep(@as([]const u8, "goldenrod lavender spring chocolate lace"), names[0]);

    // p_brand (string)
    const brands = try rg.readColumn([]const u8, 3);
    try testing.expectEqualDeep(@as([]const u8, "Brand#13"), brands[0]);

    // p_size (i32)
    const sizes = try rg.readColumn(i32, 5);
    try testing.expectEqualSlices(i32, &[_]i32{ 7, 1, 21 }, sizes[0..3]);

    // p_retailprice (decimal(15,2))
    const prices = try rg.readColumn(Decimal, 7);
    try testing.expectApproxEqAbs(901.0, @as(f64, @floatCast(prices[0].value)), 0.01);
    try testing.expectApproxEqAbs(902.0, @as(f64, @floatCast(prices[1].value)), 0.01);
    try testing.expectApproxEqAbs(903.0, @as(f64, @floatCast(prices[2].value)), 0.01);

    try readAllRowGroups(&file);
}

test "tpch sf1: partsupp (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    const Decimal = parzig.parquet.Decimal;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/partsupp.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(7, file.metadata.row_groups.len);
    try testing.expectEqual(800000, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // ps_partkey (i64)
    const partkeys = try rg.readColumn(i64, 0);
    try testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 1 }, partkeys[0..3]);

    // ps_suppkey (i64)
    const suppkeys = try rg.readColumn(i64, 1);
    try testing.expectEqualSlices(i64, &[_]i64{ 2, 2502, 5002 }, suppkeys[0..3]);

    // ps_availqty (i64)
    const qtys = try rg.readColumn(i64, 2);
    try testing.expectEqualSlices(i64, &[_]i64{ 3325, 8076, 3956 }, qtys[0..3]);

    // ps_supplycost (decimal(15,2))
    const costs = try rg.readColumn(Decimal, 3);
    try testing.expectApproxEqAbs(771.64, @as(f64, @floatCast(costs[0].value)), 0.01);
    try testing.expectApproxEqAbs(993.49, @as(f64, @floatCast(costs[1].value)), 0.01);
    try testing.expectApproxEqAbs(337.09, @as(f64, @floatCast(costs[2].value)), 0.01);

    try readAllRowGroups(&file);
}

test "tpch sf1: customer (ci only)" {
    if (!ci_tests) return error.SkipZigTest;

    const Decimal = parzig.parquet.Decimal;

    var reader_buf: [4096]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/public-datasets/tpch-sf1/customer.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(2, file.metadata.row_groups.len);
    try testing.expectEqual(150000, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    // c_custkey (i64)
    const custkeys = try rg.readColumn(i64, 0);
    try testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, custkeys[0..3]);

    // c_name (string)
    const names = try rg.readColumn([]const u8, 1);
    try testing.expectEqualDeep(@as([]const u8, "Customer#000000001"), names[0]);

    // c_nationkey (i32)
    const nationkeys = try rg.readColumn(i32, 3);
    try testing.expectEqualSlices(i32, &[_]i32{ 15, 13, 1 }, nationkeys[0..3]);

    // c_acctbal (decimal(15,2))
    const acctbals = try rg.readColumn(Decimal, 5);
    try testing.expectApproxEqAbs(711.56, @as(f64, @floatCast(acctbals[0].value)), 0.01);
    try testing.expectApproxEqAbs(121.65, @as(f64, @floatCast(acctbals[1].value)), 0.01);
    try testing.expectApproxEqAbs(7498.12, @as(f64, @floatCast(acctbals[2].value)), 0.01);

    // c_mktsegment (string)
    const segments = try rg.readColumn([]const u8, 6);
    try testing.expectEqualDeep(@as([]const u8, "BUILDING"), segments[0]);
    try testing.expectEqualDeep(@as([]const u8, "AUTOMOBILE"), segments[1]);

    try readAllRowGroups(&file);
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
