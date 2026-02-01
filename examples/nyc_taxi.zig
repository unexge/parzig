const std = @import("std");
const parzig = @import("parzig");

const Io = std.Io;
const File = parzig.parquet.File;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var reader_buf: [4096]u8 = undefined;
    const f = try Io.Dir.cwd().openFile(io, "testdata/public-datasets/nyc-taxi/green_tripdata_2025-10.parquet", .{ .mode = .read_only });
    var file_reader = f.reader(io, &reader_buf);

    var file = try parzig.parquet.File.read(allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);

    // Static typing: specify column type at compile time
    const fares = try rg.readColumn(f64, columnIndex(&file, "fare_amount"));
    const tips = try rg.readColumn(f64, columnIndex(&file, "tip_amount"));
    const passengers = try rg.readColumn(?i64, columnIndex(&file, "passenger_count")); // nullable

    var total_fare: f64 = 0;
    var total_tips: f64 = 0;
    var total_passengers: i64 = 0;

    for (fares, tips, passengers) |fare, tip, passenger| {
        total_fare += fare;
        total_tips += tip;
        if (passenger) |p| total_passengers += p;
    }

    std.debug.print("Total rides: {}\n", .{file.metadata.num_rows});
    std.debug.print("Total fares: ${d:.2}\n", .{total_fare});
    std.debug.print("Total tips: ${d:.2}\n", .{total_tips});
    std.debug.print("Total passengers: {}\n", .{total_passengers});

    // Dynamic typing: type determined at runtime
    const dynamic = try rg.readColumnDynamic(columnIndex(&file, "fare_amount"));
    switch (dynamic) {
        .double => |values| std.debug.print("First fare: ${d:.2}\n", .{values[0].?}),
        else => unreachable,
    }
}

fn columnIndex(file: *File, name: []const u8) usize {
    return file.findSchemaElement(&.{name}).?.column_index;
}
