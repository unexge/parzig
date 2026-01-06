// Implements reading of Parquet's logical types.
//
// This module contains Parquet's logical types, their corresponding physical types,
// and how to convert logical types to physical types.
//
// See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

pub fn parse(comptime T: type) ?type {
    if (@typeInfo(T) == .optional) @compileError("This function doesn't support nullable types");

    if (T == Date) {
        return Date;
    } else {
        return null;
    }
}

pub const Date = struct {
    days_since_epoch: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?Date else []Date {
        return @ptrCast(physical);
    }
};

test "date logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/date_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const dates = try rg.readColumn(Date, 0);
    try std.testing.expectEqual(4, dates.len);

    try std.testing.expectEqual(@as(i32, 0), dates[0].days_since_epoch);
    try std.testing.expectEqual(@as(i32, 19737), dates[1].days_since_epoch);
    try std.testing.expectEqual(@as(i32, 18321), dates[2].days_since_epoch);
    try std.testing.expectEqual(@as(i32, 5679), dates[3].days_since_epoch);
}

test "date logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/date_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const dates = try rg.readColumn(?Date, 0);
    try std.testing.expectEqual(5, dates.len);

    try std.testing.expectEqual(@as(i32, 0), dates[0].?.days_since_epoch);
    try std.testing.expect(dates[1] == null);
    try std.testing.expectEqual(@as(i32, 19737), dates[2].?.days_since_epoch);
    try std.testing.expect(dates[3] == null);
    try std.testing.expectEqual(@as(i32, 18321), dates[4].?.days_since_epoch);
}

const std = @import("std");
const parquet_schema = @import("../generated/parquet.zig");
const File = @import("./File.zig");
