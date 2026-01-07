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
    } else if (T == TimestampMillis) {
        return TimestampMillis;
    } else if (T == TimestampMicros) {
        return TimestampMicros;
    } else if (T == TimestampNanos) {
        return TimestampNanos;
    } else if (T == TimeMillis) {
        return TimeMillis;
    } else if (T == TimeMicros) {
        return TimeMicros;
    } else if (T == TimeNanos) {
        return TimeNanos;
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

pub const TimestampMillis = struct {
    millis_since_epoch: i64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimestampMillis else []TimestampMillis {
        return @ptrCast(physical);
    }
};

pub const TimestampMicros = struct {
    micros_since_epoch: i64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimestampMicros else []TimestampMicros {
        return @ptrCast(physical);
    }
};

pub const TimestampNanos = struct {
    nanos_since_epoch: i64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimestampNanos else []TimestampNanos {
        return @ptrCast(physical);
    }
};

pub const TimeMillis = struct {
    millis_since_midnight: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimeMillis else []TimeMillis {
        return @ptrCast(physical);
    }
};

pub const TimeMicros = struct {
    micros_since_midnight: i64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimeMicros else []TimeMicros {
        return @ptrCast(physical);
    }
};

pub const TimeNanos = struct {
    nanos_since_midnight: i64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?TimeNanos else []TimeNanos {
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

test "timestamp millis logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_millis_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(TimestampMillis, 0);
    try std.testing.expectEqual(4, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].millis_since_epoch);
    try std.testing.expectEqual(@as(i64, 1705321845000), timestamps[1].millis_since_epoch);
    try std.testing.expectEqual(@as(i64, 1584692130000), timestamps[2].millis_since_epoch);
    try std.testing.expectEqual(@as(i64, 489869100000), timestamps[3].millis_since_epoch);
}

test "timestamp millis logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_millis_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(?TimestampMillis, 0);
    try std.testing.expectEqual(5, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].?.millis_since_epoch);
    try std.testing.expect(timestamps[1] == null);
    try std.testing.expectEqual(@as(i64, 1705321845000), timestamps[2].?.millis_since_epoch);
    try std.testing.expect(timestamps[3] == null);
    try std.testing.expectEqual(@as(i64, 1584692130000), timestamps[4].?.millis_since_epoch);
}

test "timestamp micros logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_micros_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(TimestampMicros, 0);
    try std.testing.expectEqual(4, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].micros_since_epoch);
    try std.testing.expectEqual(@as(i64, 1705321845123456), timestamps[1].micros_since_epoch);
    try std.testing.expectEqual(@as(i64, 1584692130987654), timestamps[2].micros_since_epoch);
    try std.testing.expectEqual(@as(i64, 489869100555555), timestamps[3].micros_since_epoch);
}

test "timestamp micros logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_micros_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(?TimestampMicros, 0);
    try std.testing.expectEqual(5, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].?.micros_since_epoch);
    try std.testing.expect(timestamps[1] == null);
    try std.testing.expectEqual(@as(i64, 1705321845123456), timestamps[2].?.micros_since_epoch);
    try std.testing.expect(timestamps[3] == null);
    try std.testing.expectEqual(@as(i64, 1584692130987654), timestamps[4].?.micros_since_epoch);
}

test "timestamp nanos logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_nanos_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(TimestampNanos, 0);
    try std.testing.expectEqual(4, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].nanos_since_epoch);
    try std.testing.expectEqual(@as(i64, 1705321845123456000), timestamps[1].nanos_since_epoch);
    try std.testing.expectEqual(@as(i64, 1584692130987654000), timestamps[2].nanos_since_epoch);
    try std.testing.expectEqual(@as(i64, 489869100555555000), timestamps[3].nanos_since_epoch);
}

test "timestamp nanos logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/timestamp_nanos_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const timestamps = try rg.readColumn(?TimestampNanos, 0);
    try std.testing.expectEqual(5, timestamps.len);

    try std.testing.expectEqual(@as(i64, 0), timestamps[0].?.nanos_since_epoch);
    try std.testing.expect(timestamps[1] == null);
    try std.testing.expectEqual(@as(i64, 1705321845123456000), timestamps[2].?.nanos_since_epoch);
    try std.testing.expect(timestamps[3] == null);
    try std.testing.expectEqual(@as(i64, 1584692130987654000), timestamps[4].?.nanos_since_epoch);
}

test "time millis logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_millis_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(TimeMillis, 0);
    try std.testing.expectEqual(4, times.len);

    try std.testing.expectEqual(@as(i32, 0), times[0].millis_since_midnight);
    try std.testing.expectEqual(@as(i32, 45045000), times[1].millis_since_midnight);
    try std.testing.expectEqual(@as(i32, 29730000), times[2].millis_since_midnight);
    try std.testing.expectEqual(@as(i32, 67500000), times[3].millis_since_midnight);
}

test "time millis logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_millis_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(?TimeMillis, 0);
    try std.testing.expectEqual(5, times.len);

    try std.testing.expectEqual(@as(i32, 0), times[0].?.millis_since_midnight);
    try std.testing.expect(times[1] == null);
    try std.testing.expectEqual(@as(i32, 45045000), times[2].?.millis_since_midnight);
    try std.testing.expect(times[3] == null);
    try std.testing.expectEqual(@as(i32, 29730000), times[4].?.millis_since_midnight);
}

test "time micros logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_micros_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(TimeMicros, 0);
    try std.testing.expectEqual(4, times.len);

    try std.testing.expectEqual(@as(i64, 0), times[0].micros_since_midnight);
    try std.testing.expectEqual(@as(i64, 45045123456), times[1].micros_since_midnight);
    try std.testing.expectEqual(@as(i64, 29730987654), times[2].micros_since_midnight);
    try std.testing.expectEqual(@as(i64, 67500555555), times[3].micros_since_midnight);
}

test "time micros logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_micros_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(?TimeMicros, 0);
    try std.testing.expectEqual(5, times.len);

    try std.testing.expectEqual(@as(i64, 0), times[0].?.micros_since_midnight);
    try std.testing.expect(times[1] == null);
    try std.testing.expectEqual(@as(i64, 45045123456), times[2].?.micros_since_midnight);
    try std.testing.expect(times[3] == null);
    try std.testing.expectEqual(@as(i64, 29730987654), times[4].?.micros_since_midnight);
}

test "time nanos logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_nanos_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(TimeNanos, 0);
    try std.testing.expectEqual(4, times.len);

    try std.testing.expectEqual(@as(i64, 0), times[0].nanos_since_midnight);
    try std.testing.expectEqual(@as(i64, 45045123456789), times[1].nanos_since_midnight);
    try std.testing.expectEqual(@as(i64, 29730987654321), times[2].nanos_since_midnight);
    try std.testing.expectEqual(@as(i64, 67500555555555), times[3].nanos_since_midnight);
}

test "time nanos logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/time_nanos_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const times = try rg.readColumn(?TimeNanos, 0);
    try std.testing.expectEqual(5, times.len);

    try std.testing.expectEqual(@as(i64, 0), times[0].?.nanos_since_midnight);
    try std.testing.expect(times[1] == null);
    try std.testing.expectEqual(@as(i64, 45045123456789), times[2].?.nanos_since_midnight);
    try std.testing.expect(times[3] == null);
    try std.testing.expectEqual(@as(i64, 29730987654321), times[4].?.nanos_since_midnight);
}

const std = @import("std");
const parquet_schema = @import("../generated/parquet.zig");
const File = @import("./File.zig");
