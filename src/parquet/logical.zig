// Implements reading of Parquet's logical types.
//
// This module contains Parquet's logical types, their corresponding physical types,
// and how to convert logical types to physical types.
//
// This module aims to provide zero-copy conversion between logical and physical types,
// and as a consequence, some types might be unergonomic,
// For example, `Int8` actually stores it's value as `i32` and provides `asI8` method to get the value as `i8`.
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
    } else if (T == UUID) {
        return UUID;
    } else if (T == Int8) {
        return Int8;
    } else if (T == UInt8) {
        return UInt8;
    } else if (T == Int16) {
        return Int16;
    } else if (T == UInt16) {
        return UInt16;
    } else if (T == UInt32) {
        return UInt32;
    } else if (T == UInt64) {
        return UInt64;
    } else if (T == Float16) {
        return Float16;
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

pub const UUID = struct {
    bytes: [16]u8,

    pub const physical_type = [16]u8;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?UUID else []UUID {
        return @ptrCast(physical);
    }
};

pub const String = []const u8;

pub const Enum = []const u8;

pub const Json = []const u8;

pub const Bson = []const u8;

pub const Int8 = struct {
    value: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?Int8 else []Int8 {
        return @ptrCast(physical);
    }

    pub fn asI8(self: Int8) i8 {
        return @intCast(self.value);
    }
};

pub const UInt8 = struct {
    value: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?UInt8 else []UInt8 {
        return @ptrCast(physical);
    }

    pub fn asU8(self: UInt8) u8 {
        return @intCast(self.value);
    }
};

pub const Int16 = struct {
    value: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?Int16 else []Int16 {
        return @ptrCast(physical);
    }

    pub fn asI16(self: Int16) i16 {
        return @intCast(self.value);
    }
};

pub const UInt16 = struct {
    value: i32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?UInt16 else []UInt16 {
        return @ptrCast(physical);
    }

    pub fn asU16(self: UInt16) u16 {
        return @intCast(self.value);
    }
};

pub const UInt32 = struct {
    value: u32,

    pub const physical_type = i32;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?UInt32 else []UInt32 {
        const reinterpreted: if (@typeInfo(T) == .optional) []?u32 else []u32 = @ptrCast(physical);
        return @ptrCast(reinterpreted);
    }
};

pub const UInt64 = struct {
    value: u64,

    pub const physical_type = i64;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?UInt64 else []UInt64 {
        const reinterpreted: if (@typeInfo(T) == .optional) []?u64 else []u64 = @ptrCast(physical);
        return @ptrCast(reinterpreted);
    }
};

pub const Float16 = struct {
    bytes: [2]u8,

    pub const physical_type = [2]u8;

    pub inline fn fromPhysical(comptime T: type, physical: []T) if (@typeInfo(T) == .optional) []?Float16 else []Float16 {
        return @ptrCast(physical);
    }

    pub fn asF16(self: Float16) f16 {
        return @bitCast(self.bytes);
    }
};

pub const Decimal = struct {
    value: f128,
};

pub fn convertToDecimal(comptime T: type, values: anytype, scale: i32, allocator: std.mem.Allocator) ![]T {
    const is_optional = T == ?Decimal;
    const result = try allocator.alloc(T, values.len);

    var divisor: f128 = 1.0;
    for (0..@intCast(scale)) |_| divisor *= 10.0;

    for (values, 0..) |v, i| {
        result[i] = if (is_optional)
            (if (v) |val| Decimal{ .value = toF128(val) / divisor } else null)
        else
            Decimal{ .value = toF128(v) / divisor };
    }

    return result;
}

fn toF128(value: anytype) f128 {
    const V = @TypeOf(value);
    if (V == i32 or V == i64) return @floatFromInt(value);
    return @floatFromInt(parseBigEndianSigned(value));
}

fn parseBigEndianSigned(bytes: anytype) i128 {
    if (bytes.len == 0) return 0;
    const is_negative = (bytes[0] & 0x80) != 0;
    var result: i128 = if (is_negative) -1 else 0;
    for (bytes) |b| {
        result = (result << 8) | b;
    }
    return result;
}

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

test "uuid logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uuid_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const uuids = try rg.readColumn(UUID, 0);
    try std.testing.expectEqual(4, uuids.len);

    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, &uuids[0].bytes);
    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 }, &uuids[1].bytes);
    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8 }, &uuids[2].bytes);
    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8 }, &uuids[3].bytes);
}

test "uuid logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uuid_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const uuids = try rg.readColumn(?UUID, 0);
    try std.testing.expectEqual(5, uuids.len);

    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, &uuids[0].?.bytes);
    try std.testing.expect(uuids[1] == null);
    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 }, &uuids[2].?.bytes);
    try std.testing.expect(uuids[3] == null);
    try std.testing.expectEqualSlices(u8, &[16]u8{ 0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8 }, &uuids[4].?.bytes);
}

test "string logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/string_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const strings = try rg.readColumn(String, 0);
    try std.testing.expectEqual(4, strings.len);

    try std.testing.expectEqualStrings("", strings[0]);
    try std.testing.expectEqualStrings("hello", strings[1]);
    try std.testing.expectEqualStrings("world", strings[2]);
    try std.testing.expectEqualStrings("Zig is fast! 🚀", strings[3]);
}

test "string logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/string_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const strings = try rg.readColumn(?String, 0);
    try std.testing.expectEqual(5, strings.len);

    try std.testing.expectEqualStrings("hello", strings[0].?);
    try std.testing.expect(strings[1] == null);
    try std.testing.expectEqualStrings("café", strings[2].?);
    try std.testing.expect(strings[3] == null);
    try std.testing.expectEqualStrings("你好", strings[4].?);
}

test "string logical type with utf8" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/string_test_utf8.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const strings = try rg.readColumn(String, 0);
    try std.testing.expectEqual(5, strings.len);

    try std.testing.expectEqualStrings("ASCII only", strings[0]);
    try std.testing.expectEqualStrings("Café ☕", strings[1]);
    try std.testing.expectEqualStrings("日本語", strings[2]);
    try std.testing.expectEqualStrings("🎉🚀✨", strings[3]);
    try std.testing.expectEqualStrings("Здравствуй", strings[4]);
}

test "enum logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/enum_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const enums = try rg.readColumn(Enum, 0);
    try std.testing.expectEqual(4, enums.len);

    try std.testing.expectEqualStrings("active", enums[0]);
    try std.testing.expectEqualStrings("pending", enums[1]);
    try std.testing.expectEqualStrings("completed", enums[2]);
    try std.testing.expectEqualStrings("active", enums[3]);
}

test "enum logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/enum_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const enums = try rg.readColumn(?Enum, 0);
    try std.testing.expectEqual(5, enums.len);

    try std.testing.expectEqualStrings("active", enums[0].?);
    try std.testing.expect(enums[1] == null);
    try std.testing.expectEqualStrings("pending", enums[2].?);
    try std.testing.expect(enums[3] == null);
    try std.testing.expectEqualStrings("completed", enums[4].?);
}

test "json logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/json_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const jsons = try rg.readColumn(Json, 0);
    try std.testing.expectEqual(4, jsons.len);

    try std.testing.expectEqualSlices(u8, "{}", jsons[0]);
    try std.testing.expectEqualSlices(u8, "{\"name\":\"Alice\"}", jsons[1]);
    try std.testing.expectEqualSlices(u8, "{\"age\":30,\"city\":\"NYC\"}", jsons[2]);
    try std.testing.expectEqualSlices(u8, "[1,2,3]", jsons[3]);
}

test "json logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/json_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const jsons = try rg.readColumn(?Json, 0);
    try std.testing.expectEqual(5, jsons.len);

    try std.testing.expectEqualSlices(u8, "{\"name\":\"Alice\"}", jsons[0].?);
    try std.testing.expect(jsons[1] == null);
    try std.testing.expectEqualSlices(u8, "{\"age\":30}", jsons[2].?);
    try std.testing.expect(jsons[3] == null);
    try std.testing.expectEqualSlices(u8, "[1,2,3]", jsons[4].?);
}

test "bson logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/bson_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const bsons = try rg.readColumn(Bson, 0);
    try std.testing.expectEqual(4, bsons.len);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x05, 0x00, 0x00, 0x00, 0x00 }, bsons[0]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x16, 0x00, 0x00, 0x00, 0x02, 0x6e, 0x61, 0x6d, 0x65, 0x00, 0x06, 0x00, 0x00, 0x00, 0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00 }, bsons[1]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x1a, 0x00, 0x00, 0x00, 0x10, 0x61, 0x67, 0x65, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x02, 0x63, 0x69, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x4e, 0x59, 0x43, 0x00, 0x00 }, bsons[2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x0d, 0x00, 0x00, 0x00, 0x10, 0x69, 0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 }, bsons[3]);
}

test "bson logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/bson_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const bsons = try rg.readColumn(?Bson, 0);
    try std.testing.expectEqual(5, bsons.len);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x16, 0x00, 0x00, 0x00, 0x02, 0x6e, 0x61, 0x6d, 0x65, 0x00, 0x06, 0x00, 0x00, 0x00, 0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00 }, bsons[0].?);
    try std.testing.expect(bsons[1] == null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x1a, 0x00, 0x00, 0x00, 0x10, 0x61, 0x67, 0x65, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x02, 0x63, 0x69, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x4e, 0x59, 0x43, 0x00, 0x00 }, bsons[2].?);
    try std.testing.expect(bsons[3] == null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x0d, 0x00, 0x00, 0x00, 0x10, 0x69, 0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 }, bsons[4].?);
}

test "int8 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/int8_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(Int8, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(i8, -128), values[0].asI8());
    try std.testing.expectEqual(@as(i8, -10), values[1].asI8());
    try std.testing.expectEqual(@as(i8, 0), values[2].asI8());
    try std.testing.expectEqual(@as(i8, 10), values[3].asI8());
    try std.testing.expectEqual(@as(i8, 127), values[4].asI8());
}

test "int8 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/int8_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?Int8, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(i8, -128), values[0].?.asI8());
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(i8, 0), values[2].?.asI8());
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(i8, 127), values[4].?.asI8());
}

test "uint8 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint8_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(UInt8, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u8, 0), values[0].asU8());
    try std.testing.expectEqual(@as(u8, 50), values[1].asU8());
    try std.testing.expectEqual(@as(u8, 100), values[2].asU8());
    try std.testing.expectEqual(@as(u8, 200), values[3].asU8());
    try std.testing.expectEqual(@as(u8, 255), values[4].asU8());
}

test "uint8 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint8_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?UInt8, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u8, 0), values[0].?.asU8());
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(u8, 100), values[2].?.asU8());
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(u8, 255), values[4].?.asU8());
}

test "int16 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/int16_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(Int16, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(i16, -32768), values[0].asI16());
    try std.testing.expectEqual(@as(i16, -1000), values[1].asI16());
    try std.testing.expectEqual(@as(i16, 0), values[2].asI16());
    try std.testing.expectEqual(@as(i16, 1000), values[3].asI16());
    try std.testing.expectEqual(@as(i16, 32767), values[4].asI16());
}

test "int16 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/int16_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?Int16, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(i16, -32768), values[0].?.asI16());
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(i16, 0), values[2].?.asI16());
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(i16, 32767), values[4].?.asI16());
}

test "uint16 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint16_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(UInt16, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u16, 0), values[0].asU16());
    try std.testing.expectEqual(@as(u16, 1000), values[1].asU16());
    try std.testing.expectEqual(@as(u16, 32768), values[2].asU16());
    try std.testing.expectEqual(@as(u16, 50000), values[3].asU16());
    try std.testing.expectEqual(@as(u16, 65535), values[4].asU16());
}

test "uint16 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint16_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?UInt16, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u16, 0), values[0].?.asU16());
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(u16, 32768), values[2].?.asU16());
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(u16, 65535), values[4].?.asU16());
}

test "uint32 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint32_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(UInt32, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u32, 0), values[0].value);
    try std.testing.expectEqual(@as(u32, 1000), values[1].value);
    try std.testing.expectEqual(@as(u32, 2147483648), values[2].value);
    try std.testing.expectEqual(@as(u32, 3000000000), values[3].value);
    try std.testing.expectEqual(@as(u32, 4294967295), values[4].value);
}

test "uint32 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint32_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?UInt32, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u32, 0), values[0].?.value);
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(u32, 2147483648), values[2].?.value);
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(u32, 4294967295), values[4].?.value);
}

test "uint64 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint64_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(UInt64, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u64, 0), values[0].value);
    try std.testing.expectEqual(@as(u64, 1000), values[1].value);
    try std.testing.expectEqual(@as(u64, 9223372036854775808), values[2].value);
    try std.testing.expectEqual(@as(u64, 15000000000000000000), values[3].value);
    try std.testing.expectEqual(@as(u64, 18446744073709551615), values[4].value);
}

test "uint64 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/uint64_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?UInt64, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(u64, 0), values[0].?.value);
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(u64, 9223372036854775808), values[2].?.value);
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(u64, 18446744073709551615), values[4].?.value);
}

test "float16 logical type" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/float16_test.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(4, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(Float16, 0);
    try std.testing.expectEqual(4, values.len);

    try std.testing.expectEqual(@as(f16, 0.0), values[0].asF16());
    try std.testing.expectEqual(@as(f16, 1.5), values[1].asF16());
    try std.testing.expectEqual(@as(f16, -2.5), values[2].asF16());
    try std.testing.expectApproxEqAbs(@as(f16, 3.14), values[3].asF16(), 0.01);
}

test "float16 logical type with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/float16_test_nullable.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(5, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    const values = try rg.readColumn(?Float16, 0);
    try std.testing.expectEqual(5, values.len);

    try std.testing.expectEqual(@as(f16, 1.5), values[0].?.asF16());
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(f16, -2.5), values[2].?.asF16());
    try std.testing.expect(values[3] == null);
    try std.testing.expectEqual(@as(f16, 0.0), values[4].?.asF16());
}

const std = @import("std");
const File = @import("./File.zig");
