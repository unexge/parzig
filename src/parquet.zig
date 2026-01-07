pub const File = @import("parquet/File.zig");
pub const MapEntry = @import("parquet/nestedReader.zig").MapEntry;

const logical = @import("parquet/logical.zig");
pub const Date = logical.Date;
pub const TimestampMillis = logical.TimestampMillis;
pub const TimestampMicros = logical.TimestampMicros;
pub const TimestampNanos = logical.TimestampNanos;
pub const TimeMillis = logical.TimeMillis;
pub const TimeMicros = logical.TimeMicros;
pub const TimeNanos = logical.TimeNanos;
pub const UUID = logical.UUID;
pub const String = logical.String;
pub const Enum = logical.Enum;

test {
    _ = File;
    _ = logical;
}
