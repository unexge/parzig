pub const File = @import("parquet/File.zig");
pub const MapEntry = @import("parquet/nestedReader.zig").MapEntry;

const logical = @import("parquet/logical.zig");
pub const Date = logical.Date;

test {
    _ = File;
    _ = logical;
}
