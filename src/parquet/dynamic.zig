const parquet_schema = @import("../generated/parquet.zig");
const File = @import("./File.zig");
const readColumnComptime = @import("./rowGroupReader.zig").readColumn;

pub const Values = union(enum) {
    boolean: []?bool,
    int32: []?i32,
    int64: []?i64,
    int96: []?i96,
    float: []?f32,
    double: []?f64,
    byte_array: []?[]const u8,
    fixed_len_byte_array: []?[]const u8,
};

pub fn readColumn(file: *File, column: *parquet_schema.ColumnChunk) !Values {
    const metadata = column.meta_data orelse return error.MissingColumnMetadata;
    return switch (metadata.type) {
        .BOOLEAN => .{ .boolean = try readColumnComptime(?bool, file, column) },
        .INT32 => .{ .int32 = try readColumnComptime(?i32, file, column) },
        .INT64 => .{ .int64 = try readColumnComptime(?i64, file, column) },
        .INT96 => .{ .int96 = try readColumnComptime(?i96, file, column) },
        .FLOAT => .{ .float = try readColumnComptime(?f32, file, column) },
        .DOUBLE => .{ .double = try readColumnComptime(?f64, file, column) },
        .BYTE_ARRAY => .{ .byte_array = try readColumnComptime(?[]const u8, file, column) },
        .FIXED_LEN_BYTE_ARRAY => .{ .fixed_len_byte_array = try readColumnComptime(?[]const u8, file, column) },
    };
}
