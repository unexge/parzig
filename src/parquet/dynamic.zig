const std = @import("std");
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
    byte_array: []?[]u8,
    fixed_len_byte_array_2: []?[2]u8,
    fixed_len_byte_array_4: []?[4]u8,
    fixed_len_byte_array_6: []?[6]u8,
    fixed_len_byte_array_8: []?[8]u8,
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
        .BYTE_ARRAY => .{ .byte_array = try readColumnComptime(?[]u8, file, column) },
        .FIXED_LEN_BYTE_ARRAY => {
            const schema_info = file.findSchemaElement(metadata.path_in_schema) orelse return error.MissingSchemaElement;
            const type_length = schema_info.elem.type_length orelse return error.MissingTypeLength;

            return switch (type_length) {
                2 => .{ .fixed_len_byte_array_2 = try readColumnComptime(?[2]u8, file, column) },
                4 => .{ .fixed_len_byte_array_4 = try readColumnComptime(?[4]u8, file, column) },
                6 => .{ .fixed_len_byte_array_6 = try readColumnComptime(?[6]u8, file, column) },
                8 => .{ .fixed_len_byte_array_8 = try readColumnComptime(?[8]u8, file, column) },
                else => {
                    std.debug.print("Unsupported type length for `FIXED_LEN_BYTE_ARRAY`: {d}\n", .{type_length});
                    return error.UnsupportedFixedLength;
                },
            };
        },
    };
}
