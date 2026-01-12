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
    fixed_len_byte_array_1: []?[1]u8,
    fixed_len_byte_array_2: []?[2]u8,
    fixed_len_byte_array_3: []?[3]u8,
    fixed_len_byte_array_4: []?[4]u8,
    fixed_len_byte_array_5: []?[5]u8,
    fixed_len_byte_array_6: []?[6]u8,
    fixed_len_byte_array_7: []?[7]u8,
    fixed_len_byte_array_8: []?[8]u8,
    fixed_len_byte_array_9: []?[9]u8,
    fixed_len_byte_array_10: []?[10]u8,
    fixed_len_byte_array_11: []?[11]u8,
    fixed_len_byte_array_12: []?[12]u8,
    fixed_len_byte_array_13: []?[13]u8,
    fixed_len_byte_array_14: []?[14]u8,
    fixed_len_byte_array_15: []?[15]u8,
    fixed_len_byte_array_16: []?[16]u8,
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
                inline 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 => |len| {
                    return @unionInit(Values, std.fmt.comptimePrint("fixed_len_byte_array_{d}", .{len}), try readColumnComptime(?[len]u8, file, column));
                },
                else => {
                    std.debug.print("Unsupported type length for `FIXED_LEN_BYTE_ARRAY`: {d}\n", .{type_length});
                    return error.UnsupportedFixedLength;
                },
            };
        },
    };
}
