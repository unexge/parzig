const std = @import("std");

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const decoding = @import("./decoding.zig");
const File = @import("./File.zig");

const RowGroupReader = @This();

fn isAssignable(comptime T: type, parquet_type: parquet_schema.Type) bool {
    return switch (parquet_type) {
        .BOOLEAN => T == bool,
        .INT32 => T == i32,
        .INT64 => T == i64,
        .INT96 => T == i96,
        .FLOAT => T == f32,
        .DOUBLE => T == f64,
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => T == []const u8,
    };
}

pub fn readColumn(comptime T: type, file: *File, column: *parquet_schema.ColumnChunk) ![]T {
    const metadata = column.meta_data orelse return error.MissingColumnMetadata;
    if (!isAssignable(T, metadata.type)) {
        return error.UnexpectedType;
    }

    if (metadata.codec != .UNCOMPRESSED) {
        return error.CompressedDataNotSupported;
    }

    const arena = file.arena.allocator();
    var source = file.source;

    const levels = try file.repAndDefLevelOfColumn(metadata.path_in_schema);
    const rep_level = levels[0];
    const def_level = levels[1];

    try source.seekTo(@intCast(metadata.data_page_offset));

    const page_header_reader = protocol_compact.StructReader(parquet_schema.PageHeader);
    const page_header = try page_header_reader.read(arena, source.reader());
    switch (page_header.type) {
        .DATA_PAGE => {
            const data_page = page_header.data_page_header.?;
            const num_values: usize = @intCast(data_page.num_values);

            if (rep_level > 0) {
                const values = try file.readLevelDataV1(source.reader(), rep_level, num_values);
                defer arena.free(values);
            }

            if (def_level > 0) {
                const values = try file.readLevelDataV1(source.reader(), def_level, num_values);
                defer arena.free(values);
            }

            return decoding.decodePlain(T, arena, num_values, source.reader());
        },
        else => {
            std.debug.print("Only `DATE_PAGE` is supported for now", .{});
            return error.PageTypeNotSupported;
        },
    }
}
