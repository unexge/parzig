const std = @import("std");

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const compress = @import("../compress.zig");
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

            const decoder = try decoderForPage(arena, file.source.reader(), metadata.codec, page_header.compressed_page_size);

            if (rep_level > 0) {
                const values = try file.readLevelDataV1(decoder, rep_level, num_values);
                defer arena.free(values);
            }

            if (def_level > 0) {
                const values = try file.readLevelDataV1(decoder, def_level, num_values);
                defer arena.free(values);
            }

            return switch (data_page.encoding) {
                .PLAIN => decoding.decodePlain(T, arena, num_values, decoder),
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    const indices = try decoding.decodeRleDictionary(u32, arena, num_values, decoder);

                    try source.seekTo(@intCast(metadata.dictionary_page_offset.?));
                    const dict_page_header = try page_header_reader.read(arena, source.reader());
                    const header = dict_page_header.dictionary_page_header.?;

                    const decoder_for_dict = try decoderForPage(arena, file.source.reader(), metadata.codec, dict_page_header.compressed_page_size);

                    const dict_values = try decoding.decodePlain(T, arena, @intCast(header.num_values), decoder_for_dict);

                    const values = try arena.alloc(T, indices.len);
                    for (indices, 0..) |idx, i| {
                        values[i] = dict_values[idx];
                    }
                    return values;
                },
                else => {
                    std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                    return error.UnsupportedEncoding;
                },
            };
        },
        .DATA_PAGE_V2 => {
            const data_page = page_header.data_page_header_v2.?;
            const num_values: usize = @intCast(data_page.num_values);

            const reader = file.source.reader();

            if (rep_level > 0) {
                const values = try file.readLevelDataV2(reader, rep_level, num_values, @intCast(data_page.repetition_levels_byte_length));
                defer arena.free(values);
            }

            if (def_level > 0) {
                const values = try file.readLevelDataV2(reader, def_level, num_values, @intCast(data_page.definition_levels_byte_length));
                defer arena.free(values);
            }

            const decoder = try decoderForPage(arena, reader, metadata.codec, page_header.compressed_page_size);

            return switch (data_page.encoding) {
                .DELTA_BINARY_PACKED => decoding.decodeDeltaBinaryPacked(T, arena, num_values, decoder),
                .DELTA_LENGTH_BYTE_ARRAY => decoding.decodeDeltaLengthByteArray(T, arena, num_values, decoder),
                .DELTA_BYTE_ARRAY => decoding.decodeDeltaByteArray(T, arena, num_values, decoder),
                else => {
                    std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                    return error.UnsupportedEncoding;
                },
            };
        },
        else => {
            std.debug.print("{any} is not supported\n", .{page_header.type});
            return error.PageTypeNotSupported;
        },
    }
}

inline fn decoderForPage(gpa: std.mem.Allocator, inner_reader: anytype, codec: parquet_schema.CompressionCodec, size: i32) !std.io.AnyReader {
    var limited_reader = std.io.limitedReader(inner_reader, @intCast(size));
    return switch (codec) {
        .GZIP => blk: {
            var decompressor = std.compress.gzip.decompressor(limited_reader.reader());
            break :blk decompressor.reader().any();
        },
        .SNAPPY => blk: {
            var decompressor = compress.snappy.decoder(limited_reader.reader(), gpa);
            break :blk decompressor.reader().any();
        },
        .ZSTD => blk: {
            const window_buffer = try gpa.alloc(u8, std.compress.zstd.DecompressorOptions.default_window_buffer_len);
            var decompressor = std.compress.zstd.decompressor(limited_reader.reader(), .{ .verify_checksum = false, .window_buffer = window_buffer });
            break :blk decompressor.reader().any();
        },
        .UNCOMPRESSED => limited_reader.reader().any(),
        else => {
            std.debug.print("Unsupported codec: {any}\n", .{codec});
            return error.UnsupportedCodec;
        },
    };
}
