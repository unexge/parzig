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
    const Inner = unwrapOptional(T);

    const metadata = column.meta_data orelse return error.MissingColumnMetadata;
    if (!isAssignable(Inner, metadata.type)) {
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
            var num_encoded_values = num_values;

            const decoder = try decoderForPage(arena, file.source.reader(), metadata.codec, page_header.compressed_page_size);

            if (rep_level > 0) {
                const values = try file.readLevelDataV1(decoder, rep_level, num_values);
                defer arena.free(values);
            }

            var def_values: []u16 = undefined;
            if (def_level > 0) {
                def_values = try file.readLevelDataV1(decoder, def_level, num_values);

                num_encoded_values = blk: {
                    var i: usize = 0;
                    for (def_values) |v| {
                        i += v;
                    }
                    break :blk i;
                };
            }
            defer {
                if (def_level > 0) arena.free(def_values);
            }

            const encoded_values = switch (data_page.encoding) {
                .PLAIN => try decoding.decodePlain(Inner, arena, num_encoded_values, decoder),
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => blk: {
                    const indices = try decoding.decodeRleDictionary(u32, arena, num_encoded_values, decoder);

                    try source.seekTo(@intCast(metadata.dictionary_page_offset.?));
                    const dict_page_header = try page_header_reader.read(arena, source.reader());
                    const header = dict_page_header.dictionary_page_header.?;

                    const decoder_for_dict = try decoderForPage(arena, file.source.reader(), metadata.codec, dict_page_header.compressed_page_size);

                    const dict_values = try decoding.decodePlain(Inner, arena, @intCast(header.num_values), decoder_for_dict);

                    const values = try arena.alloc(Inner, indices.len);
                    for (indices, 0..) |idx, i| {
                        values[i] = dict_values[idx];
                    }
                    break :blk values;
                },
                .BYTE_STREAM_SPLIT => try decoding.decodeByteStreamSplit(Inner, arena, num_encoded_values, decoder),
                else => {
                    std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                    return error.UnsupportedEncoding;
                },
            };

            return decodeValues(T, arena, num_encoded_values, encoded_values, num_values, def_values);
        },
        .DATA_PAGE_V2 => {
            const data_page = page_header.data_page_header_v2.?;
            const num_values: usize = @intCast(data_page.num_values);
            const num_nulls: usize = @intCast(data_page.num_nulls);
            const num_encoded_values = num_values - num_nulls;

            const reader = file.source.reader();

            if (rep_level > 0) {
                const values = try file.readLevelDataV2(reader, rep_level, num_values, @intCast(data_page.repetition_levels_byte_length));
                defer arena.free(values);
            }

            var def_values: []u16 = undefined;
            if (def_level > 0) {
                def_values = try file.readLevelDataV2(reader, def_level, num_values, @intCast(data_page.definition_levels_byte_length));

                std.debug.assert(num_encoded_values == blk: {
                    var i: usize = 0;
                    for (def_values) |v| {
                        i += v;
                    }
                    break :blk i;
                });
            }
            defer {
                if (def_level > 0) arena.free(def_values);
            }

            const decoder = try decoderForPage(arena, reader, metadata.codec, page_header.compressed_page_size);

            const encoded_values = switch (data_page.encoding) {
                .DELTA_BINARY_PACKED => try decoding.decodeDeltaBinaryPacked(Inner, arena, num_encoded_values, decoder),
                .DELTA_LENGTH_BYTE_ARRAY => try decoding.decodeDeltaLengthByteArray(Inner, arena, num_encoded_values, decoder),
                .DELTA_BYTE_ARRAY => try decoding.decodeDeltaByteArray(Inner, arena, num_encoded_values, decoder),
                else => {
                    std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                    return error.UnsupportedEncoding;
                },
            };

            return decodeValues(T, arena, num_encoded_values, encoded_values, num_values, def_values);
        },
        else => {
            std.debug.print("{any} is not supported\n", .{page_header.type});
            return error.PageTypeNotSupported;
        },
    }
}

inline fn decodeValues(
    comptime T: type,
    arena: std.mem.Allocator,
    num_encoded_values: usize,
    encoded_values: []unwrapOptional(T),
    num_values: usize,
    def_values: []u16,
) ![]T {
    if (num_values == num_encoded_values) {
        // No nulls
        // TODO: Can we just cast `num_values` to `[]T`?
        const values = try arena.alloc(T, num_values);
        for (encoded_values, 0..) |v, i| {
            values[i] = v;
        }
        return values;
    }

    if (@typeInfo(T) != .Optional) {
        return error.NullValuesWithoutOptionalType;
    }

    const values = try arena.alloc(T, num_values);
    var last_decoded_value: usize = 0;
    for (def_values, 0..) |v, i| {
        if (v == 0) {
            values[i] = null;
        } else {
            values[i] = encoded_values[last_decoded_value];
            last_decoded_value += 1;
        }
    }

    return values;
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

fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .Optional => |*o| o.child,
        else => T,
    };
}
