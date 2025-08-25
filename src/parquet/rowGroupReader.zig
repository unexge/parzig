const std = @import("std");
const Reader = std.io.Reader;

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

    if (metadata.num_values == 0) {
        return &[_]T{};
    }

    const arena = file.arena.allocator();

    const levels = try file.repAndDefLevelOfColumn(metadata.path_in_schema);
    const rep_level = levels[0];
    const def_level = levels[1];

    try file.file_reader.seekTo(@intCast(metadata.data_page_offset));

    const page_header_reader = protocol_compact.StructReader(parquet_schema.PageHeader);
    const page_header = try page_header_reader.read(arena, &file.file_reader.interface);
    switch (page_header.type) {
        .DATA_PAGE => {
            const data_page = page_header.data_page_header.?;
            const num_values: usize = @intCast(data_page.num_values);
            var num_encoded_values = num_values;

            const decoder = try decoderForPage(arena, &file.file_reader.interface, metadata.codec);

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

                    try file.file_reader.seekTo(@intCast(metadata.dictionary_page_offset.?));
                    const dict_page_header = try page_header_reader.read(arena, &file.file_reader.interface);
                    const header = dict_page_header.dictionary_page_header.?;

                    const dict_decoder = try decoderForPage(arena, &file.file_reader.interface, metadata.codec);
                    const dict_values = try decoding.decodePlain(Inner, arena, @intCast(header.num_values), dict_decoder);

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

            var reader = file.file_reader.interface;

            if (rep_level > 0) {
                const values = try file.readLevelDataV2(&reader, rep_level, num_values, @intCast(data_page.repetition_levels_byte_length));
                defer arena.free(values);
            } else if (data_page.repetition_levels_byte_length > 0) {
                try file.file_reader.seekBy(@intCast(data_page.repetition_levels_byte_length));
            }

            var def_values: []u16 = undefined;
            const has_def_values = def_level > 0 and data_page.definition_levels_byte_length > 0;
            if (has_def_values) {
                def_values = try file.readLevelDataV2(&reader, def_level, num_values, @intCast(data_page.definition_levels_byte_length));

                std.debug.assert(num_encoded_values == blk: {
                    var i: usize = 0;
                    for (def_values) |v| {
                        i += v;
                    }
                    break :blk i;
                });
            }
            defer {
                if (has_def_values) arena.free(def_values);
            }

            const decoder = try decoderForPage(arena, &reader, metadata.codec);

            const encoded_values = switch (data_page.encoding) {
                .RLE => blk: {
                    if (Inner != bool) {
                        return error.UnsupportedType;
                    }

                    break :blk try decoding.decodeLenghtPrependedRleBitPackedHybrid(Inner, arena, num_encoded_values, 1, decoder);
                },
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

    if (@typeInfo(T) != .optional) {
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

fn decoderForPage(arena: std.mem.Allocator, inner_reader: *Reader, codec: parquet_schema.CompressionCodec) !*Reader {
    return switch (codec) {
        .GZIP => blk: {
            const buf = try arena.alloc(u8, std.compress.flate.max_window_len);
            const decompress = try arena.create(std.compress.flate.Decompress);
            decompress.* = std.compress.flate.Decompress.init(inner_reader, .gzip, buf);
            break :blk &decompress.reader;
        },
        // .SNAPPY => blk: {
        //     var decompressor = compress.snappy.decoder(limited_reader.interface.adaptToOldInterface(), gpa);
        //     break :blk decompressor.reader().any();
        // },
        .ZSTD => blk: {
            const buf = try arena.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
            const decompress = try arena.create(std.compress.zstd.Decompress);
            decompress.* = std.compress.zstd.Decompress.init(inner_reader, buf, .{
                .window_len = std.compress.zstd.default_window_len,
            });
            break :blk &decompress.reader;
        },
        .UNCOMPRESSED => inner_reader,
        else => {
            std.debug.print("Unsupported codec: {any}\n", .{codec});
            return error.UnsupportedCodec;
        },
    };
}

fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |*o| o.child,
        else => T,
    };
}
