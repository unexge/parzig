const std = @import("std");
const Reader = std.Io.Reader;

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const compress = @import("../compress.zig");
const physical = @import("./physical.zig");
const logical = @import("./logical.zig");
const File = @import("./File.zig");

const RowGroupReader = @This();

/// Holds column data along with definition and repetition levels
pub fn ColumnData(comptime T: type) type {
    return struct {
        values: []T,
        def_levels: ?[]const u16,
        rep_levels: ?[]const u16,
    };
}

pub fn readColumn(comptime T: type, file: *File, column: *parquet_schema.ColumnChunk) ![]T {
    return (try readColumnWithLevels(T, file, column)).values;
}

pub fn readColumnWithLevels(comptime T: type, file: *File, column: *parquet_schema.ColumnChunk) !ColumnData(T) {
    if (comptime logical.parse(unwrapOptional(T))) |logical_type| {
        const PhysicalT = if (@typeInfo(T) == .optional) ?logical_type.physical_type else logical_type.physical_type;
        const physical_data = try readPhysicalColumnWithLevels(PhysicalT, file, column);
        const logical_values = logical_type.fromPhysical(PhysicalT, physical_data.values);
        return ColumnData(T){
            .values = logical_values,
            .def_levels = physical_data.def_levels,
            .rep_levels = physical_data.rep_levels,
        };
    } else if (unwrapOptional(T) == logical.Decimal) {
        const is_optional = T == ?logical.Decimal;
        const metadata = column.meta_data orelse return error.MissingColumnMetadata;
        const schema = file.findSchemaElement(metadata.path_in_schema) orelse return error.UnknownField;
        const scale = schema.elem.scale orelse return error.MissingDecimalScale;

        switch (metadata.type) {
            .INT32 => {
                const physical_data = try readPhysicalColumnWithLevels(if (is_optional) ?i32 else i32, file, column);
                return .{ .values = try logical.convertToDecimal(T, physical_data.values, scale, file.arena.allocator()), .def_levels = physical_data.def_levels, .rep_levels = physical_data.rep_levels };
            },
            .INT64 => {
                const physical_data = try readPhysicalColumnWithLevels(if (is_optional) ?i64 else i64, file, column);
                return .{ .values = try logical.convertToDecimal(T, physical_data.values, scale, file.arena.allocator()), .def_levels = physical_data.def_levels, .rep_levels = physical_data.rep_levels };
            },
            .BYTE_ARRAY => {
                const physical_data = try readPhysicalColumnWithLevels(if (is_optional) ?[]const u8 else []const u8, file, column);
                return .{ .values = try logical.convertToDecimal(T, physical_data.values, scale, file.arena.allocator()), .def_levels = physical_data.def_levels, .rep_levels = physical_data.rep_levels };
            },
            .FIXED_LEN_BYTE_ARRAY => {
                const type_length = schema.elem.type_length orelse return error.MissingTypeLength;
                switch (type_length) {
                    inline 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 => |len| {
                        const physical_data = try readPhysicalColumnWithLevels(if (is_optional) ?[len]u8 else [len]u8, file, column);
                        return .{ .values = try logical.convertToDecimal(T, physical_data.values, scale, file.arena.allocator()), .def_levels = physical_data.def_levels, .rep_levels = physical_data.rep_levels };
                    },
                    else => return error.UnsupportedFixedLenDecimalSize,
                }
            },
            else => return error.UnsupportedDecimalPhysicalType,
        }
    } else {
        return readPhysicalColumnWithLevels(T, file, column);
    }
}

fn readPhysicalColumnWithLevels(comptime T: type, file: *File, column: *parquet_schema.ColumnChunk) !ColumnData(T) {
    const Inner = unwrapOptional(T);

    const metadata = column.meta_data orelse return error.MissingColumnMetadata;
    if (!isAssignable(Inner, metadata.type)) {
        return error.UnexpectedType;
    }

    if (metadata.num_values == 0) {
        return ColumnData(T){
            .values = &[_]T{},
            .def_levels = null,
            .rep_levels = null,
        };
    }

    const arena = file.arena.allocator();

    const schema_info = file.findSchemaElement(metadata.path_in_schema) orelse return error.UnknownField;
    const max_definition_level = schema_info.max_definition_level;
    const max_repetition_level = schema_info.max_repetition_level;

    const page_header_reader = protocol_compact.StructReader(parquet_schema.PageHeader);

    // Store values in the dictionary page, these values will be referenced in data pages
    // dictionary_page_offset=0 means "no dictionary" since offset 0 is where PAR1 magic is
    var dict_values: ?[]Inner = null;
    if (metadata.dictionary_page_offset) |offset| {
        if (offset > 0) {
            try file.file_reader.seekTo(@intCast(offset));
            const dictionary_page_header = try page_header_reader.read(arena, &file.file_reader.interface);
            dict_values = try readDictionaryPage(Inner, arena, dictionary_page_header, &file.file_reader.interface, metadata.codec);
        }
    }

    const read_values = try arena.alloc(T, @intCast(metadata.num_values));
    var read_values_pos: usize = 0;

    // Pre-allocate arrays for levels
    var all_def_levels: ?[]u16 = null;
    var all_rep_levels: ?[]u16 = null;
    var read_levels_pos: usize = 0;
    if (max_definition_level > 0) {
        all_def_levels = try arena.alloc(u16, @intCast(metadata.num_values));
    }
    if (max_repetition_level > 0) {
        all_rep_levels = try arena.alloc(u16, @intCast(metadata.num_values));
    }

    try file.file_reader.seekTo(@intCast(metadata.data_page_offset));

    while (metadata.num_values > read_levels_pos) {
        const page_header = try page_header_reader.read(arena, &file.file_reader.interface);
        const page_end = file.file_reader.logicalPos() + @as(u64, @intCast(page_header.compressed_page_size));
        defer {
            file.file_reader.seekTo(page_end) catch |err| {
                std.debug.print("Failed to seek to end of the page: {any}\n", .{err});
            };
        }

        switch (page_header.type) {
            .DICTIONARY_PAGE => {
                if (dict_values != null) {
                    return error.DuplicateDictionaryPages;
                }

                dict_values = try readDictionaryPage(Inner, arena, page_header, &file.file_reader.interface, metadata.codec);
            },
            .DATA_PAGE => {
                const data_page = page_header.data_page_header.?;
                const num_values: usize = @intCast(data_page.num_values);
                var num_encoded_values = num_values;

                const decoder = try decoderForPage(arena, &file.file_reader.interface, metadata.codec);

                if (max_repetition_level > 0) {
                    const rep_bit_width = levelBitWidth(max_repetition_level);
                    try file.readLevelDataV1(decoder, data_page.repetition_level_encoding, rep_bit_width, all_rep_levels.?[read_levels_pos..][0..num_values]);
                }

                var def_values: []u16 = undefined;
                if (max_definition_level > 0) {
                    const def_bit_width = levelBitWidth(max_definition_level);
                    def_values = all_def_levels.?[read_levels_pos..][0..num_values];
                    try file.readLevelDataV1(decoder, data_page.definition_level_encoding, def_bit_width, def_values);

                    num_encoded_values = countNonNulls(def_values, max_definition_level);
                }

                const encoded_values = switch (data_page.encoding) {
                    .PLAIN => blk: {
                        const buf = try arena.alloc(Inner, num_encoded_values);
                        try physical.plain(Inner, arena, decoder, buf);
                        break :blk buf;
                    },
                    .PLAIN_DICTIONARY, .RLE_DICTIONARY => blk: {
                        if (dict_values == null) return error.MissingDictionaryPage;

                        const indices = try arena.alloc(u32, num_encoded_values);
                        try physical.dictionary(u32, decoder, indices);

                        const values = try arena.alloc(Inner, indices.len);
                        for (indices, 0..) |idx, i| {
                            values[i] = dict_values.?[idx];
                        }
                        break :blk values;
                    },
                    .BYTE_STREAM_SPLIT => blk: {
                        const buf = try arena.alloc(Inner, num_encoded_values);
                        try physical.byteStreamSplit(Inner, arena, decoder, buf);
                        break :blk buf;
                    },
                    else => {
                        std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                        return error.UnsupportedEncoding;
                    },
                };

                try decodeValues(T, read_values[read_values_pos..], num_encoded_values, encoded_values, num_values, def_values, max_definition_level);
                // For non-optional types (LIST columns), increment by num_encoded_values
                // For optional types (flat columns with nulls), increment by num_values
                if (@typeInfo(T) != .optional and num_values != num_encoded_values) {
                    read_values_pos += num_encoded_values;
                } else {
                    read_values_pos += num_values;
                }
                read_levels_pos += num_values;
            },
            .DATA_PAGE_V2 => {
                const data_page = page_header.data_page_header_v2.?;
                const num_values: usize = @intCast(data_page.num_values);
                const num_nulls: usize = @intCast(data_page.num_nulls);
                const num_encoded_values = num_values - num_nulls;

                const reader = &file.file_reader.interface;

                if (max_repetition_level > 0) {
                    const rep_bit_width = levelBitWidth(max_repetition_level);
                    try file.readLevelDataV2(reader, rep_bit_width, all_rep_levels.?[read_levels_pos..][0..num_values], @intCast(data_page.repetition_levels_byte_length));
                } else if (data_page.repetition_levels_byte_length > 0) {
                    const pos = file.file_reader.logicalPos();
                    const length: u64 = @intCast(data_page.repetition_levels_byte_length);
                    // FIXME: Use file.file_reader.seekBy(data_page.repetition_levels_byte_length) once `seekBy` is fixed on stdlib.
                    try file.file_reader.seekTo(pos + length);
                }

                var def_values: []u16 = undefined;
                const has_def_values = max_definition_level > 0 and data_page.definition_levels_byte_length > 0;
                if (has_def_values) {
                    const def_bit_width = levelBitWidth(max_definition_level);
                    def_values = all_def_levels.?[read_levels_pos..][0..num_values];
                    try file.readLevelDataV2(reader, def_bit_width, def_values, @intCast(data_page.definition_levels_byte_length));

                    std.debug.assert(num_encoded_values == countNonNulls(def_values, max_definition_level));
                }

                const decoder = try decoderForPage(arena, reader, metadata.codec);

                const buf = try arena.alloc(Inner, num_encoded_values);
                switch (data_page.encoding) {
                    .PLAIN => {
                        try physical.plain(Inner, arena, decoder, buf);
                    },
                    .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                        if (dict_values == null) return error.MissingDictionaryPage;

                        const indices = try arena.alloc(u32, num_encoded_values);
                        try physical.dictionary(u32, decoder, indices);

                        for (indices, 0..) |idx, i| {
                            buf[i] = dict_values.?[idx];
                        }
                    },
                    .RLE => {
                        if (Inner != bool) {
                            return error.UnsupportedType;
                        }

                        try physical.runLengthBitPackedHybridLengthPrepended(Inner, decoder, 1, buf);
                    },
                    .DELTA_BINARY_PACKED => {
                        const num_read = try physical.delta(Inner, arena, decoder, buf);
                        std.debug.assert(num_read == num_encoded_values);
                    },
                    .DELTA_LENGTH_BYTE_ARRAY => {
                        try physical.deltaLengthByteArray(Inner, arena, decoder, buf);
                    },
                    .DELTA_BYTE_ARRAY => {
                        try physical.deltaStrings(Inner, arena, decoder, buf);
                    },
                    else => {
                        std.debug.print("Unsupported encoding: {any}\n", .{data_page.encoding});
                        return error.UnsupportedEncoding;
                    },
                }

                try decodeValues(T, read_values[read_values_pos..], num_encoded_values, buf, num_values, def_values, max_definition_level);
                // For non-optional types (LIST columns), increment by num_encoded_values
                // For optional types (flat columns with nulls), increment by num_values
                if (@typeInfo(T) != .optional and num_values != num_encoded_values) {
                    read_values_pos += num_encoded_values;
                } else {
                    read_values_pos += num_values;
                }
                read_levels_pos += num_values;
            },
            else => {
                std.debug.print("{any} is not supported\n", .{page_header.type});
                return error.PageTypeNotSupported;
            },
        }
    }

    return ColumnData(T){
        .values = read_values[0..read_values_pos],
        .def_levels = all_def_levels,
        .rep_levels = all_rep_levels,
    };
}

inline fn decodeValues(
    comptime T: type,
    dest: []T,
    num_encoded_values: usize,
    encoded_values: []unwrapOptional(T),
    num_values: usize,
    def_values: []u16,
    max_definition_level: u8,
) !void {
    // For LIST columns (non-optional T), values are already extracted via definition levels.
    // Just copy the non-null values in order.
    if (@typeInfo(T) != .optional) {
        for (encoded_values, 0..) |v, i| {
            dest[i] = v;
        }
        return;
    }

    if (num_values == num_encoded_values) {
        // No nulls, just copy
        for (encoded_values, 0..) |v, i| {
            dest[i] = v;
        }
        return;
    }

    // Has nulls: decode with null placeholders
    var last_decoded_value: usize = 0;
    for (def_values, 0..) |v, i| {
        if (v < max_definition_level) {
            dest[i] = null;
        } else {
            dest[i] = encoded_values[last_decoded_value];
            last_decoded_value += 1;
        }
    }
}

fn decoderForPage(arena: std.mem.Allocator, inner_reader: *Reader, codec: parquet_schema.CompressionCodec) !*Reader {
    return switch (codec) {
        .GZIP => blk: {
            const buf = try arena.alloc(u8, std.compress.flate.max_window_len);
            const decompress = try arena.create(std.compress.flate.Decompress);
            decompress.* = std.compress.flate.Decompress.init(inner_reader, .gzip, buf);
            break :blk &decompress.reader;
        },
        .SNAPPY => blk: {
            const buf = try arena.alloc(u8, compress.snappy.Decompress.buffer_len);
            const decompress = try arena.create(compress.snappy.Decompress);
            decompress.* = compress.snappy.Decompress.init(inner_reader, buf);
            break :blk &decompress.reader;
        },
        .ZSTD => blk: {
            const buf = try arena.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
            const decompress = try arena.create(std.compress.zstd.Decompress);
            decompress.* = std.compress.zstd.Decompress.init(inner_reader, buf, .{
                .window_len = std.compress.zstd.default_window_len,
            });
            break :blk &decompress.reader;
        },
        .LZ4 => blk: {
            const buf = try arena.alloc(u8, compress.lz4.Decompress.window_len);
            const decompress = try arena.create(compress.lz4.DecompressHadoop);
            decompress.* = try compress.lz4.DecompressHadoop.init(inner_reader, buf, arena);
            break :blk &decompress.reader;
        },
        .LZ4_RAW => blk: {
            const buf = try arena.alloc(u8, compress.lz4.Decompress.window_len);
            const decompress = try arena.create(compress.lz4.Decompress);
            decompress.* = compress.lz4.Decompress.init(inner_reader, buf);
            break :blk &decompress.reader;
        },
        .UNCOMPRESSED => inner_reader,
        else => {
            std.debug.print("Unsupported codec: {any}\n", .{codec});
            return error.UnsupportedCodec;
        },
    };
}

fn readDictionaryPage(comptime T: type, arena: std.mem.Allocator, page_header: parquet_schema.PageHeader, reader: *Reader, codec: parquet_schema.CompressionCodec) ![]T {
    const header = page_header.dictionary_page_header orelse return error.MissingDictionaryPageHeader;
    if (header.encoding != .PLAIN and header.encoding != .PLAIN_DICTIONARY) {
        std.debug.print("Unsupported encoding in dictionary page: {any}\n", .{header.encoding});
        return error.UnexpectedEncodingInDictionaryPage;
    }

    const dict_decoder = try decoderForPage(arena, reader, codec);
    const dict_values = try arena.alloc(T, @intCast(header.num_values));
    try physical.plain(T, arena, dict_decoder, dict_values);
    return dict_values;
}

fn isAssignable(comptime T: type, parquet_type: parquet_schema.Type) bool {
    return switch (parquet_type) {
        .BOOLEAN => T == bool,
        .INT32 => T == i32,
        .INT64 => T == i64,
        .INT96 => T == i96,
        .FLOAT => T == f32,
        .DOUBLE => T == f64,
        .BYTE_ARRAY => T == []const u8 or T == []u8,
        .FIXED_LEN_BYTE_ARRAY => switch (@typeInfo(T)) {
            .array => |arr| arr.child == u8,
            else => false,
        },
    };
}

fn levelBitWidth(max_level: u8) u8 {
    return std.math.log2_int_ceil(u8, max_level + 1);
}

fn countNonNulls(
    def_values: []u16,
    max_definition_level: u8,
) usize {
    var count: usize = 0;
    for (def_values) |v| {
        if (v == max_definition_level) {
            count += 1;
        }
    }
    return count;
}

pub fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |*o| o.child,
        else => T,
    };
}
