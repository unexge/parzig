const std = @import("std");
const protocol_compact = @import("../thrift.zig").protocol_compact;

pub fn decodePlain(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: anytype) ![]T {
    const buf = try gpa.alloc(T, len);
    const is_byte_array = T == []const u8;
    for (0..len) |i| {
        if (is_byte_array) {
            const num_bytes = try reader.readInt(u32, .little);
            const elem_buf = try gpa.alloc(u8, num_bytes);
            _ = try reader.readAll(elem_buf);
            buf[i] = elem_buf;
        } else {
            buf[i] = try reader.readInt(T, .little);
        }
    }
    return buf;
}

pub fn decodeRleDictionary(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: anytype) ![]T {
    const bit_width = try reader.readByte();
    const buf = try gpa.alloc(T, len);
    try decodeRleBitPackedHybrid(T, buf, bit_width, reader);
    return buf;
}

pub fn decodeRleBitPackedHybrid(comptime T: type, buf: []T, bit_width: u8, reader: anytype) !void {
    var bit_reader = std.io.bitReader(.little, reader);
    var pos: usize = 0;
    while (buf.len > pos) {
        const header = try std.leb.readULEB128(i64, reader);
        if (header & 1 == 1) {
            // bit packet run
            const len = @min(buf.len - pos, @as(usize, @intCast((header >> 1) * 8)));
            for (0..len) |i| {
                buf[pos + i] = try bit_reader.readBitsNoEof(T, bit_width);
            }
            pos += len;
        } else {
            // rle run
            const len = @min(buf.len - pos, @as(usize, @intCast(header >> 1)));
            const val = try bit_reader.readBitsNoEof(T, bit_width);
            for (0..len) |i| {
                buf[pos + i] = val;
            }
            pos += len;
        }
    }
}

pub fn decodeDeltaBinaryPacked(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: anytype) ![]T {
    if (T != i32 and T != i64) {
        return error.UnsupportedType;
    }

    const block_size = try std.leb.readULEB128(usize, reader);
    const miniblock_count = try std.leb.readULEB128(usize, reader);
    const total_count = try std.leb.readULEB128(usize, reader);
    if (total_count != len) {
        return error.UnexpectedValueCount;
    }

    const first_value = try protocol_compact.readZigZagInt(T, reader);

    const values_per_miniblock = block_size / miniblock_count;
    if (values_per_miniblock % 32 != 0) {
        return error.InvalidMiniblockSize;
    }

    const result = try gpa.alloc(T, len);
    result[0] = first_value;

    var values_read: usize = 1;
    var last_value = first_value;

    while (values_read < len) {
        const min_delta = try protocol_compact.readZigZagInt(T, reader);

        const bit_widths = try gpa.alloc(u8, miniblock_count);
        defer gpa.free(bit_widths);
        _ = try reader.readAll(bit_widths);

        var bit_reader = std.io.bitReader(.little, reader);

        for (bit_widths) |bit_width| {
            if (bit_width == 0) {
                const end = @min(values_read + values_per_miniblock, len);
                while (values_read < end) : (values_read += 1) {
                    // Allow wrapping addition for zero-width blocks
                    last_value = @addWithOverflow(last_value, min_delta)[0];
                    result[values_read] = last_value;
                }
            } else {
                const end = @min(values_read + values_per_miniblock, len);
                while (values_read < end) : (values_read += 1) {
                    // Read the bits
                    var raw_value: u64 = 0;
                    var bits_read: u8 = 0;
                    while (bits_read < bit_width) {
                        const bits_to_read = @min(8, bit_width - bits_read);
                        const byte = try bit_reader.readBitsNoEof(u8, bits_to_read);
                        raw_value |= @as(u64, byte) << @as(u6, @intCast(bits_read));
                        bits_read += bits_to_read;
                    }

                    // Handle sign extension if needed
                    if (@typeInfo(T).Int.signedness == .signed and bit_width < 64) {
                        const sign_bit = @as(u64, 1) << @as(u6, @intCast(bit_width - 1));
                        if (raw_value & sign_bit != 0) {
                            raw_value |= (~@as(u64, 0)) << @as(u6, @intCast(bit_width));
                        }
                    }

                    // Convert to target type safely
                    const packed_value = if (@typeInfo(T).Int.signedness == .signed)
                        std.math.cast(T, @as(i64, @bitCast(raw_value))) orelse return error.ValueOutOfRange
                    else
                        std.math.cast(T, raw_value) orelse return error.ValueOutOfRange;

                    // Allow wrapping addition for delta encoding
                    const delta = @addWithOverflow(packed_value, min_delta)[0];
                    last_value = @addWithOverflow(last_value, delta)[0];
                    result[values_read] = last_value;
                }
            }

            if (values_read >= len) break;
        }
    }

    return result;
}

// Tests are borrowed from https://github.com/apache/arrow-rs/blob/ac51632af79b01738dbc87a27c4a95512cde2faf/parquet/src/encodings/rle.rs#L526

test "rle decode i32" {
    // Test data: 0-7 with bit width 3
    // 00000011 10001000 11000110 11111010
    var fbs = std.io.fixedBufferStream(&[_]u8{ 0x03, 0x88, 0xC6, 0xFA });
    var buf: [8]i32 = undefined;
    try decodeRleBitPackedHybrid(i32, &buf, 3, fbs.reader());
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 3, 4, 5, 6, 7 }, &buf);
}

test "rle decode bool" {
    // RLE test data: 50 1s followed by 50 0s
    // 01100100 00000001 01100100 00000000
    var data1 = std.io.fixedBufferStream(&[_]u8{ 0x64, 0x01, 0x64, 0x00 });

    // Bit-packing test data: alternating 1s and 0s, 100 total
    // 100 / 8 = 13 groups
    // 00011011 10101010 ... 00001010
    var data2 = std.io.fixedBufferStream(&[_]u8{
        0x1B, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0x0A,
    });

    {
        var buf: [100]u1 = undefined;
        var expected: [100]u1 = undefined;
        for (0..100) |i| {
            expected[i] = if (i < 50) 1 else 0;
        }
        try decodeRleBitPackedHybrid(u1, &buf, 1, data1.reader());
        try std.testing.expectEqualSlices(u1, &expected, &buf);
    }

    {
        var buf: [100]u1 = undefined;
        var expected: [100]u1 = undefined;
        for (0..100) |i| {
            expected[i] = if (i % 2 != 0) 1 else 0;
        }
        try decodeRleBitPackedHybrid(u1, &buf, 1, data2.reader());
        try std.testing.expectEqualSlices(u1, &expected, &buf);
    }
}
