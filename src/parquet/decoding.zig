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
    // "the block size __is a multiple of 128__; it is stored as a ULEB128 int"
    if (block_size % 128 != 0) {
        return error.IncorrectBlockSize;
    }

    const miniblock_count = try std.leb.readULEB128(usize, reader);
    const miniblock_value_count = block_size / miniblock_count;
    // "the miniblock count per block is a divisor of the block size such that their quotient,
    //  the number of values in a miniblock, __is a multiple of 32__; it is stored as a ULEB128 int"
    if (miniblock_value_count % 32 != 0) {
        return error.IncorrectMiniBlockSize;
    }

    const value_count = try std.leb.readULEB128(usize, reader);
    if (value_count != len) {
        return error.IncorrectValueCount;
    }

    const first_value = try protocol_compact.readZigZagInt(T, reader);

    const result = try gpa.alloc(T, value_count);
    result[0] = first_value;

    const bit_widths = try gpa.alloc(u8, miniblock_count);

    var read: usize = 1;
    var current_value = first_value;

    values: while (value_count > read) {
        const min_delta = try protocol_compact.readZigZagInt(T, reader);
        try reader.readNoEof(bit_widths);

        var bit_reader = std.io.bitReader(.little, reader);

        for (bit_widths) |bit_width| {
            for (0..miniblock_value_count) |_| {
                const delta: T = @truncate(try bit_reader.readBitsNoEof(i256, bit_width));
                const total_delta = @addWithOverflow(delta, min_delta);
                const new_val = @addWithOverflow(current_value, total_delta[0]);
                current_value = new_val[0];
                result[read] = current_value;
                read += 1;

                if (read >= value_count) {
                    break :values;
                }
            }
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
