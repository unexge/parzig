const std = @import("std");

pub fn decodePlain(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: anytype) ![]T {
    const buf = try gpa.alloc(T, len);
    const is_byte_array = T == []u8;
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
