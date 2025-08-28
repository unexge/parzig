const std = @import("std");
const Reader = std.Io.Reader;
const bitReader = @import("../bit_reader.zig").bitReader;
const protocol_compact = @import("../thrift.zig").protocol_compact;

pub fn decodePlain(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    if (T == bool) {
        return @ptrCast(try decodeBitPacked(u1, gpa, len, 1, reader));
    }

    const buf = try gpa.alloc(T, len);
    for (0..len) |i| {
        if (T == []const u8) {
            const num_bytes = try reader.takeInt(u32, .little);
            const elem_buf = try gpa.alloc(u8, num_bytes);
            _ = try reader.readSliceAll(elem_buf);
            buf[i] = elem_buf;
        } else if (T == f32) {
            buf[i] = @bitCast(try reader.takeInt(u32, .little));
        } else if (T == f64) {
            buf[i] = @bitCast(try reader.takeInt(u64, .little));
        } else {
            buf[i] = try reader.takeInt(T, .little);
        }
    }
    return buf;
}

pub fn decodeRleDictionary(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    const bit_width = try reader.takeByte();
    const buf = try gpa.alloc(T, len);
    try decodeRleBitPackedHybrid(T, buf, bit_width, reader);
    return buf;
}

pub fn decodeLenghtPrependedRleBitPackedHybrid(comptime T: type, gpa: std.mem.Allocator, len: usize, bit_width: u8, reader: *Reader) ![]T {
    const lenght = try reader.takeVarInt(u32, .little, 4);
    if (lenght == 0) return error.EmptyBuffer;

    var limited_buf: [1024]u8 = undefined;
    var limited_reader = reader.limited(.limited(lenght), &limited_buf);

    const values = try gpa.alloc(T, len);
    try decodeRleBitPackedHybrid(T, values, bit_width, &limited_reader.interface);
    return values;
}

pub fn decodeRleBitPackedHybrid(comptime T: type, buf: []T, bit_width: u8, reader: *Reader) !void {
    var pos: usize = 0;
    while (buf.len > pos) {
        const header = try std.leb.readUleb128(i64, reader.adaptToOldInterface());
        if (header & 1 == 1) {
            // bit packet run
            var bit_reader = bitReader(.little, reader.adaptToOldInterface());
            const len = @min(buf.len - pos, @as(usize, @intCast((header >> 1) * 8)));
            for (0..len) |i| {
                buf[pos + i] = try bit_reader.readBitsNoEof(T, bit_width);
            }
            pos += len;
        } else {
            // rle run
            var bit_reader = bitReader(.little, reader.adaptToOldInterface());
            const len = @min(buf.len - pos, @as(usize, @intCast(header >> 1)));
            const val = try bit_reader.readBitsNoEof(T, bit_width);
            for (0..len) |i| {
                buf[pos + i] = val;
            }
            pos += len;
        }
    }
}

pub fn decodeBitPacked(comptime T: type, gpa: std.mem.Allocator, len: usize, bit_width: u8, reader: *Reader) ![]T {
    const values = try gpa.alloc(T, len);
    var bit_reader = bitReader(.little, reader.adaptToOldInterface());
    for (0..len) |i| {
        values[i] = try bit_reader.readBitsNoEof(T, bit_width);
    }
    return values;
}

pub fn decodeDeltaBinaryPacked(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    if (T != i32 and T != i64) {
        return error.UnsupportedType;
    }

    const block_size = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
    // "the block size __is a multiple of 128__; it is stored as a ULEB128 int"
    if (block_size % 128 != 0) {
        return error.IncorrectBlockSize;
    }

    const miniblock_count = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
    const miniblock_value_count = block_size / miniblock_count;
    // "the miniblock count per block is a divisor of the block size such that their quotient,
    //  the number of values in a miniblock, __is a multiple of 32__; it is stored as a ULEB128 int"
    if (miniblock_value_count % 32 != 0) {
        return error.IncorrectMiniBlockSize;
    }

    const value_count = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
    if (value_count != len) {
        std.debug.print("Incorrect value count: {}, expected: {}\n", .{ value_count, len });
        return error.IncorrectValueCount;
    }

    const first_value = try protocol_compact.readZigZagInt(T, reader);

    const result = try gpa.alloc(T, value_count);
    if (value_count == 0) {
        return result;
    }

    result[0] = first_value;

    const bit_widths = try gpa.alloc(u8, miniblock_count);

    var read: usize = 1;
    var current_value = first_value;

    while (value_count > read) {
        const min_delta = try protocol_compact.readZigZagInt(T, reader);
        try reader.readSliceAll(bit_widths);

        var bit_reader = bitReader(.little, reader.adaptToOldInterface());

        for (bit_widths) |bit_width| {
            if (read == value_count) {
                break;
            }

            for (0..miniblock_value_count) |_| {
                const delta: T = @truncate(try bit_reader.readBitsNoEof(i256, bit_width));
                if (read == value_count) {
                    continue;
                }

                const total_delta = @addWithOverflow(delta, min_delta);
                const new_val = @addWithOverflow(current_value, total_delta[0]);
                current_value = new_val[0];
                result[read] = current_value;
                read += 1;
            }
        }
    }

    return result;
}

pub fn decodeDeltaLengthByteArray(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    if (T != []const u8) {
        return error.UnsupportedType;
    }

    const lengths = try decodeDeltaBinaryPacked(i32, gpa, len, reader);
    var total_length: usize = 0;
    for (lengths) |length| {
        total_length += @intCast(length);
    }

    const buf = try gpa.alloc(u8, total_length);
    try reader.readSliceAll(buf);

    const values = try gpa.alloc(T, len);
    var pos: usize = 0;
    for (lengths, 0..) |length, i| {
        const start = pos;
        pos += @intCast(length);
        values[i] = buf[start..pos];
    }

    return values;
}

pub fn decodeDeltaByteArray(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    if (T != []const u8) {
        return error.UnsupportedType;
    }

    const prefix_lengths = try decodeDeltaBinaryPacked(i32, gpa, len, reader);
    const suffixes = try decodeDeltaLengthByteArray(T, gpa, len, reader);
    if (suffixes.len < 2) {
        return suffixes;
    }

    for (suffixes[1..], prefix_lengths[1..], 1..) |suffix, prefix_len, i| {
        const prefix = suffixes[i - 1];
        if (prefix.len < prefix_len) {
            return error.InvalidPrefixLength;
        }

        const final = try std.mem.concat(gpa, u8, &[_]T{ prefix[0..@intCast(prefix_len)], suffix });
        suffixes[i] = final;
    }

    return suffixes;
}

pub fn decodeByteStreamSplit(comptime T: type, gpa: std.mem.Allocator, len: usize, reader: *Reader) ![]T {
    if (T != f32 and T != f64) {
        return error.UnsupportedType;
    }

    const Bytesize = @typeInfo(T).float.bits / 8;
    const Int = if (T == f32) u32 else u64;
    const size = Bytesize * len;

    const buf = try gpa.alloc(u8, size);
    try reader.readSliceAll(buf);

    const values = try gpa.alloc(T, len);
    for (0..len) |i| {
        var val: [Bytesize]u8 = undefined;
        inline for (0..Bytesize) |k| {
            val[k] = buf[i + len * k];
        }

        const int: Int = @bitCast(val);
        values[i] = @bitCast(int);
    }
    return values;
}

// Tests are borrowed from https://github.com/apache/arrow-rs/blob/ac51632af79b01738dbc87a27c4a95512cde2faf/parquet/src/encodings/rle.rs#L526

test "rle decode i32" {
    // Test data: 0-7 with bit width 3
    // 00000011 10001000 11000110 11111010
    var r: Reader = .fixed(&[_]u8{ 0x03, 0x88, 0xC6, 0xFA });
    var buf: [8]i32 = undefined;
    try decodeRleBitPackedHybrid(i32, &buf, 3, &r);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 1, 2, 3, 4, 5, 6, 7 }, &buf);
}

test "rle decode bool" {
    // RLE test data: 50 1s followed by 50 0s
    // 01100100 00000001 01100100 00000000
    var data1: Reader = .fixed(&[_]u8{ 0x64, 0x01, 0x64, 0x00 });

    // Bit-packing test data: alternating 1s and 0s, 100 total
    // 100 / 8 = 13 groups
    // 00011011 10101010 ... 00001010
    var data2: Reader = .fixed(&[_]u8{
        0x1B, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0x0A,
    });

    {
        var buf: [100]u1 = undefined;
        var expected: [100]u1 = undefined;
        for (0..100) |i| {
            expected[i] = if (i < 50) 1 else 0;
        }
        try decodeRleBitPackedHybrid(u1, &buf, 1, &data1);
        try std.testing.expectEqualSlices(u1, &expected, &buf);
    }

    {
        var buf: [100]u1 = undefined;
        var expected: [100]u1 = undefined;
        for (0..100) |i| {
            expected[i] = if (i % 2 != 0) 1 else 0;
        }
        try decodeRleBitPackedHybrid(u1, &buf, 1, &data2);
        try std.testing.expectEqualSlices(u1, &expected, &buf);
    }
}
