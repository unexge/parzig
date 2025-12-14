// Implements reading of Parquet's physical types, which are (with corresponding Zig types):
//  - BOOLEAN: bool
//  - INT32: i32
//  - INT64: i64
//  - INT96: i96
//  - FLOAT: f32
//  - DOUBLE: f64
//  - BYTE_ARRAY: []u8
//  - FIXED_LEN_BYTE_ARRAY: [N]u8
//
// See https://parquet.apache.org/docs/file-format/data-pages/encodings/

pub fn plain(comptime T: type, arena: Allocator, reader: *Reader, buf: []T) !void {
    if (T == bool) {
        var bit_reader: BitReader = .init(reader, .little, 1);
        for (buf) |*item| {
            item.* = try bit_reader.take(bool);
        }
        return;
    }

    for (buf) |*item| {
        switch (T) {
            i32, i64, i96 => {
                item.* = try reader.takeInt(T, .little);
            },
            f32 => {
                item.* = @bitCast(try reader.takeInt(u32, .little));
            },
            f64 => {
                item.* = @bitCast(try reader.takeInt(u64, .little));
            },
            []u8, []const u8 => {
                const num_bytes = try reader.takeInt(u32, .little);
                const byte_buf = try arena.alloc(u8, num_bytes);
                try reader.readSliceAll(byte_buf);
                item.* = byte_buf;
            },
            else => {
                switch (@typeInfo(T)) {
                    .array => |arr| {
                        if (arr.child == u8) {
                            const byte_buf = try arena.alloc(u8, arr.len);
                            try reader.readSliceAll(byte_buf);
                            item.* = byte_buf[0..arr.len].*;
                        } else {
                            @compileError("Array child type must be u8, not: " ++ @typeName(arr.child));
                        }
                    },
                    else => {
                        @compileError("Unsupported type: " ++ @typeName(T));
                    },
                }
            },
        }
    }
}

pub fn dictionary(comptime T: type, reader: *Reader, buf: []T) !void {
    const bit_width = try reader.takeByte();
    return runLengthBitPackedHybrid(T, reader, bit_width, buf);
}

pub fn runLengthBitPackedHybridLengthPrepended(comptime T: type, reader: *Reader, bit_width: u8, buf: []T) !void {
    const lenght = try reader.takeVarInt(u32, .little, 4);
    if (lenght == 0) return error.EmptyBuffer;

    return runLengthBitPackedHybrid(T, reader, bit_width, buf);
}

pub fn runLengthBitPackedHybrid(comptime T: type, reader: *Reader, bit_width: u8, buf: []T) !void {
    var pos: usize = 0;
    while (buf.len > pos) {
        const header = try reader.takeLeb128(u64);
        if (header & 1 == 1) {
            // bit-packed run
            var bit_reader: BitReader = .init(reader, .little, bit_width);
            const len = @min(buf.len - pos, @as(usize, @intCast((header >> 1) * 8)));
            for (0..len) |i| {
                buf[pos + i] = try bit_reader.take(T);
            }
            pos += len;
        } else {
            // run length run
            var bit_reader: BitReader = .init(reader, .little, bit_width);
            const len = @min(buf.len - pos, @as(usize, @intCast(header >> 1)));
            const val = try bit_reader.take(T);
            for (0..len) |i| {
                buf[pos + i] = val;
            }
            pos += len;
        }
    }
}

pub fn bitPacked(comptime T: type, reader: *Reader, bit_width: u8, buf: []T) !void {
    var bit_reader: BitReader = .init(reader, .big, bit_width);
    for (0..buf.len) |i| {
        buf[i] = try bit_reader.take(T);
    }
}

pub fn delta(comptime T: type, arena: Allocator, reader: *Reader, buf: []T) !usize {
    if (T != i32 and T != i64) {
        return error.UnsupportedType;
    }

    const block_size = try reader.takeLeb128(usize);
    // "the block size __is a multiple of 128__; it is stored as a ULEB128 int"
    if (block_size % 128 != 0) {
        return error.IncorrectBlockSize;
    }

    const miniblock_count = try reader.takeLeb128(usize);
    const miniblock_value_count = block_size / miniblock_count;
    // "the miniblock count per block is a divisor of the block size such that their quotient,
    //  the number of values in a miniblock, __is a multiple of 32__; it is stored as a ULEB128 int"
    if (miniblock_value_count % 32 != 0) {
        return error.IncorrectMiniBlockSize;
    }

    const value_count = try reader.takeLeb128(usize);
    if (value_count != buf.len) {
        return error.IncorrectValueCount;
    }

    const first_value = try protocol_compact.readZigZagInt(T, reader);
    if (value_count == 0) {
        return 0;
    }

    buf[0] = first_value;

    const bit_widths = try arena.alloc(u8, miniblock_count);
    defer arena.free(bit_widths);

    var read: usize = 1;
    var current_value = first_value;

    while (value_count > read) {
        const min_delta = try protocol_compact.readZigZagInt(T, reader);
        try reader.readSliceAll(bit_widths);

        for (bit_widths) |bit_width| {
            if (read == value_count) {
                break;
            }

            var bit_reader: BitReader = .init(reader, .little, bit_width);

            for (0..miniblock_value_count) |_| {
                const delta_val: T = @truncate(try bit_reader.take(i256));
                if (read == value_count) {
                    continue;
                }

                const total_delta = @addWithOverflow(delta_val, min_delta);
                const new_val = @addWithOverflow(current_value, total_delta[0]);
                current_value = new_val[0];
                buf[read] = current_value;
                read += 1;
            }
        }
    }

    return value_count;
}

pub fn deltaLengthByteArray(comptime T: type, arena: Allocator, reader: *Reader, buf: []T) !void {
    if (T != []u8) {
        return error.UnsupportedType;
    }

    const lengths = try arena.alloc(i32, buf.len);
    const n = try delta(i32, arena, reader, lengths);
    if (n != lengths.len) {
        return error.InvalidLength;
    }

    var total_length: usize = 0;
    for (lengths) |length| {
        total_length += @intCast(length);
    }

    const text_buf = try arena.alloc(u8, total_length);
    try reader.readSliceAll(text_buf);

    var pos: usize = 0;
    for (lengths, 0..) |length, i| {
        const start = pos;
        pos += @intCast(length);
        buf[i] = text_buf[start..pos];
    }
}

pub fn deltaStrings(comptime T: type, arena: Allocator, reader: *Reader, buf: []T) !void {
    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (ptr.child != u8) {
                std.debug.print("Slice child type must be u8, not: {any}\n", .{@typeName(ptr.child)});
                return error.UnsupportedType;
            }
            if (ptr.size != .slice) {
                std.debug.print("Pointer must be a slice, not: {any}\n", .{@typeName(ptr)});
                return error.UnsupportedType;
            }
        },
        else => {
            std.debug.print("Unsupported type: {any}\n", .{@typeName(T)});
            return error.UnsupportedType;
        },
    }

    const prefix_lengths = try arena.alloc(i32, buf.len);
    const n = try delta(i32, arena, reader, prefix_lengths);
    if (n != prefix_lengths.len) {
        return error.InvalidPrefixLength;
    }

    try deltaLengthByteArray(T, arena, reader, buf);
    if (buf.len < 2) {
        return;
    }

    for (buf[1..], prefix_lengths[1..], 1..) |suffix, prefix_len, i| {
        const prefix = buf[i - 1];
        if (prefix.len < prefix_len) {
            return error.InvalidPrefixLength;
        }

        const final = try std.mem.concat(arena, u8, &[_]T{ prefix[0..@intCast(prefix_len)], suffix });
        buf[i] = final;
    }
}

pub fn byteStreamSplit(comptime T: type, arena: Allocator, reader: *Reader, buf: []T) !void {
    if (T != f32 and T != f64) {
        return error.UnsupportedType;
    }

    const Bytesize = @typeInfo(T).float.bits / 8;
    const Int = if (T == f32) u32 else u64;
    const size = Bytesize * buf.len;

    const value_buf = try arena.alloc(u8, size);
    try reader.readSliceAll(value_buf);

    for (0..buf.len) |i| {
        var val: [Bytesize]u8 = undefined;
        inline for (0..Bytesize) |k| {
            val[k] = value_buf[i + buf.len * k];
        }

        const int: Int = @bitCast(val);
        buf[i] = @bitCast(int);
    }
}

test bitPacked {
    var input: Reader = .fixed(&.{ 0b00000101, 0b00111001, 0b01110111 });
    const buf = try testing.allocator.alloc(u8, 8);
    defer testing.allocator.free(buf);

    try bitPacked(u8, &input, 3, buf);

    try testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3, 4, 5, 6, 7 }, buf);
}

const testing = std.testing;

const protocol_compact = @import("../thrift.zig").protocol_compact;
const BitReader = @import("./BitReader.zig");
const Allocator = std.mem.Allocator;
const Reader = Io.Reader;
const Io = std.Io;
const std = @import("std");
