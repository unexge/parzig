const std = @import("std");
const assert = std.debug.assert;
const Reader = std.Io.Reader;
const Endian = std.builtin.Endian;

const BitReader = @This();

input: *Reader,
endian: Endian,
bit_width: u8,
buf: u8,
len: u8,

pub fn init(input: *Reader, endian: Endian, bit_width: u8) BitReader {
    assert(bit_width <= 256);
    return .{ .input = input, .endian = endian, .bit_width = bit_width, .buf = 0, .len = 0 };
}

pub fn take(self: *BitReader, comptime T: type) !T {
    if (T == bool) {
        return try self.take(u1) != 0;
    }

    var bits: [256]u1 = undefined;
    for (0..self.bit_width) |i| {
        bits[i] = try self.takeBit();
    }

    var res: T = 0;
    for (0..self.bit_width) |i| {
        const idx = switch (self.endian) {
            .little => self.bit_width - 1 - i,
            .big => i,
        };
        res <<|= 1;
        res |= bits[idx];
    }
    return res;
}

pub fn assertZeroPad(self: *BitReader) void {
    assert(self.buf == 0);
    assert(self.len == 0);
}

fn takeBit(self: *BitReader) !u1 {
    if (self.len == 0) {
        self.buf = try self.input.takeByte();
        self.len = 8;
    }

    self.len -= 1;

    switch (self.endian) {
        .little => {
            const res: u1 = @truncate(self.buf);
            self.buf >>= 1;
            return res;
        },
        .big => {
            const res: u1 = @truncate(self.buf >> 7);
            self.buf <<= 1;
            return res;
        },
    }
}

test BitReader {
    var input: Reader = .fixed(&.{ 0b10001000, 0b11000110, 0b11111010 });
    var reader = BitReader.init(&input, .little, 3);
    defer reader.assertZeroPad();

    for (0..8) |i| {
        try testing.expectEqual(i, try reader.take(u32));
    }
}

test "bool" {
    var input: Reader = .fixed(&.{0b01010101});
    var reader = BitReader.init(&input, .little, 1);
    defer reader.assertZeroPad();

    for ([_]bool{ true, false, true, false, true, false, true, false }) |b| {
        try testing.expectEqual(b, try reader.take(bool));
    }
}

const testing = std.testing;
