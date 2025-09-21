const std = @import("std");
const assert = std.debug.assert;
const Reader = std.Io.Reader;

const BitReader = @This();

input: *Reader,
bit_width: u8,
buf: u8,
len: u8,

pub fn init(input: *Reader, bit_width: u8) BitReader {
    assert(bit_width <= 256);
    return .{ .input = input, .bit_width = bit_width, .buf = 0, .len = 0 };
}

pub fn take(self: *BitReader, comptime T: type) !T {
    if (T == bool) {
        return try self.take(u1) != 0;
    }

    // Values packed from LSB to MSB, so we read the bits first and then process them in reverse order to construct the value
    var bits: [256]u1 = undefined;
    for (0..self.bit_width) |i| {
        bits[i] = try self.takeBit();
    }

    var res: T = 0;
    var i = self.bit_width;
    while (i > 0) {
        i -= 1;
        res <<|= 1;
        res |= bits[i];
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

    const res: u1 = @truncate(self.buf);
    self.len -= 1;
    self.buf >>= 1;
    return res;
}

test BitReader {
    var input: Reader = .fixed(&.{ 0b10001000, 0b11000110, 0b11111010 });
    var reader = BitReader.init(&input, 3);
    defer reader.assertZeroPad();

    for (0..8) |i| {
        try testing.expectEqual(i, try reader.take(u32));
    }
}

test "bool" {
    var input: Reader = .fixed(&.{0b01010101});
    var reader = BitReader.init(&input, 1);
    defer reader.assertZeroPad();

    for ([_]bool{ true, false, true, false, true, false, true, false }) |b| {
        try testing.expectEqual(b, try reader.take(bool));
    }
}

const testing = std.testing;
