const std = @import("std");
const Io = std.Io;
const Reader = Io.Reader;
const Limit = Io.Limit;
const Writer = Io.Writer;

pub const Decompress = struct {
    pub const window_len = 65536;

    input: *Reader,
    remaining: usize,
    reader: Reader,

    state: union(enum) {
        length,
        literal: usize,
        copy: struct {
            offset: usize,
            len: usize,
        },
        eof,
    },

    const Element = enum(u2) {
        literal = 0,
        copy1b = 1,
        copy2b = 2,
        copy4b = 3,

        pub fn init(tag: u8) Element {
            const el: u2 = @truncate(tag);
            return @enumFromInt(el);
        }
    };

    pub fn init(input: *Reader, buffer: []u8) Decompress {
        return .{
            .input = input,
            .remaining = 0,
            .reader = .{
                .vtable = &.{
                    .stream = Decompress.stream,
                },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .state = .length,
        };
    }

    fn stream(r: *Reader, w: *Writer, limit: Limit) Reader.StreamError!usize {
        const d: *Decompress = @fieldParentPtr("reader", r);
        const remaining = @intFromEnum(limit);

        read: switch (d.state) {
            .eof => {
                return error.EndOfStream;
            },
            .length => {
                const size = d.input.takeLeb128(u32) catch |err| {
                    switch (err) {
                        error.Overflow => return error.ReadFailed,
                        else => |other| return other,
                    }
                };
                d.remaining = @intCast(size);
                try d.readTag();
                std.debug.assert(d.state != .length);
                continue :read d.state;
            },
            .literal => |*literal_remaining| {
                if (literal_remaining.* == 0) {
                    try d.readTag();
                    continue :read d.state;
                }

                const out = limit.min(@enumFromInt(literal_remaining.*)).min(@enumFromInt(d.remaining)).slice(try w.writableSliceGreedyPreserve(Decompress.window_len, 1));
                try d.input.readSliceAll(out);

                literal_remaining.* -= out.len;
                d.remaining -= out.len;
                w.advance(out.len);
                return out.len;
            },
            .copy => |*copy| {
                if (copy.len == 0) {
                    try d.readTag();
                    continue :read d.state;
                }

                const n = @min(copy.len, remaining);
                std.debug.assert(n <= 64);

                const start = w.end - copy.offset;
                const end = @min(start + n, w.end);
                const length = end - start;

                const src = w.buffer[start..end];
                const dst = try w.writableSliceGreedyPreserve(Decompress.window_len, length);
                @memmove(dst[0..length], src);

                copy.len -= length;
                d.remaining -= length;
                w.advance(length);

                return length;
            },
        }
    }

    fn readTag(self: *Decompress) !void {
        const tag = self.input.takeByte() catch |err| {
            if (err == error.EndOfStream) {
                self.state = .eof;
            }
            return err;
        };

        const tag_higher: u6 = @truncate(tag >> 2);

        switch (Element.init(tag)) {
            .literal => {
                const len: u64 = switch (tag_higher) {
                    60 => try self.input.takeInt(u8, .little),
                    61 => try self.input.takeInt(u16, .little),
                    62 => try self.input.takeInt(u24, .little),
                    63 => try self.input.takeInt(u32, .little),
                    else => tag_higher,
                };

                self.state = .{ .literal = len + 1 };
            },
            .copy1b => {
                const tag_lower: u3 = @truncate(tag_higher);
                const len: u8 = @as(u8, @intCast(tag_lower)) + 4;

                std.debug.assert(len >= 4);
                std.debug.assert(len <= 11);

                const offset_higher: u3 = @truncate(tag_higher >> 3);
                const offset_lower = try self.input.takeByte();
                const offset: u11 = (@as(u11, offset_higher) << 8) | offset_lower;

                std.debug.assert(len <= 2047);

                self.state = .{ .copy = .{
                    .offset = offset,
                    .len = len,
                } };
            },
            .copy2b => {
                const len: u8 = @as(u8, tag_higher) + 1;
                const offset = try self.input.takeInt(u16, .little);

                std.debug.assert(len <= 64);
                std.debug.assert(offset <= 65535);

                self.state = .{ .copy = .{
                    .offset = offset,
                    .len = len,
                } };
            },
            .copy4b => {
                const len: u8 = tag_higher + 1;
                const offset = try self.input.takeInt(u32, .little);

                std.debug.assert(len <= 64);

                self.state = .{ .copy = .{
                    .offset = offset,
                    .len = len,
                } };
            },
        }
    }
};

const testing = std.testing;

// Tests are borrowed from https://github.com/golang/snappy/blob/43d5d4cd4e0e3390b0b645d5c3ef1187642403d8/snappy_test.go.

test "literal inline tag length" {
    try expectDecoded("\x03\x08\xff\xff\xff", "\xff\xff\xff");
}

test "literal 1-byte length" {
    try expectDecoded("\x03\xf0\x02\xff\xff\xff", "\xff\xff\xff");
}

test "literal 2-byte length" {
    try expectDecoded("\x03\xf4\x02\x00\xff\xff\xff", "\xff\xff\xff");
}

test "literal 3-byte length" {
    try expectDecoded("\x03\xf8\x02\x00\x00\xff\xff\xff", "\xff\xff\xff");
}

test "literal 4-byte length" {
    try expectDecoded("\x03\xfc\x02\x00\x00\x00\xff\xff\xff", "\xff\xff\xff");
}

test "copy 1-byte" {
    {
        const input = "\x0d" ++ // decodedLen=13;
            "\x0cabcd" ++ // tagLiteral (4 bytes "abcd");
            "\x15\x04"; // tagCopy1; length=9 offset=4;
        const expected = "abcdabcdabcda";

        try expectDecoded(input, expected);
    }

    {
        const input = "\x08" ++ // decodedLen=8;
            "\x0cabcd" ++ // tagLiteral (4 bytes "abcd");
            "\x01\x04"; // tagCopy1; length=4 offset=4;
        const expected = "abcdabcd";

        try expectDecoded(input, expected);
    }

    {
        const input = "\x08" ++ // decodedLen=8;
            "\x0cabcd" ++ // tagLiteral (4 bytes "abcd");
            "\x01\x02"; // tagCopy1; length=4 offset=2;
        const expected = "abcdcdcd";

        try expectDecoded(input, expected);
    }

    {
        const input = "\x08" ++ // decodedLen=8;
            "\x0cabcd" ++ // tagLiteral (4 bytes "abcd");
            "\x01\x01"; // tagCopy1; length=4 offset=1;
        const expected = "abcddddd";

        try expectDecoded(input, expected);
    }
}

test "copy 2-byte" {
    const input = "\x06" ++ // decodedLen=6;
        "\x0cabcd" ++ // tagLiteral (4 bytes "abcd");
        "\x06\x03\x00"; // tagCopy2; length=2 offset=3;
    const expected = "abcdbc";

    try expectDecoded(input, expected);
}

test "copy 4-byte" {
    const dots = "." ** 65536;

    const input = "\x89\x80\x04" ++ // decodedLen=65545;
        "\x0cpqrs" ++ // 4-byte literal "pqrs";
        "\xf4\xff\xff" ++ dots ++ // 65536-byte literal dots;
        "\x13\x04\x00\x01\x00"; // tagCopy4; length=5 offset=65540;
    const expected = "pqrs" ++ dots ++ "pqrs.";

    try expectDecoded(input, expected);
}

test "golden" {
    const compressed_file = try Io.Dir.cwd().openFile(testing.io, "testdata/compress/snappy/Isaac.Newton-Opticks.txt.rawsnappy", .{ .mode = .read_only });
    var compressed_buf: [1024]u8 = undefined;
    var compressed_reader = compressed_file.reader(testing.io, &compressed_buf);
    const compressed = try compressed_reader.interface.allocRemaining(testing.allocator, .unlimited);
    defer testing.allocator.free(compressed);

    const source_file = try Io.Dir.cwd().openFile(testing.io, "testdata/compress/snappy/Isaac.Newton-Opticks.txt", .{ .mode = .read_only });
    var source_buf: [1024]u8 = undefined;
    var source_reader = source_file.reader(testing.io, &source_buf);
    const source = try source_reader.interface.allocRemaining(testing.allocator, .unlimited);
    defer testing.allocator.free(source);

    try expectDecoded(compressed, source);
}

fn expectDecoded(input: []const u8, expected: []const u8) !void {
    var fixed: Reader = .fixed(input);

    var decompress_buf: [1024]u8 = undefined;
    var decompress = Decompress.init(&fixed, &decompress_buf);

    var decoded: Writer.Allocating = .init(testing.allocator);
    defer decoded.deinit();
    _ = try decompress.reader.streamRemaining(&decoded.writer);

    try testing.expectEqualStrings(expected, decoded.written());
}
