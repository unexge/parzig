const std = @import("std");

pub fn Decoder(comptime Inner: type) type {
    return struct {
        inner: Inner,
        inner_remaining: usize = std.math.maxInt(usize),

        gpa: std.mem.Allocator,
        // TODO: Keeping a copy of encoded buffer is not very efficient.
        //       Maybe this type shouldn't be a reader and return a buffer instead.
        buffer: []const u8 = undefined,
        buffer_pos: usize = 0,

        state: union(enum) {
            length,
            literal: usize,
            copy: struct {
                start: usize,
                end: usize,
            },
            eof,
        } = .length,

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

        pub const Error = error{
            Overflow,
            EndOfStream,
            OutOfMemory,
        } || Inner.Error;
        pub const Reader = std.io.Reader(*Self, Error, read);

        const Self = @This();

        pub fn init(inner: Inner, gpa: std.mem.Allocator) Self {
            return .{ .inner = inner, .gpa = gpa };
        }

        pub fn deinit(self: *Self) void {
            self.gpa.free(self.buffer);
        }

        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }

        pub fn read(self: *Self, buffer: []u8) Error!usize {
            var buffer_pos: usize = 0;

            while (buffer_pos < buffer.len and self.inner_remaining > 0) {
                switch (self.state) {
                    .eof => {
                        return buffer_pos;
                    },
                    .length => {
                        self.inner_remaining = try std.leb.readULEB128(u32, self.inner);
                        self.buffer = try self.gpa.alloc(u8, self.inner_remaining);
                        try self.readTag();
                    },
                    .literal => |remaining| {
                        if (remaining == 0) {
                            try self.readTag();
                            continue;
                        }

                        const buffer_remaining = buffer.len - buffer_pos;
                        const to_read = @min(remaining, @min(self.inner_remaining, buffer_remaining));
                        const buffer_end_pos = buffer_pos + to_read;

                        const n = try self.inner.read(buffer[buffer_pos..buffer_end_pos]);
                        @memcpy(@constCast(self.buffer[self.buffer_pos..(self.buffer_pos + n)]), buffer[buffer_pos..(buffer_pos + n)]);

                        self.inner_remaining -= n;
                        self.state.literal -= n;
                        buffer_pos += n;
                        self.buffer_pos += n;
                    },
                    .copy => |*copy| {
                        if (copy.*.start >= copy.*.end) {
                            std.debug.assert(copy.*.start == copy.*.end);
                            try self.readTag();
                            continue;
                        }

                        const copy_remaining = copy.*.end - copy.*.start;
                        const buffer_remaining = buffer.len - buffer_pos;
                        const to_read = @min(copy_remaining, buffer_remaining);
                        std.debug.assert(to_read <= 64);

                        const src = self.buffer[copy.*.start..(copy.*.start + to_read)];

                        @memcpy(buffer[buffer_pos..(buffer_pos + to_read)], src);
                        std.mem.copyForwards(u8, @constCast(self.buffer[self.buffer_pos..(self.buffer_pos + to_read)]), src);

                        copy.*.start += to_read;
                        buffer_pos += to_read;
                        self.buffer_pos += to_read;
                    },
                }
            }

            return buffer_pos;
        }

        fn readTag(self: *Self) !void {
            const tag = try blk: {
                const tag = self.inner.readByte();
                if (tag == error.EndOfStream) {
                    self.state = .eof;
                    return;
                }
                break :blk tag;
            };

            const tag_higher: u6 = @truncate(tag >> 2);

            switch (Element.init(tag)) {
                .literal => {
                    const len: u64 = switch (tag_higher) {
                        60 => try self.inner.readInt(u8, .little),
                        61 => try self.inner.readInt(u16, .little),
                        62 => try self.inner.readInt(u24, .little),
                        63 => try self.inner.readInt(u32, .little),
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
                    const offset_lower = try self.inner.readByte();
                    const offset: u11 = (@as(u11, offset_higher) << 8) | offset_lower;

                    std.debug.assert(len <= 2047);

                    const start = self.buffer_pos - offset;
                    const end = start + len;

                    self.state = .{ .copy = .{
                        .start = start,
                        .end = end,
                    } };
                },
                .copy2b => {
                    const len: u8 = @as(u8, tag_higher) + 1;
                    const offset = try self.inner.readInt(u16, .little);

                    std.debug.assert(len <= 64);
                    std.debug.assert(offset <= 65535);

                    const start = self.buffer_pos - offset;
                    const end = start + len;

                    self.state = .{ .copy = .{
                        .start = start,
                        .end = end,
                    } };
                },
                .copy4b => {
                    const len: u8 = tag_higher + 1;
                    const offset = try self.inner.readInt(u32, .little);

                    std.debug.assert(len <= 64);

                    const start = self.buffer_pos - offset;
                    const end = start + len;

                    self.state = .{ .copy = .{
                        .start = start,
                        .end = end,
                    } };
                },
            }
        }
    };
}

pub fn decoder(reader: anytype, gpa: std.mem.Allocator) Decoder(@TypeOf(reader)) {
    return Decoder(@TypeOf(reader)).init(reader, gpa);
}

const testing = std.testing;

// Tests are borrowed from https://github.com/golang/snappy/blob/43d5d4cd4e0e3390b0b645d5c3ef1187642403d8/snappy_test.go.

test "literal inline tag length" {
    expectDecoded("\x03\x08\xff\xff\xff", "\xff\xff\xff");
}

test "literal 1-byte length" {
    expectDecoded("\x03\xf0\x02\xff\xff\xff", "\xff\xff\xff");
}

test "literal 2-byte length" {
    expectDecoded("\x03\xf4\x02\x00\xff\xff\xff", "\xff\xff\xff");
}

test "literal 3-byte length" {
    expectDecoded("\x03\xf8\x02\x00\x00\xff\xff\xff", "\xff\xff\xff");
}

test "literal 4-byte length" {
    expectDecoded("\x03\xfc\x02\x00\x00\x00\xff\xff\xff", "\xff\xff\xff");
}

test "copy 4-byte" {
    const dots = "." ** 65536;

    const input =
        "\x89\x80\x04" ++ // decodedLen = 65545.
        "\x0cpqrs" ++ // 4-byte literal "pqrs".
        "\xf4\xff\xff" ++ dots ++ // 65536-byte literal dots.
        "\x13\x04\x00\x01\x00"; // tagCopy4; length=5 offset=65540.
    const expected = "pqrs" ++ dots ++ "pqrs.";

    expectDecoded(input, expected);
}

test "golden" {
    const compressed_file = try std.fs.cwd().openFile("testdata/compress/snappy/Isaac.Newton-Opticks.txt.rawsnappy", .{ .mode = .read_only });
    const compressed = try compressed_file.reader().readAllAlloc(std.testing.allocator, std.math.maxInt(usize));

    const source_file = try std.fs.cwd().openFile("testdata/compress/snappy/Isaac.Newton-Opticks.txt", .{ .mode = .read_only });
    const source = try source_file.reader().readAllAlloc(std.testing.allocator, std.math.maxInt(usize));

    expectDecoded(compressed, source);
}

fn expectDecoded(input: []const u8, expected: []const u8) void {
    var fbs = std.io.fixedBufferStream(input);
    var dec = decoder(fbs.reader(), testing.allocator);
    defer dec.deinit();

    const decoded = dec.reader().readAllAlloc(testing.allocator, std.math.maxInt(usize)) catch unreachable;
    defer testing.allocator.free(decoded);

    std.testing.expectEqualStrings(expected, decoded) catch unreachable;
}
