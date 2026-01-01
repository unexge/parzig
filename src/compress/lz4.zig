const std = @import("std");
const Io = std.Io;
const Reader = Io.Reader;
const Limit = Io.Limit;
const Writer = Io.Writer;

pub const Decompress = struct {
    pub const window_len = 65536;

    input: *Reader,
    reader: Reader,

    state: union(enum) {
        token,
        literal: struct {
            remaining: usize,
            match_len_short: u8,
        },
        read_offset: struct {
            match_len_short: u8,
        },
        read_match_len: struct {
            offset: usize,
            match_len_short: u8,
        },
        match: struct {
            offset: usize,
            len: usize,
        },
        eof,
    },

    pub fn init(input: *Reader, buffer: []u8) Decompress {
        std.debug.assert(buffer.len >= window_len);
        return .{
            .input = input,
            .reader = .{
                .vtable = &.{
                    .stream = Decompress.stream,
                },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .state = .token,
        };
    }

    fn stream(r: *Reader, w: *Writer, limit: Limit) Reader.StreamError!usize {
        const d: *Decompress = @fieldParentPtr("reader", r);
        const remaining = @intFromEnum(limit);

        read: switch (d.state) {
            .eof => {
                return error.EndOfStream;
            },
            .token => {
                const token = d.input.takeByte() catch |err| {
                    if (err == error.EndOfStream) {
                        d.state = .eof;
                    }
                    return err;
                };

                const literal_len_short: u4 = @truncate(token >> 4);
                const match_len_short: u4 = @truncate(token);

                // Read literal length (continuation bytes if literal_len_short == 15)
                var literal_len: usize = literal_len_short;
                if (literal_len_short == 15) {
                    while (true) {
                        const byte = try d.input.takeByte();
                        literal_len += byte;
                        if (byte != 255) break;
                        // Prevent malformed blocks from causing excessive memory allocation
                        if (literal_len > window_len) return error.ReadFailed;
                    }
                }

                // Transition to literal state (will read literals first)
                // Store match_len_short for later
                d.state = .{ .literal = .{
                    .remaining = literal_len,
                    .match_len_short = match_len_short,
                } };
                continue :read d.state;
            },
            .literal => |*lit| {
                if (lit.remaining == 0) {
                    // Done with literals
                    if (lit.match_len_short == 0) {
                        // Literal-only block (end of block marker)
                        d.state = .token;
                    } else {
                        // Read the offset for the match
                        d.state = .{ .read_offset = .{
                            .match_len_short = lit.match_len_short,
                        } };
                    }
                    continue :read d.state;
                }

                const out = limit.min(@enumFromInt(lit.remaining)).slice(try w.writableSliceGreedyPreserve(Decompress.window_len, 1));
                try d.input.readSliceAll(out);

                lit.remaining -= out.len;
                w.advance(out.len);
                return out.len;
            },
            .read_offset => |*ro| {
                const offset = try d.input.takeInt(u16, .little);
                if (offset == 0) {
                    // Invalid offset
                    d.state = .eof;
                    return error.EndOfStream;
                }

                const match_len_short = ro.match_len_short;

                // Now read match length continuation (if needed)
                if (match_len_short == 15) {
                    d.state = .{ .read_match_len = .{
                        .offset = offset,
                        .match_len_short = match_len_short,
                    } };
                    continue :read d.state;
                }

                const match_len = match_len_short + 4;
                d.state = .{ .match = .{
                    .offset = offset,
                    .len = match_len,
                } };
                continue :read d.state;
            },
            .read_match_len => |*rml| {
                // Read match length continuation bytes
                // Start with the short value shifted by 4 bits
                var match_len: usize = rml.match_len_short + 4;
                while (true) {
                    const byte = try d.input.takeByte();
                    match_len += byte;
                    if (byte != 255) break;
                    // Prevent malformed blocks from causing excessive match lengths
                    if (match_len > window_len) return error.ReadFailed;
                }

                d.state = .{ .match = .{
                    .offset = rml.offset,
                    .len = match_len,
                } };
                continue :read d.state;
            },
            .match => |*match_data| {
                if (match_data.len == 0) {
                    d.state = .token;
                    continue :read d.state;
                }

                std.debug.assert(match_data.offset > 0);
                // Note: w.end is the total bytes written so far, should be able to match
                if (match_data.offset > w.end) {
                    std.debug.print("ERROR: Match offset {d} > w.end {d}\n", .{ match_data.offset, w.end });
                    return error.EndOfStream;
                }

                // For overlapping matches, copy one byte at a time to handle the case
                // where the match source hasn't been fully written yet
                const n = @min(match_data.len, remaining);
                const src_start = w.end - match_data.offset;
                const dst = try w.writableSliceGreedyPreserve(Decompress.window_len, n);

                // Copy byte by byte from the source position
                for (0..n) |i| {
                    const src_idx = src_start + (i % match_data.offset);
                    dst[i] = w.buffer[src_idx];
                }

                match_data.len -= n;
                w.advance(n);

                return n;
            },
        }
    }
};

const testing = std.testing;

test "single byte literal" {
    try expectDecoded("\x10A", "A");
}

test "multiple byte literal" {
    try expectDecoded("\x30ABC", "ABC");
}

test "14 byte literal" {
    try expectDecoded("\xE0ABCDEFGHIJKLMN", "ABCDEFGHIJKLMN");
}

test "15 byte literal (extended length)" {
    try expectDecoded("\xF0\x00ABCDEFGHIJKLMNO", "ABCDEFGHIJKLMNO");
}

test "16 byte literal (extended length)" {
    try expectDecoded("\xF0\x01ABCDEFGHIJKLMNOP", "ABCDEFGHIJKLMNOP");
}

test "270 byte literal (extended length)" {
    const literals = "A" ** 270;
    try expectDecoded("\xF0\xFF\x00" ++ literals, literals);
}

test "literal and match" {
    try expectDecoded("\x11A\x01\x00", "AAAAAA");
}

test "literal with extended match length" {
    try expectDecoded("\x1fA\x01\x00\x01", "A" ** 21);
}

test "minimum match length" {
    try expectDecoded("\x14A\x01\x00", "A" ** 9);
}

test "match with 255+ length continuation" {
    const input = "\x1FA\x01\x00\xFF\x01";
    var expected: [1 + 275]u8 = undefined;
    @memset(expected[0..], 'A');
    try expectDecoded(input, expected[0..]);
}

test "multiple blocks" {
    try expectDecoded("\x10A\x10B", "AB");
}

test "match copying backwards in buffer" {
    try expectDecoded("\x24AB\x02\x00", "ABABABABAB");
}

fn expectDecoded(input: []const u8, expected: []const u8) !void {
    var fixed: Reader = .fixed(input);

    var decompress_buf: [Decompress.window_len]u8 = undefined;
    var decompress = Decompress.init(&fixed, &decompress_buf);

    var decoded: Writer.Allocating = .init(testing.allocator);
    defer decoded.deinit();

    _ = try decompress.reader.streamRemaining(&decoded.writer);
    try testing.expectEqualSlices(u8, expected, decoded.written());
}
