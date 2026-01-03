const std = @import("std");
const Io = std.Io;
const Reader = Io.Reader;
const Limit = Io.Limit;
const Writer = Io.Writer;

/// LZ4 block format decompressor with streaming API.
///
/// Implements the LZ4 raw block format (without framing) for decompressing
/// data pages in Parquet files. The decompressor uses a state machine to
/// handle partial decompression across multiple calls, allowing efficient
/// streaming decompression without loading entire blocks into memory.
///
/// The window_len constant (64KB) specifies the maximum lookback distance
/// for match references. The provided buffer must be at least window_len
/// bytes to properly handle all valid match offsets.
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

    fn readContinuationBytes(input: *Reader, base_value: usize) Reader.StreamError!usize {
        var result = base_value;
        while (true) {
            const byte = try input.takeByte();
            const sum = @addWithOverflow(result, @as(usize, byte));
            if (sum[1] != 0) return error.ReadFailed;
            result = sum[0];
            if (byte != 255) break;
            // Prevent malformed blocks from causing excessive values
            if (result > window_len) return error.ReadFailed;
        }
        return result;
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
                    literal_len = try readContinuationBytes(d.input, literal_len_short);
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
                    // Done with literals. Check if there's more data for the match.
                    // The last sequence in a block is literal-only (no match),
                    // which we detect by reaching end of input.
                    const next_byte = d.input.takeByte() catch |err| {
                        if (err == error.EndOfStream) {
                            // End of input after literals - this was the last sequence
                            d.state = .eof;
                        }
                        return err;
                    };

                    // We have more data - this byte is the low byte of the offset.
                    // Read the high byte of offset.
                    const offset_high = try d.input.takeByte();
                    const offset: usize = @as(usize, next_byte) + @as(usize, offset_high) * 256;

                    if (offset == 0) {
                        return error.ReadFailed;
                    }

                    // Match length is match_len_short + 4 (minimum match is 4)
                    var match_len: usize = lit.match_len_short + 4;

                    // Read match length continuation bytes if match_len_short == 15
                    if (lit.match_len_short == 15) {
                        match_len = try readContinuationBytes(d.input, match_len);
                    }

                    d.state = .{ .match = .{
                        .offset = offset,
                        .len = match_len,
                    } };
                    continue :read d.state;
                }

                const out = limit.min(@enumFromInt(lit.remaining)).slice(try w.writableSliceGreedyPreserve(Decompress.window_len, 1));
                try d.input.readSliceAll(out);

                lit.remaining -= out.len;
                w.advance(out.len);
                return out.len;
            },
            .match => |*match_data| {
                if (match_data.len == 0) {
                    d.state = .token;
                    continue :read d.state;
                }

                if (match_data.offset > w.end) {
                    return error.ReadFailed;
                }

                const n = @min(match_data.len, remaining);
                const src_start = w.end - match_data.offset;
                const dst = try w.writableSliceGreedyPreserve(Decompress.window_len, n);

                if (match_data.offset >= n) {
                    // Non-overlapping match: source and destination do not overlap, so we can
                    // copy the bytes directly without modulo.
                    const src = w.buffer[src_start .. src_start + n];
                    @memcpy(dst[0..n], src);
                } else {
                    // Overlapping match: copy byte-by-byte using modulo to repeat the source
                    // sequence as needed.
                    for (0..n) |i| {
                        const src_idx = src_start + (i % match_data.offset);
                        dst[i] = w.buffer[src_idx];
                    }
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

test "literal-only block" {
    // A block with only literals (no matches) - last sequence doesn't need offset
    // 0x20 = lit_len=2, match_len_short=0, followed by 2 literal bytes "AB"
    try expectDecoded("\x20AB", "AB");
}

test "match copying backwards in buffer" {
    try expectDecoded("\x24AB\x02\x00", "ABABABABAB");
}

test "zero offset error" {
    try expectDecodedError("\x11A\x00\x00");
}

test "match offset exceeding available data error" {
    try expectDecodedError("\x14X\xFF\x00");
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

fn expectDecodedError(input: []const u8) !void {
    var fixed: Reader = .fixed(input);

    var decompress_buf: [Decompress.window_len]u8 = undefined;
    var decompress = Decompress.init(&fixed, &decompress_buf);

    var decoded: Writer.Allocating = .init(testing.allocator);
    defer decoded.deinit();

    const result = decompress.reader.streamRemaining(&decoded.writer);
    try testing.expect(result == error.ReadFailed or result == error.EndOfStream);
}
