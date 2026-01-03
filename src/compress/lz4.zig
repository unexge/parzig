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
    total_written: usize,

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
            .total_written = 0,
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
                d.total_written += out.len;
                return out.len;
            },
            .match => |*match_data| {
                if (match_data.len == 0) {
                    d.state = .token;
                    continue :read d.state;
                }

                // Match offset must not exceed the window size or the amount of data written,
                // whichever is smaller
                const max_offset = @min(d.total_written, window_len);
                if (match_data.offset > max_offset) {
                    return error.ReadFailed;
                }

                const n = @min(match_data.len, remaining);
                // Use modulo to handle circular buffer wraparound for large files
                // When w.end < match_data.offset (buffer wrapped), we need to add window_len first
                const src_start = if (w.end >= match_data.offset)
                    (w.end - match_data.offset) % window_len
                else
                    (w.end + window_len - match_data.offset) % window_len;
                const dst = try w.writableSliceGreedyPreserve(Decompress.window_len, n);

                if (match_data.offset >= n) {
                    // Non-overlapping match: source and destination do not overlap, so we can
                    // copy the bytes directly. Handle buffer wraparound if needed.
                    if (src_start + n <= window_len) {
                        // No wraparound
                        const src = w.buffer[src_start .. src_start + n];
                        @memcpy(dst[0..n], src);
                    } else {
                        // Wraparound case: copy in two parts
                        const first_part_len = window_len - src_start;
                        @memcpy(dst[0..first_part_len], w.buffer[src_start..window_len]);
                        @memcpy(dst[first_part_len..n], w.buffer[0 .. n - first_part_len]);
                    }
                } else {
                    // Overlapping match: copy byte-by-byte using modulo to repeat the source
                    // sequence as needed.
                    for (0..n) |i| {
                        const src_idx = (src_start + (i % match_data.offset)) % window_len;
                        dst[i] = w.buffer[src_idx];
                    }
                }

                match_data.len -= n;
                w.advance(n);
                d.total_written += n;

                return n;
            },
        }
    }
};

/// LZ4 decompressor for Hadoop/Parquet deprecated LZ4 codec.
///
/// The Hadoop LZ4 format has a special framing structure:
/// - 4 bytes: Total uncompressed size (big-endian)
/// - For each block:
///   - 4 bytes: Compressed block size (big-endian)
///   - N bytes: LZ4-compressed block data
///
/// This decompressor provides a streaming API that decompresses blocks on-demand.
pub const DecompressHadoop = struct {
    /// Maximum allowed size for a compressed block (128 MB)
    /// This prevents malicious files from causing excessive memory allocation
    const max_compressed_block_size: u32 = 128 * 1024 * 1024;

    /// Maximum allowed total uncompressed size (256 MB)
    /// This prevents malicious files from causing excessive memory allocation
    const max_total_uncompressed_size: u32 = 256 * 1024 * 1024;

    input: *Reader,
    reader: Reader,

    total_uncompressed: u32,
    decompressed_so_far: usize,

    // Current block state
    current_block: ?struct {
        decompress: Decompress,
        compressed_data: []u8,
        decompress_buffer: []u8,
    },

    allocator: std.mem.Allocator,

    pub fn init(input: *Reader, buffer: []u8, allocator: std.mem.Allocator) !DecompressHadoop {
        std.debug.assert(buffer.len >= Decompress.window_len);

        // Read total uncompressed size (big-endian)
        const total_uncompressed = try input.takeInt(u32, .big);

        // Validate total uncompressed size to prevent excessive memory allocation
        if (total_uncompressed > max_total_uncompressed_size) {
            return error.ReadFailed;
        }

        return .{
            .input = input,
            .reader = .{
                .vtable = &.{
                    .stream = DecompressHadoop.stream,
                },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
            .total_uncompressed = total_uncompressed,
            .decompressed_so_far = 0,
            .current_block = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *DecompressHadoop) void {
        if (self.current_block) |*block| {
            self.allocator.free(block.compressed_data);
            self.allocator.free(block.decompress_buffer);
            self.current_block = null;
        }
    }

    fn stream(r: *Reader, w: *Writer, limit: Limit) Reader.StreamError!usize {
        const d: *DecompressHadoop = @fieldParentPtr("reader", r);

        // Use a loop instead of recursion to avoid potential stack overflow
        // with files containing many small blocks
        while (true) {
            // Check if we've decompressed everything
            if (d.decompressed_so_far >= d.total_uncompressed) {
                // Free current block buffers before returning to prevent memory leak
                if (d.current_block) |*block| {
                    d.allocator.free(block.compressed_data);
                    d.allocator.free(block.decompress_buffer);
                    d.current_block = null;
                }
                return error.EndOfStream;
            }

            // If no current block, read the next one
            if (d.current_block == null) {
                // Read compressed block size (big-endian)
                const compressed_size = try d.input.takeInt(u32, .big);

                // Validate compressed size to prevent excessive memory allocation
                if (compressed_size > max_compressed_block_size) {
                    return error.ReadFailed;
                }

                // Allocate buffers for this block
                // Note: We manually free these buffers despite using an arena allocator
                // to reduce peak memory usage when processing large data pages
                const compressed_data = d.allocator.alloc(u8, compressed_size) catch return error.ReadFailed;
                errdefer d.allocator.free(compressed_data);

                const decompress_buffer = d.allocator.alloc(u8, Decompress.window_len) catch return error.ReadFailed;
                errdefer d.allocator.free(decompress_buffer);

                // Read compressed block data
                try d.input.readSliceAll(compressed_data);

                // Create a fixed reader over the compressed data
                var block_reader: Reader = .fixed(compressed_data);

                // Initialize decompressor for this block
                const decompress = Decompress.init(&block_reader, decompress_buffer);

                d.current_block = .{
                    .decompress = decompress,
                    .compressed_data = compressed_data,
                    .decompress_buffer = decompress_buffer,
                };
            }

            // Stream from current block
            const n = d.current_block.?.decompress.reader.stream(w, limit) catch |err| {
                // Always free current block buffers on error to prevent memory leaks
                d.allocator.free(d.current_block.?.compressed_data);
                d.allocator.free(d.current_block.?.decompress_buffer);
                d.current_block = null;

                // If block finished and more data expected, continue to next block
                if (err == error.EndOfStream and d.decompressed_so_far < d.total_uncompressed) {
                    continue;
                }
                return err;
            };

            d.decompressed_so_far += n;
            return n;
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
