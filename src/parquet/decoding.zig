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

pub fn decodeRleBitPackedHybrid(gpa: std.mem.Allocator, bit_width: u8, reader: anytype) !void {
    // TODO: Proper implementation.

    // TODO: Lenght is not always prepended.
    const lenght = try reader.readVarInt(u32, .little, 4);
    if (lenght == 0) {
        return error.EmptyBuffer;
    }

    const buf = try gpa.alloc(u8, lenght);
    _ = try reader.readNoEof(buf);
    var buf_reader = std.io.fixedBufferStream(buf);

    const header = try std.leb.readULEB128(u64, buf_reader.reader());
    if (header & 1 == 1) {
        // TODO: bit packet run.
        const len = (header >> 1) * 8;
        var bit_reader = std.io.bitReader(.little, buf_reader.reader());
        // TODO: Need to read `len / u16` times.
        _ = try bit_reader.readBitsNoEof(u16, bit_width);
        _ = len;
    } else {
        // TODO: rle run.
        return error.RleRunNotSupported;
    }
}
