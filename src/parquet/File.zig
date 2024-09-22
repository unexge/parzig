const std = @import("std");

const File = @This();

const MAGIC = "PAR1";
const METADATA_LENGHT_SIZE = 4;
const MIN_SIZE = MAGIC.len * 2 + METADATA_LENGHT_SIZE;

pub fn read(source_const: anytype) !File {
    var source = @constCast(&source_const);
    var reader = source.reader();

    const size = try source.getEndPos();
    if (size < MIN_SIZE) {
        return error.IncorrectFile;
    }

    if (!std.mem.eql(u8, MAGIC, &try reader.readBytesNoEof(MAGIC.len))) {
        return error.MissingMagicHeader;
    }

    try source.seekTo(size - (MAGIC.len + METADATA_LENGHT_SIZE));
    const footer = try reader.readBytesNoEof(MAGIC.len + METADATA_LENGHT_SIZE);
    if (!std.mem.eql(u8, MAGIC, footer[METADATA_LENGHT_SIZE..])) {
        return error.MissingMagicFooter;
    }

    const metadataLenght = std.mem.readInt(u32, footer[0..METADATA_LENGHT_SIZE], .little);

    _ = metadataLenght;

    return File{};
}

test "missing PAR1 header" {
    const buf = "noheader" ** 2;
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.MissingMagicHeader, read(source));
}

test "missing PAR1 footer" {
    const buf = "PAR1nofooter" ** 2;
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.MissingMagicFooter, read(source));
}

test "missing metadata length" {
    const buf = "PAR1aPAR1";
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.IncorrectFile, read(source));
}

test "reading a file" {
    const simple_file = try std.fs.cwd().openFile("testdata/simple.parquet", .{ .mode = .read_only });
    defer simple_file.close();
    const source = std.io.StreamSource{ .file = simple_file };
    _ = try read(source);
}
