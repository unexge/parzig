const std = @import("std");

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;

const File = @This();

const MAGIC = "PAR1";
const METADATA_LENGHT_SIZE = 4;
const FOOTER_SIZE = MAGIC.len + METADATA_LENGHT_SIZE;
const MIN_SIZE = MAGIC.len + FOOTER_SIZE;

// TODO: Its probably better to track allocated memory properly.
arena: std.heap.ArenaAllocator,
metadata: parquet_schema.FileMetaData,

pub fn read(gpa: std.mem.Allocator, source_const: anytype) !File {
    var source = @constCast(&source_const);
    var reader = source.reader();

    const size = try source.getEndPos();
    if (size < MIN_SIZE) {
        return error.IncorrectFile;
    }

    if (!std.mem.eql(u8, MAGIC, &try reader.readBytesNoEof(MAGIC.len))) {
        return error.MissingMagicHeader;
    }

    try source.seekTo(size - FOOTER_SIZE);
    const footer = try reader.readBytesNoEof(FOOTER_SIZE);
    if (!std.mem.eql(u8, MAGIC, footer[METADATA_LENGHT_SIZE..])) {
        return error.MissingMagicFooter;
    }

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    const metadata_lenght = std.mem.readInt(u32, footer[0..METADATA_LENGHT_SIZE], .little);
    try source.seekTo(size - metadata_lenght - FOOTER_SIZE);

    const metadata_buf = try alloc.alloc(u8, metadata_lenght);
    _ = try reader.readNoEof(metadata_buf);

    var metadata_fbs = std.io.fixedBufferStream(metadata_buf);
    const metadata_reader = protocol_compact.StructReader(parquet_schema.FileMetaData);

    const metadata = try metadata_reader.read(alloc, metadata_fbs.reader());
    return File{ .arena = arena, .metadata = metadata };
}

pub fn deinit(self: *File) void {
    self.arena.deinit();
}

test "missing PAR1 header" {
    const buf = "noheader" ** 2;
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.MissingMagicHeader, read(std.testing.allocator, source));
}

test "missing PAR1 footer" {
    const buf = "PAR1nofooter" ** 2;
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.MissingMagicFooter, read(std.testing.allocator, source));
}

test "missing metadata length" {
    const buf = "PAR1aPAR1";
    const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
    try std.testing.expectError(error.IncorrectFile, read(std.testing.allocator, source));
}

test "reading a simple file metadata" {
    const simple_file = try std.fs.cwd().openFile("testdata/simple.parquet", .{ .mode = .read_only });
    defer simple_file.close();
    const source = std.io.StreamSource{ .file = simple_file };
    var file = try read(std.testing.allocator, source);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.version);
    try std.testing.expectEqual(5, file.metadata.num_rows);
    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqualStrings("Polars", file.metadata.created_by.?);

    try std.testing.expectEqualStrings("root", file.metadata.schema[0].name);
    try std.testing.expectEqualStrings("foo", file.metadata.schema[1].name);
    try std.testing.expectEqualStrings("bar", file.metadata.schema[2].name);
    try std.testing.expectEqualStrings("ham", file.metadata.schema[3].name);

    try std.testing.expectEqual(.INT64, file.metadata.schema[1].type);
    try std.testing.expectEqual(.INT64, file.metadata.schema[2].type);
    try std.testing.expectEqual(.BYTE_ARRAY, file.metadata.schema[3].type);

    try std.testing.expectEqual(232, file.metadata.row_groups[0].total_byte_size);
    try std.testing.expectEqual(5, file.metadata.row_groups[0].num_rows);
}
