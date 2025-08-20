const std = @import("std");
const Reader = std.Io.Reader;
const mem = std.mem;
const fs = std.fs;

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const decoding = @import("./decoding.zig");
const dynamic = @import("./dynamic.zig");
const rowGroupReader = @import("./rowGroupReader.zig");

const File = @This();

const MAGIC = "PAR1";
const METADATA_LENGTH_SIZE = 4;
const FOOTER_SIZE = MAGIC.len + METADATA_LENGTH_SIZE;
const MIN_SIZE = MAGIC.len + FOOTER_SIZE;

arena: std.heap.ArenaAllocator,
file_reader: *fs.File.Reader,
metadata: parquet_schema.FileMetaData,

pub const RowGroup = struct {
    file: *File,
    rg: parquet_schema.RowGroup,

    pub fn readColumn(self: *RowGroup, comptime T: type, index: usize) ![]T {
        return rowGroupReader.readColumn(T, self.file, &self.rg.columns[index]);
    }

    pub fn readColumnDynamic(self: *RowGroup, index: usize) !dynamic.Values {
        return dynamic.readColumn(self.file, &self.rg.columns[index]);
    }
};

pub fn read(allocator: mem.Allocator, file_reader: *fs.File.Reader) !File {
    const size = try file_reader.getSize();
    if (size < MIN_SIZE) {
        return error.IncorrectFile;
    }

    var magic_header_buf: [MAGIC.len]u8 = undefined;
    try file_reader.interface.readSliceAll(&magic_header_buf);
    if (!mem.eql(u8, MAGIC, &magic_header_buf)) {
        return error.MissingMagicHeader;
    }

    try file_reader.seekTo(size - FOOTER_SIZE);
    var footer_buf: [FOOTER_SIZE]u8 = undefined;
    try file_reader.interface.readSliceAll(&footer_buf);
    if (!mem.eql(u8, MAGIC, footer_buf[METADATA_LENGTH_SIZE..])) {
        return error.MissingMagicFooter;
    }

    const metadata_length = mem.readInt(u32, footer_buf[0..METADATA_LENGTH_SIZE], .little);
    try file_reader.seekTo(size - metadata_length - FOOTER_SIZE);

    var arena = std.heap.ArenaAllocator.init(allocator);
    const metadata_buf = try file_reader.interface.readAlloc(arena.allocator(), metadata_length);

    const metadata_reader = protocol_compact.StructReader(parquet_schema.FileMetaData);
    var metadata_buf_reader: Reader = .fixed(metadata_buf);
    const metadata = try metadata_reader.read(arena.allocator(), &metadata_buf_reader);

    return File{ .arena = arena, .file_reader = file_reader, .metadata = metadata };
}

pub fn rowGroup(self: *File, index: usize) RowGroup {
    return RowGroup{ .file = self, .rg = self.metadata.row_groups[index] };
}

pub fn deinit(self: *File) void {
    self.file_reader.file.close();
    self.arena.deinit();
}

pub fn repAndDefLevelOfColumn(self: *File, path: [][]const u8) !std.meta.Tuple(&[_]type{ u8, u8 }) {
    if (path.len == 0) {
        return error.MissingField;
    }
    if (path.len > 1) {
        return error.NestedFieldsAreNotSupported;
    }

    const field = for (self.metadata.schema) |elem| {
        if (mem.eql(u8, elem.name, path[0])) {
            break elem;
        }
    } else return error.UnkonwnField;

    const repetition_type = field.repetition_type orelse parquet_schema.FieldRepetitionType.OPTIONAL;

    return .{ if (repetition_type == .REPEATED) 1 else 0, 1 };
}

pub fn readLevelDataV1(self: *File, reader: *Reader, bit_width: u8, num_values: usize) ![]u16 {
    return decoding.decodeLenghtPrependedRleBitPackedHybrid(u16, self.arena.allocator(), num_values, bit_width, reader);
}

pub fn readLevelDataV2(self: *File, reader: *Reader, bit_width: u8, num_values: usize, length: u32) ![]u16 {
    var reader_buf: [1024]u8 = undefined;
    var limited_reader = reader.limited(.limited(length), &reader_buf);

    const values = try self.arena.allocator().alloc(u16, num_values);
    try decoding.decodeRleBitPackedHybrid(u16, values, bit_width, &limited_reader.interface);
    return values;
}

test {
    _ = decoding;
}

// TODO: Fix these tests using real files.
// test "missing PAR1 header" {
//     const buf = "noheader" ** 2;
//     const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
//     try std.testing.expectError(error.MissingMagicHeader, read(std.testing.allocator, source));
// }

// test "missing PAR1 footer" {
//     const buf = "PAR1nofooter" ** 2;
//     const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
//     try std.testing.expectError(error.MissingMagicFooter, read(std.testing.allocator, source));
// }

// test "missing metadata length" {
//     const buf = "PAR1aPAR1";
//     const source = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(buf) };
//     try std.testing.expectError(error.IncorrectFile, read(std.testing.allocator, source));
// }

test "reading metadata of a simple file" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.fs.cwd().openFile("testdata/simple.parquet", .{ .mode = .read_only })).reader(&reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
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

test "reading a row group of a simple file" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.fs.cwd().openFile("testdata/simple.parquet", .{ .mode = .read_only })).reader(&reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, try rg.readColumn(i64, 0));
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 7, 8, 9, 10 }, try rg.readColumn(i64, 1));
    try std.testing.expectEqualDeep(&[_][]const u8{ "a", "b", "c", "d", "e" }, try rg.readColumn([]const u8, 2));
}

test "reading a row group of a simple file with dynamic types" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.fs.cwd().openFile("testdata/simple.parquet", .{ .mode = .read_only })).reader(&reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 1, 2, 3, 4, 5 }, (try rg.readColumnDynamic(0)).int64);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 6, 7, 8, 9, 10 }, (try rg.readColumnDynamic(1)).int64);
    try std.testing.expectEqualDeep(&[_]?[]const u8{ "a", "b", "c", "d", "e" }, (try rg.readColumnDynamic(2)).byte_array);
}

// test "reading gzipped file" {
//     var file = try readTestFile("testdata/gzipped.parquet");
//     defer file.deinit();

//     var rg = file.rowGroup(0);
//     const questions = try rg.readColumn([]const u8, 0);
//     const anwsers = try rg.readColumn([]const u8, 1);

//     try std.testing.expectEqualStrings("Natalia sold clips to 48 of her friends in April, and then she sold half as many clips in May. How many clips did Natalia sell altogether in April and May?", questions[0]);
//     try std.testing.expectEqualStrings("Natalia sold 48/2 = <<48/2=24>>24 clips in May.\nNatalia sold 48+24 = <<48+24=72>>72 clips altogether in April and May.\n#### 72", anwsers[0]);
// }

test "reading simple file with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.fs.cwd().openFile("testdata/simple_with_nulls.parquet", .{ .mode = .read_only })).reader(&reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(3, file.metadata.schema.len);

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 1, 2, null, 4 }, try rg.readColumn(?i64, 0));
    try std.testing.expectEqualDeep(&[_]?[]const u8{ null, "foo", "bar", null }, try rg.readColumn(?[]const u8, 1));
}

test "reading simple file with nulls with dynamic types" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.fs.cwd().openFile("testdata/simple_with_nulls.parquet", .{ .mode = .read_only })).reader(&reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(3, file.metadata.schema.len);

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 1, 2, null, 4 }, (try rg.readColumnDynamic(0)).int64);
    try std.testing.expectEqualDeep(&[_]?[]const u8{ null, "foo", "bar", null }, (try rg.readColumnDynamic(1)).byte_array);
}
