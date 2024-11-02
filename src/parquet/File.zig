const std = @import("std");

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const decoding = @import("./decoding.zig");
const rowGroupReader = @import("./rowGroupReader.zig");

const File = @This();

const MAGIC = "PAR1";
const METADATA_LENGHT_SIZE = 4;
const FOOTER_SIZE = MAGIC.len + METADATA_LENGHT_SIZE;
const MIN_SIZE = MAGIC.len + FOOTER_SIZE;

// TODO: Its probably better to track allocated memory properly.
arena: std.heap.ArenaAllocator,
source: std.io.StreamSource,
metadata: parquet_schema.FileMetaData,

pub const RowGroup = struct {
    file: *File,
    rg: parquet_schema.RowGroup,

    pub fn readColumn(self: *RowGroup, comptime T: type, index: usize) ![]T {
        return rowGroupReader.readColumn(T, self.file, &self.rg.columns[index]);
    }
};

pub fn read(gpa: std.mem.Allocator, source_const: std.io.StreamSource) !File {
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
    return File{ .arena = arena, .source = source_const, .metadata = metadata };
}

pub fn rowGroup(self: *File, index: usize) RowGroup {
    return RowGroup{ .file = self, .rg = self.metadata.row_groups[index] };
}

pub fn deinit(self: *File) void {
    switch (self.source) {
        .file => |*f| f.close(),
        else => {},
    }
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
        if (std.mem.eql(u8, elem.name, path[0])) {
            break elem;
        }
    } else return error.UnkonwnField;

    const repetition_type = field.repetition_type orelse parquet_schema.FieldRepetitionType.OPTIONAL;

    return .{ if (repetition_type == .REPEATED) 1 else 0, 1 };
}

pub fn readLevelDataV1(self: *File, reader: anytype, bit_width: u8, num_values: usize) ![]u16 {
    const lenght = try reader.readVarInt(u32, .little, 4);
    if (lenght == 0) return error.EmptyBuffer;

    const buf = try self.arena.allocator().alloc(u8, @as(usize, @intCast(lenght)));
    defer self.arena.allocator().free(buf);
    try reader.readNoEof(buf);
    var fbs = std.io.fixedBufferStream(buf);

    const values = try self.arena.allocator().alloc(u16, num_values);
    try decoding.decodeRleBitPackedHybrid(u16, values, bit_width, fbs.reader());
    return values;
}

test {
    _ = decoding;
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

test "reading metadata of a simple file" {
    var file = try readTestFile("testdata/simple.parquet");
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
    var file = try readTestFile("testdata/simple.parquet");
    defer file.deinit();

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, try rg.readColumn(i64, 0));
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 7, 8, 9, 10 }, try rg.readColumn(i64, 1));
    try std.testing.expectEqualDeep(&[_][]const u8{ "a", "b", "c", "d", "e" }, try rg.readColumn([]const u8, 2));
}

test "reading gzipped file" {
    var file = try readTestFile("testdata/gzipped.parquet");
    defer file.deinit();

    var rg = file.rowGroup(0);
    const questions = try rg.readColumn([]const u8, 0);
    const anwsers = try rg.readColumn([]const u8, 1);

    try std.testing.expectEqualStrings("Natalia sold clips to 48 of her friends in April, and then she sold half as many clips in May. How many clips did Natalia sell altogether in April and May?", questions[0]);
    try std.testing.expectEqualStrings("Natalia sold 48/2 = <<48/2=24>>24 clips in May.\nNatalia sold 48+24 = <<48+24=72>>72 clips altogether in April and May.\n#### 72", anwsers[0]);
}

fn readTestFile(path: []const u8) !File {
    const simple_file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    const source = std.io.StreamSource{ .file = simple_file };
    return read(std.testing.allocator, source);
}
