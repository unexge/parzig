const std = @import("std");
const Io = std.Io;
const FileReader = std.Io.File.Reader;
const Reader = std.Io.Reader;
const mem = std.mem;

const parquet_schema = @import("../generated/parquet.zig");
const protocol_compact = @import("../thrift.zig").protocol_compact;
const dynamic = @import("./dynamic.zig");
const physical = @import("./physical.zig");
const rowGroupReader = @import("./rowGroupReader.zig");
const nestedReader = @import("./nestedReader.zig");

const File = @This();

const MAGIC = "PAR1";
const METADATA_LENGTH_SIZE = 4;
const FOOTER_SIZE = MAGIC.len + METADATA_LENGTH_SIZE;
const MIN_SIZE = MAGIC.len + FOOTER_SIZE;

io: Io,
arena: std.heap.ArenaAllocator,
file_reader: *FileReader,
metadata: parquet_schema.FileMetaData,

pub const RowGroup = struct {
    file: *File,
    rg: parquet_schema.RowGroup,

    pub fn readColumn(self: *RowGroup, comptime T: type, index: usize) ![]T {
        return rowGroupReader.readColumn(T, self.file, &self.rg.columns[index]);
    }

    pub fn readListColumn(self: *RowGroup, comptime T: type, index: usize) ![][]const T {
        return nestedReader.readList(T, self.file, &self.rg.columns[index]);
    }

    pub fn readMapColumn(self: *RowGroup, comptime K: type, comptime V: type, key_index: usize, value_index: usize) ![][]const nestedReader.MapEntry(K, V) {
        return nestedReader.readMap(K, V, self.file, &self.rg.columns[key_index], &self.rg.columns[value_index]);
    }

    pub fn readStructColumn(self: *RowGroup, comptime T: type, base_index: usize) ![]T {
        return nestedReader.readStruct(T, self.file, self.rg.columns, base_index, @intCast(self.rg.num_rows));
    }

    pub fn readColumnDynamic(self: *RowGroup, index: usize) !dynamic.Values {
        return dynamic.readColumn(self.file, &self.rg.columns[index]);
    }
};

pub fn read(allocator: mem.Allocator, file_reader: *FileReader) !File {
    const io = file_reader.io;

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
    errdefer arena.deinit();
    const metadata_buf = try file_reader.interface.readAlloc(arena.allocator(), metadata_length);

    const metadata_reader = protocol_compact.StructReader(parquet_schema.FileMetaData);
    var metadata_buf_reader: Reader = .fixed(metadata_buf);
    const metadata = try metadata_reader.read(arena.allocator(), &metadata_buf_reader);

    return File{ .arena = arena, .io = io, .file_reader = file_reader, .metadata = metadata };
}

pub fn rowGroup(self: *File, index: usize) RowGroup {
    return RowGroup{ .file = self, .rg = self.metadata.row_groups[index] };
}

pub fn deinit(self: *File) void {
    self.file_reader.file.close(self.io);
    self.arena.deinit();
}

/// Find a schema element by path and return its index, definition level, repetition level, and the element itself.
/// Returns null if the path is not found or invalid.
pub fn findSchemaElement(self: *File, path: [][]const u8) ?struct { index: usize, max_definition_level: u8, max_repetition_level: u8, elem: parquet_schema.SchemaElement } {
    if (path.len == 0 or self.metadata.schema.len < 2) {
        return null;
    }

    var current_idx: usize = 1; // Skip the root element
    var max_definition_level: u8 = 0;
    var max_repetition_level: u8 = 0;
    var elem: parquet_schema.SchemaElement = undefined;

    for (path) |part| {
        // Search `part` starting from the `current_idx`
        const found_idx = blk: {
            while (current_idx < self.metadata.schema.len) {
                elem = self.metadata.schema[current_idx];
                if (mem.eql(u8, elem.name, part)) {
                    break :blk current_idx;
                }

                // Skip this element and all its children
                var skip_idx = current_idx;
                var nodes_to_skip: i32 = 1; // Need to skip the current one at least

                while (nodes_to_skip > 0 and skip_idx < self.metadata.schema.len) {
                    if (self.metadata.schema[skip_idx].num_children) |num_children| {
                        nodes_to_skip += @as(i32, @intCast(num_children));
                    }

                    nodes_to_skip -= 1;
                    skip_idx += 1;
                }

                current_idx = skip_idx;
            }

            // `part` not found
            return null;
        };

        elem = self.metadata.schema[found_idx];

        if (elem.repetition_type) |repetition_type| {
            switch (repetition_type) {
                .REQUIRED => {},
                .OPTIONAL => max_definition_level += 1,
                .REPEATED => {
                    max_repetition_level += 1;
                    max_definition_level += 1;
                },
            }
        }

        // Proceed to first child of the found element
        current_idx = found_idx + 1;
    }

    return .{ .index = current_idx - 1, .max_definition_level = max_definition_level, .max_repetition_level = max_repetition_level, .elem = elem };
}

pub fn readLevelDataV1(_: *File, reader: *Reader, encoding: parquet_schema.Encoding, bit_width: u8, dest: []u16) !void {
    switch (encoding) {
        .BIT_PACKED => {
            try physical.bitPacked(u16, reader, bit_width, dest);
        },
        .RLE => {
            try physical.runLengthBitPackedHybridLengthPrepended(u16, reader, bit_width, dest);
        },
        else => {
            std.debug.print("Unsupported repetition/definition level encoding: {any}\n", .{encoding});
            return error.UnsupportedDefinitionLevelEncoding;
        },
    }
}

pub fn readLevelDataV2(_: *File, reader: *Reader, bit_width: u8, dest: []u16, length: u32) !void {
    var reader_buf: [1024]u8 = undefined;
    var limited_reader = reader.limited(.limited(length), &reader_buf);

    try physical.runLengthBitPackedHybrid(u16, &limited_reader.interface, bit_width, dest);
}

test {
    _ = physical;
}

test "missing PAR1 header" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(std.testing.io, "test", .{ .exclusive = true, .read = true });
    defer file.close(std.testing.io);
    var writer_buf: [1024]u8 = undefined;
    var file_writer = file.writer(std.testing.io, &writer_buf);
    try file_writer.interface.writeAll("noheader" ** 2);
    try file_writer.interface.flush();

    var reader_buf: [1024]u8 = undefined;
    var file_reader = file.reader(std.testing.io, &reader_buf);

    try std.testing.expectError(error.MissingMagicHeader, read(std.testing.allocator, &file_reader));
}

test "missing PAR1 footer" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(std.testing.io, "test", .{ .exclusive = true, .read = true });
    defer file.close(std.testing.io);
    var writer_buf: [1024]u8 = undefined;
    var file_writer = file.writer(std.testing.io, &writer_buf);
    try file_writer.interface.writeAll("PAR1nofooter" ** 2);
    try file_writer.interface.flush();

    var reader_buf: [1024]u8 = undefined;
    var file_reader = file.reader(std.testing.io, &reader_buf);

    try std.testing.expectError(error.MissingMagicFooter, read(std.testing.allocator, &file_reader));
}

test "missing metadata length" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(std.testing.io, "test", .{ .exclusive = true, .read = true });
    defer file.close(std.testing.io);
    var writer_buf: [1024]u8 = undefined;
    var file_writer = file.writer(std.testing.io, &writer_buf);
    try file_writer.interface.writeAll("PAR1aPAR1");
    try file_writer.interface.flush();

    var reader_buf: [1024]u8 = undefined;
    var file_reader = file.reader(std.testing.io, &reader_buf);

    try std.testing.expectError(error.IncorrectFile, read(std.testing.allocator, &file_reader));
}

test "reading metadata of a simple file" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/simple.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
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
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/simple.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, try rg.readColumn(i64, 0));
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 7, 8, 9, 10 }, try rg.readColumn(i64, 1));
    try std.testing.expectEqualDeep(&[_][]const u8{ "a", "b", "c", "d", "e" }, try rg.readColumn([]const u8, 2));
}

test "reading a row group of a simple file with dynamic types" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/simple.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 1, 2, 3, 4, 5 }, (try rg.readColumnDynamic(0)).int64);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 6, 7, 8, 9, 10 }, (try rg.readColumnDynamic(1)).int64);
    try std.testing.expectEqualDeep(&[_]?[]const u8{ "a", "b", "c", "d", "e" }, (try rg.readColumnDynamic(2)).byte_array);
}

test "reading gzipped file" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/gzipped.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);
    const questions = try rg.readColumn([]const u8, 0);
    const anwsers = try rg.readColumn([]const u8, 1);

    try std.testing.expectEqualStrings("Natalia sold clips to 48 of her friends in April, and then she sold half as many clips in May. How many clips did Natalia sell altogether in April and May?", questions[0]);
    try std.testing.expectEqualStrings("Natalia sold 48/2 = <<48/2=24>>24 clips in May.\nNatalia sold 48+24 = <<48+24=72>>72 clips altogether in April and May.\n#### 72", anwsers[0]);
}

test "reading simple file with nulls" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/simple_with_nulls.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
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
    var file_reader = (try std.Io.Dir.cwd().openFile(std.testing.io, "testdata/simple_with_nulls.parquet", .{ .mode = .read_only })).reader(std.testing.io, &reader_buf);
    var file = try File.read(std.testing.allocator, &file_reader);
    defer file.deinit();

    try std.testing.expectEqual(1, file.metadata.row_groups.len);
    try std.testing.expectEqual(3, file.metadata.schema.len);

    var rg = file.rowGroup(0);
    try std.testing.expectEqualSlices(?i64, &[_]?i64{ 1, 2, null, 4 }, (try rg.readColumnDynamic(0)).int64);
    try std.testing.expectEqualDeep(&[_]?[]const u8{ null, "foo", "bar", null }, (try rg.readColumnDynamic(1)).byte_array);
}
