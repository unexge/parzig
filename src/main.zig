const std = @import("std");
const parzig = @import("parzig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    const allocator = gpa.allocator();

    var args = std.process.args();
    _ = args.next() orelse unreachable; // program name
    const path = args.next() orelse return error.MissingArgument;

    std.debug.print("Parsing {s}\n", .{path});

    const file = try std.fs.cwd().openFile(path, .{});
    const source = std.io.StreamSource{ .file = file };
    var parquet_file = try parzig.parquet.File.read(allocator, source);
    defer parquet_file.deinit();

    std.debug.print("File Metadata:\n", .{});
    std.debug.print("\tFormat version: {d}\n", .{parquet_file.metadata.version});
    if (parquet_file.metadata.created_by) |created_by| {
        std.debug.print("\tCreated by: {s}\n", .{created_by});
    }
    std.debug.print("\tNumber of rows: {d}\n", .{parquet_file.metadata.num_rows});
    // There is always `root` in the schema, don't count it.
    const num_columns = parquet_file.metadata.schema.len - 1;
    std.debug.print("\tNumber of columns: {d}\n", .{num_columns});
    std.debug.print("\tNumber of row groups: {d}\n", .{parquet_file.metadata.row_groups.len});

    const total_byte_size = blk: {
        var total: i64 = 0;
        for (parquet_file.metadata.row_groups) |rg| {
            total += rg.total_byte_size;
        }
        break :blk total;
    };
    std.debug.print("\tTotal byte size of data: {d}\n", .{total_byte_size});

    std.debug.print("-------\n", .{});

    _ = parquet_file.rowGroup(0) catch null;

    return std.process.cleanExit();
}
