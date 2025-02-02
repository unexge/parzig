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

    for (parquet_file.metadata.row_groups, 0..) |rg_metadata, rg_idx| {
        var rg = parquet_file.rowGroup(rg_idx);

        for (rg_metadata.columns, 0..) |column, i| {
            const key = column.meta_data.?.path_in_schema;
            const ty = column.meta_data.?.type;
            std.debug.print("{s} - {any}, values:\n", .{ key, ty });
            switch (try rg.readColumnDynamic(i)) {
                .boolean => |data| printValues(bool, data),
                .int32 => |data| printValues(i32, data),
                .int64 => |data| printValues(i64, data),
                .int96 => |data| printValues(i96, data),
                .float => |data| printValues(f32, data),
                .double => |data| printValues(f64, data),
                .byte_array => |data| printValues([]const u8, data),
                .fixed_len_byte_array => |data| printValues([]const u8, data),
            }
        }
    }

    return std.process.cleanExit();
}

fn printValues(comptime T: type, data: []?T) void {
    // TODO: `{?s}` to format slice of optional strings doesn't work. Upstream issue?
    const fmt_specifier = if (T == []const u8) "{any}" else "{any}";
    if (data.len > 10) {
        std.debug.print(fmt_specifier ++ "\n", .{data[0..10]});
        std.debug.print("..\n", .{});
        std.debug.print("..\n", .{});
        std.debug.print("{d} more\n", .{data.len - 10});
    } else {
        std.debug.print(fmt_specifier ++ "\n\n", .{data});
    }
}
