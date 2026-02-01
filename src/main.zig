const std = @import("std");
const parzig = @import("parzig");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip(); // program name
    const path = args.next() orelse return error.MissingArgument;
    std.debug.print("Parsing {s}\n", .{path});

    var threaded: Io.Threaded = .init(allocator, .{ .environ = init.minimal.environ });
    defer threaded.deinit();
    const io = threaded.io();

    const file = try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
    var read_buffer: [10240]u8 = undefined;
    var file_reader = file.reader(io, &read_buffer);
    var parquet_file = try parzig.parquet.File.read(allocator, &file_reader);
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
            const ty = column.meta_data.?.type;
            const column_path = try std.mem.join(allocator, ".", column.meta_data.?.path_in_schema);
            defer allocator.free(column_path);
            std.debug.print("{s} - {any}, values:\n", .{ column_path, ty });
            switch (try rg.readColumnDynamic(i)) {
                .boolean => |data| printValues(bool, data),
                .int32 => |data| printValues(i32, data),
                .int64 => |data| printValues(i64, data),
                .int96 => |data| printValues(i96, data),
                .float => |data| printValues(f32, data),
                .double => |data| printValues(f64, data),
                .byte_array => |data| printValues([]u8, data),
                .fixed_len_byte_array_1 => |data| printValues([1]u8, data),
                .fixed_len_byte_array_2 => |data| printValues([2]u8, data),
                .fixed_len_byte_array_3 => |data| printValues([3]u8, data),
                .fixed_len_byte_array_4 => |data| printValues([4]u8, data),
                .fixed_len_byte_array_5 => |data| printValues([5]u8, data),
                .fixed_len_byte_array_6 => |data| printValues([6]u8, data),
                .fixed_len_byte_array_7 => |data| printValues([7]u8, data),
                .fixed_len_byte_array_8 => |data| printValues([8]u8, data),
                .fixed_len_byte_array_9 => |data| printValues([9]u8, data),
                .fixed_len_byte_array_10 => |data| printValues([10]u8, data),
                .fixed_len_byte_array_11 => |data| printValues([11]u8, data),
                .fixed_len_byte_array_12 => |data| printValues([12]u8, data),
                .fixed_len_byte_array_13 => |data| printValues([13]u8, data),
                .fixed_len_byte_array_14 => |data| printValues([14]u8, data),
                .fixed_len_byte_array_15 => |data| printValues([15]u8, data),
                .fixed_len_byte_array_16 => |data| printValues([16]u8, data),
            }
        }
    }

    return std.process.cleanExit(io);
}

fn printValues(comptime T: type, data: []?T) void {
    if (T == []const u8 or T == []u8) {
        if (data.len > 10) {
            for (data[0..10], 0..) |item, i| {
                if (i == 9) {
                    std.debug.print("{?s}\n", .{item});
                } else {
                    std.debug.print("{?s}, ", .{item});
                }
            }

            std.debug.print("..\n", .{});
            std.debug.print("{d} more\n\n", .{data.len - 10});
        } else {
            for (data, 0..) |item, i| {
                if (i == data.len - 1) {
                    std.debug.print("{?s}\n\n", .{item});
                } else {
                    std.debug.print("{?s}, ", .{item});
                }
            }
        }

        return;
    }

    if (data.len > 10) {
        std.debug.print("{any}\n", .{data[0..10]});
        std.debug.print("..\n", .{});
        std.debug.print("{d} more\n\n", .{data.len - 10});
    } else {
        std.debug.print("{any}\n\n", .{data});
    }
}
