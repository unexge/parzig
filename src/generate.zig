const std = @import("std");
const parzig = @import("parzig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    const allocator = gpa.allocator();

    const input = "./parquet.thrift";
    const output = "./src/generated/parquet.zig";
    std.debug.print("Generating {s} from {s}\n", .{ output, input });

    var input_file = try std.fs.cwd().openFile(input, .{});
    defer input_file.close();

    const source = try input_file.readToEndAlloc(allocator, std.math.maxInt(u32));
    defer allocator.free(source);

    var document = try parzig.thrift.Document.init(allocator, source);
    defer document.deinit();

    var root = try parzig.thrift.translate(allocator, &document);
    defer root.deinit(allocator);
    defer allocator.free(root.source);

    var output_file = try std.fs.cwd().createFile(output, .{});
    defer output_file.close();

    const formatted = try root.render(allocator);
    defer allocator.free(formatted);

    try output_file.writeAll(
        \\// Generated by `zig build generate`.
        \\// DO NOT EDIT.
        \\
        \\
    );
    try output_file.writeAll(formatted);

    std.debug.print("Done\n", .{});

    return std.process.cleanExit();
}
