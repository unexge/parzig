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

    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const source = try file.readToEndAlloc(allocator, std.math.maxInt(u32));
    defer allocator.free(source);

    var document = try parzig.thrift.Document.init(allocator, source);
    defer document.deinit();

    var root = try parzig.thrift.translate(allocator, &document);
    defer root.deinit(allocator);
    defer allocator.free(root.source);

    const formatted = try root.render(allocator);
    defer allocator.free(formatted);

    try std.io.getStdOut().writeAll(formatted);
    return std.process.cleanExit();
}
