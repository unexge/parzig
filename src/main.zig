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

    var scanner = parzig.thrift.Scanner.init(source);
    while (true) {
        const token = scanner.next();
        switch (token.kind) {
            .@"enum" => {
                const identifier = scanner.next();
                std.debug.assert(identifier.kind == .identifier);
                const name = source[identifier.pos.start..identifier.pos.end];
                std.debug.print("Enum {s}\n", .{name});
            },
            .@"union" => {
                const identifier = scanner.next();
                std.debug.assert(identifier.kind == .identifier);
                const name = source[identifier.pos.start..identifier.pos.end];
                std.debug.print("Union {s}\n", .{name});
            },
            .@"struct" => {
                const identifier = scanner.next();
                std.debug.assert(identifier.kind == .identifier);
                const name = source[identifier.pos.start..identifier.pos.end];
                std.debug.print("Struct {s}\n", .{name});
            },
            .end_of_document => {
                std.debug.print("End of document\n", .{});
                break;
            },
            else => {},
        }
    }
}
