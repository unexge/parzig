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

    for (document.definitions.items) |def| {
        switch (def) {
            .@"enum" => |*e| {
                std.debug.print("Enum {s}\n", .{e.name});
                var iter = e.values.iterator();
                while (iter.next()) |kv| {
                    std.debug.print("\t{s} = {d}\n", .{kv.key_ptr.*, kv.value_ptr.*});
                }
            },
            .@"struct" => |*s| {
                std.debug.print("Struct {s}\n", .{s.name});
                var iter = s.fields.iterator();
                while (iter.next()) |kv| {
                    std.debug.print("\t{s} (id: {any})\n", .{kv.key_ptr.*, kv.value_ptr.*.id});
                }
            },
            .@"union" => |*u| {
                std.debug.print("Union {s}\n", .{u.name});
                var iter = u.fields.iterator();
                while (iter.next()) |kv| {
                    std.debug.print("\t{s} (id: {any})\n", .{kv.key_ptr.*, kv.value_ptr.*.id});
                }
            },
        }
    }
}
