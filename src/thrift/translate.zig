//! Translates parsed Thrift AST into Zig AST, heavily inspired by `aro_translate_c`.

const std = @import("std");
const Ast = std.zig.Ast;
const Node = Ast.Node;
const Token = std.zig.Token;
const thrift = @import("../thrift.zig");

const Context = struct {
    allocator: std.mem.Allocator,
    buf: std.ArrayList(u8),
    tokens: Ast.TokenList = .{},
    nodes: Ast.NodeList = .{},
    extra_data: std.ArrayList(Node.Index),

    fn deinit(self: *Context) void {
        self.buf.deinit();
        self.tokens.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
        self.extra_data.deinit();
    }

    fn addToken(self: *Context, tag: Token.Tag, text: []const u8) !Ast.TokenIndex {
        const index = self.tokens.len;
        try self.tokens.append(self.allocator, .{
            .tag = tag,
            .start = @intCast(self.buf.items.len),
        });
        try self.buf.writer().writeAll(text);
        return @intCast(index);
    }

    fn addNode(self: *Context, node: Node) !Node.Index {
        const index = self.nodes.len;
        try self.nodes.append(self.allocator, node);
        return @intCast(index);
    }

    fn renderDefinition(self: *Context, def: thrift.Definition) !Node.Index {
        switch (def) {
            .@"enum" => |*e| {
                const const_tok = try self.addToken(.keyword_const, "const");
                _ = try self.addToken(.identifier, e.name);
                _ = try self.addToken(.equal, "=");
                const enum_node = inner: {
                    const enum_tok = try self.addToken(.keyword_enum, "enum");
                    _ = try self.addToken(.l_brace, "{");

                    const fields = try self.allocator.alloc(Node.Index, e.values.count());
                    defer self.allocator.free(fields);

                    var iter = e.values.iterator();
                    var i: usize = 0;
                    while (iter.next()) |kv| {
                        const field_tok = try self.addToken(.identifier, kv.key_ptr.*);
                        _ = try self.addToken(.equal, "=");
                        const num_literal = try std.fmt.allocPrint(self.allocator, "{}", .{kv.value_ptr.*});
                        defer self.allocator.free(num_literal);
                        const value_index = try self.addNode(.{
                            .tag = .number_literal,
                            .main_token = try self.addToken(
                                .number_literal,
                                num_literal,
                            ),
                            .data = undefined,
                        });
                        fields[i] = try self.addNode(.{
                            .tag = .container_field_init,
                            .main_token = field_tok,
                            .data = .{
                                .lhs = 0,
                                .rhs = value_index,
                            },
                        });
                        _ = try self.addToken(.comma, ",");
                        i += 1;
                    }

                    _ = try self.addToken(.r_brace, "}");

                    const lhs = self.extra_data.items.len;
                    for (fields) |f| {
                        try self.extra_data.append(f);
                    }
                    const rhs = self.extra_data.items.len;
                    const is_empty = lhs == rhs;
                    break :inner try self.addNode(.{
                        .tag = if (is_empty) .container_decl_two else .container_decl_trailing,
                        .main_token = enum_tok,
                        .data = .{
                            .lhs = if (is_empty) 0 else @intCast(lhs),
                            .rhs = if (is_empty) 0 else @intCast(rhs),
                        },
                    });
                };

                _ = try self.addToken(.semicolon, ";");

                return self.addNode(.{
                    .tag = .simple_var_decl,
                    .main_token = const_tok,
                    .data = .{
                        .lhs = 0,
                        .rhs = @intCast(enum_node),
                    },
                });
            },
            .@"struct" => |*s| {
                const const_tok = try self.addToken(.keyword_const, "const");
                _ = try self.addToken(.identifier, s.name);
                _ = try self.addToken(.equal, "=");
                const struct_node = inner: {
                    const struct_tok = try self.addToken(.keyword_struct, "struct");
                    _ = try self.addToken(.l_brace, "{");
                    _ = try self.addToken(.r_brace, "}");
                    _ = try self.addToken(.semicolon, ";");
                    break :inner try self.addNode(.{
                        .tag = .container_decl_two,
                        .main_token = struct_tok,
                        .data = .{
                            .lhs = 0,
                            .rhs = 0,
                        },
                    });
                };

                return self.addNode(.{
                    .tag = .simple_var_decl,
                    .main_token = const_tok,
                    .data = .{
                        .lhs = 0,
                        .rhs = @intCast(struct_node),
                    },
                });
            },
            .@"union" => |*s| {
                const const_tok = try self.addToken(.keyword_const, "const");
                _ = try self.addToken(.identifier, s.name);
                _ = try self.addToken(.equal, "=");
                const union_node = inner: {
                    const union_tok = try self.addToken(.keyword_union, "union");
                    _ = try self.addToken(.l_brace, "{");
                    _ = try self.addToken(.r_brace, "}");
                    _ = try self.addToken(.semicolon, ";");
                    break :inner try self.addNode(.{
                        .tag = .container_decl_two,
                        .main_token = union_tok,
                        .data = .{
                            .lhs = 0,
                            .rhs = 0,
                        },
                    });
                };

                return self.addNode(.{
                    .tag = .simple_var_decl,
                    .main_token = const_tok,
                    .data = .{
                        .lhs = 0,
                        .rhs = @intCast(union_node),
                    },
                });
            },
        }
    }
};

pub fn translate(allocator: std.mem.Allocator, document: *thrift.Document) !Ast {
    var ctx = Context{
        .allocator = allocator,
        .buf = std.ArrayList(u8).init(allocator),
        .extra_data = std.ArrayList(Node.Index).init(allocator),
    };
    defer ctx.deinit();

    try ctx.nodes.append(allocator, .{ .tag = .root, .main_token = 0, .data = .{
        .lhs = undefined,
        .rhs = undefined,
    } });

    const items = try allocator.alloc(Node.Index, document.definitions.items.len);
    defer allocator.free(items);

    for (document.definitions.items, 0..) |def, idx| {
        items[idx] = try ctx.renderDefinition(def);
    }

    const lhs = ctx.extra_data.items.len;
    for (items) |idx| {
        try ctx.extra_data.append(idx);
    }
    const rhs = ctx.extra_data.items.len;

    ctx.nodes.items(.data)[0] = .{
        .lhs = @intCast(lhs),
        .rhs = @intCast(rhs),
    };

    try ctx.tokens.append(allocator, .{
        .tag = .eof,
        .start = @intCast(ctx.buf.items.len),
    });

    return Ast{
        .source = try ctx.buf.toOwnedSliceSentinel(0),
        .tokens = ctx.tokens.toOwnedSlice(),
        .nodes = ctx.nodes.toOwnedSlice(),
        .extra_data = try ctx.extra_data.toOwnedSlice(),
        .errors = &.{},
        .mode = .zig,
    };
}

test "empty enum" {
    try expectTranslated(
        \\enum Foo {}
    ,
        \\const Foo = enum {};
    );
}

test "enum" {
    try expectTranslated(
        \\enum Foo {
        \\  BAR = 0;
        \\  BAZ = 1;
        \\}
    ,
        \\const Foo = enum {
        \\    BAR = 0,
        \\    BAZ = 1,
        \\};
    );
}

test "empty struct" {
    try expectTranslated(
        \\struct Foo {}
        \\struct Bar {}
    ,
        \\const Foo = struct {};
        \\const Bar = struct {};
    );
}

test "empty union" {
    try expectTranslated(
        \\union Foo {}
        \\union Bar {}
    ,
        \\const Foo = union {};
        \\const Bar = union {};
    );
}

fn expectTranslated(source: []const u8, expected: []const u8) !void {
    const allocator = std.testing.allocator;

    var document = try thrift.Document.init(allocator, source);
    defer document.deinit();

    var root = try translate(allocator, &document);
    defer root.deinit(allocator);
    defer allocator.free(root.source);

    const formatted = try root.render(allocator);
    defer allocator.free(formatted);

    try std.testing.expectEqualStrings(
        std.mem.trim(u8, expected, "\n"),
        std.mem.trim(u8, formatted, "\n"),
    );
}
