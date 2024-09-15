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
    std_imports: std.StringHashMap([]const u8),

    fn deinit(self: *Context) void {
        self.buf.deinit();
        self.tokens.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
        self.extra_data.deinit();
        self.std_imports.deinit();
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
                const enum_node = try self.renderEnum(e);
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
                const struct_node = try self.renderStruct(s);
                _ = try self.addToken(.semicolon, ";");

                return self.addNode(.{
                    .tag = .simple_var_decl,
                    .main_token = const_tok,
                    .data = .{
                        .lhs = 0,
                        .rhs = @intCast(struct_node),
                    },
                });
            },
            .@"union" => |*u| {
                const const_tok = try self.addToken(.keyword_const, "const");
                _ = try self.addToken(.identifier, u.name);
                _ = try self.addToken(.equal, "=");
                const union_node = try self.renderUnion(u);
                _ = try self.addToken(.semicolon, ";");

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

    fn renderType(self: *Context, ty: *const thrift.Type) !Node.Index {
        const name = switch (ty.*) {
            .builtin => |*b| switch (b.*) {
                .bool => "bool",
                .byte => "u8",
                .i8 => "i8",
                .i16 => "i16",
                .i32 => "i32",
                .i64 => "i64",
                .double => "f64",
                .string, .binary => {
                    const l_bracket = try self.addToken(.l_bracket, "[");
                    _ = try self.addToken(.r_bracket, "]");
                    // TODO: Make this `const u8`.
                    const const_u8 = try self.addNode(.{
                        .tag = .identifier,
                        .main_token = try self.addToken(.identifier, "u8"),
                        .data = undefined,
                    });
                    return try self.addNode(.{
                        .tag = .ptr_type_aligned,
                        .main_token = l_bracket,
                        .data = .{
                            .lhs = 0,
                            .rhs = const_u8,
                        },
                    });
                },
                .uuid => @panic("uuid is not implemented"),
                .list => |*l| {
                    _ = try self.std_imports.put("List", "ArrayList");
                    const arrayList = try self.addNode(.{
                        .tag = .identifier,
                        .main_token = try self.addToken(.identifier, "List"),
                        .data = undefined,
                    });
                    const call = try self.addNode(.{
                        .tag = .call_one,
                        .main_token = try self.addToken(.l_paren, "("),
                        .data = .{
                            .lhs = arrayList,
                            .rhs = try self.renderType(l.element),
                        },
                    });
                    _ = try self.addToken(.r_paren, ")");

                    return call;
                },
                .set => @panic("set is not implemented"),
                .map => @panic("map is not implemented"),
            },
            .custom => |*c| c.*.name,
        };

        return try self.addNode(.{
            .tag = .identifier,
            .main_token = try self.addToken(.identifier, name),
            .data = undefined,
        });
    }

    fn renderUnion(self: *Context, s: *const thrift.Definition.Union) !Node.Index {
        const union_tok = try self.addToken(.keyword_union, "union");
        _ = try self.addToken(.l_brace, "{");

        const fields = try self.allocator.alloc(Node.Index, s.fields.count());
        defer self.allocator.free(fields);

        var iter = s.fields.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const field_tok = try self.addToken(.identifier, kv.key_ptr.*);
            _ = try self.addToken(.colon, ":");

            const type_node = try self.renderType(kv.value_ptr.*.type);
            fields[i] = try self.addNode(.{
                .tag = .container_field_init,
                .main_token = field_tok,
                .data = .{
                    .lhs = type_node,
                    .rhs = 0,
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
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else .container_decl_trailing,
            .main_token = union_tok,
            .data = .{
                .lhs = if (is_empty) 0 else @intCast(lhs),
                .rhs = if (is_empty) 0 else @intCast(rhs),
            },
        });
    }

    fn renderStruct(self: *Context, s: *const thrift.Definition.Struct) !Node.Index {
        const struct_tok = try self.addToken(.keyword_struct, "struct");
        _ = try self.addToken(.l_brace, "{");

        const fields = try self.allocator.alloc(Node.Index, s.fields.count());
        defer self.allocator.free(fields);

        var iter = s.fields.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const field_tok = try self.addToken(.identifier, kv.key_ptr.*);
            _ = try self.addToken(.colon, ":");

            const type_node = try self.renderType(kv.value_ptr.*.type);
            fields[i] = try self.addNode(.{
                .tag = .container_field_init,
                .main_token = field_tok,
                .data = .{
                    .lhs = type_node,
                    .rhs = 0,
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
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else .container_decl_trailing,
            .main_token = struct_tok,
            .data = .{
                .lhs = if (is_empty) 0 else @intCast(lhs),
                .rhs = if (is_empty) 0 else @intCast(rhs),
            },
        });
    }

    fn renderEnum(self: *Context, e: *const thrift.Definition.Enum) !Node.Index {
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
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else .container_decl_trailing,
            .main_token = enum_tok,
            .data = .{
                .lhs = if (is_empty) 0 else @intCast(lhs),
                .rhs = if (is_empty) 0 else @intCast(rhs),
            },
        });
    }

    fn renderPreludeImports(self: *Context) !void {
        if (self.std_imports.count() == 0) {
            return;
        }

        {
            const const_tok = try self.addToken(.keyword_const, "const");
            _ = try self.addToken(.identifier, "std");
            _ = try self.addToken(.equal, "=");

            const import_tok = try self.addToken(.builtin, "@import");
            _ = try self.addToken(.l_paren, "(");
            const std_node = try self.addNode(.{
                .tag = .string_literal,
                .main_token = try self.addToken(.string_literal, "\"std\""),
                .data = undefined,
            });
            _ = try self.addToken(.r_paren, ")");
            _ = try self.addToken(.semicolon, ";");

            const import_std = try self.addNode(.{
                .tag = .builtin_call_two,
                .main_token = import_tok,
                .data = .{
                    .lhs = std_node,
                    .rhs = 0,
                },
            });

            try self.extra_data.append(try self.addNode(.{
                .tag = .simple_var_decl,
                .main_token = const_tok,
                .data = .{
                    .lhs = 0,
                    .rhs = import_std,
                },
            }));
        }

        var iter = self.std_imports.iterator();
        while (iter.next()) |kv| {
            const alias = kv.key_ptr.*;
            const element = kv.value_ptr.*;

            const const_tok = try self.addToken(.keyword_const, "const");
            _ = try self.addToken(.identifier, alias);
            _ = try self.addToken(.equal, "=");

            const std_node = try self.addNode(.{
                .tag = .identifier,
                .main_token = try self.addToken(.identifier, "std"),
                .data = undefined,
            });
            const dot_tok = try self.addToken(.period, ".");
            const element_tok = try self.addToken(.identifier, element);
            _ = try self.addToken(.semicolon, ";");

            const import_node = try self.addNode(.{
                .tag = .field_access,
                .main_token = dot_tok,
                .data = .{
                    .lhs = std_node,
                    .rhs = element_tok,
                },
            });

            try self.extra_data.append(try self.addNode(.{
                .tag = .simple_var_decl,
                .main_token = const_tok,
                .data = .{
                    .lhs = 0,
                    .rhs = import_node,
                },
            }));
        }
    }
};

pub fn translate(allocator: std.mem.Allocator, document: *thrift.Document) !Ast {
    var ctx = Context{
        .allocator = allocator,
        .buf = std.ArrayList(u8).init(allocator),
        .extra_data = std.ArrayList(Node.Index).init(allocator),
        .std_imports = std.StringHashMap([]const u8).init(allocator),
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
    try ctx.renderPreludeImports();
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

test "struct" {
    try expectTranslated(
        \\struct Bar {}
        \\struct Foo {
        \\  i32 foo;
        \\  Bar bar;
        \\  list<i64> baz;
        \\}
    ,
        \\const std = @import("std");
        \\const List = std.ArrayList;
        \\const Bar = struct {};
        \\const Foo = struct {
        \\    foo: i32,
        \\    bar: Bar,
        \\    baz: List(i64),
        \\};
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

test "union" {
    try expectTranslated(
        \\struct Bar {}
        \\union Foo {
        \\  1: i32 baz;
        \\  2: Bar bar;
        \\}
    ,
        \\const Bar = struct {};
        \\const Foo = union {
        \\    baz: i32,
        \\    bar: Bar,
        \\};
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
