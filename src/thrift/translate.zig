//! Translates parsed Thrift AST into Zig AST, heavily inspired by `aro_translate_c`.

const std = @import("std");
const Ast = std.zig.Ast;
const Node = Ast.Node;
const Token = std.zig.Token;
pub const OrderedStringHashMap = @import("../ordered_map.zig").OrderedStringHashMap;
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

    fn addExtra(self: *Context, extra: anytype) !Node.Index {
        const fields = std.meta.fields(@TypeOf(extra));
        const result: Node.Index = @intCast(self.extra_data.items.len);
        inline for (fields) |field| {
            comptime std.debug.assert(field.type == Node.Index);
            try self.extra_data.append(@field(extra, field.name));
        }
        return result;
    }

    fn renderDefinition(self: *Context, def: thrift.Definition) !Node.Index {
        switch (def) {
            .@"enum" => |*e| {
                _ = try self.addToken(.keyword_pub, "pub");
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
                _ = try self.addToken(.keyword_pub, "pub");
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
                _ = try self.addToken(.keyword_pub, "pub");
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

    fn renderFieldType(self: *Context, field: *const thrift.Definition.Field) !Node.Index {
        var opt_idx: ?Node.Index = null;
        if (field.req == .optional) {
            opt_idx = try self.addToken(.question_mark, "?");
        }

        const type_idx = try self.renderType(field.type);

        if (opt_idx) |idx| {
            return try self.addNode(.{
                .tag = .optional_type,
                .main_token = idx,
                .data = .{
                    .lhs = type_idx,
                    .rhs = undefined,
                },
            });
        }
        return type_idx;
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

            const type_node = try self.renderFieldType(kv.value_ptr);
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

    fn renderFieldIdMethod(self: *Context, field_ids: OrderedStringHashMap(u32)) !Node.Index {
        _ = try self.addToken(.keyword_pub, "pub");
        const fn_tok = try self.addToken(.keyword_fn, "fn");
        _ = try self.addToken(.identifier, "fieldId");
        _ = try self.addToken(.l_paren, "(");
        _ = try self.addToken(.keyword_comptime, "comptime");
        _ = try self.addToken(.identifier, "field");
        _ = try self.addToken(.colon, ":");

        const field_enum = try self.renderFieldAccess(.{ "std", "meta", "FieldEnum" });
        const field_enum_paren = try self.addToken(.l_paren, "(");
        const this_tok = try self.addToken(.builtin, "@This");
        _ = try self.addToken(.l_paren, "(");
        _ = try self.addToken(.r_paren, ")");
        _ = try self.addToken(.r_paren, ")");
        const at_this = try self.addNode(.{
            .tag = .builtin_call_two,
            .main_token = this_tok,
            .data = .{
                .lhs = undefined,
                .rhs = undefined,
            },
        });
        const fields_type = try self.addNode(.{
            .tag = .call_one,
            .main_token = field_enum_paren,
            .data = .{
                .lhs = field_enum,
                .rhs = at_this,
            },
        });
        _ = try self.addToken(.r_paren, ")");

        const return_type = try self.addNode(.{
            .tag = .optional_type,
            .main_token = try self.addToken(.question_mark, "?"),
            .data = .{
                .lhs = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = try self.addToken(.identifier, "u32"),
                    .data = undefined,
                }),
                .rhs = undefined,
            },
        });

        const fn_proto = try self.addNode(.{
            .tag = .fn_proto_simple,
            .main_token = fn_tok,
            .data = .{
                .lhs = fields_type,
                .rhs = return_type,
            },
        });

        const fn_body_start = try self.addToken(.l_brace, "{");

        const switch_tok = try self.addToken(.keyword_switch, "switch");
        _ = try self.addToken(.l_paren, "(");
        const field_tok = try self.addToken(.identifier, "field");
        _ = try self.addToken(.r_paren, ")");
        _ = try self.addToken(.l_brace, "{");

        const cases_start = self.extra_data.items.len;

        var iter = field_ids.iterator();
        while (iter.next()) |kv| {
            const dot_tok = try self.addToken(.period, ".");
            const field_ident = try self.addToken(.identifier, kv.key_ptr.*);
            const field_case = try self.addNode(.{
                .tag = .enum_literal,
                .main_token = field_ident,
                .data = .{
                    .lhs = dot_tok,
                    .rhs = 0,
                },
            });
            const arrow = try self.addToken(.equal_angle_bracket_right, "=>");
            const num_literal = try std.fmt.allocPrint(self.allocator, "{}", .{kv.value_ptr.*});
            defer self.allocator.free(num_literal);
            const else_return = try self.addNode(.{
                .tag = .@"return",
                .main_token = try self.addToken(.keyword_return, "return"),
                .data = .{
                    .lhs = try self.addNode(.{
                        .tag = .number_literal,
                        .main_token = try self.addToken(.number_literal, num_literal),
                        .data = .{
                            .lhs = undefined,
                            .rhs = undefined,
                        },
                    }),
                    .rhs = undefined,
                },
            });
            _ = try self.addToken(.comma, ",");
            _ = try self.extra_data.append(try self.addNode(.{
                .tag = .switch_case_one,
                .main_token = arrow,
                .data = .{
                    .lhs = field_case,
                    .rhs = else_return,
                },
            }));
        }

        _ = try self.addToken(.keyword_else, "else");
        const else_arrow = try self.addToken(.equal_angle_bracket_right, "=>");
        const else_return = try self.addNode(.{
            .tag = .@"return",
            .main_token = try self.addToken(.keyword_return, "return"),
            .data = .{
                .lhs = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = try self.addToken(.identifier, "null"),
                    .data = .{
                        .lhs = undefined,
                        .rhs = undefined,
                    },
                }),
                .rhs = undefined,
            },
        });
        _ = try self.extra_data.append(try self.addNode(.{
            .tag = .switch_case_one,
            .main_token = else_arrow,
            .data = .{
                .lhs = 0,
                .rhs = else_return,
            },
        }));

        _ = try self.addToken(.r_brace, "}");

        const cases_end = self.extra_data.items.len;

        const switch_expression = try self.addNode(.{
            .tag = .@"switch",
            .main_token = switch_tok,
            .data = .{
                .lhs = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = field_tok,
                    .data = .{
                        .lhs = undefined,
                        .rhs = undefined,
                    },
                }),
                .rhs = try self.addExtra(Node.SubRange{
                    .start = @intCast(cases_start),
                    .end = @intCast(cases_end),
                }),
            },
        });
        const fn_body = try self.addNode(.{
            .tag = .block_two,
            .main_token = fn_body_start,
            .data = .{
                .lhs = switch_expression,
                .rhs = 0,
            },
        });
        _ = try self.addToken(.r_brace, "}");

        return self.addNode(.{
            .tag = .fn_decl,
            .main_token = fn_tok,
            .data = .{
                .lhs = fn_proto,
                .rhs = fn_body,
            },
        });
    }

    fn renderStruct(self: *Context, s: *const thrift.Definition.Struct) !Node.Index {
        const struct_tok = try self.addToken(.keyword_struct, "struct");
        _ = try self.addToken(.l_brace, "{");

        const fields = try self.allocator.alloc(Node.Index, s.fields.count());
        defer self.allocator.free(fields);

        var field_ids = OrderedStringHashMap(u32).init(self.allocator);
        defer field_ids.deinit();

        var iter = s.fields.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const field_tok = try self.addToken(.identifier, kv.key_ptr.*);
            _ = try self.addToken(.colon, ":");

            const type_node = try self.renderFieldType(kv.value_ptr);
            fields[i] = try self.addNode(.{
                .tag = .container_field_init,
                .main_token = field_tok,
                .data = .{
                    .lhs = type_node,
                    .rhs = 0,
                },
            });

            if (kv.value_ptr.*.id) |id| {
                try field_ids.put(kv.key_ptr.*, id);
            }

            _ = try self.addToken(.comma, ",");
            i += 1;
        }

        var field_id_method: ?Node.Index = null;
        if (field_ids.count() > 0) {
            field_id_method = try self.renderFieldIdMethod(field_ids);
        }

        _ = try self.addToken(.r_brace, "}");

        const lhs = self.extra_data.items.len;
        for (fields) |f| {
            try self.extra_data.append(f);
        }
        if (field_id_method) |idx| {
            try self.extra_data.append(idx);
        }
        const rhs = self.extra_data.items.len;
        const is_empty = lhs == rhs;
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else if (field_id_method == null) .container_decl_trailing else .container_decl,
            .main_token = struct_tok,
            .data = .{
                .lhs = if (is_empty) 0 else @intCast(lhs),
                .rhs = if (is_empty) 0 else @intCast(rhs),
            },
        });
    }

    fn renderEnum(self: *Context, e: *const thrift.Definition.Enum) !Node.Index {
        const enum_tok = try self.addToken(.keyword_enum, "enum");
        const is_empty = e.values.count() == 0;
        var u8_ident_tok: Node.Index = undefined;
        if (!is_empty) {
            _ = try self.addToken(.l_paren, "(");
            u8_ident_tok = try self.addToken(.identifier, "u8");
            _ = try self.addToken(.r_paren, ")");
        }
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

        const members_start = self.extra_data.items.len;
        for (fields) |f| {
            try self.extra_data.append(f);
        }
        const members_end = self.extra_data.items.len;
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else .container_decl_arg_trailing,
            .main_token = enum_tok,
            .data = .{
                .lhs = if (is_empty) 0 else try self.addNode(.{
                    .tag = .identifier,
                    .main_token = u8_ident_tok,
                    .data = undefined,
                }),
                .rhs = if (is_empty) 0 else try self.addExtra(Node.SubRange{
                    .start = @intCast(members_start),
                    .end = @intCast(members_end),
                }),
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

    fn renderFieldAccess(self: *Context, comptime parts: anytype) !Node.Index {
        if (parts.len < 2) {
            @compileError("`parts` must contain at least two elements");
        }

        var prev: ?Node.Index = null;
        inline for (parts) |part| {
            if (prev) |p| {
                prev = try self.addNode(.{
                    .tag = .field_access,
                    .main_token = try self.addToken(.period, "."),
                    .data = .{
                        .lhs = p,
                        .rhs = try self.addToken(.identifier, part),
                    },
                });
            } else {
                prev = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = try self.addToken(.identifier, part),
                    .data = .{
                        .lhs = undefined,
                        .rhs = undefined,
                    },
                });
            }
        }

        return prev.?;
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
        \\pub const Foo = enum {};
    );
}

test "enum" {
    try expectTranslated(
        \\enum Foo {
        \\  BAR = 0;
        \\  BAZ = 1;
        \\}
    ,
        \\pub const Foo = enum(u8) {
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
        \\pub const Foo = struct {};
        \\pub const Bar = struct {};
    );
}

test "struct" {
    try expectTranslated(
        \\struct Bar {}
        \\struct Foo {
        \\  i32 foo;
        \\  Bar bar;
        \\  list<i64> baz;
        \\  optional byte opt;
        \\}
    ,
        \\const std = @import("std");
        \\const List = std.ArrayList;
        \\pub const Bar = struct {};
        \\pub const Foo = struct {
        \\    foo: i32,
        \\    bar: Bar,
        \\    baz: List(i64),
        \\    opt: ?u8,
        \\};
    );
}

test "struct with field id" {
    try expectTranslated(
        \\struct Bar {}
        \\struct Foo {
        \\  1: i32 foo;
        \\  5: required Bar bar;
        \\  2: optional list<i64> baz;
        \\  optional byte qux;
        \\}
    ,
        \\const std = @import("std");
        \\const List = std.ArrayList;
        \\pub const Bar = struct {};
        \\pub const Foo = struct {
        \\    foo: i32,
        \\    bar: Bar,
        \\    baz: ?List(i64),
        \\    qux: ?u8,
        \\    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        \\        switch (field) {
        \\            .foo => return 1,
        \\            .bar => return 5,
        \\            .baz => return 2,
        \\            else => return null,
        \\        }
        \\    }
        \\};
    );
}

test "empty union" {
    try expectTranslated(
        \\union Foo {}
        \\union Bar {}
    ,
        \\pub const Foo = union {};
        \\pub const Bar = union {};
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
        \\pub const Bar = struct {};
        \\pub const Foo = union {
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
