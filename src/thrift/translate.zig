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
    extra_data: std.ArrayListUnmanaged(u32),
    import_std: bool = false,

    fn deinit(self: *Context) void {
        self.buf.deinit(self.allocator);
        self.tokens.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
        self.extra_data.deinit(self.allocator);
    }

    fn addToken(self: *Context, tag: Token.Tag, text: []const u8) !Ast.TokenIndex {
        const index = self.tokens.len;
        try self.tokens.append(self.allocator, .{
            .tag = tag,
            .start = @intCast(self.buf.items.len),
        });
        try self.buf.appendSlice(self.allocator, text);
        return @intCast(index);
    }

    fn addNode(self: *Context, node: Node) !Node.Index {
        const index = self.nodes.len;
        try self.nodes.append(self.allocator, node);
        return @enumFromInt(index);
    }

    fn addExtra(self: *Context, extra: anytype) !Ast.ExtraIndex {
        const fields = std.meta.fields(@TypeOf(extra));
        try self.extra_data.ensureUnusedCapacity(self.allocator, fields.len);
        const result: Ast.ExtraIndex = @enumFromInt(self.extra_data.items.len);
        inline for (fields) |field| {
            const data: u32 = switch (field.type) {
                Ast.ExtraIndex,
                => @intFromEnum(@field(extra, field.name)),
                else => @compileError("unexpected field type"),
            };
            self.extra_data.appendAssumeCapacity(data);
        }
        return result;
    }

    fn listToSpan(self: *Context, list: []const Node.Index) !Node.SubRange {
        try self.extra_data.appendSlice(self.allocator, @ptrCast(list));
        return Node.SubRange{
            .start = @enumFromInt(self.extra_data.items.len - list.len),
            .end = @enumFromInt(self.extra_data.items.len),
        };
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
                        .opt_node_and_opt_node = .{
                            .none,
                            enum_node.toOptional(),
                        },
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
                        .opt_node_and_opt_node = .{
                            .none,
                            struct_node.toOptional(),
                        },
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
                        .opt_node_and_opt_node = .{
                            .none,
                            union_node.toOptional(),
                        },
                    },
                });
            },
        }
    }

    fn renderFieldType(self: *Context, field: *const thrift.Definition.Field) !Node.Index {
        var opt_idx: ?Ast.TokenIndex = null;
        if (field.req == .optional) {
            opt_idx = try self.addToken(.question_mark, "?");
        }

        const type_idx = try self.renderType(field.type);

        if (opt_idx) |idx| {
            return try self.addNode(.{
                .tag = .optional_type,
                .main_token = idx,
                .data = .{
                    .node = type_idx,
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
                    _ = try self.addToken(.keyword_const, "const");
                    const u8_node = try self.addNode(.{
                        .tag = .identifier,
                        .main_token = try self.addToken(.identifier, "u8"),
                        .data = undefined,
                    });
                    return try self.addNode(.{
                        .tag = .ptr_type_aligned,
                        .main_token = l_bracket,
                        .data = .{
                            .opt_node_and_node = .{
                                .none,
                                u8_node,
                            },
                        },
                    });
                },
                .uuid => @panic("uuid is not implemented"),
                .list => |*l| {
                    const l_bracket = try self.addToken(.l_bracket, "[");
                    _ = try self.addToken(.r_bracket, "]");
                    return try self.addNode(.{
                        .tag = .ptr_type_aligned,
                        .main_token = l_bracket,
                        .data = .{
                            .opt_node_and_node = .{
                                .none,
                                try self.renderType(l.element),
                            },
                        },
                    });
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

        const fields = try self.allocator.alloc(Node.Index, s.fields.count() + 1);
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
                    .node_and_opt_node = .{
                        type_node,
                        .none,
                    },
                },
            });

            if (kv.value_ptr.*.id) |id| {
                try field_ids.put(kv.key_ptr.*, id);
            }

            _ = try self.addToken(.comma, ",");
            i += 1;
        }

        const has_field_id_method = field_ids.count() > 0;
        if (has_field_id_method) {
            fields[i] = try self.renderFieldIdMethod(field_ids);
        }

        _ = try self.addToken(.r_brace, "}");

        const is_empty = s.fields.count() == 0;
        const span = try self.listToSpan(fields);
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else if (has_field_id_method) .container_decl else .container_decl_trailing,
            .main_token = union_tok,
            .data = if (is_empty) .{
                .opt_node_and_opt_node = .{
                    .none,
                    .none,
                },
            } else .{
                .extra_range = span,
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

        self.import_std = true;
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
                .opt_node_and_opt_node = .{
                    .none,
                    .none,
                },
            },
        });
        const fields_type = try self.addNode(.{
            .tag = .call_one,
            .main_token = field_enum_paren,
            .data = .{
                .node_and_opt_node = .{
                    field_enum,
                    at_this.toOptional(),
                },
            },
        });
        _ = try self.addToken(.r_paren, ")");

        const return_type = try self.addNode(.{
            .tag = .identifier,
            .main_token = try self.addToken(.identifier, "u32"),
            .data = undefined,
        });

        const fn_proto = try self.addNode(.{
            .tag = .fn_proto_simple,
            .main_token = fn_tok,
            .data = .{
                .opt_node_and_opt_node = .{
                    fields_type.toOptional(),
                    return_type.toOptional(),
                },
            },
        });

        const fn_body_start = try self.addToken(.l_brace, "{");

        const switch_tok = try self.addToken(.keyword_switch, "switch");
        _ = try self.addToken(.l_paren, "(");
        const field_tok = try self.addToken(.identifier, "field");
        _ = try self.addToken(.r_paren, ")");
        _ = try self.addToken(.l_brace, "{");

        var cases = try self.allocator.alloc(Node.Index, field_ids.count());
        defer self.allocator.free(cases);

        var iter = field_ids.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const dot_tok = try self.addToken(.period, ".");
            const field_ident = try self.addToken(.identifier, kv.key_ptr.*);
            const field_case = try self.addNode(.{
                .tag = .enum_literal,
                .main_token = field_ident,
                .data = .{
                    .opt_node_and_opt_node = .{
                        (@as(Node.Index, @enumFromInt(dot_tok))).toOptional(),
                        .none,
                    },
                },
            });
            const arrow = try self.addToken(.equal_angle_bracket_right, "=>");
            const num_literal = try std.fmt.allocPrint(self.allocator, "{}", .{kv.value_ptr.*});
            defer self.allocator.free(num_literal);
            const return_stmt = try self.addNode(.{
                .tag = .@"return",
                .main_token = try self.addToken(.keyword_return, "return"),
                .data = .{
                    .opt_node = (try self.addNode(.{
                        .tag = .number_literal,
                        .main_token = try self.addToken(.number_literal, num_literal),
                        .data = undefined,
                    })).toOptional(),
                },
            });

            if (iter.index != field_ids.count()) {
                _ = try self.addToken(.comma, ",");
            }

            cases[i] = try self.addNode(.{
                .tag = .switch_case_one,
                .main_token = arrow,
                .data = .{
                    .opt_node_and_node = .{
                        field_case.toOptional(),
                        return_stmt,
                    },
                },
            });
            i += 1;
        }

        _ = try self.addToken(.r_brace, "}");

        const span = try self.listToSpan(cases);

        const switch_expression = try self.addNode(.{
            .tag = .@"switch",
            .main_token = switch_tok,
            .data = .{
                .node_and_extra = .{
                    try self.addNode(.{
                        .tag = .identifier,
                        .main_token = field_tok,
                        .data = undefined,
                    }),
                    try self.addExtra(Node.SubRange{
                        .start = span.start,
                        .end = span.end,
                    }),
                },
            },
        });
        const fn_body = try self.addNode(.{
            .tag = .block_two,
            .main_token = fn_body_start,
            .data = .{
                .opt_node_and_opt_node = .{
                    switch_expression.toOptional(),
                    .none,
                },
            },
        });
        _ = try self.addToken(.r_brace, "}");

        return self.addNode(.{
            .tag = .fn_decl,
            .main_token = fn_tok,
            .data = .{
                .node_and_node = .{
                    fn_proto,
                    fn_body,
                },
            },
        });
    }

    fn renderStruct(self: *Context, s: *const thrift.Definition.Struct) !Node.Index {
        const struct_tok = try self.addToken(.keyword_struct, "struct");
        _ = try self.addToken(.l_brace, "{");

        const fields = try self.allocator.alloc(Node.Index, s.fields.count() + 1);
        defer self.allocator.free(fields);

        var field_ids = OrderedStringHashMap(u32).init(self.allocator);
        defer field_ids.deinit();

        var iter = s.fields.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const field_tok = try self.addToken(.identifier, kv.key_ptr.*);
            _ = try self.addToken(.colon, ":");

            const type_node = try self.renderFieldType(kv.value_ptr);
            const init_expr = if (kv.value_ptr.*.req == .optional) blk: {
                _ = try self.addToken(.equal, "=");
                const node = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = try self.addToken(.identifier, "null"),
                    .data = undefined,
                });
                break :blk node.toOptional();
            } else .none;

            fields[i] = try self.addNode(.{
                .tag = .container_field_init,
                .main_token = field_tok,
                .data = .{
                    .node_and_opt_node = .{
                        type_node,
                        init_expr,
                    },
                },
            });

            if (kv.value_ptr.*.id) |id| {
                try field_ids.put(kv.key_ptr.*, id);
            }

            _ = try self.addToken(.comma, ",");
            i += 1;
        }

        const has_field_id_method = field_ids.count() > 0;
        const size = if (has_field_id_method) blk: {
            fields[i] = try self.renderFieldIdMethod(field_ids);
            break :blk fields.len;
        } else blk: {
            break :blk s.fields.count();
        };

        _ = try self.addToken(.r_brace, "}");

        const is_empty = i == 0;
        const span = try self.listToSpan(fields[0..size]);
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_two else if (has_field_id_method) .container_decl else .container_decl_trailing,
            .main_token = struct_tok,
            .data = if (is_empty) .{
                .opt_node_and_opt_node = .{
                    .none,
                    .none,
                },
            } else .{
                .extra_range = span,
            },
        });
    }

    fn renderEnum(self: *Context, e: *const thrift.Definition.Enum) !Node.Index {
        const enum_tok = try self.addToken(.keyword_enum, "enum");
        const is_empty = e.values.count() == 0;
        _ = try self.addToken(.l_paren, "(");
        const u8_ident = try self.addNode(.{
            .tag = .identifier,
            .main_token = try self.addToken(.identifier, "u8"),
            .data = undefined,
        });
        _ = try self.addToken(.r_paren, ")");
        _ = try self.addToken(.l_brace, "{");
        const fields = try self.allocator.alloc(Node.Index, e.values.count());
        defer self.allocator.free(fields);

        var iter = e.values.iterator();
        var i: usize = 0;
        while (iter.next()) |kv| {
            const field_token = try self.addToken(.identifier, kv.key_ptr.*);
            const field_index = try self.addNode(.{
                .tag = .identifier,
                .main_token = field_token,
                .data = undefined,
            });

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
                .main_token = field_token,
                .data = .{
                    .node_and_opt_node = .{
                        field_index,
                        value_index.toOptional(),
                    },
                },
            });
            _ = try self.addToken(.comma, ",");
            i += 1;
        }

        _ = try self.addToken(.r_brace, "}");
        const span = try self.listToSpan(fields);
        return try self.addNode(.{
            .tag = if (is_empty) .container_decl_arg else .container_decl_arg_trailing,
            .main_token = enum_tok,
            .data = .{ .node_and_extra = .{
                u8_ident,
                try self.addExtra(span),
            } },
        });
    }

    fn renderPrelude(self: *Context) !?Node.Index {
        if (!self.import_std) {
            return null;
        }

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

        return try self.addNode(.{
            .tag = .simple_var_decl,
            .main_token = const_tok,
            .data = .{
                .opt_node_and_opt_node = .{
                    .none, (try self.addNode(.{
                        .tag = .builtin_call_two,
                        .main_token = import_tok,
                        .data = .{
                            .opt_node_and_opt_node = .{
                                std_node.toOptional(),
                                .none,
                            },
                        },
                    })).toOptional(),
                },
            },
        });
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
                        .node_and_token = .{
                            p,
                            try self.addToken(.identifier, part),
                        },
                    },
                });
            } else {
                prev = try self.addNode(.{
                    .tag = .identifier,
                    .main_token = try self.addToken(.identifier, part),
                    .data = undefined,
                });
            }
        }

        return prev.?;
    }
};

pub fn translate(allocator: std.mem.Allocator, document: *thrift.Document) !Ast {
    var ctx = Context{
        .allocator = allocator,
        .buf = .empty,
        .extra_data = .empty,
    };
    defer ctx.deinit();

    try ctx.nodes.append(allocator, .{
        .tag = .root,
        .main_token = 0,
        .data = undefined,
    });

    const items = try allocator.alloc(Node.Index, document.definitions.items.len + 1);
    defer allocator.free(items);

    for (document.definitions.items, 0..) |def, idx| {
        items[idx] = try ctx.renderDefinition(def);
    }

    const size = if (try ctx.renderPrelude()) |index| blk: {
        items[items.len - 1] = index;
        break :blk items.len;
    } else blk: {
        break :blk items.len - 1;
    };

    ctx.nodes.items(.data)[0] = .{ .extra_range = try ctx.listToSpan(items[0..size]) };

    try ctx.tokens.append(allocator, .{
        .tag = .eof,
        .start = @intCast(ctx.buf.items.len),
    });

    return Ast{
        .source = try ctx.buf.toOwnedSliceSentinel(ctx.allocator, 0),
        .tokens = ctx.tokens.toOwnedSlice(),
        .nodes = ctx.nodes.toOwnedSlice(),
        .extra_data = try ctx.extra_data.toOwnedSlice(ctx.allocator),
        .errors = &.{},
        .mode = .zig,
    };
}

test "empty enum" {
    try expectTranslated(
        \\enum Foo {}
    ,
        \\pub const Foo = enum(u8) {};
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
        \\pub const Bar = struct {};
        \\pub const Foo = struct {
        \\    foo: i32,
        \\    bar: Bar,
        \\    baz: []i64,
        \\    opt: ?u8 = null,
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
        \\  3: string foobar;
        \\  optional byte qux;
        \\}
    ,
        \\pub const Bar = struct {};
        \\pub const Foo = struct {
        \\    foo: i32,
        \\    bar: Bar,
        \\    baz: ?[]i64 = null,
        \\    foobar: []const u8,
        \\    qux: ?u8 = null,
        \\    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) u32 {
        \\        switch (field) {
        \\            .foo => return 1,
        \\            .bar => return 5,
        \\            .baz => return 2,
        \\            .foobar => return 3,
        \\        }
        \\    }
        \\};
        \\const std = @import("std");
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
        \\    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) u32 {
        \\        switch (field) {
        \\            .baz => return 1,
        \\            .bar => return 2,
        \\        }
        \\    }
        \\};
        \\const std = @import("std");
    );
}

fn expectTranslated(source: []const u8, expected: []const u8) !void {
    const allocator = std.testing.allocator;

    var document = try thrift.Document.init(allocator, source);
    defer document.deinit();

    var root = try translate(allocator, &document);
    defer root.deinit(allocator);
    defer allocator.free(root.source);

    const formatted = try root.renderAlloc(allocator);
    defer allocator.free(formatted);

    try std.testing.expectEqualStrings(
        std.mem.trim(u8, expected, "\n"),
        std.mem.trim(u8, formatted, "\n"),
    );
}
