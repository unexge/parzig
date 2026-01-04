//! A Thrift parser aimed on parsing `parquet.thrift` only.

const std = @import("std");

pub const PeekableScanner = @import("./thrift/PeekableScanner.zig");
pub const OrderedStringHashMap = @import("./ordered_map.zig").OrderedStringHashMap;
pub const protocol_compact = @import("./thrift/protocol/compact.zig");
pub const translate = @import("./thrift/translate.zig").translate;

pub const Document = struct {
    arena: std.heap.ArenaAllocator,

    headers: std.ArrayList(Header),
    definitions: std.ArrayList(Definition),

    pub fn init(gpa: std.mem.Allocator, source: []const u8) !Document {
        var document = Document{
            .arena = .init(gpa),
            .definitions = .empty,
            .headers = .empty,
        };
        errdefer document.deinit();
        try document.parse(source);
        return document;
    }

    pub fn deinit(self: *Document) void {
        self.arena.deinit();
    }

    fn parse(self: *Document, source: []const u8) !void {
        var scanner = PeekableScanner.init(source);

        while (true) {
            skipComments(&scanner);
            if (try Header.parse(&scanner)) |header| {
                try self.headers.append(self.arena.allocator(), header);
            } else {
                break;
            }
        }

        while (true) {
            skipComments(&scanner);
            const def = Definition.parse(self.arena.allocator(), &scanner);
            if (def == error.EndOfDocument) {
                break;
            }
            try self.definitions.append(self.arena.allocator(), try def);
            skipComments(&scanner);
        }
    }
};

pub const Header = union(enum) {
    // `include` and `cpp_include` are not supported.
    namespace: Namespace,

    const Namespace = struct {
        identifier: Identifier,
        scope: Scope,

        const Scope = enum {
            glob,
            c_glib,
            cpp,
            delphi,
            haxe,
            go,
            java,
            js,
            lua,
            netstd,
            perl,
            php,
            py,
            py_twisted,
            rb,
            st,
            xsd,
        };
    };

    fn parse(scanner: *PeekableScanner) !?Header {
        const token = scanner.peek();
        switch (token.kind) {
            .namespace => {
                _ = try scanner.expect(.namespace);
                const scope = switch (scanner.next().kind) {
                    .namespace_scope_glob => Namespace.Scope.glob,
                    .namespace_scope_c_glib => Namespace.Scope.c_glib,
                    .namespace_scope_cpp => Namespace.Scope.cpp,
                    .namespace_scope_delphi => Namespace.Scope.delphi,
                    .namespace_scope_haxe => Namespace.Scope.haxe,
                    .namespace_scope_go => Namespace.Scope.go,
                    .namespace_scope_java => Namespace.Scope.java,
                    .namespace_scope_js => Namespace.Scope.js,
                    .namespace_scope_lua => Namespace.Scope.lua,
                    .namespace_scope_netstd => Namespace.Scope.netstd,
                    .namespace_scope_perl => Namespace.Scope.perl,
                    .namespace_scope_php => Namespace.Scope.php,
                    .namespace_scope_py => Namespace.Scope.py,
                    .namespace_scope_py_twisted => Namespace.Scope.py_twisted,
                    .namespace_scope_rb => Namespace.Scope.rb,
                    .namespace_scope_st => Namespace.Scope.st,
                    .namespace_scope_xsd => Namespace.Scope.xsd,
                    else => return error.ExpectedNamespaceScope,
                };
                const identifier = try parseIdentifier(scanner);

                return Header{ .namespace = Header.Namespace{ .identifier = identifier, .scope = scope } };
            },
            .include, .cpp_include => return error.IncludeIsNotSupported,
            else => return null,
        }
    }
};

pub const Definition = union(enum) {
    @"enum": Enum,
    @"struct": Struct,
    @"union": Union,
    // Other definitions such as `const`, `typedef`, `service` are not supported

    pub const Enum = struct {
        name: Identifier,
        values: OrderedStringHashMap(u64),
    };

    pub const Struct = struct {
        name: Identifier,
        fields: OrderedStringHashMap(Field),
    };

    pub const Union = struct {
        name: Identifier,
        fields: OrderedStringHashMap(Field),
    };

    pub const Field = struct {
        id: ?u32 = null,
        req: Requiredness = .default,
        type: *Type,
        default: ?ConstValue = null,

        const Requiredness = enum { required, optional, default };

        fn parse(allocator: std.mem.Allocator, scanner: *PeekableScanner) !struct { identifier: Identifier, field: Field } {
            skipComments(scanner);

            var id: ?u32 = null;
            if (scanner.peek().kind == .number_literal) {
                id = try parseInt(u32, scanner);
                _ = try scanner.expect(.colon);
            }

            var req: Field.Requiredness = .default;
            if (scanner.peek().kind == .required) {
                _ = try scanner.expect(.required);
                req = .required;
            } else if (scanner.peek().kind == .optional) {
                _ = try scanner.expect(.optional);
                req = .optional;
            }

            const @"type" = try Type.parse(allocator, scanner);
            errdefer @"type".deinit(allocator);
            const identifier = try parseIdentifier(scanner);

            var default: ?ConstValue = null;
            if (scanner.nextIf(.equal)) |_| {
                default = try ConstValue.parse(scanner);
            }

            skipListSeperator(scanner);
            skipComments(scanner);

            return .{ .identifier = identifier, .field = Field{
                .id = id,
                .req = req,
                .type = @"type",
                .default = default,
            } };
        }

        fn deinit(self: *Field, allocator: std.mem.Allocator) void {
            self.type.deinit(allocator);
        }
    };

    fn parse(allocator: std.mem.Allocator, scanner: *PeekableScanner) !Definition {
        skipComments(scanner);
        const token = scanner.next();
        switch (token.kind) {
            .@"enum" => {
                return .{ .@"enum" = try parseEnum(allocator, scanner) };
            },
            .@"struct" => {
                return .{ .@"struct" = try parseStruct(allocator, scanner) };
            },
            .@"union" => {
                return .{ .@"union" = try parseUnion(allocator, scanner) };
            },
            .end_of_document => return error.EndOfDocument,
            else => return error.UnexpectedToken,
        }
    }

    fn parseEnum(allocator: std.mem.Allocator, scanner: *PeekableScanner) !Enum {
        var values = OrderedStringHashMap(u64).init(allocator);
        var nextValue: u64 = 0;

        const name = try parseIdentifier(scanner);
        _ = try scanner.expect(.brace_left);
        while (true) {
            skipComments(scanner);
            // empty enum
            if (scanner.peek().kind == .brace_right) {
                break;
            }

            const field = try parseIdentifier(scanner);

            const value = if (scanner.nextIf(.equal)) |_| try parseInt(u64, scanner) else nextValue;
            nextValue = value + 1;

            skipListSeperator(scanner);
            skipComments(scanner);

            try values.put(field, value);

            if (scanner.peek().kind == .brace_right) {
                break;
            }
        }
        _ = try scanner.expect(.brace_right);

        return Definition.Enum{
            .name = name,
            .values = values,
        };
    }

    fn parseStruct(allocator: std.mem.Allocator, scanner: *PeekableScanner) !Struct {
        var fields = OrderedStringHashMap(Field).init(allocator);

        const name = try parseIdentifier(scanner);
        _ = try scanner.expect(.brace_left);
        while (true) {
            skipComments(scanner);
            // empty struct
            if (scanner.peek().kind == .brace_right) {
                break;
            }
            const field = try Field.parse(allocator, scanner);
            try fields.put(field.identifier, field.field);
            if (scanner.peek().kind == .brace_right) {
                break;
            }
        }
        _ = try scanner.expect(.brace_right);

        return Definition.Struct{
            .name = name,
            .fields = fields,
        };
    }

    fn parseUnion(allocator: std.mem.Allocator, scanner: *PeekableScanner) !Union {
        var fields = OrderedStringHashMap(Field).init(allocator);

        const name = try parseIdentifier(scanner);
        _ = try scanner.expect(.brace_left);
        while (true) {
            skipComments(scanner);
            // empty union
            if (scanner.peek().kind == .brace_right) {
                break;
            }
            const field = try Field.parse(allocator, scanner);
            try fields.put(field.identifier, field.field);
            if (scanner.peek().kind == .brace_right) {
                break;
            }
        }
        _ = try scanner.expect(.brace_right);

        return Definition.Union{
            .name = name,
            .fields = fields,
        };
    }
};

pub const Identifier = []const u8;

pub const Type = union(enum) {
    builtin: union(enum) {
        bool,
        byte,
        i8,
        i16,
        i32,
        i64,
        double,
        string,
        binary,
        uuid,
        map: struct { key: *const Type, value: *const Type },
        set: struct { element: *const Type },
        list: struct { element: *const Type },
    },
    custom: struct { name: []const u8 },

    fn parse(allocator: std.mem.Allocator, scanner: *PeekableScanner) !*Type {
        const ty = try allocator.create(Type);
        errdefer allocator.destroy(ty);

        const token = scanner.next();
        switch (token.kind) {
            .type_bool => {
                ty.* = .{ .builtin = .bool };
            },
            .type_byte => {
                ty.* = .{ .builtin = .byte };
            },
            .type_i8 => {
                ty.* = .{ .builtin = .i8 };
            },
            .type_i16 => {
                ty.* = .{ .builtin = .i16 };
            },
            .type_i32 => {
                ty.* = .{ .builtin = .i32 };
            },
            .type_i64 => {
                ty.* = .{ .builtin = .i64 };
            },
            .type_double => {
                ty.* = .{ .builtin = .double };
            },
            .type_string => {
                ty.* = .{ .builtin = .string };
            },
            .type_binary => {
                ty.* = .{ .builtin = .binary };
            },
            .type_uuid => {
                ty.* = .{ .builtin = .uuid };
            },
            .type_list => {
                _ = try scanner.expect(.angle_bracket_left);
                const elem = try Type.parse(allocator, scanner);
                errdefer elem.deinit(allocator);
                _ = try scanner.expect(.angle_bracket_right);
                ty.* = .{
                    .builtin = .{
                        .list = .{ .element = elem },
                    },
                };
            },
            .type_set => {
                _ = try scanner.expect(.angle_bracket_left);
                const elem = try Type.parse(allocator, scanner);
                errdefer elem.deinit(allocator);
                _ = try scanner.expect(.angle_bracket_right);
                ty.* = .{
                    .builtin = .{
                        .set = .{ .element = elem },
                    },
                };
            },
            .type_map => {
                _ = try scanner.expect(.angle_bracket_left);
                const key = try Type.parse(allocator, scanner);
                errdefer key.deinit(allocator);
                _ = try scanner.expect(.comma);
                const value = try Type.parse(allocator, scanner);
                errdefer value.deinit(allocator);
                _ = try scanner.expect(.angle_bracket_right);

                ty.* = .{
                    .builtin = .{
                        .map = .{ .key = key, .value = value },
                    },
                };
            },
            .identifier => {
                ty.* = .{
                    .custom = .{
                        .name = scanner.text(&token.range),
                    },
                };
            },
            else => return error.UnexpectedToken,
        }

        return ty;
    }

    pub fn deinit(self: *Type, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .builtin => |*b| switch (b.*) {
                .map => |*m| {
                    @constCast(m.key).deinit(allocator);
                    @constCast(m.value).deinit(allocator);
                },
                .set => |*s| {
                    @constCast(s.element).deinit(allocator);
                },
                .list => |*l| {
                    @constCast(l.element).deinit(allocator);
                },
                else => {},
            },
            .custom => {},
        }

        allocator.destroy(self);
    }
};

pub const ConstValue = union(enum) {
    // Only booleans are supported as constant values.
    bool: bool,

    fn parse(scanner: *PeekableScanner) !ConstValue {
        switch (scanner.next().kind) {
            .true => return ConstValue{ .bool = true },
            .false => return ConstValue{ .bool = false },
            else => return error.ConstValueNotSupported,
        }
    }
};

fn parseIdentifier(scanner: *PeekableScanner) ![]const u8 {
    const identifier = try scanner.expect(.identifier);
    return scanner.text(&identifier.range);
}

fn parseInt(comptime T: type, scanner: *PeekableScanner) !T {
    const num = try scanner.expect(.number_literal);
    return std.fmt.parseInt(T, scanner.text(&num.range), 10);
}

fn skipListSeperator(scanner: *PeekableScanner) void {
    _ = scanner.nextIf(.comma) orelse scanner.nextIf(.semicolon);
}

fn skipComments(scanner: *PeekableScanner) void {
    while (scanner.nextIf(.line_comment) orelse scanner.nextIf(.multiline_comment)) |_| {}
}

test {
    _ = PeekableScanner;
    _ = protocol_compact;
    _ = @import("./thrift/translate.zig");
}

test "enum" {
    var document = try expectParse(
        \\enum Foo {
        \\  BAR = 0;
        \\  BAZ = 1;
        \\}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"enum";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(2, foo.values.count());
    try std.testing.expectEqual(0, foo.values.get("BAR").?);
    try std.testing.expectEqual(1, foo.values.get("BAZ").?);
}

test "enum with implicit values" {
    {
        var document = try expectParse(
            \\enum Foo {
            \\  BAR;
            \\  BAZ;
            \\}
        );
        defer document.deinit();

        const foo = document.definitions.items[0].@"enum";
        try std.testing.expectEqual(0, foo.values.get("BAR").?);
        try std.testing.expectEqual(1, foo.values.get("BAZ").?);
    }

    {
        var document = try expectParse(
            \\enum Foo {
            \\  BAR;
            \\  BAZ = 2;
            \\}
        );
        defer document.deinit();

        const foo = document.definitions.items[0].@"enum";
        try std.testing.expectEqual(0, foo.values.get("BAR").?);
        try std.testing.expectEqual(2, foo.values.get("BAZ").?);
    }

    {
        var document = try expectParse(
            \\enum Foo {
            \\  BAR = 42;
            \\  BAZ = 2;
            \\}
        );
        defer document.deinit();

        const foo = document.definitions.items[0].@"enum";
        try std.testing.expectEqual(42, foo.values.get("BAR").?);
        try std.testing.expectEqual(2, foo.values.get("BAZ").?);
    }

    {
        var document = try expectParse(
            \\enum Foo {
            \\  BAR = 42;
            \\  BAZ;
            \\}
        );
        defer document.deinit();

        const foo = document.definitions.items[0].@"enum";
        try std.testing.expectEqual(42, foo.values.get("BAR").?);
        try std.testing.expectEqual(43, foo.values.get("BAZ").?);
    }

    {
        var document = try expectParse(
            \\enum Foo {
            \\  BAR;
            \\  BAZ = 7;
            \\  QUX;
            \\}
        );
        defer document.deinit();

        const foo = document.definitions.items[0].@"enum";
        try std.testing.expectEqual(0, foo.values.get("BAR").?);
        try std.testing.expectEqual(7, foo.values.get("BAZ").?);
        try std.testing.expectEqual(8, foo.values.get("QUX").?);
    }
}

test "struct" {
    var document = try expectParse(
        \\struct Foo {
        \\  1: optional i32 bar;
        \\  required Foo self;
        \\  list<i32> baz;
        \\}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"struct";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(3, foo.fields.count());
    {
        const field = foo.fields.get("bar").?;
        try std.testing.expectEqual(1, field.id.?);
        try std.testing.expectEqual(.optional, field.req);
        try std.testing.expectEqual(.i32, field.type.builtin);
    }
    {
        const field = foo.fields.get("self").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.required, field.req);
        try std.testing.expectEqualStrings("Foo", field.type.custom.name);
    }
    {
        const field = foo.fields.get("baz").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.default, field.req);
        try std.testing.expectEqualDeep(Type{ .builtin = .{ .list = .{ .element = &Type{ .builtin = .i32 } } } }, field.type.*);
    }
}

test "struct with default value" {
    var document = try expectParse(
        \\struct Foo {
        \\  1: optional bool foo = true;
        \\  2: bool bar = false;
        \\  3: bool baz;
        \\}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"struct";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(3, foo.fields.count());
    {
        const field = foo.fields.get("foo").?;
        try std.testing.expectEqual(1, field.id.?);
        try std.testing.expectEqual(.optional, field.req);
        try std.testing.expectEqual(.bool, field.type.builtin);
        try std.testing.expectEqual(ConstValue{ .bool = true }, field.default);
    }
    {
        const field = foo.fields.get("bar").?;
        try std.testing.expectEqual(2, field.id.?);
        try std.testing.expectEqual(.default, field.req);
        try std.testing.expectEqual(.bool, field.type.builtin);
        try std.testing.expectEqual(ConstValue{ .bool = false }, field.default);
    }
    {
        const field = foo.fields.get("baz").?;
        try std.testing.expectEqual(3, field.id.?);
        try std.testing.expectEqual(.default, field.req);
        try std.testing.expectEqual(.bool, field.type.builtin);
        try std.testing.expectEqual(null, field.default);
    }
}

test "empty struct" {
    var document = try expectParse(
        \\struct Foo {}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"struct";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(0, foo.fields.count());
}

test "union" {
    var document = try expectParse(
        \\union Foo {
        \\  1: optional i32 bar;
        \\  required Foo self;
        \\  list<i32> baz;
        \\}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"union";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(3, foo.fields.count());
    {
        const field = foo.fields.get("bar").?;
        try std.testing.expectEqual(1, field.id.?);
        try std.testing.expectEqual(.optional, field.req);
        try std.testing.expectEqual(.i32, field.type.builtin);
    }
    {
        const field = foo.fields.get("self").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.required, field.req);
        try std.testing.expectEqualStrings("Foo", field.type.custom.name);
    }
    {
        const field = foo.fields.get("baz").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.default, field.req);
        try std.testing.expectEqualDeep(Type{ .builtin = .{ .list = .{ .element = &Type{ .builtin = .i32 } } } }, field.type.*);
    }
}

test "headers" {
    var document = try expectParse(
        \\// This is a Thrift document.
        \\namespace java parzig
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.headers.items.len);
    try std.testing.expectEqual(0, document.definitions.items.len);

    const java_ns = document.headers.items[0].namespace;
    try std.testing.expectEqualStrings("parzig", java_ns.identifier);
    try std.testing.expectEqual(.java, java_ns.scope);
}

test "comments" {
    var document = try expectParse(
        \\/**
        \\ * comment for Foo
        \\ */
        \\struct Foo {
        \\  // comment
        \\  1: optional i32 bar; // comment
        \\  /**
        \\   * comment
        \\   * more comment
        \\   */
        \\  required Foo self; # even more comment
        \\  list<i32> baz;
        \\}
    );
    defer document.deinit();

    try std.testing.expectEqual(1, document.definitions.items.len);
    const foo = document.definitions.items[0].@"struct";
    try std.testing.expectEqualStrings("Foo", foo.name);
    try std.testing.expectEqual(3, foo.fields.count());
    {
        const field = foo.fields.get("bar").?;
        try std.testing.expectEqual(1, field.id.?);
        try std.testing.expectEqual(.optional, field.req);
        try std.testing.expectEqual(.i32, field.type.builtin);
    }
    {
        const field = foo.fields.get("self").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.required, field.req);
        try std.testing.expectEqualStrings("Foo", field.type.custom.name);
    }
    {
        const field = foo.fields.get("baz").?;
        try std.testing.expectEqual(null, field.id);
        try std.testing.expectEqual(.default, field.req);
        try std.testing.expectEqualDeep(Type{ .builtin = .{ .list = .{ .element = &Type{ .builtin = .i32 } } } }, field.type.*);
    }
}

test "type" {
    try expectType("bool", Type{ .builtin = .bool });
    try expectType("byte", Type{ .builtin = .byte });
    try expectType("i8", Type{ .builtin = .i8 });
    try expectType("i16", Type{ .builtin = .i16 });
    try expectType("i32", Type{ .builtin = .i32 });
    try expectType("i64", Type{ .builtin = .i64 });
    try expectType("double", Type{ .builtin = .double });
    try expectType("string", Type{ .builtin = .string });
    try expectType("binary", Type{ .builtin = .binary });
    try expectType("uuid", Type{ .builtin = .uuid });

    try expectType("list<i32>", Type{ .builtin = .{ .list = .{ .element = &Type{ .builtin = .i32 } } } });
    try expectType("set<double>", Type{ .builtin = .{ .set = .{ .element = &Type{ .builtin = .double } } } });

    try expectType("map<uuid, byte>", Type{ .builtin = .{
        .map = .{ .key = &Type{ .builtin = .uuid }, .value = &Type{ .builtin = .byte } },
    } });

    try expectType("Foo", Type{ .custom = .{ .name = "Foo" } });
    try expectType("list<Foo>", Type{ .builtin = .{ .list = .{ .element = &Type{ .custom = .{ .name = "Foo" } } } } });

    try expectType("list<map<Bar, set<Baz>>>", Type{ .builtin = .{
        .list = .{
            .element = &Type{
                .builtin = .{
                    .map = .{
                        .key = &Type{ .custom = .{ .name = "Bar" } },
                        .value = &Type{ .builtin = .{ .set = .{ .element = &Type{ .custom = .{ .name = "Baz" } } } } },
                    },
                },
            },
        },
    } });
}

test "type parse errors" {
    // Missing closing bracket for list
    try expectTypeParseError("list<i32", error.UnexpectedToken);
    // Missing closing bracket for set
    try expectTypeParseError("set<i32", error.UnexpectedToken);
    // Missing closing bracket for map
    try expectTypeParseError("map<i32, i64", error.UnexpectedToken);
    // Missing comma in map
    try expectTypeParseError("map<i32 i64>", error.UnexpectedToken);
    // Nested type parse error
    try expectTypeParseError("list<map<i32, i64>", error.UnexpectedToken);
    try expectTypeParseError("map<list<i32, i64>", error.UnexpectedToken);
}

test "struct parse errors" {
    // Missing field identifier after type
    try std.testing.expectError(error.UnexpectedToken, expectParse(
        \\struct Foo {
        \\  i32
        \\}
    ));

    // Invalid default value
    try std.testing.expectError(error.ConstValueNotSupported, expectParse(
        \\struct Foo {
        \\  i32 bar = 42
        \\}
    ));
}

fn expectParse(source: []const u8) !Document {
    return Document.init(std.testing.allocator, source);
}

fn expectType(source: []const u8, expected_ty: Type) !void {
    var scanner = PeekableScanner.init(source);
    const ty = try Type.parse(std.testing.allocator, &scanner);
    defer ty.deinit(std.testing.allocator);
    try std.testing.expectEqualDeep(expected_ty, ty.*);
}

fn expectTypeParseError(source: []const u8, expected_err: anyerror) !void {
    var scanner = PeekableScanner.init(source);
    const result = Type.parse(std.testing.allocator, &scanner);
    try std.testing.expectError(expected_err, result);
}
