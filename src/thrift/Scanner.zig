//! A scanner for Thrift IDL, heavily inspired by `std.zig.Tokenizer`.

const std = @import("std");

pub const Token = struct {
    kind: Kind,
    range: Range,

    pub const Kind = enum {
        include,
        cpp_include,

        line_comment,
        multiline_comment,

        namespace,
        namespace_scope_glob,
        namespace_scope_c_glib,
        namespace_scope_cpp,
        namespace_scope_delphi,
        namespace_scope_haxe,
        namespace_scope_go,
        namespace_scope_java,
        namespace_scope_js,
        namespace_scope_lua,
        namespace_scope_netstd,
        namespace_scope_perl,
        namespace_scope_php,
        namespace_scope_py,
        namespace_scope_py_twisted,
        namespace_scope_rb,
        namespace_scope_st,
        namespace_scope_xsd,

        type_bool,
        type_byte,
        type_i8,
        type_i16,
        type_i32,
        type_i64,
        type_double,
        type_string,
        type_binary,
        type_uuid,
        type_map,
        type_set,
        type_list,

        @"const",
        typedef,
        @"enum",
        @"struct",
        @"union",
        exception,
        service,

        equal,
        comma,
        colon,
        semicolon,
        angle_bracket_left,
        angle_bracket_right,
        brace_left,
        brace_right,
        bracket_left,
        bracket_right,

        required,
        optional,

        literal,
        number_literal,
        identifier,
        true,
        false,

        invalid,
        end_of_document,
    };

    pub const Range = struct {
        start: usize,
        end: usize,
    };

    pub const keywords = std.StaticStringMap(Kind).initComptime(.{
        .{ "include", .include },
        .{ "cpp_include", .cpp_include },
        .{ "namespace", .namespace },

        .{ "*", .namespace_scope_glob },
        .{ "c_glib", .namespace_scope_c_glib },
        .{ "cpp", .namespace_scope_cpp },
        .{ "delphi", .namespace_scope_delphi },
        .{ "haxe", .namespace_scope_haxe },
        .{ "go", .namespace_scope_go },
        .{ "java", .namespace_scope_java },
        .{ "js", .namespace_scope_js },
        .{ "lua", .namespace_scope_lua },
        .{ "netstd", .namespace_scope_netstd },
        .{ "perl", .namespace_scope_perl },
        .{ "php", .namespace_scope_php },
        .{ "py", .namespace_scope_py },
        .{ "py.twisted", .namespace_scope_py_twisted },
        .{ "rb", .namespace_scope_rb },
        .{ "st", .namespace_scope_st },
        .{ "xsd", .namespace_scope_xsd },

        .{ "bool", .type_bool },
        .{ "byte", .type_byte },
        .{ "i8", .type_i8 },
        .{ "i16", .type_i16 },
        .{ "i32", .type_i32 },
        .{ "i64", .type_i64 },
        .{ "double", .type_double },
        .{ "string", .type_string },
        .{ "binary", .type_binary },
        .{ "uuid", .type_uuid },
        .{ "map", .type_map },
        .{ "set", .type_set },
        .{ "list", .type_list },

        .{ "const", .@"const" },
        .{ "typedef", .typedef },
        .{ "enum", .@"enum" },
        .{ "struct", .@"struct" },
        .{ "union", .@"union" },
        .{ "exception", .exception },
        .{ "service", .service },

        .{ "true", .true },
        .{ "false", .false },

        .{ "required", .required },
        .{ "optional", .optional },
    });

    pub fn eql(self: *const Token, other: *const Token) bool {
        return self == other or
            (self.kind == other.kind and
                self.range.start == other.range.start and
                self.range.end == other.range.end);
    }
};

const Scanner = @This();

source: []const u8,
index: usize = 0,

pub fn init(source: []const u8) Scanner {
    return Scanner{ .source = source };
}

pub fn text(self: *Scanner, range: *const Token.Range) []const u8 {
    return self.source[range.start..range.end];
}

pub fn next(self: *Scanner) Token {
    var state: enum {
        start,
        literal_single_quote,
        literal_double_quote,
        number_literal,
        identifier,
        comment_start,
        line_comment,
        multiline_comment,
        multiline_comment_end,
    } = .start;

    var token = Token{
        .kind = .end_of_document,
        .range = .{ .start = self.index, .end = undefined },
    };

    while (self.source.len > self.index) : (self.index += 1) {
        const c = self.source[self.index];

        switch (state) {
            .start => switch (c) {
                ' ', '\n', '\t', '\r' => {
                    token.range.start = self.index + 1;
                },
                '\'' => {
                    state = .literal_single_quote;
                    token.kind = .literal;
                },
                '"' => {
                    state = .literal_double_quote;
                    token.kind = .literal;
                },
                'a'...'z', 'A'...'Z', '_' => {
                    state = .identifier;
                    token.kind = .identifier;
                },
                '0'...'9', '+', '-' => {
                    state = .number_literal;
                    token.kind = .number_literal;
                },
                '#' => {
                    state = .line_comment;
                    token.kind = .line_comment;
                },
                '/' => {
                    state = .comment_start;
                    token.kind = .invalid;
                },
                '*' => {
                    token.kind = .namespace_scope_glob;
                    self.index += 1;
                    break;
                },
                '=' => {
                    token.kind = .equal;
                    self.index += 1;
                    break;
                },
                ',' => {
                    token.kind = .comma;
                    self.index += 1;
                    break;
                },
                ';' => {
                    token.kind = .semicolon;
                    self.index += 1;
                    break;
                },
                ':' => {
                    token.kind = .colon;
                    self.index += 1;
                    break;
                },
                '<' => {
                    token.kind = .angle_bracket_left;
                    self.index += 1;
                    break;
                },
                '>' => {
                    token.kind = .angle_bracket_right;
                    self.index += 1;
                    break;
                },
                '{' => {
                    token.kind = .brace_left;
                    self.index += 1;
                    break;
                },
                '}' => {
                    token.kind = .brace_right;
                    self.index += 1;
                    break;
                },
                '[' => {
                    token.kind = .bracket_left;
                    self.index += 1;
                    break;
                },
                ']' => {
                    token.kind = .bracket_right;
                    self.index += 1;
                    break;
                },
                else => break,
            },
            .identifier => switch (c) {
                'a'...'z', 'A'...'Z', '0'...'9', '.', '_' => {},
                else => break,
            },
            .literal_single_quote => switch (c) {
                '\'' => {
                    self.index += 1;
                    break;
                },
                else => {},
            },
            .literal_double_quote => switch (c) {
                '"' => {
                    self.index += 1;
                    break;
                },
                else => {},
            },
            .number_literal => switch (c) {
                '0'...'9', '.', '-', '+', 'E', 'e' => {},
                else => break,
            },
            .comment_start => switch (c) {
                '*' => {
                    state = .multiline_comment;
                },
                '/' => {
                    state = .line_comment;
                    token.kind = .line_comment;
                },
                else => break,
            },
            .line_comment => switch (c) {
                '\n' => {
                    self.index += 1;
                    break;
                },
                else => {},
            },
            .multiline_comment => switch (c) {
                '*' => {
                    state = .multiline_comment_end;
                },
                else => {},
            },
            .multiline_comment_end => switch (c) {
                '/' => {
                    token.kind = .multiline_comment;
                    self.index += 1;
                    break;
                },
                '*' => {
                    state = .multiline_comment_end;
                },
                else => {
                    state = .multiline_comment;
                },
            },
        }
    }

    token.range.end = self.index;

    if (token.kind == .identifier) {
        const identifier = self.text(&token.range);
        if (Token.keywords.get(identifier)) |keyword| {
            token.kind = keyword;
        }
    }

    return token;
}

test Scanner {
    try expectTokensWithTexts(
        \\namespace * parzig
        \\
        \\struct Foo {
        \\  1: string bar,
        \\  2: i32 baz
        \\}
    , &.{
        .{ .namespace, "namespace" },
        .{ .namespace_scope_glob, "*" },
        .{ .identifier, "parzig" },
        .{ .@"struct", "struct" },
        .{ .identifier, "Foo" },
        .{ .brace_left, "{" },
        .{ .number_literal, "1" },
        .{ .colon, ":" },
        .{ .type_string, "string" },
        .{ .identifier, "bar" },
        .{ .comma, "," },
        .{ .number_literal, "2" },
        .{ .colon, ":" },
        .{ .type_i32, "i32" },
        .{ .identifier, "baz" },
        .{ .brace_right, "}" },
    });
}

test "thrift include" {
    try expectTokens(
        \\include "foo"
        \\include 'bar'
    , &.{
        .include,
        .literal,
        .include,
        .literal,
    });
}

test "cpp include" {
    try expectTokens(
        \\cpp_include "foo"
        \\cpp_include 'bar'
    , &.{
        .cpp_include,
        .literal,
        .cpp_include,
        .literal,
    });
}

test "namespace" {
    try expectTokens(
        \\namespace * foo
        \\namespace cpp bar
        \\namespace java com.foo.bar
    , &.{
        .namespace,
        .namespace_scope_glob,
        .identifier,
        .namespace,
        .namespace_scope_cpp,
        .identifier,
        .namespace,
        .namespace_scope_java,
        .identifier,
    });
}

test "const" {
    try expectTokens(
        \\const i64 secret = 42
    , &.{
        .@"const",
        .type_i64,
        .identifier,
        .equal,
        .number_literal,
    });
    try expectTokens(
        \\const list<binary> values = [ foo, "bar"; -42.0 ];
    , &.{
        .@"const",
        .type_list,
        .angle_bracket_left,
        .type_binary,
        .angle_bracket_right,
        .identifier,
        .equal,
        .bracket_left,
        .identifier,
        .comma,
        .literal,
        .semicolon,
        .number_literal,
        .bracket_right,
        .semicolon,
    });
    try expectTokens(
        \\const map<uuid, foo> identities = { bar: "baz"; },
    , &.{
        .@"const",
        .type_map,
        .angle_bracket_left,
        .type_uuid,
        .comma,
        .identifier,
        .angle_bracket_right,
        .identifier,
        .equal,
        .brace_left,
        .identifier,
        .colon,
        .literal,
        .semicolon,
        .brace_right,
        .comma,
    });
}

test "typedef" {
    try expectTokens(
        \\typedef i32 foo
    , &.{
        .typedef,
        .type_i32,
        .identifier,
    });
}

test "enum" {
    try expectTokens(
        \\enum foo {}
    , &.{
        .@"enum",
        .identifier,
        .brace_left,
        .brace_right,
    });
    try expectTokens(
        \\enum foo {
        \\  bar,
        \\  baz;
        \\}
    , &.{
        .@"enum",
        .identifier,
        .brace_left,
        .identifier,
        .comma,
        .identifier,
        .semicolon,
        .brace_right,
    });
    try expectTokens(
        \\enum foo {
        \\  bar = 1,
        \\}
    , &.{
        .@"enum",
        .identifier,
        .brace_left,
        .identifier,
        .equal,
        .number_literal,
        .comma,
        .brace_right,
    });
}

test "struct" {
    try expectTokens(
        \\struct foo {}
    , &.{
        .@"struct",
        .identifier,
        .brace_left,
        .brace_right,
    });
    try expectTokens(
        \\struct foo {
        \\  1: required i32 bar;
        \\  optional i8 baz = 42;
        \\}
    , &.{
        .@"struct",
        .identifier,
        .brace_left,
        .number_literal,
        .colon,
        .required,
        .type_i32,
        .identifier,
        .semicolon,
        .optional,
        .type_i8,
        .identifier,
        .equal,
        .number_literal,
        .semicolon,
        .brace_right,
    });
}

test "literal" {
    try expectTokens(
        \\"hello" 'world'
    , &.{
        .literal,
        .literal,
    });
}

test "number literal" {
    try expectTokens(
        \\42 42.0 -42 +42.0 42E0 42.0e-9
    , &.{
        .number_literal,
        .number_literal,
        .number_literal,
        .number_literal,
        .number_literal,
        .number_literal,
    });
}

test "identifier" {
    try expectTokens(
        \\foo bar1 _bar qux. foo.bar_
    , &.{
        .identifier,
        .identifier,
        .identifier,
        .identifier,
        .identifier,
    });
}

test "bool" {
    try expectTokens(
        \\true false
    , &.{
        .true,
        .false,
    });
}

test "comments" {
    try expectTokens(
        \\# this is a line comment
        \\# also this
    , &.{
        .line_comment,
        .line_comment,
    });
    try expectTokens(
        \\// this is a line comment
        \\// also this
    , &.{
        .line_comment,
        .line_comment,
    });
    try expectTokens(
        \\/**
        \\ * this is a
        \\ * multiline comment
        \\ */
    , &.{
        .multiline_comment,
    });
    try expectTokens(
        \\/**
        \\ * this is a
        \\ * multiline comment
        \\ **/
    , &.{
        .multiline_comment,
    });
}

test "empty document" {
    try expectTokens("", &.{});
}

fn expectTokens(source: []const u8, expected_tokens: []const Token.Kind) !void {
    var scanner = Scanner.init(source);

    for (expected_tokens) |expected| {
        try std.testing.expectEqual(expected, scanner.next().kind);
    }

    const token = scanner.next();
    try std.testing.expectEqual(.end_of_document, token.kind);
    try std.testing.expectEqual(source.len, token.range.start);
    try std.testing.expectEqual(source.len, token.range.end);
}

fn expectTokensWithTexts(source: []const u8, expected_tokens_and_texts: []const struct { Token.Kind, []const u8 }) !void {
    var scanner = Scanner.init(source);

    for (expected_tokens_and_texts) |expected| {
        const token = scanner.next();
        try std.testing.expectEqual(expected[0], token.kind);
        try std.testing.expectEqualStrings(expected[1], scanner.text(&token.range));
    }

    const token = scanner.next();
    try std.testing.expectEqual(.end_of_document, token.kind);
    try std.testing.expectEqual(source.len, token.range.start);
    try std.testing.expectEqual(source.len, token.range.end);
}
