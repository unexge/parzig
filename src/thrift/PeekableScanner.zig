const std = @import("std");

const Scanner = @import("Scanner.zig");

const PeekableScanner = @This();
pub const Token = Scanner.Token;

scanner: Scanner,
head: ?Token = null,

pub fn init(source: []const u8) PeekableScanner {
    return PeekableScanner{ .scanner = Scanner.init(source) };
}

pub fn text(self: *PeekableScanner, range: *const Token.Range) []const u8 {
    return self.scanner.text(range);
}

pub fn next(self: *PeekableScanner) Token {
    if (self.head) |token| {
        self.head = null;
        return token;
    }

    return self.scanner.next();
}

pub fn peek(self: *PeekableScanner) Token {
    if (self.head) |token| {
        return token;
    }

    const token = self.scanner.next();
    self.head = token;
    return token;
}

pub fn expect(self: *PeekableScanner, kind: Token.Kind) error{UnexpectedToken}!Token {
    const token = self.next();
    if (token.kind != kind) {
        return error.UnexpectedToken;
    }
    return token;
}

pub fn nextIf(self: *PeekableScanner, kind: Token.Kind) ?Token {
    const peeked = self.peek();
    if (peeked.kind != kind) {
        return null;
    }
    const token = self.next();
    std.debug.assert(token.eql(&peeked));
    return token;
}

test {
    _ = Scanner;
}

test PeekableScanner {
    var scanner = PeekableScanner.init(
        \\namespace * parzig
        \\
        \\struct Foo {
        \\  1: string bar,
        \\  2: i32 baz
        \\}
    );

    try std.testing.expectEqual(.namespace, scanner.peek().kind);
    try std.testing.expectEqual(.namespace, scanner.peek().kind);
    try std.testing.expectEqual(.namespace, scanner.next().kind);
    try std.testing.expectEqual(.namespace_scope_glob, scanner.next().kind);

    try std.testing.expectEqual(null, scanner.nextIf(.end_of_document));
    try std.testing.expectEqual(.identifier, scanner.next().kind);

    try std.testing.expectEqual(.@"struct", scanner.nextIf(.@"struct").?.kind);

    try std.testing.expectEqual(.identifier, scanner.peek().kind);
    try std.testing.expectEqual(.identifier, (try scanner.expect(.identifier)).kind);
    try std.testing.expectError(error.UnexpectedToken, scanner.expect(.end_of_document));
}
