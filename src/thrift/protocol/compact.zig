const std = @import("std");
const io = @import("../io.zig");

const Reader = io.Buf.Reader;

const testing = std.testing;
const Io = io.Io;

fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |o| o.child,
        else => T,
    };
}

fn readZigZagInt(comptime T: type, reader: *Reader) !T {
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    const n = try reader.takeLeb128(U);
    return @bitCast(@as(U, n >> 1) ^ (-%@as(U, @intCast(n & 1))));
}

pub fn ListReader(comptime E: type) type {
    return struct {
        pub fn read(arena: std.mem.Allocator, reader: *Reader) ![]E {
            const header = try reader.takeByte();
            const size_short: u4 = @truncate(header >> 4);
            const size: usize = @intCast(if (size_short == 0b1111)
                try reader.takeLeb128(u32)
            else
                size_short);

            const elem_type_id: u4 = @truncate(header);

            var result = try arena.alloc(E, size);
            for (0..size) |i| {
                switch (elem_type_id) {
                    1, 2 => {
                        if (E != bool) {
                            return error.UnexpectedBoolean;
                        }

                        result[i] = elem_type_id == 1;
                    },
                    3 => {
                        if (E != i8) {
                            return error.UnexpectedI8;
                        }

                        result[i] = try readZigZagInt(i8, reader);
                    },
                    4 => {
                        if (E != i16) {
                            return error.UnexpectedI16;
                        }

                        result[i] = try readZigZagInt(i16, reader);
                    },
                    5 => {
                        if (E != i32) {
                            return error.UnexpectedI32;
                        }

                        result[i] = try readZigZagInt(i32, reader);
                    },
                    6 => {
                        if (E != i64) {
                            return error.UnexpectedI64;
                        }

                        result[i] = try readZigZagInt(i64, reader);
                    },
                    7 => return error.DoubleNotSupported,
                    8 => {
                        if (E != []const u8) {
                            return error.UnexpectedBinary;
                        }

                        const length = try reader.takeLeb128(u64);
                        result[i] = try reader.takeBytes(arena, length);
                    },
                    9 => {
                        if (@typeInfo(E) != .pointer) {
                            return error.UnexpectedList;
                        }

                        const elem_type = @typeInfo(E).pointer.child;
                        result[i] = try ListReader(elem_type).read(arena, reader);
                    },
                    10 => return error.SetNotSupported,
                    11 => return error.MapNotSupported,
                    12 => {
                        if (@typeInfo(E) != .@"struct" and @typeInfo(E) != .@"union") {
                            return error.UnexpectedStruct;
                        }

                        result[i] = try StructReader(E).read(arena, reader);
                    },
                    13 => return error.UuidNotSupported,
                    else => return error.InvalidFieldType,
                }
            }

            return result;
        }
    };
}

pub fn StructReader(comptime T: type) type {
    const STOP = 0;

    const FieldType = enum(u4) {
        stop = 0,
        boolean_true = 1,
        boolean_false = 2,
        i8 = 3,
        i16 = 4,
        i32 = 5,
        i64 = 6,
        double = 7,
        binary = 8,
        list = 9,
        set = 10,
        map = 11,
        @"struct" = 12,
        uuid = 13,

        fn fromEnum(id: u4) !@This() {
            if (id > 13) {
                return error.InvalidFieldType;
            }
            return @enumFromInt(id);
        }
    };

    const fields = switch (@typeInfo(T)) {
        .@"struct" => |*s| s.fields,
        .@"union" => |*u| u.fields,
        else => @compileError("Expected struct or union"),
    };

    const max_field_id = blk: {
        comptime var max_field_id = 0;
        @setEvalBranchQuota(3000);
        inline for (fields, 0..) |_, i| {
            const field_id = comptime T.fieldId(@enumFromInt(i));
            if (field_id == 0) {
                @compileError("Field id must be > 0");
            }
            max_field_id = @max(max_field_id, field_id);
        }
        break :blk max_field_id;
    };

    const field_types = blk: {
        var field_types: [max_field_id]type = @splat(void);
        @setEvalBranchQuota(3000);
        inline for (fields, 0..) |field, i| {
            const field_id = comptime T.fieldId(@enumFromInt(i));
            field_types[field_id - 1] = unwrapOptional(field.type);
        }
        break :blk field_types;
    };

    const field_names = blk: {
        var field_names: [max_field_id][]const u8 = undefined;
        @setEvalBranchQuota(5000);
        inline for (fields, 0..) |field, i| {
            const field_id = comptime T.fieldId(@enumFromInt(i));
            field_names[field_id - 1] = field.name;
        }
        break :blk field_names;
    };

    const Skipper = struct {
        fn skipFieldData(field_type_id: u4, reader_ptr: *Reader, arena: std.mem.Allocator) !void {
            const field_type = try FieldType.fromEnum(field_type_id);

            switch (field_type) {
                .stop => {},
                .boolean_true, .boolean_false => {},
                .i8, .i16, .i32, .i64 => _ = try reader_ptr.takeLeb128(u64),
                .double => return error.DoubleNotSupported,
                .binary => {
                    const length = try reader_ptr.takeLeb128(u64);
                    try reader_ptr.skipBytes(length);
                },
                .list => {
                    const header = try reader_ptr.takeByte();
                    const size_short: u4 = @truncate(header >> 4);
                    const size: usize = @intCast(if (size_short == 0b1111)
                        try reader_ptr.takeLeb128(u32)
                    else
                        size_short);
                    const elem_type_id: u4 = @truncate(header);

                    for (0..size) |_| {
                        try skipFieldData(elem_type_id, reader_ptr, arena);
                    }
                },
                .set => return error.SetNotSupported,
                .map => return error.MapNotSupported,
                .@"struct" => {
                    var last_field_id: i16 = 0;
                    while (true) {
                        const header = try reader_ptr.takeByte();
                        if (header == 0) break;

                        const field_id_delta: u4 = @truncate(header >> 4);
                        const field_id = if (field_id_delta == 0)
                            try readZigZagInt(i16, reader_ptr)
                        else
                            last_field_id + field_id_delta;

                        if (field_id == 0) break;
                        last_field_id = field_id;

                        const nested_field_type_id: u4 = @truncate(header);
                        if (nested_field_type_id == 0) break;

                        try skipFieldData(nested_field_type_id, reader_ptr, arena);
                    }
                },
                .uuid => return error.UuidNotSupported,
            }
        }
    };

    return struct {
        pub fn read(arena: std.mem.Allocator, reader: *Reader) !T {
            var fields_set = std.mem.zeroes([max_field_id]bool);
            var result: T = undefined;
            var last_field_id: i16 = 0;
            while (true) {
                const header = try reader.takeByte();
                if (header == STOP) {
                    break;
                }

                const field_id_delta: u4 = @truncate(header >> 4);
                const field_id = if (field_id_delta == 0)
                    try readZigZagInt(i16, reader)
                else
                    last_field_id + field_id_delta;

                if (field_id <= 0) {
                    return error.InvalidFieldId;
                }

                last_field_id = field_id;

                const field_type_id: u4 = @truncate(header);

                if (field_id > max_field_id) {
                    try Skipper.skipFieldData(field_type_id, reader, arena);
                    continue;
                }

                const field_idx = field_id - 1;
                if (field_types[field_idx] == void) {
                    try Skipper.skipFieldData(field_type_id, reader, arena);
                    continue;
                }

                fields_set[field_idx] = true;

                inline for (fields, 0..) |field, i| {
                    if (comptime T.fieldId(@enumFromInt(i)) == field_id) {
                        const FieldT = unwrapOptional(field.type);

                        switch (field_type_id) {
                            1, 2 => {
                                if (FieldT != bool) {
                                    return error.UnexpectedBoolean;
                                }

                                if (field.type == ?bool) {
                                    @field(result, field.name) = field_type_id == 1;
                                } else {
                                    @field(result, field.name) = field_type_id == 1;
                                }
                            },
                            3 => {
                                if (FieldT != i8) {
                                    return error.UnexpectedI8;
                                }

                                const value = try readZigZagInt(i8, reader);
                                if (field.type == ?i8) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            4 => {
                                if (FieldT != i16) {
                                    return error.UnexpectedI16;
                                }

                                const value = try readZigZagInt(i16, reader);
                                if (field.type == ?i16) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            5 => {
                                if (FieldT != i32) {
                                    return error.UnexpectedI32;
                                }

                                const value = try readZigZagInt(i32, reader);
                                if (field.type == ?i32) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            6 => {
                                if (FieldT != i64) {
                                    return error.UnexpectedI64;
                                }

                                const value = try readZigZagInt(i64, reader);
                                if (field.type == ?i64) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            7 => return error.DoubleNotSupported,
                            8 => {
                                if (FieldT != []const u8) {
                                    return error.UnexpectedBinary;
                                }

                                const length = try reader.takeLeb128(u64);
                                const value = try reader.takeBytes(arena, length);
                                if (field.type == ?[]const u8) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            9 => {
                                const field_type_info = @typeInfo(FieldT);
                                if (field_type_info != .pointer) {
                                    return error.UnexpectedList;
                                }

                                const elem_type = field_type_info.pointer.child;
                                const value = try ListReader(elem_type).read(arena, reader);
                                if (@typeInfo(field.type) == .optional) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            10 => return error.SetNotSupported,
                            11 => return error.MapNotSupported,
                            12 => {
                                if (@typeInfo(FieldT) != .@"struct" and @typeInfo(FieldT) != .@"union") {
                                    return error.UnexpectedStruct;
                                }

                                const value = try StructReader(FieldT).read(arena, reader);
                                if (@typeInfo(field.type) == .optional) {
                                    @field(result, field.name) = value;
                                } else {
                                    @field(result, field.name) = value;
                                }
                            },
                            13 => return error.UuidNotSupported,
                            else => return error.InvalidFieldType,
                        }
                    }
                }
            }

            inline for (fields, 0..) |field, i| {
                const field_idx = T.fieldId(@enumFromInt(i)) - 1;
                if (!fields_set[field_idx]) {
                    if (@typeInfo(field.type) != .optional) {
                        std.debug.print("missing required field: {s}\n", .{field.name});
                        return error.MissingRequiredField;
                    }

                    @field(result, field.name) = null;
                }
            }

            return result;
        }
    };
}

test "read struct" {
    var reader_buf: [1024]u8 = undefined;
    const data = [_]u8{
        0x15, // field 1, type i32
        0x96, 0x01, // 150
        0x00, // stop
    };

    const TestStruct = struct {
        id: i32,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .id => 1,
            };
        }
    };

    var reader = io.Buf.from(&data).reader(io, &reader_buf);
    const result = try StructReader(TestStruct).read(testing.allocator, &reader);
    try testing.expectEqual(150, result.id);
}

test "read struct with optional field" {
    var reader_buf: [1024]u8 = undefined;
    const data = [_]u8{
        0x15, // field 1, type i32
        0x96, 0x01, // 150
        0x00, // stop
    };

    const TestStruct = struct {
        id: i32,
        name: ?[]const u8,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .id => 1,
                .name => 2,
            };
        }
    };

    var reader = io.Buf.from(&data).reader(io, &reader_buf);
    const result = try StructReader(TestStruct).read(testing.allocator, &reader);
    try testing.expectEqual(150, result.id);
    try testing.expectEqual(null, result.name);
}

test "read struct with list" {
    var reader_buf: [1024]u8 = undefined;
    const data = [_]u8{
        0x19, // field 1, type list
        0x35, // size 3, element type i32
        0x96, 0x01, // 150
        0xd2, 0x01, // 210
        0x02, // 2
        0x00, // stop
    };

    const TestStruct = struct {
        values: []i32,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .values => 1,
            };
        }
    };

    var reader = io.Buf.from(&data).reader(io, &reader_buf);
    const result = try StructReader(TestStruct).read(testing.allocator, &reader);
    defer testing.allocator.free(result.values);
    try testing.expectEqualSlices(i32, &[_]i32{ 150, 210, 2 }, result.values);
}

test "read nested struct" {
    var reader_buf: [1024]u8 = undefined;
    const data = [_]u8{
        0x1c, // field 1, type struct
        0x15, // field 1, type i32
        0x96, 0x01, // 150
        0x00, // stop (inner struct)
        0x00, // stop (outer struct)
    };

    const Inner = struct {
        value: i32,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .value => 1,
            };
        }
    };

    const Outer = struct {
        inner: Inner,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .inner => 1,
            };
        }
    };

    var reader = io.Buf.from(&data).reader(io, &reader_buf);
    const result = try StructReader(Outer).read(testing.allocator, &reader);
    try testing.expectEqual(150, result.inner.value);
}

test "skip unknown field" {
    var reader_buf: [1024]u8 = undefined;
    const data = [_]u8{
        0x15, // field 1, type i32
        0x96, 0x01, // 150
        0x25, // field 2, type i32 (unknown field)
        0x64, // 100
        0x35, // field 3, type i32
        0xc8, 0x01, // 200
        0x00, // stop
    };

    const TestStruct = struct {
        id: i32,
        value: i32,

        fn fieldId(field: std.meta.FieldEnum(@This())) i16 {
            return switch (field) {
                .id => 1,
                .value => 3,
            };
        }
    };

    var reader = io.Buf.from(&data).reader(io, &reader_buf);
    const result = try StructReader(TestStruct).read(testing.allocator, &reader);
    try testing.expectEqual(150, result.id);
    try testing.expectEqual(200, result.value);
}
