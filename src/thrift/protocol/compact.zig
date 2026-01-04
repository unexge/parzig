const std = @import("std");
const Reader = std.Io.Reader;

pub fn readZigZagInt(comptime T: type, reader: *Reader) !T {
    if (@sizeOf(T) > @sizeOf(i64)) {
        @compileError("Maximum 64-bit integers are supported");
    }

    const unsigned = try reader.takeLeb128(u64);
    const decoded = if (unsigned & 1 == 0)
        @as(i128, @intCast(unsigned >> 1))
    else
        ~@as(i128, @intCast(unsigned >> 1));

    if (decoded > std.math.maxInt(T) or decoded < std.math.minInt(T)) {
        return error.Overflow;
    }

    return @intCast(decoded);
}

pub fn readBinary(arena: std.mem.Allocator, reader: *Reader) ![]const u8 {
    const length = try reader.takeLeb128(u64);
    return reader.readAlloc(arena, length);
}

fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |*o| o.child,
        else => T,
    };
}

pub fn ListReader(comptime E: type) type {
    const LONG_FORM: u4 = 0b1111;

    const ElemType = enum(u4) {
        bool = 2,
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
            if (id < 2 or id > 13) {
                return error.InvalidElemType;
            }
            return @enumFromInt(id);
        }
    };

    return struct {
        pub fn read(arena: std.mem.Allocator, reader: *Reader) ![]E {
            const header = try reader.takeByte();

            const size_short: u4 = @truncate(header >> 4);
            const size: usize = @intCast(if (size_short == LONG_FORM)
                try reader.takeLeb128(u32)
            else
                size_short);

            const elem_type = try ElemType.fromEnum(@truncate(header));

            const result = try arena.alloc(E, size);

            for (0..size) |i| {
                switch (elem_type) {
                    .bool => {
                        if (E != bool) {
                            return error.UnexpectedBool;
                        }
                        result[i] = readZigZagInt(i8, reader) == 1;
                    },
                    .i8, .i16, .i32, .i64 => {
                        if (@typeInfo(E) != .int and @typeInfo(E) != .@"enum") {
                            return error.UnexpectedInt;
                        }

                        result[i] = if (@typeInfo(E) == .@"enum")
                            @enumFromInt(try readZigZagInt(i32, reader))
                        else
                            try readZigZagInt(E, reader);
                    },
                    .double => return error.DoubleNotSupported,
                    .binary => {
                        switch (@typeInfo(E)) {
                            .pointer => |*p| {
                                if (p.size != .slice or !p.is_const or p.child != u8) {
                                    return error.UnexpectedBinary;
                                }
                            },
                            else => return error.UnexpectedBinary,
                        }

                        result[i] = try readBinary(arena, reader);
                    },
                    .list => {
                        const inner_elem_type = switch (@typeInfo(E)) {
                            .pointer => |*p| blk: {
                                if (p.size != .slice) {
                                    return error.UnexpectedList;
                                }
                                break :blk p.child;
                            },
                            else => return error.UnexpectedList,
                        };

                        result[i] = try ListReader(inner_elem_type).read(arena, reader);
                    },
                    .set => return error.SetNotSupported,
                    .map => return error.MapNotSupported,
                    .@"struct" => {
                        if (@typeInfo(E) != .@"struct" and @typeInfo(E) != .@"union") {
                            return error.UnexpectedStruct;
                        }

                        result[i] = try StructReader(E).read(arena, reader);
                    },
                    .uuid => return error.UuidNotSupported,
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

                if (field_id == 0) {
                    break;
                }
                if (field_id < 1 or field_id > max_field_id) {
                    return error.InvalidFieldId;
                }
                last_field_id = field_id;

                if (@as(u4, @truncate(header)) == 15) {
                    break;
                }

                const field_type = try FieldType.fromEnum(@truncate(header));
                if (field_type == .stop) {
                    break;
                }

                inline for (field_types, field_names, 0..) |expected_field_type, field_name, i| {
                    if (expected_field_type == void) {
                        continue;
                    }

                    const field_id_idx: usize = @intCast(field_id - 1);
                    if (field_id_idx == i) {
                        fields_set[field_id_idx] = true;

                        switch (field_type) {
                            .stop => unreachable,
                            .boolean_true => {
                                if (expected_field_type != bool) {
                                    return error.UnexpectedBool;
                                }
                                @field(result, field_name) = true;
                            },
                            .boolean_false => {
                                if (expected_field_type != bool) {
                                    return error.UnexpectedBool;
                                }
                                @field(result, field_name) = false;
                            },
                            .i8, .i16, .i32, .i64 => {
                                if (@typeInfo(expected_field_type) != .int and @typeInfo(expected_field_type) != .@"enum") {
                                    std.debug.print("Unexpected {any}\n", .{expected_field_type});
                                    return error.UnexpectedInt;
                                }

                                @field(result, field_name) = if (@typeInfo(expected_field_type) == .@"enum")
                                    @enumFromInt(try readZigZagInt(i32, reader))
                                else
                                    try readZigZagInt(expected_field_type, reader);
                            },
                            .double => return error.DoubleNotSupported,
                            .binary => {
                                switch (@typeInfo(expected_field_type)) {
                                    .pointer => |*p| {
                                        if (p.size != .slice or !p.is_const or p.child != u8) {
                                            return error.UnexpectedBinary;
                                        }
                                    },
                                    else => return error.UnexpectedBinary,
                                }

                                @field(result, field_name) = try readBinary(arena, reader);
                            },
                            .list => {
                                const elem_type = switch (@typeInfo(expected_field_type)) {
                                    .pointer => |*p| blk: {
                                        if (p.size != .slice) {
                                            return error.UnexpectedList;
                                        }
                                        break :blk p.child;
                                    },
                                    else => return error.UnexpectedList,
                                };

                                @field(result, field_name) = try ListReader(unwrapOptional(elem_type)).read(arena, reader);
                            },
                            .set => return error.SetNotSupported,
                            .map => return error.MapNotSupported,
                            .@"struct" => {
                                if (@typeInfo(expected_field_type) != .@"struct" and @typeInfo(expected_field_type) != .@"union") {
                                    return error.UnexpectedStruct;
                                }

                                const value = try StructReader(expected_field_type).read(arena, reader);
                                if (@typeInfo(T) == .@"union") {
                                    result = @unionInit(T, field_name, value);
                                } else {
                                    @field(result, field_name) = value;
                                }
                            },
                            .uuid => return error.UuidNotSupported,
                        }
                    }
                }
            }

            @setEvalBranchQuota(3000);
            inline for (fields, 0..) |field, i| {
                const field_id_idx = comptime T.fieldId(@enumFromInt(i)) - 1;
                if (!fields_set[field_id_idx] and @typeInfo(field.type) == .optional) {
                    @field(result, field.name) = null;
                }
            }

            return result;
        }
    };
}
