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

    return struct {
        fn skipField(reader: *Reader, field_type: FieldType) !void {
            switch (field_type) {
                .stop, .boolean_true, .boolean_false => {},
                .i8, .i16, .i32, .i64 => _ = try reader.takeLeb128(u64),
                .double => try reader.discardAll(8),
                .binary => try reader.discardAll64(try reader.takeLeb128(u64)),
                .list, .set => {
                    const header = try reader.takeByte();
                    const size_short: u4 = @truncate(header >> 4);
                    const size: u32 = if (size_short == 0x0f)
                        try reader.takeLeb128(u32)
                    else
                        size_short;
                    const elem_type: FieldType = @enumFromInt(@as(u4, @truncate(header)));
                    for (0..size) |_| try skipField(reader, elem_type);
                },
                .map => {
                    const size = try reader.takeLeb128(u32);
                    if (size > 0) {
                        const types = try reader.takeByte();
                        for (0..size) |_| {
                            try skipField(reader, @enumFromInt(@as(u4, @truncate(types >> 4))));
                            try skipField(reader, @enumFromInt(@as(u4, @truncate(types))));
                        }
                    }
                },
                .@"struct" => {
                    while (true) {
                        const header = try reader.takeByte();
                        if (header == 0) break;
                        try skipField(reader, @enumFromInt(@as(u4, @truncate(header))));
                    }
                },
                .uuid => try reader.discardAll(16),
            }
        }

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
                last_field_id = field_id;

                const field_type: FieldType = @enumFromInt(@as(u4, @truncate(header)));
                if (field_type == .stop) {
                    break;
                }

                // Skip unknown field IDs
                if (field_id < 1 or field_id > max_field_id) {
                    try skipField(reader, field_type);
                    continue;
                }

                inline for (field_types, field_names, 0..) |expected_field_type, field_name, i| {
                    if (expected_field_type == void) {
                        continue;
                    }

                    const field_id_idx: usize = @intCast(field_id - 1);
                    if (field_id_idx == i) {
                        switch (field_type) {
                            .stop => unreachable,
                            .boolean_true, .boolean_false => {
                                if (expected_field_type != bool) {
                                    try skipField(reader, field_type);
                                } else {
                                    fields_set[field_id_idx] = true;
                                    @field(result, field_name) = field_type == .boolean_true;
                                }
                            },
                            .i8, .i16, .i32, .i64 => {
                                if (@typeInfo(expected_field_type) != .int and @typeInfo(expected_field_type) != .@"enum") {
                                    try skipField(reader, field_type);
                                } else {
                                    fields_set[field_id_idx] = true;
                                    @field(result, field_name) = if (@typeInfo(expected_field_type) == .@"enum")
                                        @enumFromInt(try readZigZagInt(i32, reader))
                                    else
                                        try readZigZagInt(expected_field_type, reader);
                                }
                            },
                            .double => try skipField(reader, field_type),
                            .binary => {
                                const is_binary = switch (@typeInfo(expected_field_type)) {
                                    .pointer => |*p| p.size == .slice and p.is_const and p.child == u8,
                                    else => false,
                                };
                                if (!is_binary) {
                                    try skipField(reader, field_type);
                                } else {
                                    fields_set[field_id_idx] = true;
                                    @field(result, field_name) = try readBinary(arena, reader);
                                }
                            },
                            .list => {
                                const is_list = switch (@typeInfo(expected_field_type)) {
                                    .pointer => |*p| p.size == .slice,
                                    else => false,
                                };
                                if (!is_list) {
                                    try skipField(reader, field_type);
                                } else {
                                    const elem_type = switch (@typeInfo(expected_field_type)) {
                                        .pointer => |*p| p.child,
                                        else => unreachable,
                                    };
                                    fields_set[field_id_idx] = true;
                                    @field(result, field_name) = try ListReader(unwrapOptional(elem_type)).read(arena, reader);
                                }
                            },
                            .set, .map, .uuid => try skipField(reader, field_type),
                            .@"struct" => {
                                if (@typeInfo(expected_field_type) != .@"struct" and @typeInfo(expected_field_type) != .@"union") {
                                    try skipField(reader, field_type);
                                } else {
                                    fields_set[field_id_idx] = true;
                                    const value = try StructReader(expected_field_type).read(arena, reader);
                                    if (@typeInfo(T) == .@"union") {
                                        result = @unionInit(T, field_name, value);
                                    } else {
                                        @field(result, field_name) = value;
                                    }
                                }
                            },
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
