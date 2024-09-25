const std = @import("std");

pub fn readInt(comptime T: type, reader: anytype) !T {
    if (@sizeOf(T) > @sizeOf(i64)) {
        @compileError("Maximum 64-bit integers are supported");
    }
    const num = try std.leb.readULEB128(i64, reader);
    return @intCast((num >> 1) ^ -(num & 1));
}

pub fn readBinary(arena: std.mem.Allocator, reader: anytype) ![]const u8 {
    const lenght = try std.leb.readULEB128(u64, reader);
    const buf = try arena.alloc(u8, @intCast(lenght));
    try reader.readNoEof(buf);
    return buf;
}

fn unwrapOptional(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .Optional => |*o| o.child,
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
        pub fn read(arena: std.mem.Allocator, reader: anytype) ![]E {
            const header = try reader.readByte();

            const size_short: u4 = @truncate(header >> 4);
            const size: usize = @intCast(if (size_short == LONG_FORM)
                try readInt(i32, reader)
            else
                size_short);

            const elem_type = try ElemType.fromEnum(@truncate(header));

            const result = try arena.alloc(E, size);

            for (0..size) |i| {
                switch (elem_type) {
                    .bool => {
                        if (E != bool) {
                            return error.ExpectedBool;
                        }
                        result[i] = readInt(i8, reader) == 1;
                    },
                    .i8, .i16, .i32, .i64 => {
                        if (@typeInfo(E) != .Int and @typeInfo(E) != .Enum) {
                            return error.ExpectedInt;
                        }

                        result[i] = if (@typeInfo(E) == .Enum)
                            @enumFromInt(try readInt(i32, reader))
                        else
                            try readInt(E, reader);
                    },
                    .double => return error.DoubleNotSupported,
                    .binary => {
                        switch (@typeInfo(E)) {
                            .Pointer => |*p| {
                                if (p.size != .Slice or !p.is_const or p.child != u8) {
                                    return error.ExpectedBinary;
                                }
                            },
                            else => return error.ExpectedBinary,
                        }

                        result[i] = try readBinary(arena, reader);
                    },
                    .list => {
                        const inner_elem_type = switch (@typeInfo(E)) {
                            .Pointer => |*p| blk: {
                                if (p.size != .Slice) {
                                    return error.ExpectedList;
                                }
                                break :blk p.child;
                            },
                            else => return error.ExpectedList,
                        };

                        result[i] = try ListReader(inner_elem_type).read(arena, reader);
                    },
                    .set => return error.SetNotSupported,
                    .map => return error.MapNotSupported,
                    .@"struct" => {
                        if (@typeInfo(E) != .Struct and @typeInfo(E) != .Union) {
                            return error.ExpectedStruct;
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
        .Struct => |*s| s.fields,
        .Union => |*u| u.fields,
        else => @compileError("Expected struct or union"),
    };

    const max_field_id = blk: {
        comptime var max_field_id = 0;
        inline for (fields, 0..) |_, i| {
            _ = comptime std.meta.intToEnum(std.meta.FieldEnum(T), i) catch continue;
            const field_id = comptime T.fieldId(@enumFromInt(i));
            if (field_id == 0) {
                @compileError("Field id must be > 0");
            }
            max_field_id = @max(max_field_id, field_id);
        }
        break :blk max_field_id;
    };

    const field_types = blk: {
        var field_types: [max_field_id]type = undefined;
        inline for (fields, 0..) |field, i| {
            field_types[i] = void;
            _ = comptime std.meta.intToEnum(std.meta.FieldEnum(T), i) catch continue;
            const field_id = comptime T.fieldId(@enumFromInt(i));
            field_types[field_id - 1] = unwrapOptional(field.type);
        }
        break :blk field_types;
    };

    const field_names = blk: {
        var field_names: [max_field_id][]const u8 = undefined;
        inline for (fields, 0..) |field, i| {
            _ = comptime std.meta.intToEnum(std.meta.FieldEnum(T), i) catch continue;
            const field_id = comptime T.fieldId(@enumFromInt(i));
            field_names[field_id - 1] = field.name;
        }
        break :blk field_names;
    };

    return struct {
        pub fn read(arena: std.mem.Allocator, reader: anytype) !T {
            var result: T = undefined;
            var last_field_id: i16 = 0;
            while (true) {
                const header = try reader.readByte();
                if (header == STOP) {
                    break;
                }

                const field_id_delta: u4 = @truncate(header >> 4);
                const field_id = if (field_id_delta == 0)
                    try readInt(i16, reader)
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

                inline for (field_types, field_names, 0..) |expected_field_type, field_name, i| {
                    if (expected_field_type == void) {
                        continue;
                    }

                    if (@as(usize, @intCast(field_id)) - 1 == i) {
                        switch (field_type) {
                            .boolean_true => {
                                if (expected_field_type != bool) {
                                    return error.ExpectedBool;
                                }
                                @field(result, field_name) = true;
                            },
                            .boolean_false => {
                                if (expected_field_type != bool) {
                                    return error.ExpectedBool;
                                }
                                @field(result, field_name) = false;
                            },
                            .i8, .i16, .i32, .i64 => {
                                if (@typeInfo(expected_field_type) != .Int and @typeInfo(expected_field_type) != .Enum) {
                                    return error.ExpectedInt;
                                }

                                @field(result, field_name) = if (@typeInfo(expected_field_type) == .Enum)
                                    @enumFromInt(try readInt(i32, reader))
                                else
                                    try readInt(expected_field_type, reader);
                            },
                            .double => return error.DoubleNotSupported,
                            .binary => {
                                switch (@typeInfo(expected_field_type)) {
                                    .Pointer => |*p| {
                                        if (p.size != .Slice or !p.is_const or p.child != u8) {
                                            return error.ExpectedBinary;
                                        }
                                    },
                                    else => return error.ExpectedBinary,
                                }

                                @field(result, field_name) = try readBinary(arena, reader);
                            },
                            .list => {
                                const elem_type = switch (@typeInfo(expected_field_type)) {
                                    .Pointer => |*p| blk: {
                                        if (p.size != .Slice) {
                                            return error.ExpectedList;
                                        }
                                        break :blk p.child;
                                    },
                                    else => return error.ExpectedList,
                                };

                                @field(result, field_name) = try ListReader(unwrapOptional(elem_type)).read(arena, reader);
                            },
                            .set => return error.SetNotSupported,
                            .map => return error.MapNotSupported,
                            .@"struct" => {
                                if (@typeInfo(expected_field_type) != .Struct and @typeInfo(expected_field_type) != .Union) {
                                    return error.ExpectedStruct;
                                }

                                const value = try StructReader(expected_field_type).read(arena, reader);
                                if (@typeInfo(T) == .Union) {
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

            return result;
        }
    };
}
