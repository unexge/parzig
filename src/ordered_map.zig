const std = @import("std");

pub fn OrderedStringHashMap(comptime T: anytype) type {
    return struct {
        const Self = @This();

        map: std.StringHashMap(T),
        keys: std.ArrayList([]const u8),

        const Iterator = struct {
            ordered: *const Self,
            index: usize = 0,

            pub fn next(self: *Iterator) ?std.StringHashMap(T).Entry {
                if (self.index >= self.ordered.keys.items.len) {
                    return null;
                }

                const k = self.ordered.keys.items[self.index];
                self.index += 1;
                return self.ordered.map.getEntry(k).?;
            }
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .map = std.StringHashMap(T).init(allocator),
                .keys = std.ArrayList([]const u8).init(allocator),
            };
        }

        pub fn count(self: *const Self) usize {
            return self.keys.items.len;
        }

        pub fn put(self: *Self, key: []const u8, value: T) !void {
            if (!self.map.contains(key)) {
                try self.keys.append(key);
            }
            try self.map.put(key, value);
        }

        pub fn get(self: *const Self, key: []const u8) ?T {
            return self.map.get(key);
        }

        pub fn iterator(self: *const Self) Iterator {
            return .{ .ordered = self };
        }

        pub fn valueIterator(self: *Self) std.StringHashMap(T).ValueIterator {
            return self.map.valueIterator();
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
            self.keys.deinit();
        }
    };
}

test OrderedStringHashMap {
    var map = OrderedStringHashMap(u64).init(std.testing.allocator);
    defer map.deinit();
    try map.put("foo", 1);
    try map.put("bar", 2);
    try map.put("foo", 3);

    var iter = map.iterator();
    {
        const elem = iter.next().?;
        try std.testing.expectEqualStrings("foo", elem.key_ptr.*);
        try std.testing.expectEqual(3, elem.value_ptr.*);
    }
    {
        const elem = iter.next().?;
        try std.testing.expectEqualStrings("bar", elem.key_ptr.*);
        try std.testing.expectEqual(2, elem.value_ptr.*);
    }
    try std.testing.expectEqual(null, iter.next());
}
