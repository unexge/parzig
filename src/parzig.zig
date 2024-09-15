pub const thrift = @import("./thrift.zig");

test {
    _ = thrift;
    _ = @import("./ordered_map.zig");
}
