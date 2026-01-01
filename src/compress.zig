pub const snappy = @import("compress/snappy.zig");
pub const lz4 = @import("compress/lz4.zig");

test {
    _ = snappy;
    _ = lz4;
}
