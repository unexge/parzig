pub const thrift = @import("./thrift.zig");
pub const parquet = @import("./parquet.zig");
pub const compress = @import("./compress.zig");

test {
    _ = thrift;
    _ = parquet;
    _ = compress;
    _ = @import("./ordered_map.zig");
}
