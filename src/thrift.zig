//! A Thrift parser aimed on parsing `parquet.thrift` only.

pub const Scanner = @import("./thrift/Scanner.zig");

test {
    _ = Scanner;
}
