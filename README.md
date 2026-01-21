# parzig

A Parquet parser written in Zig using only the standard library.

## Usage

```zig
const parzig = @import("parzig");

var file = try parzig.parquet.File.read(allocator, &file_reader);
defer file.deinit();

var rg = file.rowGroup(0);

// Static typing: specify type at compile time
const values = try rg.readColumn(i64, 0);
const nullable = try rg.readColumn(?i64, 1);

// Dynamic typing: type determined at runtime
const dynamic = try rg.readColumnDynamic(0);
switch (dynamic) {
    .int64 => |data| // ...
    .byte_array => |data| // ...
    // ...
}

// Nested types
const list = try rg.readListColumn(i32, 0);
const map = try rg.readMapColumn([]const u8, i64, 0, 1);
```
