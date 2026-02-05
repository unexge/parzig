# parzig

A Parquet parser written in Zig using only the standard library.

## Usage

Here's an example analyzing [NYC taxi trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

```zig
const std = @import("std");
const parzig = @import("parzig");

const Io = std.Io;
const File = parzig.parquet.File;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var reader_buf: [4096]u8 = undefined;
    const f = try Io.Dir.cwd().openFile(io, "green_tripdata_2025-10.parquet", .{ .mode = .read_only });
    var file_reader = f.reader(io, &reader_buf);

    var file = try parzig.parquet.File.read(allocator, &file_reader);
    defer file.deinit();

    var rg = file.rowGroup(0);

    // Static typing: specify column type at compile time
    const fares = try rg.readColumn(f64, columnIndex(&file, "fare_amount"));
    const tips = try rg.readColumn(f64, columnIndex(&file, "tip_amount"));
    const passengers = try rg.readColumn(?i64, columnIndex(&file, "passenger_count")); // nullable

    var total_fare: f64 = 0;
    var total_tips: f64 = 0;
    var total_passengers: i64 = 0;

    for (fares, tips, passengers) |fare, tip, passenger| {
        total_fare += fare;
        total_tips += tip;
        if (passenger) |p| total_passengers += p;
    }

    std.debug.print("Total rides: {}\n", .{file.metadata.num_rows});
    std.debug.print("Total fares: ${d:.2}\n", .{total_fare});
    std.debug.print("Total tips: ${d:.2}\n", .{total_tips});
    std.debug.print("Total passengers: {}\n", .{total_passengers});

    // Dynamic typing: type determined at runtime
    const dynamic = try rg.readColumnDynamic(columnIndex(&file, "fare_amount"));
    switch (dynamic) {
        .double => |values| std.debug.print("First fare: ${d:.2}\n", .{values[0].?}),
        else => unreachable,
    }
}

fn columnIndex(file: *File, name: []const u8) usize {
    return file.findSchemaElement(&.{name}).?.column_index;
}
```

Output:
```
Total rides: 49416
Total fares: $898727.45
Total tips: $136046.83
Total passengers: 57441
First fare: $5.80
```

### Column Access

**Static typing** - specify the column type at compile time:
```zig
const values = try rg.readColumn(i64, 0);
const nullable = try rg.readColumn(?i64, 1);
```

**Dynamic typing** - type determined at runtime:
```zig
const dynamic = try rg.readColumnDynamic(0);
switch (dynamic) {
    .int64 => |data| // ...
    .double => |data| // ...
    .byte_array => |data| // ...
    // ...
}
```

**Nested types** - lists and maps:
```zig
const list = try rg.readListColumn(i32, 0);
const map = try rg.readMapColumn([]const u8, i64, 0, 1);
```

**Logical types** - choose how to interpret physical values:
```zig
const logical = parzig.parquet.logical;

const dates = try rg.readColumn(logical.Date, 0);
const timestamps = try rg.readColumn(logical.TimestampMicros, 1);
const decimals = try rg.readColumn(logical.Decimal, 2);
const uuids = try rg.readColumn(logical.UUID, 3);
```

Supported logical types:
- **Temporal**: `Date`, `TimeMillis`, `TimeMicros`, `TimeNanos`, `TimestampMillis`, `TimestampMicros`, `TimestampNanos`
- **Numeric**: `Int8`, `UInt8`, `Int16`, `UInt16`, `UInt32`, `UInt64`, `Float16`, `Decimal`
- **Other**: `UUID`, `String`, `Enum`, `Json`, `Bson`
