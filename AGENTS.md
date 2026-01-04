# AGENTS.md

This is a Parquet parser written from scratch in Zig using only the standard library. 
It only supports reading Parquet files. You can find Parquet schema in `parquet.thrift`.

## Commands
- Build project: `zig build`
- Run tests: `zig build test`
- Format code: `zig fmt .`
- Parse a Parquet file: `zig build run -- ./testdata/parquet-testing/data/byte_stream_split_extended.gzip.parquet`

## Development workflow
Before making any changes, make a plan first and run through it with the operator.
Keep your plan simple, concise and break it down into small, logical steps. Eventually each logical step should be commited separately.
Ensure to each commit compiles, passes all tests and also formatted. Before making changes, make sure to add failing tests first.

Also, make sure to create a new edit before making any change.

Use Jujutsu (`jj`) for version control.
- Check history: `jj log`
- Create a new edit from the current working copy: `jj new`
  - Do this only if the current edit is non-empty. Don't create empty commits.
- Check current status: `jj status`
- Check current diff: `jj diff -f main`
- Commit changes: `jj commit -m "Your commit message"`

## Code style
- Look out for potential memory leaks. Ensure to use `errdefer` to free allocated memory in an error condition.
- Make sure to look out for double-free errors, and again use `errdefer` when needed to clean up resources.
- Use arena allocator to scope allocations, for example in the row group reader, allocate all data page in an arena and free all together.
- Don't add comments unless they are absolutely necessary - only add them if there is something unusual or non-obvious.

## Addressing PR feedback
Each change you make will go through a code review process on GitHub. To address feedback, you can run: 
```bash
$ gh pr-review review view https://github.com/unexge/parzig/pull/<PR_NUMBER> --unresolved --not_outdated
```

## Testing
Make sure to add tests for any changes. The tests are either located in the same file as the code in `test` blocks,
or in the `src/parquet_testing.zig` file for Parquet comformance tests.

You can see `testdata/parquet-testing` submodule for the comformance testdata and details.
After completing any big changes, make sure to check `TESTING.md` for the current status of the conformance tests and update them where needed.

To add new comformance tests, use Polars as a reference implementation to generate test files.

You can use `uvx` to run Python snippets using Polars: 
```bash
$ uvx --from "polars[all]" --with pyarrow python -c 'import polars as pl; pl.Config.set_tbl_rows(-1); print(pl.read_parquet("testdata/parquet-testing/data/fixed_length_decimal.parquet"))'
```

Once you get the expected output from Polars, you can create a new Zig test case in `src/parquet_testing.zig`, and assert the expected output, for example:
```zig
test "all types tiny pages plain" {
    var reader_buf: [1024]u8 = undefined;
    var file_reader = (try Io.Dir.cwd().openFile(io, "testdata/parquet-testing/data/alltypes_tiny_pages_plain.parquet", .{ .mode = .read_only })).reader(io, &reader_buf);
    var file = try File.read(testing.allocator, &file_reader);
    defer file.deinit();

    try testing.expectEqual(1, file.metadata.row_groups.len);
    try testing.expectEqual(7300, file.metadata.num_rows);

    var rg = file.rowGroup(0);

    try testing.expectEqualSlices(i32, &[_]i32{ 122, 123, 124, 125, 126, 127, 128, 129, 130, 131 }, (try rg.readColumn(i32, 0))[0..10]);
    // ... more assertions for other fields
}
```
Make sure to compare all fields and all values. Also, don't try to read `src/parquet_testing.zig` without grepping it, as it is a huge file. Just follow the testing structure above.
