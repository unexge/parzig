# Testing

parzig tries to test against the testing data provided in https://github.com/apache/parquet-testing.

parzig includes parquet-testing as a submodule in [`./testdata/parquet-testing`](./testdata/parquet-testing) and includes test cases in [`./src/parquet_testing.zig`](./src/parquet_testing.zig).

| File                                             | Status | Reason                    |
| ------------------------------------------------ | ------ | ------------------------- |
| `alltypes_dictionary.parquet`                    | ✅     |                           |
| `alltypes_plain.parquet`                         | ✅     |                           |
| `alltypes_plain.snappy.parquet`                  | ✅     |                           |
| `alltypes_tiny_pages.parquet`                    | ✅     |                           |
| `alltypes_tiny_pages_plain.parquet`              | ✅     |                           |
| `binary.parquet`                                 | ✅     |                           |
| `byte_array_decimal.parquet`                     | ✅     |                           |
| `byte_stream_split.zstd.parquet`                 | ✅     |                           |
| `byte_stream_split_extended.gzip.parquet`        | ✅     |                           |
| `column_chunk_key_value_metadata.parquet`        | ✅     |                           |
| `concatenated_gzip_members.parquet`              | 🚧     | Multi-part GZIP           |
| `data_index_bloom_encoding_stats.parquet`        | ✅     |                           |
| `data_index_bloom_encoding_with_length.parquet`  | ✅     |                           |
| `datapage_v1-corrupt-checksum.parquet`           | ✅     |                           |
| `datapage_v1-snappy-compressed-checksum.parquet` | ✅     |                           |
| `datapage_v1-uncompressed-checksum.parquet`      | ✅     |                           |
| `datapage_v2.snappy.parquet`                     | ✅     |                           |
| `delta_binary_packed.parquet`                    | ✅     |                           |
| `delta_byte_array.parquet`                       | ✅     |                           |
| `delta_encoding_optional_column.parquet`         | ✅     |                           |
| `delta_encoding_required_column.parquet`         | ✅     |                           |
| `delta_length_byte_array.parquet`                | ✅     |                           |
| `dict-page-offset-zero.parquet`                  | ✅     |                           |
| `fixed_length_byte_array.parquet`                | 🚧     | Malformed file            |
| `fixed_length_decimal.parquet`                   | ✅     |                           |
| `fixed_length_decimal_legacy.parquet`            | ✅     |                           |
| `float16_nonzeros_and_nans.parquet`              | ✅     |                           |
| `float16_zeros_and_nans.parquet`                 | ✅     |                           |
| `hadoop_lz4_compressed.parquet`                  | ✅     |                           |
| `hadoop_lz4_compressed_larger.parquet`           | 🚧     | LZ4 (Hadoop) large file   |
| `incorrect_map_schema.parquet`                   | 🚧     | Non-standard MAP schema   |
| `int32_decimal.parquet`                          | ✅     |                           |
| `int32_with_null_pages.parquet`                  | ✅     |                           |
| `int64_decimal.parquet`                          | ✅     |                           |
| `large_string_map.brotli.parquet`                | 🚧     | BROTLI compression        |
| `list_columns.parquet`                           | ✅     |                           |
| `lz4_raw_compressed.parquet`                     | ✅     |                           |
| `lz4_raw_compressed_larger.parquet`              | 🚧     | LZ4 (raw) compression     |
| `map_no_value.parquet`                           | ✅     |                           |
| `nan_in_stats.parquet`                           | ✅     |                           |
| `nation.dict-malformed.parquet`                  | ✅     |                           |
| `nested_lists.snappy.parquet`                    | 🚧     | Deeply nested lists       |
| `nested_maps.snappy.parquet`                     | 🚧     | Deeply nested maps        |
| `nested_structs.rust.parquet`                    | ✅     |                           |
| `non_hadoop_lz4_compressed.parquet`              | 🚧     | LZ4 compression           |
| `nonnullable.impala.parquet`                     | ✅     |                           |
| `null_list.parquet`                              | ✅     |                           |
| `nullable.impala.parquet`                        | 🚧     | Deeply nested lists       |
| `nulls.snappy.parquet`                           | ✅     |                           |
| `old_list_structure.parquet`                     | ✅     |                           |
| `overflow_i16_page_cnt.parquet`                  | ✅     |                           |
| `page_v2_empty_compressed.parquet`               | ✅     |                           |
| `plain-dict-uncompressed-checksum.parquet`       | ✅     |                           |
| `repeated_no_annotation.parquet`                 | ✅     |                           |
| `repeated_primitive_no_list.parquet`             | ✅     |                           |
| `rle-dict-snappy-checksum.parquet`               | ✅     |                           |
| `rle-dict-uncompressed-corrupt-checksum.parquet` | ✅     |                           |
| `rle_boolean_encoding.parquet`                   | ✅     |                           |
| `single_nan.parquet`                             | ✅     |                           |
| `sort_columns.parquet`                           | ✅     |                           |

## Failing Test Categories

The failing tests (🚧) can be grouped into the following categories:

### Files with Special Issues

#### `fixed_length_byte_array.parquet`
- **parzig**: Unsupported fixed-length size (11 bytes)
- **Pandas/PyArrow**: Cannot read (OSError: "Unexpected end of stream")
- **Status**: Malformed file or unsupported edge case

### Deeply Nested Types
parzig now has full support for basic nested types (LIST, MAP, STRUCT) with proper Dremel-based reconstruction using definition and repetition levels. However, some files with specific nested structures still have issues:
- `nested_lists.snappy.parquet` - Triple-nested LIST columns (list<list<list<str>>>): file parses without crashing but returns all null values instead of actual data
- `nested_maps.snappy.parquet` - MAP columns with nested MAP values (fails with empty encoded_values in RLE decoder)
- `nullable.impala.parquet` - LIST of LIST columns (fails with empty encoded_values in RLE decoder)

**Supported nested features:**
- ✅ Basic LIST columns with nullable elements (`list_columns.parquet`)
- ✅ MAP columns with key-value pairs (`map_no_value.parquet`)
- ✅ STRUCT columns with multiple fields (`nested_structs.rust.parquet`)
- ✅ REPEATED fields with and without LIST annotation (`repeated_no_annotation.parquet`, `nonnullable.impala.parquet`)
- ✅ Definition and repetition level reconstruction

**Known limitations:**
- ❌ Multi-level nested LISTs (list<list<T>>): Files parse but return incorrect null values
- ❌ Nested MAP values (map<K, map<K2, V2>>): RLE decoder fails with empty encoded_values

### Compression
These files use compression codecs that have partial or no support:

**LZ4:**
✅ Basic support implemented for Hadoop LZ4 format (codec value 5) and raw LZ4 blocks (codec value 7)
- ✅ `hadoop_lz4_compressed.parquet` - Working
- 🚧 `hadoop_lz4_compressed_larger.parquet` - Large file issue (EndOfStream during decompression)
- 🚧 `lz4_raw_compressed_larger.parquet` - File format quirk (invalid match offset)
- 🚧 `non_hadoop_lz4_compressed.parquet` - Syscall error

**BROTLI:**
- `large_string_map.brotli.parquet`

**Note on LZ4 Compression:**
The Parquet format specifies two different LZ4 compression codecs:
- **LZ4** (codec value 5): Deprecated codec with Hadoop framing (4-byte size prefixes)
- **LZ4_RAW** (codec value 7): Modern codec using pure LZ4 block format

parzig supports both formats with circular buffer handling for large decompressed data (>64KB sliding window).

### Multi-part GZIP
This file uses concatenated GZIP members which requires special handling:
- `concatenated_gzip_members.parquet`

### Non-standard Schema
This file has a non-spec-compliant MAP schema (optional keys instead of required):
- `incorrect_map_schema.parquet`
