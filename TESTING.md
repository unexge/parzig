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
| `dict-page-offset-zero.parquet`                  | 🚧     | Non-standard metadata     |
| `fixed_length_byte_array.parquet`                | 🚧     | Malformed file            |
| `fixed_length_decimal.parquet`                   | ✅     |                           |
| `fixed_length_decimal_legacy.parquet`            | ✅     |                           |
| `float16_nonzeros_and_nans.parquet`              | ✅     |                           |
| `float16_zeros_and_nans.parquet`                 | ✅     |                           |
| `hadoop_lz4_compressed.parquet`                  | 🚧     | LZ4 (Hadoop) compression  |
| `hadoop_lz4_compressed_larger.parquet`           | 🚧     | LZ4 (Hadoop) compression  |
| `incorrect_map_schema.parquet`                   | 🚧     | Non-standard MAP schema   |
| `int32_decimal.parquet`                          | ✅     |                           |
| `int32_with_null_pages.parquet`                  | ✅     |                           |
| `int64_decimal.parquet`                          | ✅     |                           |
| `large_string_map.brotli.parquet`                | 🚧     | BROTLI compression        |
| `list_columns.parquet`                           | 🚧     | Repetition levels         |
| `lz4_raw_compressed.parquet`                     | ✅     |                           |
| `lz4_raw_compressed_larger.parquet`              | 🚧     | LZ4 (raw) compression     |
| `map_no_value.parquet`                           | 🚧     | Repetition levels         |
| `nan_in_stats.parquet`                           | ✅     |                           |
| `nation.dict-malformed.parquet`                  | ✅     |                           |
| `nested_lists.snappy.parquet`                    | 🚧     | Repetition levels         |
| `nested_maps.snappy.parquet`                     | 🚧     | Repetition levels         |
| `nested_structs.rust.parquet`                    | 🚧     | Repetition levels         |
| `non_hadoop_lz4_compressed.parquet`              | 🚧     | LZ4 compression           |
| `nonnullable.impala.parquet`                     | 🚧     | Repetition levels         |
| `null_list.parquet`                              | ✅     |                           |
| `nullable.impala.parquet`                        | 🚧     | Repetition levels         |
| `nulls.snappy.parquet`                           | ✅     |                           |
| `old_list_structure.parquet`                     | ✅     |                           |
| `overflow_i16_page_cnt.parquet`                  | ✅     |                           |
| `page_v2_empty_compressed.parquet`               | ✅     |                           |
| `plain-dict-uncompressed-checksum.parquet`       | ✅     |                           |
| `repeated_no_annotation.parquet`                 | 🚧     | Repetition levels         |
| `repeated_primitive_no_list.parquet`             | ✅     |                           |
| `rle-dict-snappy-checksum.parquet`               | ✅     |                           |
| `rle-dict-uncompressed-corrupt-checksum.parquet` | ✅     |                           |
| `rle_boolean_encoding.parquet`                   | ✅     |                           |
| `single_nan.parquet`                             | ✅     |                           |
| `sort_columns.parquet`                           | ✅     |                           |

## Failing Test Categories

The failing tests (🚧) can be grouped into the following categories:

### Files with Special Issues
- `dict-page-offset-zero.parquet` - Non-standard Thrift metadata that causes parsing errors
- `fixed_length_byte_array.parquet` - Malformed file (fails in both Polars and PyArrow with "Unexpected end of stream")

### Repetition Levels
These files use nested schemas (LIST, MAP, STRUCT) that require repetition level support to properly reconstruct the nested data:
- `list_columns.parquet` - LIST columns
- `map_no_value.parquet` - MAP with null values
- `nested_lists.snappy.parquet` - Nested LIST columns
- `nested_maps.snappy.parquet` - Nested MAP columns
- `nested_structs.rust.parquet` - Nested STRUCT columns
- `nonnullable.impala.parquet` - LIST and MAP columns
- `nullable.impala.parquet` - LIST and MAP columns
- `repeated_no_annotation.parquet` - REPEATED fields without LIST annotation

### Compression
These files use compression codecs that are not yet implemented:

**LZ4 (deprecated Hadoop format):**
- `hadoop_lz4_compressed.parquet`
- `hadoop_lz4_compressed_larger.parquet`

**LZ4 (raw format):**
- `lz4_raw_compressed_larger.parquet`

**LZ4 (non-Hadoop/deprecated format):**
- `non_hadoop_lz4_compressed.parquet`

**BROTLI:**
- `large_string_map.brotli.parquet`

**Note on LZ4 vs LZ4_RAW:**
The Parquet format specifies two different LZ4 compression codecs:
- **LZ4** (codec value 5): Deprecated codec with Hadoop framing (undocumented extra bytes)
- **LZ4_RAW** (codec value 7): Modern codec using pure LZ4 block format, specified in Parquet format v2.9.0+

### Multi-part GZIP
This file uses concatenated GZIP members which requires special handling:
- `concatenated_gzip_members.parquet`

### Non-standard Schema
This file has a non-spec-compliant MAP schema (optional keys instead of required):
- `incorrect_map_schema.parquet`
