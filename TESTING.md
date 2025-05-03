# Testing

parzig tries to test against the testing data provided in https://github.com/apache/parquet-testing.

parzig includes parquet-testing as a submodule in [`./testdata/parquet-testing`](./testdata/parquet-testing) and includes test cases in [`./src/parquet_testing.zig`](./src/parquet_testing.zig).

| File                                             | Status |
| ------------------------------------------------ | ------ |
| `alltypes_dictionary.parquet`                    | ✅     |
| `alltypes_plain.parquet`                         | ✅     |
| `alltypes_plain.snappy.parquet`                  | ✅     |
| `alltypes_tiny_pages.parquet`                    | 🚧     |
| `alltypes_tiny_pages_plain.parquet`              | 🚧     |
| `binary.parquet`                                 | ✅     |
| `byte_array_decimal.parquet`                     | ✅     |
| `byte_stream_split.zstd.parquet`                 | ✅     |
| `byte_stream_split_extended.gzip.parquet`        | 🚧     |
| `column_chunk_key_value_metadata.parquet`        | ✅     |
| `concatenated_gzip_members.parquet`              | 🚧     |
| `data_index_bloom_encoding_stats.parquet`        | 🚧     |
| `data_index_bloom_encoding_with_length.parquet`  | 🚧     |
| `datapage_v1-corrupt-checksum.parquet`           | 🚧     |
| `datapage_v1-snappy-compressed-checksum.parquet` | 🚧     |
| `datapage_v1-uncompressed-checksum.parquet`      | 🚧     |
| `datapage_v2.snappy.parquet`                     | 🚧     |
| `delta_binary_packed.parquet`                    | ✅     |
| `delta_byte_array.parquet`                       | ✅     |
| `delta_encoding_optional_column.parquet`         | ✅     |
| `delta_encoding_required_column.parquet`         | ✅     |
| `delta_length_byte_array.parquet`                | ✅     |
| `dict-page-offset-zero.parquet`                  | 🚧     |
| `fixed_length_byte_array.parquet`                | 🚧     |
| `fixed_length_decimal.parquet`                   | 🚧     |
| `fixed_length_decimal_legacy.parquet`            | 🚧     |
| `float16_nonzeros_and_nans.parquet`              | 🚧     |
| `float16_zeros_and_nans.parquet`                 | 🚧     |
| `hadoop_lz4_compressed.parquet`                  | 🚧     |
| `hadoop_lz4_compressed_larger.parquet`           | 🚧     |
| `incorrect_map_schema.parquet`                   | 🚧     |
| `int32_decimal.parquet`                          | 🚧     |
| `int32_with_null_pages.parquet`                  | 🚧     |
| `int64_decimal.parquet`                          | 🚧     |
| `large_string_map.brotli.parquet`                | 🚧     |
| `list_columns.parquet`                           | 🚧     |
| `lz4_raw_compressed.parquet`                     | 🚧     |
| `lz4_raw_compressed_larger.parquet`              | 🚧     |
| `map_no_value.parquet`                           | 🚧     |
| `nan_in_stats.parquet`                           | 🚧     |
| `nation.dict-malformed.parquet`                  | 🚧     |
| `nested_lists.snappy.parquet`                    | 🚧     |
| `nested_maps.snappy.parquet`                     | 🚧     |
| `nested_structs.rust.parquet`                    | 🚧     |
| `non_hadoop_lz4_compressed.parquet`              | 🚧     |
| `nonnullable.impala.parquet`                     | 🚧     |
| `null_list.parquet`                              | 🚧     |
| `nullable.impala.parquet`                        | 🚧     |
| `nulls.snappy.parquet`                           | 🚧     |
| `old_list_structure.parquet`                     | 🚧     |
| `overflow_i16_page_cnt.parquet`                  | 🚧     |
| `page_v2_empty_compressed.parquet`               | 🚧     |
| `plain-dict-uncompressed-checksum.parquet`       | 🚧     |
| `repeated_no_annotation.parquet`                 | 🚧     |
| `repeated_primitive_no_list.parquet`             | 🚧     |
| `rle-dict-snappy-checksum.parquet`               | 🚧     |
| `rle-dict-uncompressed-corrupt-checksum.parquet` | 🚧     |
| `rle_boolean_encoding.parquet`                   | ✅     |
| `single_nan.parquet`                             | ✅     |
| `sort_columns.parquet`                           | 🚧     |
