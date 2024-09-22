// Generated by `zig build generate`.
// DO NOT EDIT.

const std = @import("std");
pub const Type = enum(u8) {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};
pub const ConvertedType = enum(u8) {
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
};
pub const FieldRepetitionType = enum(u8) {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};
pub const SizeStatistics = struct {
    unencoded_byte_array_data_bytes: ?i64,
    repetition_level_histogram: ?std.ArrayList(i64),
    definition_level_histogram: ?std.ArrayList(i64),
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .unencoded_byte_array_data_bytes => return 1,
            .repetition_level_histogram => return 2,
            .definition_level_histogram => return 3,
            else => return null,
        }
    }
};
pub const Statistics = struct {
    max: ?[]u8,
    min: ?[]u8,
    null_count: ?i64,
    distinct_count: ?i64,
    max_value: ?[]u8,
    min_value: ?[]u8,
    is_max_value_exact: ?bool,
    is_min_value_exact: ?bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .max => return 1,
            .min => return 2,
            .null_count => return 3,
            .distinct_count => return 4,
            .max_value => return 5,
            .min_value => return 6,
            .is_max_value_exact => return 7,
            .is_min_value_exact => return 8,
            else => return null,
        }
    }
};
pub const StringType = struct {};
pub const UUIDType = struct {};
pub const MapType = struct {};
pub const ListType = struct {};
pub const EnumType = struct {};
pub const DateType = struct {};
pub const Float16Type = struct {};
pub const NullType = struct {};
pub const DecimalType = struct {
    scale: i32,
    precision: i32,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .scale => return 1,
            .precision => return 2,
            else => return null,
        }
    }
};
pub const MilliSeconds = struct {};
pub const MicroSeconds = struct {};
pub const NanoSeconds = struct {};
pub const TimeUnit = union {
    MILLIS: MilliSeconds,
    MICROS: MicroSeconds,
    NANOS: NanoSeconds,
};
pub const TimestampType = struct {
    isAdjustedToUTC: bool,
    unit: TimeUnit,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .isAdjustedToUTC => return 1,
            .unit => return 2,
            else => return null,
        }
    }
};
pub const TimeType = struct {
    isAdjustedToUTC: bool,
    unit: TimeUnit,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .isAdjustedToUTC => return 1,
            .unit => return 2,
            else => return null,
        }
    }
};
pub const IntType = struct {
    bitWidth: i8,
    isSigned: bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .bitWidth => return 1,
            .isSigned => return 2,
            else => return null,
        }
    }
};
pub const JsonType = struct {};
pub const BsonType = struct {};
pub const LogicalType = union {
    STRING: StringType,
    MAP: MapType,
    LIST: ListType,
    ENUM: EnumType,
    DECIMAL: DecimalType,
    DATE: DateType,
    TIME: TimeType,
    TIMESTAMP: TimestampType,
    INTEGER: IntType,
    UNKNOWN: NullType,
    JSON: JsonType,
    BSON: BsonType,
    UUID: UUIDType,
    FLOAT16: Float16Type,
};
pub const SchemaElement = struct {
    type: ?Type,
    type_length: ?i32,
    repetition_type: ?FieldRepetitionType,
    name: []u8,
    num_children: ?i32,
    converted_type: ?ConvertedType,
    scale: ?i32,
    precision: ?i32,
    field_id: ?i32,
    logicalType: ?LogicalType,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .type => return 1,
            .type_length => return 2,
            .repetition_type => return 3,
            .name => return 4,
            .num_children => return 5,
            .converted_type => return 6,
            .scale => return 7,
            .precision => return 8,
            .field_id => return 9,
            .logicalType => return 10,
            else => return null,
        }
    }
};
pub const Encoding = enum(u8) {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
    BYTE_STREAM_SPLIT = 9,
};
pub const CompressionCodec = enum(u8) {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
    LZ4_RAW = 7,
};
pub const PageType = enum(u8) {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3,
};
pub const BoundaryOrder = enum(u8) {
    UNORDERED = 0,
    ASCENDING = 1,
    DESCENDING = 2,
};
pub const DataPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    definition_level_encoding: Encoding,
    repetition_level_encoding: Encoding,
    statistics: ?Statistics,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .num_values => return 1,
            .encoding => return 2,
            .definition_level_encoding => return 3,
            .repetition_level_encoding => return 4,
            .statistics => return 5,
            else => return null,
        }
    }
};
pub const IndexPageHeader = struct {};
pub const DictionaryPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    is_sorted: ?bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .num_values => return 1,
            .encoding => return 2,
            .is_sorted => return 3,
            else => return null,
        }
    }
};
pub const DataPageHeaderV2 = struct {
    num_values: i32,
    num_nulls: i32,
    num_rows: i32,
    encoding: Encoding,
    definition_levels_byte_length: i32,
    repetition_levels_byte_length: i32,
    is_compressed: ?bool,
    statistics: ?Statistics,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .num_values => return 1,
            .num_nulls => return 2,
            .num_rows => return 3,
            .encoding => return 4,
            .definition_levels_byte_length => return 5,
            .repetition_levels_byte_length => return 6,
            .is_compressed => return 7,
            .statistics => return 8,
            else => return null,
        }
    }
};
pub const SplitBlockAlgorithm = struct {};
pub const BloomFilterAlgorithm = union {
    BLOCK: SplitBlockAlgorithm,
};
pub const XxHash = struct {};
pub const BloomFilterHash = union {
    XXHASH: XxHash,
};
pub const Uncompressed = struct {};
pub const BloomFilterCompression = union {
    UNCOMPRESSED: Uncompressed,
};
pub const BloomFilterHeader = struct {
    numBytes: i32,
    algorithm: BloomFilterAlgorithm,
    hash: BloomFilterHash,
    compression: BloomFilterCompression,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .numBytes => return 1,
            .algorithm => return 2,
            .hash => return 3,
            .compression => return 4,
            else => return null,
        }
    }
};
pub const PageHeader = struct {
    type: PageType,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    crc: ?i32,
    data_page_header: ?DataPageHeader,
    index_page_header: ?IndexPageHeader,
    dictionary_page_header: ?DictionaryPageHeader,
    data_page_header_v2: ?DataPageHeaderV2,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .type => return 1,
            .uncompressed_page_size => return 2,
            .compressed_page_size => return 3,
            .crc => return 4,
            .data_page_header => return 5,
            .index_page_header => return 6,
            .dictionary_page_header => return 7,
            .data_page_header_v2 => return 8,
            else => return null,
        }
    }
};
pub const KeyValue = struct {
    key: []u8,
    value: ?[]u8,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .key => return 1,
            .value => return 2,
            else => return null,
        }
    }
};
pub const SortingColumn = struct {
    column_idx: i32,
    descending: bool,
    nulls_first: bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .column_idx => return 1,
            .descending => return 2,
            .nulls_first => return 3,
            else => return null,
        }
    }
};
pub const PageEncodingStats = struct {
    page_type: PageType,
    encoding: Encoding,
    count: i32,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .page_type => return 1,
            .encoding => return 2,
            .count => return 3,
            else => return null,
        }
    }
};
pub const ColumnMetaData = struct {
    type: Type,
    encodings: std.ArrayList(Encoding),
    path_in_schema: std.ArrayList([]u8),
    codec: CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    key_value_metadata: ?std.ArrayList(KeyValue),
    data_page_offset: i64,
    index_page_offset: ?i64,
    dictionary_page_offset: ?i64,
    statistics: ?Statistics,
    encoding_stats: ?std.ArrayList(PageEncodingStats),
    bloom_filter_offset: ?i64,
    bloom_filter_length: ?i32,
    size_statistics: ?SizeStatistics,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .type => return 1,
            .encodings => return 2,
            .path_in_schema => return 3,
            .codec => return 4,
            .num_values => return 5,
            .total_uncompressed_size => return 6,
            .total_compressed_size => return 7,
            .key_value_metadata => return 8,
            .data_page_offset => return 9,
            .index_page_offset => return 10,
            .dictionary_page_offset => return 11,
            .statistics => return 12,
            .encoding_stats => return 13,
            .bloom_filter_offset => return 14,
            .bloom_filter_length => return 15,
            .size_statistics => return 16,
            else => return null,
        }
    }
};
pub const EncryptionWithFooterKey = struct {};
pub const EncryptionWithColumnKey = struct {
    path_in_schema: std.ArrayList([]u8),
    key_metadata: ?[]u8,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .path_in_schema => return 1,
            .key_metadata => return 2,
            else => return null,
        }
    }
};
pub const ColumnCryptoMetaData = union {
    ENCRYPTION_WITH_FOOTER_KEY: EncryptionWithFooterKey,
    ENCRYPTION_WITH_COLUMN_KEY: EncryptionWithColumnKey,
};
pub const ColumnChunk = struct {
    file_path: ?[]u8,
    file_offset: i64,
    meta_data: ?ColumnMetaData,
    offset_index_offset: ?i64,
    offset_index_length: ?i32,
    column_index_offset: ?i64,
    column_index_length: ?i32,
    crypto_metadata: ?ColumnCryptoMetaData,
    encrypted_column_metadata: ?[]u8,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .file_path => return 1,
            .file_offset => return 2,
            .meta_data => return 3,
            .offset_index_offset => return 4,
            .offset_index_length => return 5,
            .column_index_offset => return 6,
            .column_index_length => return 7,
            .crypto_metadata => return 8,
            .encrypted_column_metadata => return 9,
            else => return null,
        }
    }
};
pub const RowGroup = struct {
    columns: std.ArrayList(ColumnChunk),
    total_byte_size: i64,
    num_rows: i64,
    sorting_columns: ?std.ArrayList(SortingColumn),
    file_offset: ?i64,
    total_compressed_size: ?i64,
    ordinal: ?i16,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .columns => return 1,
            .total_byte_size => return 2,
            .num_rows => return 3,
            .sorting_columns => return 4,
            .file_offset => return 5,
            .total_compressed_size => return 6,
            .ordinal => return 7,
            else => return null,
        }
    }
};
pub const TypeDefinedOrder = struct {};
pub const ColumnOrder = union {
    TYPE_ORDER: TypeDefinedOrder,
};
pub const PageLocation = struct {
    offset: i64,
    compressed_page_size: i32,
    first_row_index: i64,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .offset => return 1,
            .compressed_page_size => return 2,
            .first_row_index => return 3,
            else => return null,
        }
    }
};
pub const OffsetIndex = struct {
    page_locations: std.ArrayList(PageLocation),
    unencoded_byte_array_data_bytes: ?std.ArrayList(i64),
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .page_locations => return 1,
            .unencoded_byte_array_data_bytes => return 2,
            else => return null,
        }
    }
};
pub const ColumnIndex = struct {
    null_pages: std.ArrayList(bool),
    min_values: std.ArrayList([]u8),
    max_values: std.ArrayList([]u8),
    boundary_order: BoundaryOrder,
    null_counts: ?std.ArrayList(i64),
    repetition_level_histograms: ?std.ArrayList(i64),
    definition_level_histograms: ?std.ArrayList(i64),
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .null_pages => return 1,
            .min_values => return 2,
            .max_values => return 3,
            .boundary_order => return 4,
            .null_counts => return 5,
            .repetition_level_histograms => return 6,
            .definition_level_histograms => return 7,
            else => return null,
        }
    }
};
pub const AesGcmV1 = struct {
    aad_prefix: ?[]u8,
    aad_file_unique: ?[]u8,
    supply_aad_prefix: ?bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .aad_prefix => return 1,
            .aad_file_unique => return 2,
            .supply_aad_prefix => return 3,
            else => return null,
        }
    }
};
pub const AesGcmCtrV1 = struct {
    aad_prefix: ?[]u8,
    aad_file_unique: ?[]u8,
    supply_aad_prefix: ?bool,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .aad_prefix => return 1,
            .aad_file_unique => return 2,
            .supply_aad_prefix => return 3,
            else => return null,
        }
    }
};
pub const EncryptionAlgorithm = union {
    AES_GCM_V1: AesGcmV1,
    AES_GCM_CTR_V1: AesGcmCtrV1,
};
pub const FileMetaData = struct {
    version: i32,
    schema: std.ArrayList(SchemaElement),
    num_rows: i64,
    row_groups: std.ArrayList(RowGroup),
    key_value_metadata: ?std.ArrayList(KeyValue),
    created_by: ?[]u8,
    column_orders: ?std.ArrayList(ColumnOrder),
    encryption_algorithm: ?EncryptionAlgorithm,
    footer_signing_key_metadata: ?[]u8,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .version => return 1,
            .schema => return 2,
            .num_rows => return 3,
            .row_groups => return 4,
            .key_value_metadata => return 5,
            .created_by => return 6,
            .column_orders => return 7,
            .encryption_algorithm => return 8,
            .footer_signing_key_metadata => return 9,
            else => return null,
        }
    }
};
pub const FileCryptoMetaData = struct {
    encryption_algorithm: EncryptionAlgorithm,
    key_metadata: ?[]u8,
    pub fn fieldId(comptime field: std.meta.FieldEnum(@This())) ?u32 {
        switch (field) {
            .encryption_algorithm => return 1,
            .key_metadata => return 2,
            else => return null,
        }
    }
};
