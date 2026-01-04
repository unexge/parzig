const parquet_schema = @import("../generated/parquet.zig");
const File = @import("./File.zig");
const rowGroupReader = @import("./rowGroupReader.zig");

pub fn MapEntry(comptime K: type, comptime V: type) type {
    return struct {
        key: K,
        value: V,
    };
}

pub fn readMap(
    comptime K: type,
    comptime V: type,
    file: *File,
    key_column: *parquet_schema.ColumnChunk,
    value_column: *parquet_schema.ColumnChunk,
) ![][]const MapEntry(K, V) {
    const arena = file.arena.allocator();
    const Entry = MapEntry(K, V);

    const key_data = try rowGroupReader.readColumnWithLevels(K, file, key_column);
    const value_data = try rowGroupReader.readColumnWithLevels(V, file, value_column);

    const key_metadata = key_column.meta_data orelse return error.MissingColumnMetadata;
    const key_schema_info = file.findSchemaElement(key_metadata.path_in_schema) orelse return error.UnknownField;
    if (key_schema_info.max_repetition_level == 0) {
        return error.NotAMapColumn;
    }

    const rep_levels = key_data.rep_levels orelse return error.MissingRepetitionLevels;
    var num_maps: usize = 0;
    for (rep_levels) |rep_level| {
        if (rep_level == 0) num_maps += 1;
    }

    const maps = try arena.alloc([]const Entry, num_maps);
    var map_data = try arena.alloc(Entry, key_data.values.len);
    var map_data_pos: usize = 0;
    var map_idx: usize = 0;
    var map_start: usize = 0;

    for (rep_levels, 0..) |rep_level, i| {
        if (rep_level == 0 and i > 0) {
            maps[map_idx] = map_data[map_start..map_data_pos];
            map_idx += 1;
            map_start = map_data_pos;
        }

        map_data[map_data_pos] = .{
            .key = key_data.values[map_data_pos],
            .value = value_data.values[map_data_pos],
        };
        map_data_pos += 1;
    }

    if (map_idx < num_maps) {
        maps[map_idx] = map_data[map_start..map_data_pos];
    }

    return maps;
}

pub fn readStruct(
    comptime T: type,
    file: *File,
    columns: []parquet_schema.ColumnChunk,
    base_index: usize,
    num_rows: usize,
) ![]T {
    const arena = file.arena.allocator();
    const fields = @typeInfo(T).@"struct".fields;
    const result = try arena.alloc(T, num_rows);

    inline for (fields, 0..) |field, field_idx| {
        const column_values = try rowGroupReader.readColumn(field.type, file, &columns[base_index + field_idx]);
        for (0..num_rows) |row_idx| {
            @field(result[row_idx], field.name) = column_values[row_idx];
        }
    }

    return result;
}

pub fn readList(
    comptime T: type,
    file: *File,
    column: *parquet_schema.ColumnChunk,
) ![][]const T {
    const arena = file.arena.allocator();
    const Inner = rowGroupReader.unwrapOptional(T);
    const is_nullable = Inner != T;

    const column_data = try rowGroupReader.readColumnWithLevels(Inner, file, column);

    const metadata = column.meta_data orelse return error.MissingColumnMetadata;
    const schema_info = file.findSchemaElement(metadata.path_in_schema) orelse return error.UnknownField;
    const max_def_level = schema_info.max_definition_level;
    if (schema_info.max_repetition_level == 0) {
        return error.NotAListColumn;
    }

    const def_levels = column_data.def_levels orelse return error.MissingDefinitionLevels;
    const rep_levels = column_data.rep_levels orelse return error.MissingRepetitionLevels;
    const values = column_data.values;

    var num_lists: usize = 0;
    for (rep_levels) |rep_level| {
        if (rep_level == 0) num_lists += 1;
    }

    const lists = try arena.alloc([]const T, num_lists);
    var list_data = try arena.alloc(T, rep_levels.len);
    var list_data_pos: usize = 0;
    var list_idx: usize = 0;
    var value_idx: usize = 0;
    var list_start: usize = 0;

    for (rep_levels, 0..) |rep_level, i| {
        const def_level = def_levels[i];

        if (rep_level == 0 and i > 0) {
            lists[list_idx] = list_data[list_start..list_data_pos];
            list_idx += 1;
            list_start = list_data_pos;
        }

        if (def_level >= max_def_level - 1) {
            if (def_level == max_def_level) {
                list_data[list_data_pos] = values[value_idx];
                value_idx += 1;
            } else if (is_nullable) {
                list_data[list_data_pos] = null;
            }
            list_data_pos += 1;
        }
    }

    if (list_idx < num_lists) {
        lists[list_idx] = list_data[list_start..list_data_pos];
    }

    return lists;
}
