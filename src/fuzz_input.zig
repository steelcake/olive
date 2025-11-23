const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const arrow = @import("arrow");
const arr = arrow.array;
const data_type = arrow.data_type;

const fuzzin = @import("fuzzin");
const FuzzInput = fuzzin.FuzzInput;
const Error = fuzzin.Error;

const schema_mod = @import("./schema.zig");
const Schema = schema_mod.Schema;
const TableSchema = schema_mod.TableSchema;

const MAX_DEPTH = 8;

pub fn schema(input: *FuzzInput, alloc: Allocator) Error!Schema {
    const num_tables = (try input.int(u8)) % 10 + 1;

    var prng = Prng.init();
    const rand = prng.random();

    const table_names = try fuzzin.allocate([:0]const u8, num_tables, alloc);
    for (0..table_names.len) |idx| {
        table_names[idx] = try arrow.fuzz_input.uniq_name(table_names[0..idx], rand, alloc);
    }

    const table_schemas = try fuzzin.allocate(TableSchema, num_tables, alloc);
    for (0..table_schemas.len) |idx| {
        table_schemas[idx] = try table_schema(input, alloc);
    }

    return Schema{
        .table_names = table_names,
        .table_schemas = table_schemas,
    };
}

pub fn table_schema(input: *FuzzInput, alloc: Allocator) Error!TableSchema {
    const num_fields = (try input.int(u8)) % 10 + 1;

    var prng = Prng.init();
    const rand = prng.random();

    const field_names = try fuzzin.allocate([:0]const u8, num_fields, alloc);
    for (0..field_names.len) |idx| {
        field_names[idx] = try arrow.fuzz_input.uniq_name(field_names[0..idx], rand, alloc);
    }

    const field_types = try fuzzin.allocate(data_type.DataType, num_fields, alloc);
    for (0..field_types.len) |idx| {
        field_types[idx] = try arrow.fuzz_input.data_type(input, alloc, MAX_DEPTH);
    }

    const sch = TableSchema{
        .field_names = field_names,
        .field_types = field_types,
    };

    sch.validate() catch unreachable;

    return sch;
}

pub fn arrow_chunk(sch: *const Schema, input: *FuzzInput, alloc: Allocator) Error![]const []const arr.Array {
    const tables = try fuzzin.allocate([]const arr.Array, sch.table_names.len);
    for (0..tables.len) |table_idx| {
        tables[table_idx] = try arrow_table(&sch.table_schemas[table_idx], input, alloc);
    }

    sch.check(tables) catch unreachable;

    return tables;
}

pub fn arrow_table(sch: *const TableSchema, input: *FuzzInput, alloc: Allocator) Error![]const arr.Array {
    const fields = try fuzzin.allocate(arr.Array, sch.field_names.len);

    const len = try input.int(u8);

    for (0..fields.len) |field_idx| {
        fields[field_idx] = try arrow.fuzz_input.array(input, &sch.field_types[field_idx], len, alloc);
    }

    sch.check(fields) catch unreachable;

    return fields;
}
