const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const arrow = @import("arrow");
const arr = arrow.array;
const data_type = arrow.data_type;

const dict_impl = @import("./dict.zig");
const chunk_mod = @import("./chunk.zig");
const schema_mod = @import("./schema.zig");
const header_mod = @import("./header.zig");
const compression_mod = @import("./compression.zig");

const Error = error{ OutOfMemory, ShortInput };

const MAX_DEPTH = 10;

/// This struct implements structured fuzzing.
///
/// It generates headers, schemas, chunk etc. based on given fuzzer generated random data.
/// Normal random number generator seeded by fuzzer input is used in some places where the specific values don't change execution but we want them to be random.
///
/// It consumes the `data` buffer as it generates more output. Which means the `data` field will point to progressively smaller slices as it is consumed.
pub const FuzzInput = struct {
    inner: arrow.fuzz_input.FuzzInput,

    pub fn init(input: []const u8) FuzzInput {
        return .{
            .inner = .{ .data = input },
        };
    }

    pub fn make_chunk(self: *FuzzInput, alloc: Allocator, scratch_alloc: Allocator) Error!chunk_mod.Chunk {
        const num_tables = (try self.inner.int(u8)) % 10 + 1;
        const num_dicts = (try self.inner.int(u8)) % 4;

        const dicts = try alloc.alloc(arr.FixedSizeBinaryArray, num_dicts);
        for (0..num_dicts) |dict_idx| {
            dicts[dict_idx] = try self.inner.fixed_size_binary_array((try self.inner.int(u8)) % 100 + 1, alloc);
        }

        const table_num_fields = try scratch_alloc.alloc(u8, num_tables);
        for (0..num_tables) |table_idx| {
            table_num_fields[table_idx] = (try self.inner.int(u8)) % 12 + 1;
        }

        const dict_schemas = try alloc.alloc(schema_mod.DictSchema, num_dicts);
        for (0..num_dicts) |dict_idx| {
            const num_members = (try self.inner.int(u8)) % 10;
            const members = try alloc.alloc(schema_mod.DictMember, num_members);

            var found_members: usize = 0;
            membergen: for (0..num_members) |_| {
                const table_index = (try self.inner.int(u8)) % num_tables;
                const field_index = (try self.inner.int(u8)) % table_num_fields[table_index];

                for (members[0..found_members]) |memb| {
                    if (memb.table_index == table_index and memb.field_index == field_index) {
                        // this dict already has this member
                        continue :membergen;
                    }
                }

                for (dict_schemas[0..dict_idx]) |other_dict| {
                    for (other_dict.members) |memb| {
                        if (memb.table_index == table_index and memb.field_index == field_index) {
                            // other dict has this member
                            continue :membergen;
                        }
                    }
                }

                members[found_members] = schema_mod.DictMember{
                    .table_index = table_index,
                    .field_index = field_index,
                };
                found_members += 1;
            }

            dict_schemas[dict_idx] = schema_mod.DictSchema{
                .members = members[0..found_members],
                .has_filter = true,
                .byte_width = dicts[dict_idx].byte_width,
            };
        }

        const tables = try alloc.alloc([]const arr.Array, num_tables);
        for (0..num_tables) |table_idx| {
            const num_fields = table_num_fields[table_idx];
            const num_rows = try self.inner.int(u8);

            const fields = try alloc.alloc(arr.Array, num_fields);
            for (0..num_fields) |field_idx| {
                if (schema_mod.find_dict_idx(dict_schemas, table_idx, field_idx)) |dict_idx| {
                    fields[field_idx] = try self.make_dicted_array(num_rows, &dicts[dict_idx], alloc);
                } else {
                    fields[field_idx] = try self.inner.make_array(num_rows, alloc);
                }
            }

            tables[table_idx] = fields;
        }

        var prng = try self.inner.make_prng();
        const rand = prng.random();

        const table_schemas = try alloc.alloc(schema_mod.TableSchema, num_tables);
        const table_names = try alloc.alloc([:0]const u8, num_tables);
        for (0..num_tables) |table_idx| {
            const fields = tables[table_idx];
            const num_fields = fields.len;

            const has_minmax_index = try alloc.alloc(bool, num_fields);
            const data_types = try alloc.alloc(data_type.DataType, num_fields);
            const field_names = try alloc.alloc([:0]const u8, num_fields);

            for (0..num_fields) |field_idx| {
                const dt = data_type.get_data_type(&fields[field_idx], alloc) catch |e| {
                    switch (e) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => unreachable,
                    }
                };

                data_types[field_idx] = dt;
                has_minmax_index[field_idx] = schema_mod.can_have_minmax_index(dt) and (try self.inner.boolean());

                field_names[field_idx] = try make_name(field_names[0..field_idx], rand, alloc);
            }

            table_names[table_idx] = try make_name(table_names[0..table_idx], rand, alloc);

            table_schemas[table_idx] = schema_mod.TableSchema{
                .has_minmax_index = has_minmax_index,
                .data_types = data_types,
                .field_names = field_names,
            };
        }

        const schema = try alloc.create(schema_mod.DatasetSchema);
        schema.* = schema_mod.DatasetSchema{
            .tables = table_schemas,
            .table_names = table_names,
            .dicts = dict_schemas,
        };

        schema.validate() catch unreachable;
        schema.check(tables) catch unreachable;

        const chunk = chunk_mod.Chunk.from_arrow(schema, tables, alloc, scratch_alloc) catch |e| {
            switch (e) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            }
        };
        std.debug.assert(chunk.dicts.len == schema.dicts.len);

        const out_tables = chunk.to_arrow(scratch_alloc) catch unreachable;

        std.debug.assert(out_tables.len == tables.len);
        for (out_tables, tables) |out_table, table| {
            std.debug.assert(out_table.len == table.len);
            for (out_table, table) |*out_field, *field| {
                arrow.equals.equals(out_field, field);
            }
        }

        return chunk;
    }

    pub fn make_schema(self: *FuzzInput, alloc: Allocator, scratch_alloc: Allocator) Error!schema_mod.DatasetSchema {
        const num_tables = (try self.inner.int(u8)) % 10 + 1;
        const num_dicts = (try self.inner.int(u8)) % 4;

        const table_num_fields = try scratch_alloc.alloc(u8, num_tables);
        for (0..num_tables) |table_idx| {
            table_num_fields[table_idx] = (try self.inner.int(u8)) % 12 + 1;
        }

        const dict_schemas = try alloc.alloc(schema_mod.DictSchema, num_dicts);
        for (0..num_dicts) |dict_idx| {
            const num_members = (try self.inner.int(u8)) % 10;
            const members = try alloc.alloc(schema_mod.DictMember, num_members);

            var found_members: usize = 0;
            membergen: for (0..num_members) |_| {
                const table_index = (try self.inner.int(u8)) % num_tables;
                const field_index = (try self.inner.int(u8)) % table_num_fields[table_index];

                for (members[0..found_members]) |memb| {
                    if (memb.table_index == table_index and memb.field_index == field_index) {
                        // this dict already has this member
                        continue :membergen;
                    }
                }

                for (dict_schemas[0..dict_idx]) |other_dict| {
                    for (other_dict.members) |memb| {
                        if (memb.table_index == table_index and memb.field_index == field_index) {
                            // other dict has this member
                            continue :membergen;
                        }
                    }
                }

                members[found_members] = schema_mod.DictMember{
                    .table_index = table_index,
                    .field_index = field_index,
                };
                found_members += 1;
            }

            dict_schemas[dict_idx] = schema_mod.DictSchema{
                .members = members[0..found_members],
                .has_filter = try self.inner.boolean(),
                .byte_width = try self.inner.fixed_size_binary_width(),
            };
        }

        var prng = try self.inner.make_prng();
        const rand = prng.random();

        const table_schemas = try alloc.alloc(schema_mod.TableSchema, num_tables);
        const table_names = try alloc.alloc([:0]const u8, num_tables);
        for (0..num_tables) |table_idx| {
            const num_fields = table_num_fields[table_idx];

            const has_minmax_index = try alloc.alloc(bool, num_fields);
            const data_types = try alloc.alloc(data_type.DataType, num_fields);
            const field_names = try alloc.alloc([:0]const u8, num_fields);

            for (0..num_fields) |field_idx| {
                const dt = if (schema_mod.find_dict_idx(dict_schemas, table_idx, field_idx) == null)
                    try self.inner.make_data_type(alloc)
                else
                    try self.make_dicted_data_type();

                data_types[field_idx] = dt;

                has_minmax_index[field_idx] = schema_mod.can_have_minmax_index(dt) and (try self.inner.boolean());

                field_names[field_idx] = try make_name(field_names[0..field_idx], rand, alloc);
            }

            table_names[table_idx] = try make_name(table_names[0..table_idx], rand, alloc);

            table_schemas[table_idx] = schema_mod.TableSchema{
                .has_minmax_index = has_minmax_index,
                .data_types = data_types,
                .field_names = field_names,
            };
        }

        const schema = schema_mod.DatasetSchema{
            .tables = table_schemas,
            .table_names = table_names,
            .dicts = dict_schemas,
        };

        schema.validate() catch unreachable;

        return schema;
    }

    fn make_dicted_data_type(self: *FuzzInput) Error!data_type.DataType {
        return switch ((try self.inner.int(u8)) % 7) {
            0 => .{ .binary = {} },
            1 => .{ .large_binary = {} },
            2 => .{ .binary_view = {} },
            3 => .{ .fixed_size_binary = try self.inner.fixed_size_binary_width() },
            4 => .{ .utf8 = {} },
            5 => .{ .large_utf8 = {} },
            6 => .{ .utf8_view = {} },
            else => unreachable,
        };
    }

    fn make_dicted_array(input: *FuzzInput, len: u32, dict_array: *const arr.FixedSizeBinaryArray, alloc: Allocator) Error!arr.Array {
        const keys = make_keys: {
            const keys_raw = try input.inner.primitive_array(u32, len, alloc);

            const keys = try alloc.alloc(u32, keys_raw.offset + keys_raw.len);

            var idx: u32 = 0;
            while (idx < keys_raw.offset + keys_raw.len) : (idx += 1) {
                keys.ptr[idx] = keys_raw.values.ptr[idx] % dict_array.len;
            }

            break :make_keys arr.UInt32Array{
                .values = keys,
                .validity = keys_raw.validity,
                .offset = keys_raw.offset,
                .len = keys_raw.len,
                .null_count = keys_raw.null_count,
            };
        };

        const dt: data_type.DataType = switch ((try input.inner.int(u8)) % 7) {
            0 => .{ .binary = {} },
            1 => .{ .large_binary = {} },
            2 => .{ .binary_view = {} },
            3 => .{ .fixed_size_binary = dict_array.byte_width },
            4 => .{ .utf8 = {} },
            5 => .{ .large_utf8 = {} },
            6 => .{ .utf8_view = {} },
            else => unreachable,
        };

        return dict_impl.unpack_dict(dict_array, &keys, dt, alloc) catch |e| {
            switch (e) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            }
        };
    }

    pub fn make_header(self: *FuzzInput, alloc: Allocator) Error!header_mod.Header {
        const num_dicts = try self.inner.int(u8);
        const dicts = try alloc.alloc(header_mod.Dict, num_dicts);
        for (0..num_dicts) |dict_idx| {
            const data = try self.fixed_size_binary_array_header(try self.inner.int(u8), alloc);
            dicts[dict_idx] = header_mod.Dict{
                .data = data,
                .filter = null,
            };
        }

        const num_tables = try self.inner.int(u8);
        const tables = try alloc.alloc(header_mod.Table, num_tables);
        for (0..num_tables) |table_idx| {
            const num_fields = try self.inner.int(u8);

            const fields = try alloc.alloc(header_mod.Array, num_fields);

            for (0..num_fields) |field_idx| {
                fields[field_idx] = try self.make_array_header(try self.inner.int(u8), alloc, 0);
            }

            tables[table_idx] = header_mod.Table{
                .fields = fields,
                .num_rows = try self.inner.int(u32),
            };
        }

        return .{
            .tables = tables,
            .dicts = dicts,
            .data_section_size = try self.inner.int(u32),
        };
    }

    pub fn make_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.Array {
        if (depth > MAX_DEPTH) {
            return .{ .null = .{ .len = len } };
        }

        const kind = (try self.inner.int(u8)) % 28;

        return switch (kind) {
            0 => .{ .null = .{ .len = len } },
            1 => .{ .i8 = try self.primitive_array_header(i8, len, alloc) },
            2 => .{ .i16 = try self.primitive_array_header(i16, len, alloc) },
            3 => .{ .i32 = try self.primitive_array_header(i32, len, alloc) },
            4 => .{ .i64 = try self.primitive_array_header(i64, len, alloc) },
            5 => .{ .u8 = try self.primitive_array_header(u8, len, alloc) },
            6 => .{ .u16 = try self.primitive_array_header(u16, len, alloc) },
            7 => .{ .u32 = try self.primitive_array_header(u32, len, alloc) },
            8 => .{ .u64 = try self.primitive_array_header(u64, len, alloc) },
            9 => .{ .i128 = try self.primitive_array_header(i128, len, alloc) },
            10 => .{ .i256 = try self.primitive_array_header(i256, len, alloc) },
            11 => .{ .f16 = try self.primitive_array_header(f16, len, alloc) },
            12 => .{ .f32 = try self.primitive_array_header(f32, len, alloc) },
            13 => .{ .f64 = try self.primitive_array_header(f64, len, alloc) },
            14 => .{ .binary = try self.binary_array_header(len, alloc) },
            15 => .{ .bool = try self.bool_array_header(len, alloc) },
            16 => .{ .list = try self.list_array_header(len, alloc, depth) },
            17 => .{ .struct_ = try self.struct_array_header(len, alloc, depth) },
            18 => .{ .dense_union = try self.dense_union_array_header(len, alloc, depth) },
            19 => .{ .sparse_union = try self.sparse_union_array_header(len, alloc, depth) },
            20 => .{ .fixed_size_binary = try self.fixed_size_binary_array_header(len, alloc) },
            21 => .{ .fixed_size_list = try self.fixed_size_list_array_header(len, alloc, depth) },
            22 => .{ .map = try self.map_array_header(len, alloc, depth) },
            23 => .{ .run_end_encoded = try self.run_end_encoded_array_header(len, alloc, depth) },
            24 => .{ .dict = try self.dict_array_header(len, alloc, depth) },
            25 => .{ .interval_year_month = try self.interval_array_header(len, alloc) },
            26 => .{ .interval_day_time = try self.interval_array_header(len, alloc) },
            27 => .{ .interval_month_day_nano = try self.interval_array_header(len, alloc) },
            else => unreachable,
        };
    }

    pub fn interval_array_header(self: *FuzzInput, len: u32, alloc: Allocator) Error!header_mod.IntervalArray {
        return header_mod.IntervalArray{
            .len = len,
            .values = try self.buffer_header(alloc),
            .validity = try self.validity_header(alloc),
        };
    }

    pub fn dict_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.DictArray {
        const values = try alloc.create(header_mod.Array);
        values.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);
        const keys = try alloc.create(header_mod.Array);
        keys.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);

        return header_mod.DictArray{
            .values = values,
            .keys = keys,
            .is_ordered = try self.inner.boolean(),
            .len = len,
        };
    }

    pub fn run_end_encoded_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.RunEndArray {
        const values = try alloc.create(header_mod.Array);
        values.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);
        const run_ends = try alloc.create(header_mod.Array);
        run_ends.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);

        return header_mod.RunEndArray{
            .len = len,
            .values = values,
            .run_ends = run_ends,
        };
    }

    pub fn map_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.MapArray {
        const entries = try alloc.create(header_mod.StructArray);
        entries.* = try self.struct_array_header(try self.inner.int(u8), alloc, depth);

        return header_mod.MapArray{
            .len = len,
            .validity = try self.validity_header(alloc),
            .offsets = try self.buffer_header(alloc),
            .keys_are_sorted = try self.inner.boolean(),
            .entries = entries,
        };
    }

    pub fn fixed_size_list_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.FixedSizeListArray {
        const inner = try alloc.create(header_mod.Array);
        inner.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);

        return header_mod.FixedSizeListArray{
            .validity = try self.validity_header(alloc),
            .len = len,
            .inner = inner,
        };
    }

    pub fn fixed_size_binary_array_header(self: *FuzzInput, len: u32, alloc: Allocator) Error!header_mod.FixedSizeBinaryArray {
        return header_mod.FixedSizeBinaryArray{
            .len = len,
            .validity = try self.validity_header(alloc),
            .data = try self.buffer_header(alloc),
            .minmax = try self.binary_minmax(alloc),
        };
    }

    pub fn sparse_union_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.SparseUnionArray {
        return header_mod.SparseUnionArray{
            .inner = try self.union_array_header(len, alloc, depth),
        };
    }

    pub fn dense_union_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.DenseUnionArray {
        return header_mod.DenseUnionArray{
            .inner = try self.union_array_header(len, alloc, depth),
            .offsets = try self.buffer_header(alloc),
        };
    }

    pub fn union_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.UnionArray {
        const num_fields = try self.inner.int(u8);
        const field_values = try alloc.alloc(header_mod.Array, num_fields);
        for (0..num_fields) |field_idx| {
            field_values[field_idx] = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);
        }

        return header_mod.UnionArray{
            .len = len,
            .type_ids = try self.buffer_header(alloc),
            .children = field_values,
        };
    }

    pub fn struct_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.StructArray {
        const num_fields = try self.inner.int(u8);
        const field_values = try alloc.alloc(header_mod.Array, num_fields);
        for (0..num_fields) |field_idx| {
            field_values[field_idx] = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);
        }

        return header_mod.StructArray{
            .len = len,
            .validity = try self.validity_header(alloc),
            .field_values = field_values,
        };
    }

    pub fn list_array_header(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!header_mod.ListArray {
        const inner = try alloc.create(header_mod.Array);
        inner.* = try self.make_array_header(try self.inner.int(u8), alloc, depth + 1);

        return header_mod.ListArray{
            .offsets = try self.buffer_header(alloc),
            .validity = try self.validity_header(alloc),
            .len = len,
            .inner = inner,
        };
    }

    pub fn bool_array_header(self: *FuzzInput, len: u32, alloc: Allocator) Error!header_mod.BoolArray {
        return header_mod.BoolArray{
            .validity = try self.validity_header(alloc),
            .values = try self.buffer_header(alloc),
            .len = len,
        };
    }

    pub fn binary_minmax(self: *FuzzInput, alloc: Allocator) Error!?[]const ?header_mod.MinMax([]const u8) {
        var minmax: ?[]const ?header_mod.MinMax([]const u8) = null;

        if (try self.inner.boolean()) {
            const num_minmax = try self.inner.int(u8);
            const mm = try alloc.alloc(?header_mod.MinMax([]const u8), num_minmax);

            var prng = try self.inner.make_prng();
            const rand = prng.random();

            for (0..num_minmax) |mm_idx| {
                if (try self.inner.boolean()) {
                    const min_len = try self.inner.int(u8);
                    const max_len = try self.inner.int(u8);

                    const min = try alloc.alloc(u8, min_len);
                    const max = try alloc.alloc(u8, max_len);

                    rand.bytes(min);
                    rand.bytes(max);

                    mm[mm_idx] = .{
                        .min = min,
                        .max = max,
                    };
                } else {
                    mm[mm_idx] = null;
                }
            }

            minmax = mm;
        }

        return minmax;
    }

    pub fn binary_array_header(self: *FuzzInput, len: u32, alloc: Allocator) Error!header_mod.BinaryArray {
        return header_mod.BinaryArray{
            .validity = try self.validity_header(alloc),
            .data = try self.buffer_header(alloc),
            .len = len,
            .offsets = try self.buffer_header(alloc),
            .minmax = try self.binary_minmax(alloc),
        };
    }

    pub fn primitive_array_header(self: *FuzzInput, comptime T: type, len: u32, alloc: Allocator) Error!header_mod.PrimitiveArray(T) {
        var minmax: ?[]const ?header_mod.MinMax(T) = null;

        if (try self.inner.boolean()) {
            const num_minmax = try self.inner.int(u8);
            const mm = try alloc.alloc(?header_mod.MinMax(T), num_minmax);

            var prng = try self.inner.make_prng();
            const rand = prng.random();

            for (0..num_minmax) |mm_idx| {
                if (try self.inner.boolean()) {
                    switch (@typeInfo(T)) {
                        .float => {
                            mm[mm_idx] = .{
                                .min = @floatCast(rand.float(f64)),
                                .max = @floatCast(rand.float(f64)),
                            };
                        },
                        .int => {
                            mm[mm_idx] = .{
                                .min = rand.int(T),
                                .max = rand.int(T),
                            };
                        },
                        else => unreachable,
                    }
                } else {
                    mm[mm_idx] = null;
                }
            }

            minmax = mm;
        }

        return header_mod.PrimitiveArray(T){
            .validity = try self.validity_header(alloc),
            .values = try self.buffer_header(alloc),
            .len = len,
            .minmax = minmax,
        };
    }

    pub fn validity_header(self: *FuzzInput, alloc: Allocator) Error!?header_mod.Buffer {
        return if (try self.inner.boolean())
            try self.buffer_header(alloc)
        else
            null;
    }

    pub fn buffer_header(self: *FuzzInput, alloc: Allocator) Error!header_mod.Buffer {
        const num_pages = try self.inner.int(u8);
        const pages = try alloc.alloc(header_mod.Page, num_pages);
        for (0..num_pages) |page_idx| {
            pages[page_idx] = header_mod.Page{
                .offset = try self.inner.int(u32),
                .compressed_size = try self.inner.int(u32),
                .uncompressed_size = try self.inner.int(u32),
            };
        }

        const compression = switch ((try self.inner.int(u8)) % 4) {
            0 => compression_mod.Compression{ .zstd = try self.inner.int(u8) },
            1 => compression_mod.Compression{ .lz4_hc = try self.inner.int(u8) },
            2 => compression_mod.Compression{ .lz4 = {} },
            3 => compression_mod.Compression{ .no_compression = {} },
            else => unreachable,
        };

        const num_row_index_ends = try self.inner.int(u8);
        const row_index_ends = try self.inner.slice(u32, num_row_index_ends, alloc);

        return header_mod.Buffer{
            .pages = pages,
            .compression = compression,
            .row_index_ends = row_index_ends,
        };
    }
};

fn rand_bytes_zero_sentinel(rand: std.Random, out: []u8) void {
    rand.bytes(out);

    for (0..out.len) |i| {
        if (out.ptr[i] == 0) {
            out.ptr[i] = 1;
        }
    }
}

fn make_name(existing_names: []const []const u8, rand: std.Random, alloc: Allocator) ![:0]const u8 {
    const name_len = rand.int(u8) % 30 + 1;
    const name = try alloc.allocSentinel(u8, name_len, 0);

    namegen: while (true) {
        rand_bytes_zero_sentinel(rand, name);

        for (existing_names) |other_name| {
            if (std.mem.eql(u8, name, other_name)) {
                continue :namegen;
            }
        }
        break;
    }

    return name;
}
