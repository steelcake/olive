const std = @import("std");
const Allocator = std.mem.Allocator;
const xxhash3_64 = std.hash.XxHash3.hash;

const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;

const schema = @import("./schema.zig");

pub fn hash_fixed(comptime W: comptime_int, input: [W]u8) u64 {
    return xxhash3_64(0x876f170be4f1fcb9, input);
}

pub fn DictFn(comptime W: comptime_int) type {
    switch (W) {
        20, 32 => {},
        else => @compileError("unsupported width"),
    }

    return struct {
        // dictionary value type
        pub const T = [W]u8;

        const HashMapContext = struct {
            const Self = @This();

            pub fn hash(self: Self, val: T) u64 {
                _ = self;
                return hash_fixed(W, val);
            }

            pub fn eql(self: Self, a: T, b: T) bool {
                _ = self;
                var x = true;
                inline for (0..W) |i| {
                    x &= a[i] == b[i];
                }
                return x;
            }
        };

        // maps the dictionary value to the index in the dictionary
        pub const Builder = std.HashMapUnmanaged(
            T,
            u32,
            HashMapContext,
            std.hash_map.default_max_load_percentage,
        );

        pub fn unpack_array(
            noalias dict: []const T,
            noalias array: *const arr.UInt32Array,
            alloc: Allocator,
        ) error{OutOfMemory}!arr.FixedSizeBinaryArray {
            const data = try alloc.alloc(T, array.len);

            if (dict.len == 0) {
                std.debug.assert(array.len == array.null_count);
                @memset(@as([]u8, @ptrCast(data)), 0);
            } else {
                var idx: u32 = 0;
                while (idx < array.len) : (idx += 1) {
                    data[idx] = dict[array.values[array.offset + idx]];
                }
            }

            const validity = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            return arr.FixedSizeBinaryArray{
                .offset = 0,
                .len = array.len,
                .byte_width = W,
                .data = @ptrCast(data),
                .validity = validity,
                .null_count = array.null_count,
            };
        }

        pub fn push_array_to_builder(
            noalias array: *const arr.FixedSizeBinaryArray,
            noalias builder: *Builder,
            alloc: Allocator,
        ) error{OutOfMemory}!void {
            std.debug.assert(array.byte_width == W);

            if (array.len == 0) return;

            if (array.null_count > 0) {
                const validity = array.validity orelse unreachable;

                if (array.null_count == array.len) return;
                try builder.ensureUnusedCapacity(alloc, array.len - array.null_count);

                const Closure = struct {
                    a: *const arr.FixedSizeBinaryArray,
                    b: *Builder,

                    fn process(self: @This(), idx: u32) void {
                        const byte_offset = idx * W;
                        self.b.putAssumeCapacity(
                            @as([]const T, @ptrCast(self.a.data[byte_offset .. byte_offset + W]))[0],
                            0,
                        );
                    }
                };

                arrow.bitmap.for_each(
                    Closure,
                    Closure.process,
                    Closure{
                        .a = array,
                        .b = builder,
                    },
                    validity,
                    array.offset,
                    array.len,
                );
            } else {
                const data: []const T = @ptrCast(array.data[array.offset * W .. (array.offset + array.len) * W]);
                try builder.ensureUnusedCapacity(alloc, @intCast(data.len));
                for (data) |v| {
                    builder.putAssumeCapacity(v, 0);
                }
            }
        }

        pub fn apply_builder_to_array(
            noalias builder: *const Builder,
            noalias array: *const arr.FixedSizeBinaryArray,
            alloc: Allocator,
        ) error{OutOfMemory}!arr.UInt32Array {
            const values = try alloc.alloc(u32, array.len);

            const data: []const T = @ptrCast(
                array.data[array.offset * W .. (array.offset + array.len) * W],
            );

            var idx: u32 = 0;
            while (idx < array.len) : (idx += 1) {
                values[idx] = builder.get(data[idx]) orelse 0;
            }

            const validity = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            return arr.UInt32Array{
                .values = values,
                .len = array.len,
                .offset = 0,
                .validity = validity,
                .null_count = array.null_count,
            };
        }

        pub fn build_dict(
            noalias builder: *Builder,
            alloc: Allocator,
        ) error{OutOfMemory}![]const T {
            var elems = try alloc.alloc(T, builder.size);
            var iter = builder.keyIterator();
            var idx: u32 = 0;
            while (iter.next()) |v| {
                elems[idx] = v.*;
                idx += 1;
            }

            std.mem.sortUnstable(T, elems, {}, less_than_fn);

            idx = 0;
            for (elems) |v| {
                builder.putAssumeCapacity(v, idx);
                idx += 1;
            }

            return elems;
        }

        pub fn less_than_fn(_: void, l: T, r: T) bool {
            var x = true;
            inline for (0..W) |idx| {
                x &= l[idx] < r[idx];
            }
            return x;
        }
    };
}

pub const DictFn32 = DictFn(32);
pub const DictFn20 = DictFn(20);

pub const DictWidths = .{ 20, 32 };

pub const DictContext = struct {
    dict20: []const [20]u8,
    dict32: []const [32]u8,
};

const BuilderContext = struct {
    dict32builder: *DictFn32.Builder,
    dict20builder: *DictFn20.Builder,
};

pub fn decode_chunk(
    ctx: DictContext,
    chunk_schema: *const schema.Schema,
    tables: []const []const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}![]const []const arr.Array {
    if (tables.len == 0) return &.{};

    const tables_o = try alloc.alloc([]const arr.Array, tables.len);
    for (0..tables.len) |idx| {
        tables_o[idx] = try unpack_table(
            ctx,
            &chunk_schema.table_schemas[idx],
            tables[idx],
            alloc,
        );
    }

    return tables_o;
}

fn unpack_table(
    ctx: DictContext,
    table_schema: *const schema.TableSchema,
    table: []const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}![]const arr.Array {
    if (table.len == 0) return table;

    var out = try alloc.alloc(arr.Array, table.len);
    for (0..table.len) |idx| {
        out[idx] = try unpack_field(
            ctx,
            &table_schema.field_types[idx],
            &table[idx],
            alloc,
        );
    }

    return out;
}

fn unpack_field(
    ctx: DictContext,
    dt: *const DataType,
    field: *const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}!arr.Array {
    switch (dt.*) {
        .null,
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .f16,
        .f32,
        .f64,
        .binary,
        .utf8,
        .bool,
        .date32,
        .date64,
        .interval_year_month,
        .interval_day_time,
        .interval_month_day_nano,
        .large_binary,
        .large_utf8,
        .binary_view,
        .utf8_view,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .time32,
        .time64,
        .timestamp,
        .duration,
        => return field.*,
        .fixed_size_binary => |bw| {
            switch (bw) {
                20 => return .{ .fixed_size_binary = try DictFn20.unpack_array(
                    ctx.dict20,
                    &field.u32,
                    alloc,
                ) },
                32 => return .{ .fixed_size_binary = try DictFn32.unpack_array(
                    ctx.dict32,
                    &field.u32,
                    alloc,
                ) },
                else => return field.*,
            }
        },
        .list => |a| {
            var out = field.list;

            const inner = try alloc.create(arr.Array);
            inner.* = try unpack_field(
                ctx,
                a,
                out.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .list = out };
        },
        .fixed_size_list => |a| {
            var out = field.fixed_size_list;

            const inner = try alloc.create(arr.Array);
            inner.* = try unpack_field(
                ctx,
                &a.inner,
                out.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .fixed_size_list = out };
        },
        .large_list => |a| {
            var out = field.large_list;

            const inner = try alloc.create(arr.Array);
            inner.* = try unpack_field(
                ctx,
                a,
                out.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .large_list = out };
        },
        .list_view => |a| {
            var out = field.list_view;

            const inner = try alloc.create(arr.Array);
            inner.* = try unpack_field(
                ctx,
                a,
                out.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .list_view = out };
        },
        .large_list_view => |a| {
            var out = field.large_list_view;

            const inner = try alloc.create(arr.Array);
            inner.* = try unpack_field(
                ctx,
                a,
                out.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .large_list_view = out };
        },
        .struct_ => |a| {
            var out = field.struct_;

            const field_values = try alloc.alloc(arr.Array, out.field_values.len);
            for (a.field_types, out.field_values, 0..) |*ft, *fv, idx| {
                field_values[idx] = try unpack_field(
                    ctx,
                    ft,
                    fv,
                    alloc,
                );
            }
            out.field_values = field_values;

            return .{ .struct_ = out };
        },
        .dense_union => |a| {
            var out = field.dense_union;

            const children = try alloc.alloc(arr.Array, out.inner.children.len);
            for (out.inner.children, a.field_types, 0..) |*fv, *ft, idx| {
                children[idx] = try unpack_field(
                    ctx,
                    ft,
                    fv,
                    alloc,
                );
            }
            out.inner.children = children;

            return .{ .dense_union = out };
        },
        .sparse_union => |a| {
            var out = field.sparse_union;

            const children = try alloc.alloc(arr.Array, out.inner.children.len);
            for (out.inner.children, a.field_types, 0..) |*fv, *ft, idx| {
                children[idx] = try unpack_field(
                    ctx,
                    ft,
                    fv,
                    alloc,
                );
            }
            out.inner.children = children;

            return .{ .sparse_union = out };
        },
        .map => |a| {
            var out = field.map;

            const field_names = .{ "keys", "values" };
            const field_types = .{ a.key.to_data_type(), a.value };

            const struct_t = try alloc.create(arrow.data_type.StructType);
            struct_t.* = arrow.data_type.StructType{
                .field_names = &field_names,
                .field_types = &field_types,
            };

            const st = DataType{
                .struct_ = struct_t,
            };

            const entries = try alloc.create(arr.StructArray);
            entries.* = (try unpack_field(
                ctx,
                &st,
                &.{ .struct_ = out.entries.* },
                alloc,
            )).struct_;
            out.entries = entries;

            return .{ .map = out };
        },
        .run_end_encoded => |a| {
            var out = field.run_end_encoded;

            const values = try alloc.create(arr.Array);
            values.* = try unpack_field(
                ctx,
                &a.value,
                out.values,
                alloc,
            );

            out.values = values;

            return .{ .run_end_encoded = out };
        },
        .dict => |a| {
            var out = field.dict;

            const values = try alloc.create(arr.Array);
            values.* = try unpack_field(
                ctx,
                &a.value,
                out.values,
                alloc,
            );
            out.values = values;

            return .{ .dict = out };
        },
    }
}

pub fn encode_chunk(
    tables: []const []const arr.Array,
    alloc: Allocator,
    scratch_alloc: Allocator,
) error{OutOfMemory}!struct {
    tables: []const []const arr.Array,
    context: DictContext,
} {
    std.debug.assert(tables.len > 0);

    var total_num_rows: u32 = 0;
    for (tables) |t| {
        total_num_rows += arrow.length.length(&t[0]);
    }

    var dict32builder = DictFn32.Builder.empty;
    try dict32builder.ensureTotalCapacity(scratch_alloc, total_num_rows);
    var dict20builder = DictFn20.Builder.empty;
    try dict20builder.ensureTotalCapacity(scratch_alloc, total_num_rows);

    const builder_ctx = BuilderContext{
        .dict32builder = &dict32builder,
        .dict20builder = &dict20builder,
    };

    for (tables) |table| {
        try push_table_to_builders(builder_ctx, table, scratch_alloc);
    }

    const ctx = DictContext{
        .dict32 = try DictFn32.build_dict(&dict32builder, alloc),
        .dict20 = try DictFn20.build_dict(&dict20builder, alloc),
    };

    const tables_o = try alloc.alloc([]const arr.Array, tables.len);
    for (0..tables.len) |idx| {
        tables_o[idx] = try apply_builders_to_table(builder_ctx, tables[idx], alloc);
    }

    return .{
        .tables = tables_o,
        .context = ctx,
    };
}

fn push_table_to_builders(
    ctx: BuilderContext,
    table: []const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}!void {
    std.debug.assert(table.len > 0);

    for (table) |*field| {
        try push_field_to_builders(
            ctx,
            field,
            alloc,
        );
    }
}

fn push_field_to_builders(
    ctx: BuilderContext,
    field: *const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}!void {
    switch (field.*) {
        .null,
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .f16,
        .f32,
        .f64,
        .binary,
        .utf8,
        .bool,
        .date32,
        .date64,
        .interval_year_month,
        .interval_day_time,
        .interval_month_day_nano,
        .large_binary,
        .large_utf8,
        .binary_view,
        .utf8_view,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .time32,
        .time64,
        .timestamp,
        .duration,
        => {},
        .fixed_size_binary => |*a| {
            switch (a.byte_width) {
                20 => try DictFn20.push_array_to_builder(
                    a,
                    ctx.dict20builder,
                    alloc,
                ),
                32 => try DictFn32.push_array_to_builder(
                    a,
                    ctx.dict32builder,
                    alloc,
                ),
                else => {},
            }
        },
        .list => |*a| {
            try push_field_to_builders(
                ctx,
                a.inner,
                alloc,
            );
        },
        .fixed_size_list => |*a| {
            try push_field_to_builders(
                ctx,
                a.inner,
                alloc,
            );
        },
        .large_list => |*a| {
            try push_field_to_builders(
                ctx,
                a.inner,
                alloc,
            );
        },
        .list_view => |*a| {
            try push_field_to_builders(
                ctx,
                a.inner,
                alloc,
            );
        },
        .large_list_view => |*a| {
            try push_field_to_builders(
                ctx,
                a.inner,
                alloc,
            );
        },
        .struct_ => |*a| {
            for (a.field_values) |*inner| {
                try push_field_to_builders(
                    ctx,
                    inner,
                    alloc,
                );
            }
        },
        .dense_union => |*a| {
            for (a.inner.children) |*inner| {
                try push_field_to_builders(
                    ctx,
                    inner,
                    alloc,
                );
            }
        },
        .sparse_union => |*a| {
            for (a.inner.children) |*inner| {
                try push_field_to_builders(
                    ctx,
                    inner,
                    alloc,
                );
            }
        },
        .map => |*a| {
            for (a.entries.field_values) |*inner| {
                try push_field_to_builders(
                    ctx,
                    inner,
                    alloc,
                );
            }
        },
        .run_end_encoded => |*a| {
            try push_field_to_builders(
                ctx,
                a.values,
                alloc,
            );
        },
        .dict => |*a| {
            try push_field_to_builders(
                ctx,
                a.values,
                alloc,
            );
        },
    }
}

fn apply_builders_to_table(
    ctx: BuilderContext,
    table: []const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}![]const arr.Array {
    if (table.len == 0) return &.{};

    var out = try alloc.alloc(arr.Array, table.len);
    for (0..table.len) |idx| {
        out[idx] = try apply_builders_to_field(
            ctx,
            &table[idx],
            alloc,
        );
    }

    return out;
}

fn apply_builders_to_field(
    ctx: BuilderContext,
    field: *const arr.Array,
    alloc: Allocator,
) error{OutOfMemory}!arr.Array {
    switch (field.*) {
        .null,
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .f16,
        .f32,
        .f64,
        .binary,
        .utf8,
        .bool,
        .date32,
        .date64,
        .interval_year_month,
        .interval_day_time,
        .interval_month_day_nano,
        .large_binary,
        .large_utf8,
        .binary_view,
        .utf8_view,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .time32,
        .time64,
        .timestamp,
        .duration,
        => return field.*,
        .fixed_size_binary => |*a| {
            return switch (a.byte_width) {
                20 => return .{ .u32 = try DictFn20.apply_builder_to_array(
                    ctx.dict20builder,
                    a,
                    alloc,
                ) },
                32 => return .{ .u32 = try DictFn32.apply_builder_to_array(
                    ctx.dict32builder,
                    a,
                    alloc,
                ) },
                else => return field.*,
            };
        },
        .list => |*a| {
            var out = a.*;

            const inner = try alloc.create(arr.Array);
            inner.* = try apply_builders_to_field(
                ctx,
                a.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .list = out };
        },
        .fixed_size_list => |*a| {
            var out = a.*;

            const inner = try alloc.create(arr.Array);
            inner.* = try apply_builders_to_field(
                ctx,
                a.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .fixed_size_list = out };
        },
        .large_list => |*a| {
            var out = a.*;

            const inner = try alloc.create(arr.Array);
            inner.* = try apply_builders_to_field(
                ctx,
                a.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .large_list = out };
        },
        .list_view => |*a| {
            var out = a.*;

            const inner = try alloc.create(arr.Array);
            inner.* = try apply_builders_to_field(
                ctx,
                a.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .list_view = out };
        },
        .large_list_view => |*a| {
            var out = a.*;

            const inner = try alloc.create(arr.Array);
            inner.* = try apply_builders_to_field(
                ctx,
                a.inner,
                alloc,
            );
            out.inner = inner;

            return .{ .large_list_view = out };
        },
        .struct_ => |*a| {
            var out = a.*;

            const field_values = try alloc.alloc(arr.Array, out.field_values.len);
            for (out.field_values, 0..) |*fv, idx| {
                field_values[idx] = try apply_builders_to_field(
                    ctx,
                    fv,
                    alloc,
                );
            }
            out.field_values = field_values;

            return .{ .struct_ = out };
        },
        .dense_union => |*a| {
            var out = a.*;

            const children = try alloc.alloc(arr.Array, out.inner.children.len);
            for (out.inner.children, 0..) |*fv, idx| {
                children[idx] = try apply_builders_to_field(
                    ctx,
                    fv,
                    alloc,
                );
            }

            out.inner.children = children;

            return .{ .dense_union = out };
        },
        .sparse_union => |*a| {
            var out = a.*;

            const children = try alloc.alloc(arr.Array, out.inner.children.len);
            for (out.inner.children, 0..) |*fv, idx| {
                children[idx] = try apply_builders_to_field(
                    ctx,
                    fv,
                    alloc,
                );
            }

            out.inner.children = children;

            return .{ .sparse_union = out };
        },
        .map => |*a| {
            var out = a.*;

            const entries = try alloc.create(arr.StructArray);
            entries.* = (try apply_builders_to_field(
                ctx,
                &.{ .struct_ = a.entries.* },
                alloc,
            )).struct_;

            out.entries = entries;

            return .{ .map = out };
        },
        .run_end_encoded => |*a| {
            var out = a.*;

            const values = try alloc.create(arr.Array);
            values.* = try apply_builders_to_field(
                ctx,
                a.values,
                alloc,
            );

            out.values = values;

            return .{ .run_end_encoded = out };
        },
        .dict => |*a| {
            var out = a.*;

            const values = try alloc.create(arr.Array);
            values.* = try apply_builders_to_field(
                ctx,
                a.values,
                alloc,
            );

            out.values = values;

            return .{ .dict = out };
        },
    }
}

fn copy_validity(
    v: []const u8,
    offset: u32,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}![]const u8 {
    std.debug.assert(len > 0);

    const n_bytes = arrow.bitmap.num_bytes(len);

    const out = try alloc.alloc(u8, n_bytes);

    arrow.bitmap.copy(len, out, 0, v, offset);

    return out;
}
