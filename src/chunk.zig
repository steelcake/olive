const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const arr = arrow.array;

const schema_impl = @import("./schema.zig");
const DatasetSchema = schema_impl.DatasetSchema;
const dict_impl = @import("./dict.zig");

const Error = error{ OutOfMemory, NonBinaryArrayWithDict };

pub const Table = struct {
    fields: []const arr.Array,
    num_rows: u32,
};

pub const Chunk = struct {
    tables: []const Table,
    dicts: []const arr.BinaryArray,

    pub fn from_arrow(schema: *const DatasetSchema, tables: []const []const arr.Array, alloc: Allocator, scratch_alloc: Allocator) Error!Chunk {
        const out = try alloc.alloc(Table, tables.len);

        const num_dicts = schema.dicts.len;
        const dict_arrays = try alloc.alloc(arr.BinaryArray, num_dicts);

        for (schema.dicts, 0..) |dict, dict_idx| {
            var num_elems: usize = 0;

            for (dict.members) |member| {
                num_elems += try dict_impl.count_array_to_dict(&tables[member.table_index][member.field_index]);
            }

            var elems = try scratch_alloc.alloc([]const u8, num_elems);

            var write_idx: usize = 0;
            for (dict.members) |member| {
                const array = &tables[member.table_index][member.field_index];
                write_idx = try dict_impl.push_array_to_dict(array, write_idx, elems);
            }

            elems = dict_impl.sort_and_dedup(elems[0..write_idx]);

            const dict_array = arrow.builder.BinaryBuilder.from_slice(elems, false, alloc) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
            };

            dict_arrays[dict_idx] = dict_array;
        }

        for (tables, 0..) |table, table_idx| {
            const num_rows = for (table) |*field| {
                break arrow.length.length(field);
            } else 0;

            for (table) |*field| {
                std.debug.assert(arrow.length.length(field) == num_rows);
            }

            const fields = try alloc.alloc(arr.Array, table.len);

            for (table, 0..) |*array, field_idx| {
                if (dict_impl.find_dict(schema.dicts, dict_arrays, table_idx, field_idx)) |dict_arr| {
                    fields[field_idx] = .{ .u32 = try dict_impl.apply_dict(dict_arr, array, alloc) };
                } else {
                    fields[field_idx] = try arrow_to_olive_array(array, alloc, scratch_alloc);
                }
            }

            out[table_idx] = .{
                .fields = fields,
                .num_rows = num_rows,
            };
        }

        return .{
            .tables = out,
            .dicts = dict_arrays,
        };
    }
};

fn arrow_to_olive_array(array: *const arr.Array, alloc: Allocator, scratch_alloc: Allocator) Error!arr.Array {
    return switch (array.*) {
        .list_view => |*a| .{ .large_list = try list_view_to_list(.i32, a, alloc, scratch_alloc) },
        .large_list_view => |*a| .{ .large_list = try list_view_to_list(.i64, a, alloc, scratch_alloc) },
        .binary_view => |*a| .{ .large_binary = try binary_view_to_binary(a, alloc) },
        .utf8_view => |*a| .{ .large_utf8 = .{ .inner = try binary_view_to_binary(&a.inner, alloc) } },
        else => array.*,
    };
}

fn list_view_to_list(comptime index_t: arr.IndexType, array: *const arr.GenericListViewArray(index_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.LargeListArray {
    var builder = arrow.builder.LargeListBuilder.with_capacity(array.len, array.null_count > 0, alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    const inner_arrays_builder = try scratch_alloc.alloc(arr.Array, array.len);
    var num_inner_arrays: u32 = 0;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.bitmap.get(validity, idx)) {
                builder.append_item(array.sizes.ptr[idx]) catch unreachable;

                inner_arrays_builder[num_inner_arrays] = arrow.slice.slice(array.inner, @intCast(array.offsets[idx]), @intCast(array.sizes[idx]));
                num_inner_arrays += 1;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_item(array.sizes.ptr[idx]) catch unreachable;
            inner_arrays_builder[num_inner_arrays] = arrow.slice.slice(array.inner, @intCast(array.offsets[idx]), @intCast(array.sizes[idx]));
            num_inner_arrays += 1;
        }
    }

    const inner_arrays = inner_arrays_builder[0..num_inner_arrays];

    const inner_dt = arrow.data_type.get_data_type(array.inner, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };
    const inner = try alloc.create(arr.Array);
    inner.* = try arrow.concat.concat(inner_dt, inner_arrays, alloc, scratch_alloc);

    return builder.finish(inner) catch unreachable;
}

fn binary_view_to_binary(array: *const arr.BinaryViewArray, alloc: Allocator) Error!arr.LargeBinaryArray {
    var total_size: u32 = 0;

    var idx: u32 = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        total_size += @as(u32, @bitCast(array.views[idx].length));
    }

    var builder = arrow.builder.LargeBinaryBuilder.with_capacity(total_size, array.len, array.null_count > 0, alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_option(arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, idx)) catch unreachable;
        }
    } else {
        idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            builder.append_value(arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, idx)) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}
