const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;

const schema = @import("./schema.zig");

const Error = error{
    NonBinaryArrayWithDict,
    OutOfMemory,
};

pub fn apply_dict(dict_array: *const arr.BinaryArray, array: *const arr.Array, scratch_alloc: Allocator) Error!arr.UInt32Array {
    switch (array.*) {
        .binary => |*a| {
            return try apply_dict_to_binary_array(.i32, dict_array, a, scratch_alloc);
        },
        .large_binary => |*a| {
            return try apply_dict_to_binary_array(.i64, dict_array, a, scratch_alloc);
        },
        .binary_view => |*a| {
            return try apply_dict_to_binary_view_array(dict_array, a, scratch_alloc);
        },
        .fixed_size_binary => |*a| {
            return try apply_dict_to_fixed_size_binary_array(dict_array, a, scratch_alloc);
        },
        .utf8 => |*a| {
            return try apply_dict_to_binary_array(.i32, dict_array, &a.inner, scratch_alloc);
        },
        .large_utf8 => |*a| {
            return try apply_dict_to_binary_array(.i64, dict_array, &a.inner, scratch_alloc);
        },
        .utf8_view => |*a| {
            return try apply_dict_to_binary_view_array(dict_array, &a.inner, scratch_alloc);
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn apply_dict_to_binary_view_array(dict_array: *const arr.BinaryArray, array: *const arr.BinaryViewArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_fixed_size_binary_array(dict_array: *const arr.BinaryArray, array: *const arr.FixedSizeBinaryArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_binary_array(comptime index_t: arr.IndexType, dict_array: *const arr.BinaryArray, array: *const arr.GenericBinaryArray(index_t), scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
            builder.append_value(find_dict_idx(dict_array, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn find_dict_idx(dict_array: *const arr.BinaryArray, val: []const u8) ?u32 {
    var idx: u32 = dict_array.offset;
    while (idx < dict_array.offset + dict_array.len) : (idx += 1) {
        const dict_elem = arrow.get.get_binary(.i32, dict_array.data.ptr, dict_array.offsets.ptr, idx);

        if (std.mem.eql(u8, val, dict_elem)) {
            return idx;
        }
    }

    return null;
}

pub fn find_dict(dicts: []const schema.DictSchema, dict_elements: []const arr.BinaryArray, table_index: usize, field_index: usize) ?*const arr.BinaryArray {
    for (dicts, 0..) |dict, dict_idx| {
        for (dict.members) |member| {
            if (member.table_index == table_index and member.field_index == field_index) {
                return &dict_elements[dict_idx];
            }
        }
    }

    return null;
}

fn stringLessThan(_: void, l: []const u8, r: []const u8) bool {
    return std.mem.order(u8, l, r) == .lt;
}

/// Ascending sort elements and deduplicate
pub fn sort_and_dedup(data: [][]const u8) [][]const u8 {
    if (data.len == 0) {
        return data;
    }

    std.mem.sortUnstable([]const u8, data, {}, stringLessThan);
    var write_idx: usize = 0;

    for (data[1..]) |s| {
        if (!std.mem.eql(u8, s, data[write_idx])) {
            write_idx += 1;
            data[write_idx] = s;
        }
    }

    return data[0 .. write_idx + 1];
}

pub fn count_array_to_dict(array: *const arr.Array) Error!usize {
    switch (array.*) {
        .binary => |*a| {
            return a.len - a.null_count;
        },
        .large_binary => |*a| {
            return a.len - a.null_count;
        },
        .binary_view => |*a| {
            return a.len - a.null_count;
        },
        .fixed_size_binary => |*a| {
            return a.len - a.null_count;
        },
        .utf8 => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        .large_utf8 => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        .utf8_view => |*a| {
            return a.inner.len - a.inner.null_count;
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

pub fn push_array_to_dict(array: *const arr.Array, write_idx: usize, elems: [][]const u8) Error!usize {
    switch (array.*) {
        .binary => |*a| {
            return push_binary_to_dict(.i32, a, write_idx, elems);
        },
        .large_binary => |*a| {
            return push_binary_to_dict(.i64, a, write_idx, elems);
        },
        .binary_view => |*a| {
            return push_binary_view_to_dict(a, write_idx, elems);
        },
        .fixed_size_binary => |*a| {
            return push_fixed_size_binary_to_dict(a, write_idx, elems);
        },
        .utf8 => |*a| {
            return push_binary_to_dict(.i32, &a.inner, write_idx, elems);
        },
        .large_utf8 => |*a| {
            return push_binary_to_dict(.i64, &a.inner, write_idx, elems);
        },
        .utf8_view => |*a| {
            return push_binary_view_to_dict(&a.inner, write_idx, elems);
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn push_binary_to_dict(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t), write_idx: usize, out: [][]const u8) usize {
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_binary_view_to_dict(array: *const arr.BinaryViewArray, write_idx: usize, out: [][]const u8) usize {
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}

fn push_fixed_size_binary_to_dict(array: *const arr.FixedSizeBinaryArray, write_idx: usize, out: [][]const u8) usize {
    if (array.len == 0) {
        return write_idx;
    }

    var wi = write_idx;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                out[wi] = s;
                wi += 1;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            out[wi] = s;
            wi += 1;
        }
    }

    return wi;
}
