const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;

const Error = error{
    NonBinaryArrayWithDict,
    OutOfMemory,
};

pub fn unpack_dict(dict_array: *const arr.FixedSizeBinaryArray, array: *const arr.UInt32Array, dt: DataType, alloc: Allocator) Error!arr.Array {
    return switch (dt) {
        .binary => .{ .binary = try unpack_dict_to_binary_array(.i32, dict_array, array, alloc) },
        .large_binary => .{ .large_binary = try unpack_dict_to_binary_array(.i64, dict_array, array, alloc) },
        .binary_view => .{ .binary_view = try unpack_dict_to_binary_view_array(dict_array, array, alloc) },
        .fixed_size_binary => .{ .fixed_size_binary = try unpack_dict_to_fixed_size_binary_array(dict_array, array, alloc) },
        .utf8 => .{ .utf8 = .{ .inner = try unpack_dict_to_binary_array(.i32, dict_array, array, alloc) } },
        .large_utf8 => .{ .large_utf8 = .{ .inner = try unpack_dict_to_binary_array(.i64, dict_array, array, alloc) } },
        .utf8_view => .{ .utf8_view = .{ .inner = try unpack_dict_to_binary_view_array(dict_array, array, alloc) } },
        else => return Error.NonBinaryArrayWithDict,
    };
}

fn unpack_dict_to_fixed_size_binary_array(dict_array: *const arr.FixedSizeBinaryArray, array: *const arr.UInt32Array, alloc: Allocator) Error!arr.FixedSizeBinaryArray {
    var builder = arrow.builder.FixedSizeBinaryBuilder.with_capacity(dict_array.byte_width, array.len, array.null_count > 0, alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.get.get_primitive_opt(u32, array.values.ptr, validity, idx)) |key| {
                builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const key = arrow.get.get_primitive(u32, array.values.ptr, idx);
            builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn unpack_dict_to_binary_view_array(dict_array: *const arr.FixedSizeBinaryArray, array: *const arr.UInt32Array, alloc: Allocator) Error!arr.BinaryViewArray {
    const byte_width: u32 = @intCast(dict_array.byte_width);

    const buffer_len: u32 = if (byte_width > 12)
        (array.len - array.null_count) * byte_width
    else
        0;

    var builder = arrow.builder.BinaryViewBuilder.with_capacity(buffer_len, array.len, array.null_count > 0, alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.get.get_primitive_opt(u32, array.values.ptr, validity, idx)) |key| {
                builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const key = arrow.get.get_primitive(u32, array.values.ptr, idx);
            builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn unpack_dict_to_binary_array(comptime index_t: arr.IndexType, dict_array: *const arr.FixedSizeBinaryArray, array: *const arr.UInt32Array, alloc: Allocator) Error!arr.GenericBinaryArray(index_t) {
    const total_size: u32 = (array.len - array.null_count) * @as(u32, @intCast(dict_array.byte_width));

    var builder = arrow.builder.GenericBinaryBuilder(index_t).with_capacity(total_size, array.len, array.null_count > 0, alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (arrow.get.get_primitive_opt(u32, array.values.ptr, validity, idx)) |key| {
                builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var idx = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const key = arrow.get.get_primitive(u32, array.values.ptr, idx);
            builder.append_value(arrow.get.get_fixed_size_binary(dict_array.data.ptr, dict_array.byte_width, key +% dict_array.offset)) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

const DictLookup = std.StringHashMapUnmanaged(u32);

pub fn apply_dict(dict: *const DictLookup, array: *const arr.Array, scratch_alloc: Allocator) Error!arr.UInt32Array {
    switch (array.*) {
        .binary => |*a| {
            return try apply_dict_to_binary_array(.i32, dict, a, scratch_alloc);
        },
        .large_binary => |*a| {
            return try apply_dict_to_binary_array(.i64, dict, a, scratch_alloc);
        },
        .binary_view => |*a| {
            return try apply_dict_to_binary_view_array(dict, a, scratch_alloc);
        },
        .fixed_size_binary => |*a| {
            return try apply_dict_to_fixed_size_binary_array(dict, a, scratch_alloc);
        },
        .utf8 => |*a| {
            return try apply_dict_to_binary_array(.i32, dict, &a.inner, scratch_alloc);
        },
        .large_utf8 => |*a| {
            return try apply_dict_to_binary_array(.i64, dict, &a.inner, scratch_alloc);
        },
        .utf8_view => |*a| {
            return try apply_dict_to_binary_view_array(dict, &a.inner, scratch_alloc);
        },
        else => return Error.NonBinaryArrayWithDict,
    }
}

fn apply_dict_to_binary_view_array(dict: *const DictLookup, array: *const arr.BinaryViewArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_fixed_size_binary_array(dict: *const DictLookup, array: *const arr.FixedSizeBinaryArray, scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

fn apply_dict_to_binary_array(comptime index_t: arr.IndexType, dict: *const DictLookup, array: *const arr.GenericBinaryArray(index_t), scratch_alloc: Allocator) Error!arr.UInt32Array {
    var builder = arrow.builder.UInt32Builder.with_capacity(array.len, array.null_count > 0, scratch_alloc) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory else unreachable;
    };

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
            } else {
                builder.append_null() catch unreachable;
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
            builder.append_value(find_dict_elem_idx(dict, s) orelse unreachable) catch unreachable;
        }
    }

    return (builder.finish() catch unreachable);
}

/// Finds the index of the given element inside the given dict_array
fn find_dict_elem_idx(dict: *const DictLookup, val: []const u8) ?u32 {
    return dict.get(val);
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
