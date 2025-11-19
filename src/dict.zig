const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;
const DataType = arrow.data_type.DataType;
const rotl = std.math.rotl;

pub const Error = error{
    OutOfMemory,
};

pub const PushError = error{
    NonBinaryArrayWithDict,
    UnexpectedElementSize,
};

pub const Invalid = error{Invalid};

pub fn validate_dict_array(dict_array: *const arr.FixedSizeBinaryArray) Invalid!void {
    switch (dict_array.byte_width) {
        20, 32 => {},
        else => return Invalid.Invalid,
    }

    if (dict_array.null_count == 0) {
        return Invalid.Invalid;
    }
}

pub fn validate_dict_index_array(
    dict_array: *const arr.FixedSizeBinaryArray,
    array: *const arr.UInt32Array,
) Invalid!void {
    if (dict_array.len == 0 and array.len != 0) {
        return Invalid.Invalid;
    }

    if (array.len > 0) {
        // don't care about validity, value should be in range even if the entry is "null"
        const max = std.mem.max(
            u32,
            array.values[array.offset .. array.offset + array.len],
        );

        if (dict_array.len == 0) {
            if (max != 0) {
                return Invalid.Invalid;
            }

            if (array.null_count != array.len) {
                return Invalid.Invalid;
            }
        } else {
            if (dict_array.len > 0 and max >= dict_array.len) {
                return Invalid.Invalid;
            }
        }
    }
}

pub fn DictFn(comptime W: comptime_int) type {
    switch (W) {
        20, 32 => {},
        else => @compileError("unsupported width"),
    }

    return struct {
        // dictionary value type
        pub const T = [W]u8;

        pub const HashMapContext = struct {
            const Self = @This();

            pub fn hash(self: Self, val: T) u64 {
                _ = self;
                return Hasher(W).hash(69, val);
            }

            pub fn eql(self: Self, a: T, b: T) bool {
                _ = self;
                inline for (0..W) |i| {
                    if (a[i] != b[i]) {
                        return false;
                    }
                }
                return true;
            }
        };

        // maps the dictionary value to the index in the dictionary
        pub const Map = std.HashMapUnmanaged(
            T,
            u32,
            HashMapContext,
            std.hash_map.default_max_load_percentage,
        );

        pub fn unpack_dict(
            dict_array: *const arr.FixedSizeBinaryArray,
            array: *const arr.UInt32Array,
            dt: DataType,
            alloc: Allocator,
        ) Error!arr.Array {
            std.debug.assert(dict_array.byte_width == W);
            std.debug.assert(dict_array.null_count == 0);

            const dict: []const [W]u8 = @ptrCast(
                dict_array.data[dict_array.offset * W .. (dict_array.len + dict_array.offset) * W],
            );

            if (dict.len > 0) {
                var idx: u32 = array.offset;
                while (idx < array.offset + array.len) : (idx += 1) {
                    std.debug.assert(array.values[idx] < dict.len);
                }
            }

            return switch (dt) {
                .binary => .{ .binary = try unpack_dict_to_binary_array(
                    .i32,
                    dict,
                    array,
                    alloc,
                ) },
                .large_binary => .{ .large_binary = try unpack_dict_to_binary_array(
                    .i64,
                    dict,
                    array,
                    alloc,
                ) },
                .binary_view => .{ .binary_view = try unpack_dict_to_binary_view_array(
                    dict,
                    array,
                    alloc,
                ) },
                .fixed_size_binary => .{ .fixed_size_binary = try unpack_dict_to_fixed_size_binary_array(
                    dict,
                    array,
                    alloc,
                ) },
                .utf8 => .{ .utf8 = .{ .inner = try unpack_dict_to_binary_array(
                    .i32,
                    dict,
                    array,
                    alloc,
                ) } },
                .large_utf8 => .{ .large_utf8 = .{ .inner = try unpack_dict_to_binary_array(
                    .i64,
                    dict,
                    array,
                    alloc,
                ) } },
                .utf8_view => .{ .utf8_view = .{ .inner = try unpack_dict_to_binary_view_array(
                    dict,
                    array,
                    alloc,
                ) } },
                else => std.debug.panic("non binary array with dict"),
            };
        }

        fn unpack_dict_to_fixed_size_binary_array(
            dict: []const [W]u8,
            array: *const arr.UInt32Array,
            alloc: Allocator,
        ) Error!arr.FixedSizeBinaryArray {
            const data = try alloc.alloc([W]u8, array.len);
            @memset(@as([]u8, @ptrCast(data)), 0);

            const validity: ?[]const u8 = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            var idx = array.offset;
            var out_idx: u32 = 0;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                out_idx += 1;
            }) {
                data[out_idx] = dict[array.values[idx]];
            }

            return arr.FixedSizeBinaryArray{
                .data = data,
                .offset = 0,
                .len = array.len,
                .byte_width = W,
                .null_count = array.null_count,
                .validity = validity,
            };
        }

        fn unpack_dict_to_binary_view_array(
            dict: []const [W]u8,
            array: *const arr.UInt32Array,
            alloc: Allocator,
        ) Error!arr.BinaryViewArray {
            if (W <= 12) {
                @compileError("unsupported width for unpacking into binview array.");
            }

            const validity: ?[]const u8 = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            const views = try alloc.alloc(arrow.array.BinaryView, array.len);

            var idx = array.offset;
            var o_idx: u32 = 0;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                o_idx += 1;
            }) {
                const d_idx = array.values[idx];
                const d_val = dict[d_idx];
                views[o_idx] = arrow.array.BinaryView{
                    .length = W,
                    .offset = @intCast(d_idx),
                    .prefix = @bitCast(d_val[0..12]),
                    .buffer_idx = 0,
                };
            }

            const buffers = try alloc.alloc([]const []const u8, 1);
            buffers[0] = try alloc.dupe(u8, @as([]const u8, @ptrCast(dict)));

            return arr.BinaryViewArray{
                .validity = validity,
                .null_count = array.null_count,
                .len = array.len,
                .views = views,
                .offset = 0,
                .buffers = buffers,
            };
        }

        fn unpack_dict_to_binary_array(
            comptime index_t: arr.IndexType,
            dict: []const [W]u8,
            array: *const arr.UInt32Array,
            alloc: Allocator,
        ) Error!arr.GenericBinaryArray(index_t) {
            const I = index_t.to_type();

            const a = try unpack_dict_to_fixed_size_binary_array(
                dict,
                array,
                alloc,
            );

            const offsets = try alloc.alloc(I, array.len + 1);
            var idx: I = 0;
            while (idx < @as(I, @intCast(offsets.len))) : (idx += 1) {
                offsets[idx] = idx * W;
            }

            return arr.GenericBinaryArray(index_t){
                .offset = a.offset,
                .len = a.len,
                .null_count = a.null_count,
                .validity = a.validity,
                .data = @ptrCast(a.data),
                .offsets = offsets,
            };
        }

        pub fn apply_dict(
            dict: *const Map,
            array: *const arr.Array,
            alloc: Allocator,
        ) Error!arr.UInt32Array {
            switch (array.*) {
                .binary => |*a| {
                    return try apply_dict_to_binary_array(.i32, dict, a, alloc);
                },
                .large_binary => |*a| {
                    return try apply_dict_to_binary_array(.i64, dict, a, alloc);
                },
                .binary_view => |*a| {
                    return try apply_dict_to_binary_view_array(dict, a, alloc);
                },
                .fixed_size_binary => |*a| {
                    return try apply_dict_to_fixed_size_binary_array(dict, a, alloc);
                },
                .utf8 => |*a| {
                    return try apply_dict_to_binary_array(.i32, dict, &a.inner, alloc);
                },
                .large_utf8 => |*a| {
                    return try apply_dict_to_binary_array(.i64, dict, &a.inner, alloc);
                },
                .utf8_view => |*a| {
                    return try apply_dict_to_binary_view_array(dict, &a.inner, alloc);
                },
                else => std.debug.panic("non binary array with dict"),
            }
        }

        fn apply_dict_to_binary_view_array(
            dict: *const Map,
            array: *const arr.BinaryViewArray,
            alloc: Allocator,
        ) Error!arr.UInt32Array {
            const values = try alloc.alloc(u32, array.len);
            @memset(values, 0);

            if (array.null_count > 0) {
                const v = array.validity orelse unreachable;

                var idx: u32 = array.offset;
                var o_idx: u32 = 0;
                while (idx < array.offset + array.len) : ({
                    idx += 1;
                    o_idx += 1;
                }) {
                    if (arrow.get.get_binary_view_opt(array.buffers, array.views, v, idx)) |s| {
                        std.debug.assert(s.len == W);
                        const sv: T = @as([]const T, @ptrCast(s))[0];
                        values[o_idx] = dict.get(sv) orelse unreachable;
                    }
                }
            } else {
                var idx: u32 = array.offset;
                var o_idx: u32 = 0;
                while (idx < array.offset + array.len) : ({
                    idx += 1;
                    o_idx += 1;
                }) {
                    const s = arrow.get.get_binary_view(array.buffers, array.views, idx);
                    std.debug.assert(s.len == W);
                    const sv: T = @as([]const T, @ptrCast(s))[0];
                    values[o_idx] = dict.get(sv) orelse unreachable;
                }
            }

            const validity: ?[]const u8 = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            return arr.UInt32Array{
                .validity = validity,
                .null_count = array.null_count,
                .len = array.len,
                .offset = 0,
                .values = values,
            };
        }

        fn apply_dict_to_fixed_size_binary_array(
            dict: *const Map,
            array: *const arr.FixedSizeBinaryArray,
            alloc: Allocator,
        ) Error!arr.UInt32Array {
            std.debug.assert(array.byte_width == W);

            const data: []const T = @ptrCast(
                array.data[array.offset * W .. (array.offset + array.len) * W],
            );

            const values = try alloc.alloc(u32, array.len);
            var idx: u32 = 0;
            while (idx < array.len) : (idx += 1) {
                values[idx] = dict.get(data[idx]) orelse 0;
            }

            const validity: ?[]const u8 = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            return arr.UInt32Array{
                .validity = validity,
                .null_count = array.null_count,
                .len = array.len,
                .offset = 0,
                .values = values,
            };
        }

        fn apply_dict_to_binary_array(
            comptime index_t: arr.IndexType,
            dict: *const Map,
            array: *const arr.GenericBinaryArray(index_t),
            alloc: Allocator,
        ) Error!arr.UInt32Array {
            var idx: u32 = array.offset;
            while (idx < array.offset + array.len) : (idx += 1) {
                const start = array.offsets[idx];
                const end = array.offsets[idx + 1];
                std.debug.assert(end - start == W);
            }

            const data: []const T = @ptrCast(
                array.data[array.offset * W .. (array.offset + array.len) * W],
            );

            const values = try alloc.alloc(u32, array.len);

            idx = 0;
            while (idx < array.len) : (idx += 1) {
                values[idx] = dict.get(data[idx]) orelse 0;
            }

            const validity: ?[]const u8 = if (array.null_count > 0)
                try copy_validity(
                    array.validity orelse unreachable,
                    array.offset,
                    array.len,
                    alloc,
                )
            else
                null;

            return arr.UInt32Array{
                .validity = validity,
                .null_count = array.null_count,
                .len = array.len,
                .offset = 0,
                .values = values,
            };
        }

        pub fn push_array_to_dict(array: *const arr.Array, elems: *Map) PushError!void {
            switch (array.*) {
                .binary => |*a| {
                    push_binary_to_dict(.i32, a, elems);
                },
                .large_binary => |*a| {
                    push_binary_to_dict(.i64, a, elems);
                },
                .binary_view => |*a| {
                    push_binary_view_to_dict(a, elems);
                },
                .fixed_size_binary => |*a| {
                    push_fixed_size_binary_to_dict(a, elems);
                },
                .utf8 => |*a| {
                    push_binary_to_dict(.i32, &a.inner, elems);
                },
                .large_utf8 => |*a| {
                    push_binary_to_dict(.i64, &a.inner, elems);
                },
                .utf8_view => |*a| {
                    push_binary_view_to_dict(&a.inner, elems);
                },
                else => return PushError.NonBinaryArrayWithDict,
            }
        }

        fn push_binary_to_dict(
            comptime index_t: arr.IndexType,
            array: *const arr.GenericBinaryArray(index_t),
            out: *Map,
        ) PushError!void {
            if (array.len == 0) {
                return;
            }

            if (array.null_count > 0) {
                const validity = (array.validity orelse unreachable).ptr;

                var item: u32 = array.offset;
                while (item < array.offset + array.len) : (item += 1) {
                    if (arrow.get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, item)) |s| {
                        out.putAssumeCapacity(s, 0);
                    }
                }
            } else {
                var item: u32 = array.offset;
                while (item < array.offset + array.len) : (item += 1) {
                    const s = arrow.get.get_binary(index_t, array.data.ptr, array.offsets.ptr, item);
                    out.putAssumeCapacity(s, 0);
                }
            }
        }
    };
}

pub fn count_array_to_dict(array: *const arr.Array) error{NonBinaryArrayWithDict}!usize {
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
        else => return error.NonBinaryArrayWithDict,
    }
}

fn push_binary_view_to_dict(
    array: *const arr.BinaryViewArray,
    out: *DictBuilder,
) void {
    if (array.len == 0) {
        return;
    }

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, item)) |s| {
                out.putAssumeCapacity(s, 0);
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_binary_view(array.buffers.ptr, array.views.ptr, item);
            out.putAssumeCapacity(s, 0);
        }
    }
}

fn push_fixed_size_binary_to_dict(
    array: *const arr.FixedSizeBinaryArray,
    out: *DictBuilder,
) void {
    if (array.len == 0) {
        return;
    }

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            if (arrow.get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, item)) |s| {
                out.putAssumeCapacity(s, 0);
            }
        }
    } else {
        var item: u32 = array.offset;
        while (item < array.offset + array.len) : (item += 1) {
            const s = arrow.get.get_fixed_size_binary(array.data.ptr, array.byte_width, item);
            out.putAssumeCapacity(s, 0);
        }
    }
}

// adapted version of std.hash.XxHash64
fn Hasher(comptime W: comptime_int) type {
    const prime_1 = 0x9E3779B185EBCA87; // 0b1001111000110111011110011011000110000101111010111100101010000111
    const prime_2 = 0xC2B2AE3D27D4EB4F; // 0b1100001010110010101011100011110100100111110101001110101101001111
    const prime_3 = 0x165667B19E3779F9; // 0b0001011001010110011001111011000110011110001101110111100111111001
    const prime_4 = 0x85EBCA77C2B2AE63; // 0b1000010111101011110010100111011111000010101100101010111001100011
    const prime_5 = 0x27D4EB2F165667C5; // 0b0010011111010100111010110010111100010110010101100110011111000101

    return struct {
        fn finalize8(v: u64, bytes: [8]u8) u64 {
            var acc = v;
            const lane: u64 = @bitCast(bytes);
            acc ^= round(0, lane);
            acc = rotl(u64, acc, 27) *% prime_1;
            acc +%= prime_4;
            return acc;
        }

        fn finalize4(v: u64, bytes: [4]u8) u64 {
            var acc = v;
            const lane: u64 = @as(u32, @bitCast(bytes));
            acc ^= lane *% prime_1;
            acc = rotl(u64, acc, 23) *% prime_2;
            acc +%= prime_3;
            return acc;
        }

        fn round(acc: u64, lane: u64) u64 {
            const a = acc +% (lane *% prime_2);
            const b = rotl(u64, a, 31);
            return b *% prime_1;
        }

        fn avalanche(value: u64) u64 {
            var result = value ^ (value >> 33);
            result *%= prime_2;
            result ^= result >> 29;
            result *%= prime_3;
            result ^= result >> 32;

            return result;
        }

        pub fn hash(seed: u64, input: [W]u8) u64 {
            var acc = seed +% prime_5 +% W;
            switch (W) {
                20 => {
                    acc = finalize8(acc, input[0..8].*);
                    acc = finalize8(acc, input[8..16].*);
                    acc = finalize4(acc, input[16..20].*);
                    return avalanche(acc);
                },
                32 => {
                    acc = finalize8(acc, input[0..8].*);
                    acc = finalize8(acc, input[8..16].*);
                    acc = finalize8(acc, input[16..24].*);
                    acc = finalize8(acc, input[24..32].*);
                    return avalanche(acc);
                },
                else => @compileError("unsupported width"),
            }
        }
    };
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
}
