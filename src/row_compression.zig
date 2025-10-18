//! This module implements functionality to compress each row of a binary array by itself

const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const IndexType = arrow.array.IndexType;
const StructArray = arrow.array.StructArray;
const GenericBinaryArray = arrow.array.GenericBinaryArray;

const sys = @import("./compression.zig").sys;

pub const RowCompression = union(enum) {
    lz4_hc: u8,
    zstd: i32,
};

pub fn compress(
    comptime index_t: IndexType,
    input: GenericBinaryArray(index_t),
    compression_level: u8,
    alloc: Allocator,
    scratch_alloc: Allocator,
) error{OutOfMemory}!struct {
    data: GenericBinaryArray(index_t),
    dict: []const u8,
    offsets: []index_t.to_type(),
} {
    const I = index_t.to_type();

    const dict = try build_zdict(index_t, input, alloc, scratch_alloc);

    const start = input.offsets[input.offset];
    const end = input.offsets[input.offset + input.len];
    const size = end - start;

    const out_buf_size = sys.ZSTD_compressBound(@intCast(size));
    const out_buf = try alloc.alloc(u8, out_buf_size);
    const out_offsets = try alloc.alloc(I, input.offsets.len);
    out_offsets[0] = 0;

    const c_dict = sys.ZSTD_createCDict(dict.ptr, dict.len, compression_level);
    defer {
        _ = sys.ZSTD_freeCDict(c_dict);
    }

    const c_ctx = sys.ZSTD_createCCtx();
    defer {
        _ = sys.ZSTD_freeCCtx(c_ctx);
    }

    var out_offset: I = 0;

    if (input.null_count > 0) {
        const v = (input.validity orelse unreachable).ptr;

        var idx: u32 = input.offset;
        var out: u32 = 0;
        while (idx < input.offset + input.len) : ({
            idx += 1;
            out += 1;
        }) {
            if (arrow.get.get_binary_opt(input.data.ptr, input.offsets.ptr, v, idx)) |s| {
                const c_len = sys.ZSTD_compress_usingCDict(
                    c_ctx,
                    out_buf.ptr,
                    out_buf.len - out_offset,
                    s.ptr,
                    s.len,
                    c_dict,
                );
                if (sys.ZSTD_isError(c_len) != 0) {
                    @panic("failed to compress");
                }
                out_offset += @intCast(c_len);
                out_offsets[out + 1] = out_offset;
            } else {
                out_offsets[out + 1] = out_offset;
            }
        }
    } else {
        var idx: u32 = input.offset;
        var out: u32 = 0;
        while (idx < input.offset + input.len) : ({
            idx += 1;
            out += 1;
        }) {
            const s = arrow.get.get_binary(input.data.ptr, input.offsets.ptr, idx);
            const c_len = sys.ZSTD_compress_usingCDict(
                c_ctx,
                out_buf.ptr,
                out_buf.len - out_offset,
                s.ptr,
                s.len,
                c_dict,
            );
            if (sys.ZSTD_isError(c_len) != 0) {
                @panic("failed to compress");
            }
            out_offset += @intCast(c_len);
            out_offsets[out + 1] = out_offset;
        }
    }

    const out_data = arrow.array.GenericBinaryArray(index_t){
        .data = out_buf[0..@as(usize, @intCast(out_offset))],
        .len = input.len,
        .offset = 0,
        .offsets = out_offsets,
        .validity = input.validity,
        .null_count = input.null_count,
    };

    const dict_arr_offsets = try alloc.alloc(i32, input.len + 1);
    @memset(dict_arr_offsets, @as(i32, @intCast(dict.len)));
    dict_arr_offsets[0] = 0;

    const dict_arr = arrow.array.BinaryArray{
        .null_count = 0,
        .len = input.len,
        .offset = 0,
        .offsets = dict_arr_offsets,
        .validity = null,
        .data = dict,
    };
}

const Error = error{
    InvalidInput,
};

pub fn decompress(comptime index_t: IndexType, input: StructArray) Error!GenericBinaryArray(index_t) {
    const I = index_t.to_type();

    if (input.field_names.len != 3) {
        return Error.InvalidInput;
    }
    if (!std.mem.eql(u8, input.field_names[0], "data")) {
        return Error.InvalidInput;
    }
    if (!std.mem.eql(u8, input.field_names[1], "dict")) {
        return Error.InvalidInput;
    }
    if (!std.mem.eql(u8, input.field_names[0], "offsets")) {
        return Error.InvalidInput;
    }
}

fn build_zdict(
    comptime index_t: IndexType,
    input: GenericBinaryArray(index_t),
    alloc: Allocator,
    scratch_alloc: Allocator,
) error{OutOfMemory}![]const u8 {
    if (input.len == 0) {
        return &.{};
    }

    const start = input.offsets[input.offset];
    const end = input.offsets[input.offset + input.len];
    const size = end - start;
    if (size == 0) {
        return &.{};
    }

    const n_samples = input.len;
    const sample_sizes = try scratch_alloc.alloc(usize, n_samples);

    if (input.null_count > 0) {
        const v = (input.validity orelse unreachable).ptr;

        var idx: u32 = input.offset;
        var out: u32 = 0;
        while (idx < input.offset + input.len) : ({
            idx += 1;
            out += 1;
        }) {
            if (arrow.bitmap.get(v, idx)) {
                sample_sizes[out] = @intCast(input.offsets.ptr[idx + 1] - input.offsets.ptr[idx]);
            } else {
                sample_sizes[out] = 0;
            }
        }
    } else {
        var idx: u32 = input.offset;
        var out: u32 = 0;
        while (idx < input.offset + input.len) : ({
            idx += 1;
            out += 1;
        }) {
            sample_sizes[out] = @intCast(input.offsets.ptr[idx + 1] - input.offsets.ptr[idx]);
        }
    }

    const dict_buf_size = 110 * 1024;
    const dict_buf = try alloc.alloc(u8, dict_buf_size);

    const dict_len = sys.ZDICT_trainFromBuffer(
        dict_buf.ptr,
        dict_buf.len,
        input.data[start..end].ptr,
        sample_sizes.ptr,
        n_samples,
    );
    if (sys.ZDICT_isError(dict_len) != 0) {
        return &.{};
    }

    return dict_buf[0..dict_len];
}
