//! This module implements functionality to compress each row of a binary array by itself

const std = @import("std");
const Allocator = std.mem.Allocator;

const arrow = @import("arrow");
const IndexType = arrow.array.IndexType;
const StructArray = arrow.array.StructArray;
const GenericBinaryArray = arrow.array.GenericBinaryArray;

const sys = @import("./compression.zig").sys;

pub const RowCompression = union(enum) {
    /// High compression variant of LZ4, very slow compression speed but same decompression speed as regular lz4.
    lz4_hc: u8,
    /// Default zstd block compression
    zstd: u8,
};

pub fn compress(comptime index_t: IndexType, input: GenericBinaryArray(index_t), compression_level: u8, alloc: Allocator, scratch_alloc: Allocator) error{OutOfMemory}!StructArray {
    const I = index_t.to_type();

    const dict = build_zdict(index_t, input, alloc, scratch_alloc);

    const start = input.offsets[input.offset];
    const end = input.offsets[input.offset + input.len];
    const size = end - start;

    const out_buf_size = sys.ZSTD_compressBound(@intCast(size));
    const out_buf = try alloc.alloc(u8, out_buf_size);
    const out_offsets = try alloc.alloc(I, input.offsets.len);

    const n_samples = input.len;
    const sample_sizes = try scratch_alloc.alloc(usize, n_samples);

    const c_dict = sys.ZSTD_createCDict(dict.ptr, dict.len, compression_level);
    defer {
        _ = sys.ZSTD_freeCDict(c_dict);
    }

    const c_ctx = sys.ZSTD_createCCtx();
    defer {
        _ = sys.ZSTD_freeCCtx(c_ctx);
    }

    if (input.null_count > 0) {
        const v = (input.validity orelse unreachable).ptr;

        var idx: u32 = input.offset;
        var out: u32 = 0;
        var out_offset: I = 0;
        while (idx < input.offset + input.len) : ({
            idx += 1;
            out += 1;
        }) {
            if (arrow.get.get_binary_opt(input.data.ptr, input.offsets.ptr, v, idx)) |s| {
                const c_len = sys.ZSTD_compress_usingCDict(c_ctx, out_buf.ptr, out_buf.len - out_offset, s.ptr, s.len, c_dict);
                if (sys.ZSTD_isError(c_len) != 0) {
                    @panic("failed to compress");
                }
            } else {
                out_offsets[idx + 1] = out_offset;
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
}

pub fn decompress(comptime index_t: IndexType, input: StructArray) GenericBinaryArray(index_t) {
    const I = index_t.to_type();
}

fn build_zdict(comptime index_t: IndexType, input: GenericBinaryArray(index_t), alloc: Allocator, scratch_alloc: Allocator) error{OutOfMemory}![]const u8 {
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

    const dict_len = compr.zdict.ZDICT_trainFromBuffer(dict_buf.ptr, dict_buf.len, input.data[start..end].ptr, sample_sizes.ptr, n_samples);
    if (compr.zdict.ZDICT_isError(dict_len) != 0) {
        return &.{};
    }

    return dict_buf[0..dict_len];
}
