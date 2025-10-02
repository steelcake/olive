const std = @import("std");

extern fn ZSTD_compressBound(src_size: usize) usize;
extern fn ZSTD_compress(dst: *anyopaque, dst_capacity: usize, src: *const anyopaque, src_size: usize, compression_level: c_int) usize;
extern fn ZSTD_decompress(dst: *anyopaque, dst_capacity: usize, src: *const anyopaque, compressed_size: usize) usize;
extern fn ZSTD_isError(code: usize) c_uint;

extern fn LZ4_compressBound(input_size: c_int) c_int;
extern fn LZ4_compress_default(src: [*]const c_char, dst: [*]c_char, src_size: c_int, dst_capacity: c_int) c_int;
extern fn LZ4_decompress_safe(src: [*]const c_char, dst: [*]c_char, compressed_size: c_int, dst_capacity: c_int) c_int;

extern fn LZ4_compress_HC(src: [*]const c_char, dst: [*]c_char, src_size: c_int, dst_capacity: c_int, compression_level: c_int) c_int;

pub const Compression = union(enum) {
    /// Memcopy
    no_compression,
    /// Default lz4 block compression
    lz4,
    /// High compression variant of LZ4, very slow compression speed but same decompression speed as regular lz4.
    lz4_hc: u8,
    /// Default zstd block compression
    zstd: u8,
};

const CompressError = error{
    CompressFail,
};

const DecompressError = error{
    DecompressFail,
};

pub fn compress_bound(input_size: usize) usize {
    return @max(ZSTD_compressBound(input_size), @as(usize, @intCast(LZ4_compressBound(@intCast(input_size)))), input_size);
}

fn lz4_compress(src: []const u8, dst: []u8) CompressError!usize {
    const lz4_size = LZ4_compress_default(@ptrCast(src.ptr), @ptrCast(dst.ptr), @intCast(src.len), @intCast(dst.len));
    if (lz4_size != 0) {
        return @intCast(lz4_size);
    } else {
        return CompressError.CompressFail;
    }
}

fn zstd_compress(src: []const u8, dst: []u8, level: u8) CompressError!usize {
    const res = ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, level);
    if (ZSTD_isError(res) == 0) {
        return res;
    } else {
        return CompressError.CompressFail;
    }
}

fn lz4_compress_hc(src: []const u8, dst: []u8, level: u8) CompressError!usize {
    const res = LZ4_compress_HC(@ptrCast(src.ptr), @ptrCast(dst.ptr), @intCast(src.len), @intCast(dst.len), level);
    if (res != 0) {
        return @intCast(res);
    } else {
        return CompressError.CompressFail;
    }
}

pub fn compress(src: []const u8, dst: []u8, algo: Compression) CompressError!usize {
    switch (algo) {
        .no_compression => {
            @memcpy(dst[0..src.len], src);
            return src.len;
        },
        .lz4 => {
            return try lz4_compress(src, dst);
        },
        .lz4_hc => |level| {
            return try lz4_compress_hc(src, dst, level);
        },
        .zstd => |level| {
            return try zstd_compress(src, dst, level);
        },
    }
}

fn zstd_decompress(src: []const u8, dst: []u8) DecompressError!void {
    const res = ZSTD_decompress(dst.ptr, dst.len, src.ptr, src.len);
    if (ZSTD_isError(res) != 0 or res != dst.len) {
        return DecompressError.DecompressFail;
    }
}

fn lz4_decompress(src: []const u8, dst: []u8) DecompressError!void {
    const res = LZ4_decompress_safe(@ptrCast(src.ptr), @ptrCast(dst.ptr), @intCast(src.len), @intCast(dst.len));
    if (res < 0 or @as(usize, @intCast(res)) != dst.len) {
        return DecompressError.DecompressFail;
    }
}

pub fn decompress(src: []const u8, dst: []u8, algo: Compression) DecompressError!void {
    switch (algo) {
        .no_compression => {
            std.debug.assert(src.len == dst.len);
            @memcpy(dst, src);
        },
        .lz4, .lz4_hc => {
            try lz4_decompress(src, dst);
        },
        .zstd => {
            try zstd_decompress(src, dst);
        },
    }
}

test "smoke compression" {
    const input = &.{ 1, 2, 3, 4, 5, 6, 7 };

    const output = try std.testing.allocator.alloc(u8, compress_bound(input.len));
    defer std.testing.allocator.free(output);

    const compressed_size = try compress(input, output, .lz4);

    const decompressed = try std.testing.allocator.alloc(u8, input.len);
    defer std.testing.allocator.free(decompressed);

    try decompress(output[0..compressed_size], decompressed, .lz4);

    try std.testing.expectEqualSlices(u8, input, decompressed);
    // try decompress(&.{}, &.{}, .lz4);
}
