const std = @import("std");
const lz4hc = @cImport(@cInclude("lz4hc.h"));
const lz4 = @cImport(@cInclude("lz4.h"));
const zstd = @cImport(@cInclude("zstd.h"));

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
    return @max(zstd.ZSTD_compressBound(input_size), @as(usize, @intCast(lz4.LZ4_compressBound(@intCast(input_size)))), input_size);
}

fn lz4_compress(src: []const u8, dst: []u8) CompressError!usize {
    const lz4_size = lz4.LZ4_compress_default(src.ptr, dst.ptr, @intCast(src.len), @intCast(dst.len));
    if (lz4_size != 0) {
        return @intCast(lz4_size);
    } else {
        return CompressError.CompressFail;
    }
}

fn zstd_compress(src: []const u8, dst: []u8, level: u8) CompressError!usize {
    const res = zstd.ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, level);
    if (zstd.ZSTD_isError(res) == 0) {
        return res;
    } else {
        return CompressError.CompressFail;
    }
}

fn lz4_compress_hc(src: []const u8, dst: []u8, level: u8) CompressError!usize {
    const res = lz4hc.LZ4_compress_HC(src.ptr, dst.ptr, @intCast(src.len), @intCast(dst.len), level);
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
    const res = zstd.ZSTD_decompress(dst.ptr, dst.len, src.ptr, src.len);
    if (zstd.ZSTD_isError(res) != 0 or res != dst.len) {
        return DecompressError.DecompressFail;
    }
}

fn lz4_decompress(src: []const u8, dst: []u8) DecompressError!void {
    const res = lz4.LZ4_decompress_safe(src.ptr, dst.ptr, @intCast(src.len), @intCast(dst.len));
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
