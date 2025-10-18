const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const sys = @cImport({
    @cInclude("zstd.h");
    @cInclude("zdict.h");
    @cInclude("lz4.h");
    @cInclude("lz4hc.h");
});

pub const Compression = union(enum) {
    /// Memcopy
    no_compression,
    /// Default lz4 block compression
    lz4,
    /// High compression variant of LZ4, very slow compression speed but same decompression speed as regular lz4.
    lz4_hc: u8,
    /// Default zstd block compression
    zstd: i32,
};

pub const Compressor = struct {
    zstd_ctx: *sys.ZSTD_CCtx,
    lz4hc_state: []align(8) u8,

    pub fn init(alloc: Allocator) error{OutOfMemory}!Compressor {
        const lz4hc_state = try alloc.alignedAlloc(
            u8,
            std.mem.Alignment.fromByteUnits(8),
            @intCast(sys.LZ4_sizeofStateHC()),
        );
        // Can use _advanced function and pass customem to make zstd context use the zig allocator too.
        // Using simple function to avoid complexity for now
        const zstd_ctx = sys.ZSTD_createCCtx() orelse unreachable;

        return .{
            .lz4hc_state = lz4hc_state,
            .zstd_ctx = zstd_ctx,
        };
    }

    pub fn deinit(self: Compressor, alloc: Allocator) void {
        alloc.free(self.lz4hc_state);
        std.debug.assert(sys.ZSTD_isError(sys.ZSTD_freeCCtx(self.zstd_ctx)) == 0);
    }

    pub fn compress(self: *Compressor, src: []const u8, dst: []u8, algo: Compression) CompressError!usize {
        switch (algo) {
            .no_compression => {
                @memcpy(dst[0..src.len], src);
                return src.len;
            },
            .lz4 => {
                return try lz4_compress(src, dst);
            },
            .lz4_hc => |level| {
                return try lz4_compress_hc(self.lz4hc_state.ptr, src, dst, level);
            },
            .zstd => |level| {
                return try zstd_compress(self.zstd_ctx, src, dst, level);
            },
        }
    }
};

pub const Decompressor = struct {
    zstd_ctx: *sys.ZSTD_DCtx,

    pub fn init() Decompressor {
        const zstd_ctx = sys.ZSTD_createDCtx() orelse unreachable;

        return .{
            .zstd_ctx = zstd_ctx,
        };
    }

    pub fn deinit(self: Decompressor) void {
        std.debug.assert(sys.ZSTD_isError(sys.ZSTD_freeDCtx(self.zstd_ctx)) == 0);
    }

    pub fn decompress(self: *Decompressor, src: []const u8, dst: []u8, algo: Compression) DecompressError!void {
        switch (algo) {
            .no_compression => {
                std.debug.assert(src.len == dst.len);
                @memcpy(dst, src);
            },
            .lz4, .lz4_hc => {
                try lz4_decompress(src, dst);
            },
            .zstd => {
                try zstd_decompress(self.zstd_ctx, src, dst);
            },
        }
    }
};

pub fn compress_bound(input_size: usize) usize {
    return @max(
        sys.ZSTD_compressBound(input_size),
        @as(usize, @intCast(sys.LZ4_compressBound(@intCast(input_size)))),
        input_size,
    );
}

fn lz4_compress(src: []const u8, dst: []u8) CompressError!usize {
    const lz4_size = sys.LZ4_compress_default(
        @ptrCast(src.ptr),
        @ptrCast(dst.ptr),
        @intCast(src.len),
        @intCast(dst.len),
    );
    if (lz4_size != 0) {
        return @intCast(lz4_size);
    } else {
        return CompressError.CompressFail;
    }
}

fn zstd_compress(ctx: *sys.ZSTD_CCtx, src: []const u8, dst: []u8, level: i32) CompressError!usize {
    const res = sys.ZSTD_compressCCtx(ctx, dst.ptr, dst.len, src.ptr, src.len, level);
    if (sys.ZSTD_isError(res) == 0) {
        return res;
    } else {
        return CompressError.CompressFail;
    }
}

fn lz4_compress_hc(state: [*]align(8) u8, src: []const u8, dst: []u8, level: u8) CompressError!usize {
    const res = sys.LZ4_compress_HC_extStateHC(
        state,
        @ptrCast(src.ptr),
        @ptrCast(dst.ptr),
        @intCast(src.len),
        @intCast(dst.len),
        level,
    );
    if (res != 0) {
        return @intCast(res);
    } else {
        return CompressError.CompressFail;
    }
}

fn zstd_decompress(ctx: *sys.ZSTD_DCtx, src: []const u8, dst: []u8) DecompressError!void {
    const res = sys.ZSTD_decompressDCtx(ctx, dst.ptr, dst.len, src.ptr, src.len);
    if (sys.ZSTD_isError(res) != 0 or res != dst.len) {
        return DecompressError.DecompressFail;
    }
}

fn lz4_decompress(src: []const u8, dst: []u8) DecompressError!void {
    const res = sys.LZ4_decompress_safe(
        @ptrCast(src.ptr),
        @ptrCast(dst.ptr),
        @intCast(src.len),
        @intCast(dst.len),
    );
    if (res < 0 or @as(usize, @intCast(res)) != dst.len) {
        return DecompressError.DecompressFail;
    }
}

const CompressError = error{
    CompressFail,
};

const DecompressError = error{
    DecompressFail,
};

test "smoke compression" {
    var arena = ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var compressor = try Compressor.init(alloc);
    defer compressor.deinit(alloc);
    var decompressor = Decompressor.init();
    defer decompressor.deinit();

    const input = &.{ 1, 2, 3, 4, 5, 6, 7 };

    const output = try std.testing.allocator.alloc(u8, compress_bound(input.len));
    defer std.testing.allocator.free(output);

    const compressed_size = try compressor.compress(input, output, .lz4);

    const decompressed = try std.testing.allocator.alloc(u8, input.len);
    defer std.testing.allocator.free(decompressed);

    try decompressor.decompress(output[0..compressed_size], decompressed, .lz4);

    try std.testing.expectEqualSlices(u8, input, decompressed);
    // try decompress(&.{}, &.{}, .lz4);
}
