extern fn ZSTD_compress(dst: [*]u8, dst_capacity: usize, src: [*]const u8, src_size: usize, compression_level: c_int) callconv(.C) usize;
extern fn ZSTD_compressBound(src_size: usize) callconv(.C) usize;
extern fn ZSTD_isError(code: usize) callconv(.C) c_uint;

extern fn LZ4_compress_default(src: [*]const u8, dst: [*]u8, src_size: c_int, dst_capacity: c_int) callconv(.C) c_int;
extern fn LZ4_compressBound(input_size: c_int) callconv(.C) c_int;

const lz4_frame_preferences = extern struct {
};

extern fn LZ4F_compressFrameBound(src_size: usize, pref: *LZ4F_preferences) callconv(.C) usize;

pub const Compression = enum {
    no_compression,
    lz4,
    zstd,
    lz4_hc,
};

const Error = error {
    CompressFail,
};

pub const UseCase = enum {
    File,
    Wire,
};

fn compress_bound(input_size: usize) usize {
    return @max(ZSTD_compressBound(input_size), LZ4_compressBound(input_size), input_size);
}

fn lz4_compress(src: []const u8, dst: []u8) Error!usize {
    const lz4_size = LZ4_compress_default(src.ptr, dst.ptr, src.len, dst.len);
    if (lz4_size != 0) {
        return lz4_size;
    } else {
        return Error.CompressFail;
    }
}

fn zstd_compress(src: []const u8, dst: []u8) Error!usize {
    const res = ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, 8);
    if (ZSTD_isError(res) == 0) {
        return res;
    } else {
        return Error.CompressFail;
    }
}

fn lz4hc_compress(src: []const u8, dst: []u8) Error!usize {

}

fn select_compression_algo(src: []const u8, dst: []u8, use_case: UseCase) Error!Compression {
    const lz4_size: f64 = switch (use_case) {
        .Wire => @floatFromInt(try lz4_compress(src, dst)),
        .File => @floatFromInt(try lz4hc_compress(src, dst)),
    };

    const zstd_size: f64 = @floatFromInt(try zstd_compress(src, dst));
    const memcpy_size: f64 = @floatFromInt(src.len);

    const lz4_points = 4970.0 / (lz4_size * lz4_size);
    const zstd_points = 1380.0 / (zstd_size * zstd_size);
    const memcpy_points = 13700.0 / (memcpy_size * memcpy_size);

    return if (zstd_points > lz4_points and zstd_points > memcpy_points)
        .zstd
    else if (lz4_points > memcpy_points and lz4_points > zstd_points) 
        switch (use_case) {
            .Wire => .lz4,
            .File => .lz4_hc,
        }
    else 
        .no_compression
    ;
}
