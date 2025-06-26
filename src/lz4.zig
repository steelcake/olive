

pub const LZ4F_blockSizeID_t = c_uint;
pub const LZ4F_frameInfo_t = extern struct {
    blockSizeID: LZ4F_blockSizeID_t = @import("std").mem.zeroes(LZ4F_blockSizeID_t),
    blockMode: LZ4F_blockMode_t = @import("std").mem.zeroes(LZ4F_blockMode_t),
    contentChecksumFlag: LZ4F_contentChecksum_t = @import("std").mem.zeroes(LZ4F_contentChecksum_t),
    frameType: LZ4F_frameType_t = @import("std").mem.zeroes(LZ4F_frameType_t),
    contentSize: c_ulonglong = @import("std").mem.zeroes(c_ulonglong),
    dictID: c_uint = @import("std").mem.zeroes(c_uint),
    blockChecksumFlag: LZ4F_blockChecksum_t = @import("std").mem.zeroes(LZ4F_blockChecksum_t),
};
pub const LZ4F_preferences_t = extern struct {
    frameInfo: LZ4F_frameInfo_t = @import("std").mem.zeroes(LZ4F_frameInfo_t),
    compressionLevel: c_int = @import("std").mem.zeroes(c_int),
    autoFlush: c_uint = @import("std").mem.zeroes(c_uint),
    favorDecSpeed: c_uint = @import("std").mem.zeroes(c_uint),
    reserved: [3]c_uint = @import("std").mem.zeroes([3]c_uint),
};

extern fn LZ4_compress_default(src: [*]const u8, dst: [*]u8, src_size: c_int, dst_capacity: c_int) callconv(.C) c_int;
extern fn LZ4_compressBound(input_size: c_int) callconv(.C) c_int;
