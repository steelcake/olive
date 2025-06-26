pub const BlockSizeID = enum (c_uint) {
    default = 0,
    max64kb = 4,
    max256kb = 5,
    max1mb = 6,
    max4mb = 7,
};

pub const BlockMode = enum (c_uint) {
    linked = 0,
    independent = 1,
};

pub const ContentChecksum = enum (c_uint) {
    no_content_checksum = 0,
    content_checksum_enabled = 1,
};

pub const BlockChecksum = enum (c_uint) {
    no_block_checksum = 0,
    block_checksum_enabled = 1,
};

pub const FrameType = enum (c_uint) {
    frame = 0,
    skippable_frame = 1,
};

pub const FrameInfo = extern struct {
    block_size_id: BlockSizeID,
    block_mode: BlockMode,
    content_checksum_flag: ContentChecksum,
    frame_type: FrameType,
    content_size: c_ulonglong,
    dict_id: c_uint,
    block_checksum_flag: BlockChecksum,
};

pub const Preferences = extern struct {
    frame_info: FrameInfo,
    compression_level: c_int,
    auto_flush: c_uint,
    favor_dec_speed: c_uint,
    reserved: [3]c_uint,
};

extern fn LZ4F_compressFrameBound(src_size: usize, pref: *const Preferences) callconv(.C) usize;

extern fn LZ4F_compressFrame(dst: [*]u8, dst_capacity: usize, src: [*]const u8, src_size: usize, pref: *const Preferences) callconv(.C) usize;

pub fn compress_bound(src_size: usize, pref: *const Preferences) usize {
    return LZ4F_compressFrameBound(src_size, pref);
}

pub fn compress_frame(dst: []u8, src: []const u8, pref: *const Preferences) usize {
    return LZ4F_compressFrame(dst.ptr, dst.len, src.ptr, src.len, pref);
}
