const std = @import("std");
const hash_fn = std.hash.XxHash3.hash;
const xorf = @import("filterz").xorf;

pub const Compression = enum {
    no_compression,
    lz4,
    zstd,
    int_delta_bit_pack,
    int_bit_pack,
};

/// 32 byte prefixes of min/max values a page has.
/// Integers are encoded as little endian.
pub const MinMax = struct {
    min: [32]u8,
    max: [32]u8,
};

pub const Page = struct {
    /// Offset of the page start inside the data section of file
    offset: u32,
    uncompressed_size: u32,
    /// Compressed size of the page, 0 if not compressed
    compressed_size: u32,
};

pub const Buffer = struct {
    pages: []const Page,
    minmax: []const MinMax,
    row_index_ends: []const u32,
    compression: Compression,
};

/// Same layout as described in Arrow Spec
pub const Array = struct {
    buffers: []const Buffer,
    children: []const Array,
};

pub const Table = struct {
    fields: []const Array,
    dict_indices: []const ?u8,
};

pub const Filter = struct {
    const Fingerprint = u16;
    const arity = 3;

    header: xorf.Header,
    fingerprints: []const Fingerprint,

    pub fn hash(key: anytype) u64 {
        return hash_fn(0, key);
    }

    pub fn check_hash(self: *const Filter, hash_: u64) bool {
        return xorf.filter_check(Fingerprint, arity, &self.header, self.fingerprints, hash_);
    }
};

pub const Dict = struct {
    data: Array,
    filter: ?Filter,
};

pub const Header = struct {
    tables: []const Table,
    dicts: []const Dict,
    data_section_size: u32,
};
