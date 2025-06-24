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

pub const RowRange = struct {
    /// The row index that the page starts from inside an array (Buffer). Inclusive
    from_idx: u32,
    /// The row index that the page ends at inside an array (Buffer). Exclusive
    to_idx: u32,
    /// Minimum value that a page has if the field has a minmax index. Otherwise it should be an empty slice.
    min: []const u8,
    /// Maximum value that a page has if the field has a minmax index. Otherwise it should be an empty slice.
    max: []const u8,
};

pub const Buffer = struct {
    /// Offset of the page start inside the data section of file
    offset: u32,
    /// Uncompressed size of the page
    size: u32,
    /// Compressed size of the page, 0 if not compressed
    compressed_size: u32,
    compression: Compression,
};

/// A page is like a section of an array. It is same as the raw representation of an array in arrow spec with array offset removed.
/// If the array is nested the page will be nested as well and contain only the relevant parts of the children.
/// All offsets etc. will be adjusted when writing so all offsets in a page makes sense by itself after loading from file.
pub const Page = struct {
    buffers: []const Buffer,
    children: []const Page,
};

pub const Array = struct {
    pages: []const Page,
    row_ranges: []const RowRange,
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
