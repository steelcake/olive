const std = @import("std");
const Allocator = std.mem.Allocator;
const hash_fn = std.hash.XxHash3.hash;
const xorf = @import("filterz").xorf;

pub const Compression = enum {
    no_compression,
    lz4,
    zstd,
};

/// Number of bytes for MinMax values
pub const MinMaxLen = 32;

/// 32 byte prefixes of min/max values a page has.
/// Integral values are encoded as little endian.
/// Null values are not included in minmax.
///
/// min will be {u8.MAX} * 32 and max will be {0} * 32 if there wasn't any non-null values in the array
pub const MinMax = struct {
    min: [MinMaxLen]u8,
    max: [MinMaxLen]u8,
};

pub const Page = struct {
    /// Offset of the page start inside the data section of file
    offset: u32,
    uncompressed_size: u32,
    /// Compressed size of the page, equals uncompressed_size if parent buffer.compression is set to `.no_compression`
    compressed_size: u32,
};

pub const Buffer = struct {
    pages: []const Page,
    minmax: ?[]const MinMax,
    row_index_ends: []const u32,
    compression: Compression,
};

/// Same layout as described in Arrow Spec
pub const Array = struct {
    buffers: []const Buffer,
    children: []const Array,
    minmax: ?MinMax,
    len: u32,
    null_count: u32,
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

    pub fn construct(elems: []const []const u8, scratch_alloc: Allocator, filter_alloc: Allocator) xorf.ConstructError!Filter {
        const hashes = try scratch_alloc.alloc(u64, elems.len);
        for (0..hashes.len) |i| {
            hashes[i] = Filter.hash(elems[i]);
        }
        // const header = xorf.calculate_header(arity, );
        // xorf.construct_fingerprints(Fingerprint)
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
