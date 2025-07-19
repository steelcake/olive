const std = @import("std");
const Allocator = std.mem.Allocator;
const hash_fn = std.hash.XxHash3.hash;
const xorf = @import("filterz").xorf;
const arrow = @import("arrow");

const Scalar = arrow.scalar.Scalar;

const Compression = @import("./compression.zig").Compression;

pub const Page = struct {
    /// Offset of the page start inside the data section of file
    offset: u32,
    uncompressed_size: u32,
    /// Compressed size of the page, equals uncompressed_size if parent buffer.compression is set to `.no_compression`
    compressed_size: u32,
};

pub const Buffer = struct {
    pages: []const Page,
    /// This is an untyped slice which should be casted using @ptrCast when using it
    /// We use the alignment of the scalar with highest alignment (i256), which has 32 bytes alignment.
    minmax: ?[]align(32) const u8,
    row_index_ends: []const u32,
    compression: Compression,
};

/// Same layout as described in Arrow Spec
pub const Array = struct {
    buffers: []const Buffer,
    children: []const Array,
    len: u32,
    null_count: u32,
};

pub const Table = struct {
    fields: []const Array,
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
        var hashes = try scratch_alloc.alloc(u64, elems.len);
        for (0..hashes.len) |i| {
            hashes[i] = Filter.hash(elems[i]);
        }
        hashes = sort_and_dedup_hashes(hashes);
        var header = xorf.calculate_header(arity, @intCast(hashes.len));
        const fingerprints = try filter_alloc.alloc(Fingerprint, header.array_length);
        try xorf.construct_fingerprints(Fingerprint, arity, fingerprints, scratch_alloc, hashes, &header);

        return .{
            .header = header,
            .fingerprints = fingerprints,
        };
    }
};

pub const Dict = struct {
    data: Array,
    filter: ?Filter,
};

pub const Header = struct {
    tables: []const Table,
    dicts: []const ?Dict,
    data_section_size: u32,
};

/// Ascending sort hashes and deduplicate
fn sort_and_dedup_hashes(hashes: []u64) []u64 {
    if (hashes.len == 0) {
        return hashes;
    }

    std.mem.sortUnstable(u64, hashes, {}, std.sort.asc(u64));
    var write_idx: usize = 0;

    for (hashes[1..]) |hash| {
        if (hash != hashes[write_idx]) {
            write_idx += 1;
            hashes[write_idx] = hash;
        }
    }

    return hashes[0 .. write_idx + 1];
}
