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
    min: []u8,
    /// Maximum value that a page has if the field has a minmax index. Otherwise it should be an empty slice.
    max: []u8,
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

/// A page is like a section of an array.
/// If the array is nested the page will be nested as well and contain only the relevant parts of the children.
/// All offsets etc. will be adjusted when writing so all offsets in a page makes sense by itself after loading from file.
pub const Page = struct {
    buffers: []Buffer,
    children: []Page,
};

pub const Array = struct {
    pages: []Page,
    row_ranges: []RowRange,
};

pub const Table = struct {
    fields: []Array,
    dict_indices: []?u8,
};

pub const Filter = struct {
    header: xorf.Header,
    fingerprints: []const u16,
};

pub const Dict = struct {
    data: Array,
    filter: ?Filter,
};

pub const Header = struct {
    tables: []Table,
    dicts: []Dict,
};
