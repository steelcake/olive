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

pub const Page = struct {
    /// Offset of the page start inside the data section of file
    offset: u32,
    /// Uncompressed size of the page
    size: u32,
    /// Compressed size of the page, 0 if not compressed
    compressed_size: u32,
};

pub const Buffer = struct {
    pages: []Page,
    /// Row range and min_max of each page
    row_ranges: []RowRange,
    /// Compression used for all pages inside this buffer
    compression: Compression,
};

pub const Array = struct {
    buffers: []Buffer,
    children: []Array,
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
