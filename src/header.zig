const xorf = @import("filterz").xorf;

pub const Compression = enum {
    no_compression,
    lz4,
    zstd,
    int_delta_bit_pack,
    int_bit_pack,
};

pub const RowRange = struct {
    from_idx: u32,
    to_idx: u32,
    min: []u8,
    max: []u8,
};

pub const Page = struct {
    offset: u32,
    size: u32,
    compressed_size: ?u32,
};

pub const Buffer = struct {
    pages: []Page,
    row_ranges: []RowRange,
};

pub const Array = struct {
    buffers: []Buffer,
    children: []Array,
};

pub const Table = struct {
    fields: []Array,
    dict_indices: []u8,
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
    data_section_size: u32,
    tables: []Table,
    dicts: []Dict,
};
