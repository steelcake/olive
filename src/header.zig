const Compression = @import("./compression.zig").Compression;

pub const Array = union(enum) {
    null: NullArray,
    primitive: PrimitiveArray,
    binary: BinaryArray,
    bool: BoolArray,
    list: ListArray,
    struct_: StructArray,
    dense_union: DenseUnionArray,
    sparse_union: SparseUnionArray,
    fixed_size_binary: FixedSizeBinaryArray,
    fixed_size_list: FixedSizeListArray,
    map: MapArray,
    run_end_encoded: RunEndArray,
    dict: DictArray,
};

pub const Page = struct {
    offset: u32,
    uncompressed_size: u32,
    compressed_size: u32,
};

pub const Buffer = struct {
    pages: []const Page,
    row_index_ends: []const u32,
    compression: Compression,
};

pub const BoolArray = struct {
    values: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub const PrimitiveArray = struct {
    values: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub const FixedSizeBinaryArray = struct {
    data: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub const DictArray = struct {
    keys: *const Array,
    values: *const Array,
    is_ordered: bool,
    len: u32,
};

pub const RunEndArray = struct {
    run_ends: *const Array,
    values: *const Array,
    len: u32,
};

pub const BinaryArray = struct {
    data: Buffer,
    offsets: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub const StructArray = struct {
    field_values: []const Array,
    validity: ?Buffer,
    len: u32,
};

pub const FixedSizeListArray = struct {
    inner: *const Array,
    validity: ?Buffer,
    len: u32,
};

pub const ListArray = struct {
    inner: *const Array,
    offsets: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub const UnionArray = struct {
    type_ids: Buffer,
    children: []const Array,
    len: u32,
};

pub const DenseUnionArray = struct {
    offsets: Buffer,
    inner: UnionArray,
};

pub const SparseUnionArray = struct {
    inner: UnionArray,
};

pub const NullArray = struct {
    len: u32,
};

pub const MapArray = struct {
    entries: *const StructArray,
    offsets: Buffer,
    validity: ?Buffer,
    len: u32,
    keys_are_sorted: bool,
};

pub const Table = struct {
    fields: []const Array,
    num_rows: u32,
};

pub const Dict = struct {
    offset: u32,
    size: u32,
};

pub const DictContext = struct {
    dict20: Dict,
    dict32: Dict,
};

pub const Header = struct {
    tables: []const Table,
    dict_ctx: DictContext,
    data_section_size: u32,
};
