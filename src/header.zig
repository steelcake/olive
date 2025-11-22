const Compression = @import("./compression.zig").Compression;

pub const Array = union(enum) {
    null: NullArray,
    i8: Int8Array,
    i16: Int16Array,
    i32: Int32Array,
    i64: Int64Array,
    i128: Int128Array,
    i256: Int256Array,
    u8: UInt8Array,
    u16: UInt16Array,
    u32: UInt32Array,
    u64: UInt64Array,
    f16: Float16Array,
    f32: Float32Array,
    f64: Float64Array,
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
    interval_year_month: IntervalArray,
    interval_day_time: IntervalArray,
    interval_month_day_nano: IntervalArray,
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

pub const IntervalArray = struct {
    values: Buffer,
    validity: ?Buffer,
    len: u32,
};

pub fn PrimitiveArray(comptime T: type) type {
    return struct {
        values: Buffer,
        validity: ?Buffer,
        len: u32,
        min: ?[]const T,
        max: ?[]const T,
    };
}

pub const UInt8Array = PrimitiveArray(u8);
pub const UInt16Array = PrimitiveArray(u16);
pub const UInt32Array = PrimitiveArray(u32);
pub const UInt64Array = PrimitiveArray(u64);
pub const Int8Array = PrimitiveArray(i8);
pub const Int16Array = PrimitiveArray(i16);
pub const Int32Array = PrimitiveArray(i32);
pub const Int64Array = PrimitiveArray(i64);
pub const Int128Array = PrimitiveArray(i128);
pub const Int256Array = PrimitiveArray(i256);
pub const Float16Array = PrimitiveArray(f16);
pub const Float32Array = PrimitiveArray(f32);
pub const Float64Array = PrimitiveArray(f64);

pub const FixedSizeBinaryArray = struct {
    data: Buffer,
    validity: ?Buffer,
    len: u32,
    min: ?[]const []const u8,
    max: ?[]const []const u8,
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
    min: ?[]const []const u8,
    max: ?[]const []const u8,
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
    min: ?[]const u8,
    max: ?[]const u8,
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
