const std = @import("std");
const Allocator = std.mem.Allocator;
const hash_fn = std.hash.XxHash3.hash;
const xorf = @import("filterz").xorf;
const arrow = @import("arrow");

const Scalar = arrow.scalar.Scalar;

const Compression = @import("./compression.zig").Compression;

pub const Array = union(enum) {
    null: NullArray,
    i8: Int8Array,
    i16: Int16Array,
    i32: Int32Array,
    i64: Int64Array,
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
};

pub const Page = struct {
    offset: u32,
    size: u32,
};

pub const Buffer = struct {
    pages: []const Page,
    row_index_ends: []const u32,
};

pub const BoolArray = struct {
    values: Buffer,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
};

pub fn PrimitiveArray(comptime T: type) type {
    return struct {
        values: Buffer,
        validity: ?Buffer,
        len: u32,
        offset: u32,
        null_count: u32,
        minmax: ?[]const struct { min: T, max: T },
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
pub const Float16Array = PrimitiveArray(f16);
pub const Float32Array = PrimitiveArray(f32);
pub const Float64Array = PrimitiveArray(f64);

pub const FixedSizeBinaryArray = struct {
    data: Buffer,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
    minmax: ?[]const struct { min: []const u8, max: []const u8 },
};

pub const DictArray = struct {
    keys: *const Array,
    values: *const Array,
    is_ordered: bool,
    /// Not in arrow spec but the len and offset here are applied on top of the len and offset of `keys` similar to how it would work in a struct array.
    len: u32,
    /// Not in arrow spec but the len and offset here are applied on top of the len and offset of `keys` similar to how it would work in a struct array.
    offset: u32,
};

pub const RunEndArray = struct {
    run_ends: *const Array,
    values: *const Array,
    len: u32,
    offset: u32,
};

pub const BinaryArray = struct {
    data: Buffer,
    offsets: Buffer,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
    minmax: ?[]const struct { min: []const u8, max: []const u8 },
};

pub const StructArray = struct {
    field_values: []const Array,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
};

pub const FixedSizeListArray = struct {
    inner: *const Array,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
};

pub const ListArray = struct {
    inner: *const Array,
    offsets: Buffer,
    validity: ?Buffer,
    len: u32,
    null_count: u32,
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
    null_count: u32,
    keys_are_sorted: bool,
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
    data: BinaryArray,
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
