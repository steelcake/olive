const std = @import("std");
const Allocator = std.mem.Allocator;
const hash_fn = std.hash.XxHash3.hash;
const xorf = @import("filterz").xorf;
const arrow = @import("arrow");

const Scalar = arrow.scalar.Scalar;

const Compression = @import("./compression.zig").Compression;

pub fn MinMax(comptime T: type) type {
    return struct { min: T, max: T };
}

pub const Range = struct {
    start: u32,
    end: u32,
};

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
        minmax: ?[]const MinMax(T),
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
    minmax: ?[]const MinMax([]const u8),
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
    minmax: ?[]const MinMax([]const u8),
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

    pub fn construct(elems: *const arrow.array.FixedSizeBinaryArray, filter_alloc: Allocator, scratch_alloc: Allocator) xorf.ConstructError!Filter {
        std.debug.assert(elems.null_count == 0);
        std.debug.assert(elems.byte_width > 0);

        var hashes = try scratch_alloc.alloc(u64, elems.len);

        var idx: u32 = elems.offset;
        var i: u32 = 0;
        while (idx < elems.offset + elems.len) : ({
            idx += 1;
            i += 1;
        }) {
            hashes[i] = Filter.hash(arrow.get.get_fixed_size_binary(elems.data.ptr, elems.byte_width, idx));
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
    data: FixedSizeBinaryArray,
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
