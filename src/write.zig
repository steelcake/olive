const std = @import("std");
const Allocator = std.mem.Allocator;
const arrow = @import("arrow");
const arr = arrow.array;

const native_endian = @import("builtin").target.cpu.arch.endian();

const header = @import("./header.zig");
const chunk = @import("./chunk.zig");
const schema = @import("./schema.zig");

pub const Error = error{
    UnsupportedTypeForMinMax,
    OutOfMemory,
    DictArrayNotSupported,
    RunEndEncodedArrayNotSupported,
    BinaryViewArrayNotSupported,
    ListViewArrayNotSupported,
    DataSectionOverflow,
};

fn to_little_endian(val: anytype) @TypeOf(val) {
    return switch (native_endian) {
        .big => @byteSwap(val),
        .little => val,
    };
}

pub const Write = struct {
    /// Input data and schema.
    chunk: *const chunk.Chunk,
    /// Allocator that is used for allocating any dynamic memory relating to outputted header.
    /// Lifetime of the header is tied to this allocator after creation.
    header_alloc: Allocator,
    /// Allocator for allocating temporary memory used for constructing the output
    scratch_alloc: Allocator,
    /// For outputting the buffers
    data_section: []u8,
};

pub fn write(params: Write) Error!header.Header {
    // Construct dictionaries
    //
    // Construct Pages/Buffers
    //
    //

    const num_dicts = params.chunk.schema.dict_has_filter.len;
    const dicts = try params.header_alloc.alloc(header.Dict, num_dicts);

    const tables = try params.header_alloc.alloc(header.Table, params.chunk.data.len);

    return .{
        .dicts = dicts,
        .tables = tables,
    };
}
