const std = @import("std");
const xorf = @import("filterz").xorf;
const arrow = @import("arrow");

pub const write = @import("./write.zig");
pub const chunk = @import("./chunk.zig");

pub const Header = struct {};

pub const DataType = union(enum) {};

pub const IndexType = enum {
    xorf,
    minmax,
};

pub const Compression = enum {};

pub const Field = struct {
    name: [:0]const u8,
    data_type: DataType,
    dict_index: u8,
    indices: []IndexType,
};

pub const Schema = struct {
    fields: []const Field,
};

pub const Table = struct {
    schema: Schema,
    data: arrow.array.StructArray,
};

pub const Dict = struct {};

pub const Chunk = struct {
    table_names: [:0]const u8,
    tables: []const Table,
    dicts: []const Dict,
};
