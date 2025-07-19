const native_endian = @import("builtin").target.cpu.arch.endian();

comptime {
    if (native_endian != .little) {
        @compileError("olive only supports little-endian architectures.");
    }
}

pub const write = @import("./write.zig");
pub const schema = @import("./schema.zig");
pub const header = @import("./header.zig");
pub const compression = @import("./compression.zig");
pub const read = @import("./read.zig");
const dict = @import("./dict.zig");

test {
    _ = dict;
    _ = read;
    _ = write;
    _ = schema;
    _ = header;
    _ = compression;
}
