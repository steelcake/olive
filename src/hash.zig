const rotl = @import("std").math.rotl;

// adapted version of std.hash.XxHash64
fn Hasher(comptime W: comptime_int) type {
    const prime_1 = 0x9E3779B185EBCA87; // 0b1001111000110111011110011011000110000101111010111100101010000111
    const prime_2 = 0xC2B2AE3D27D4EB4F; // 0b1100001010110010101011100011110100100111110101001110101101001111
    const prime_3 = 0x165667B19E3779F9; // 0b0001011001010110011001111011000110011110001101110111100111111001
    const prime_4 = 0x85EBCA77C2B2AE63; // 0b1000010111101011110010100111011111000010101100101010111001100011
    const prime_5 = 0x27D4EB2F165667C5; // 0b0010011111010100111010110010111100010110010101100110011111000101

    return struct {
        fn finalize8(v: u64, bytes: [8]u8) u64 {
            var acc = v;
            const lane: u64 = @bitCast(bytes);
            acc ^= round(0, lane);
            acc = rotl(u64, acc, 27) *% prime_1;
            acc +%= prime_4;
            return acc;
        }

        fn finalize4(v: u64, bytes: [4]u8) u64 {
            var acc = v;
            const lane: u32 = @bitCast(bytes);
            acc ^= lane *% prime_1;
            acc = rotl(u64, acc, 23) *% prime_2;
            acc +%= prime_3;
            return acc;
        }

        fn round(acc: u64, lane: u64) u64 {
            const a = acc +% (lane *% prime_2);
            const b = rotl(u64, a, 31);
            return b *% prime_1;
        }

        fn avalanche(value: u64) u64 {
            var result = value ^ (value >> 33);
            result *%= prime_2;
            result ^= result >> 29;
            result *%= prime_3;
            result ^= result >> 32;

            return result;
        }

        pub fn hash(seed: u64, input: [W]u8) u64 {
            var acc = seed +% prime_5 +% W;
            switch (W) {
                20 => {
                    acc = finalize8(acc, input[0..8]);
                    acc = finalize8(acc, input[8..16]);
                    acc = finalize4(acc, input[16..20]);
                    return avalanche(acc);
                },
                32 => {
                    acc = finalize8(acc, input[0..8]);
                    acc = finalize8(acc, input[8..16]);
                    acc = finalize8(acc, input[16..24]);
                    acc = finalize8(acc, input[24..32]);
                    return avalanche(acc);
                },
                else => @compileError("unsupported width"),
            }
        }
    };
}
