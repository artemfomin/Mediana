using System.Security.Cryptography;

namespace Mediana.Messaging;

/// <summary>
/// UUIDv7 (RFC 9562): time-ordered, index-friendly message identifier (D10).
/// net10.0 — Guid.CreateVersion7(); netstandard2.1 — inon fromand
/// andthenand but (T-08 fix: RandomNumberGenerator inthen System.Random)
/// </summary>
public static class GuidV7
{
#if !NET10_0
    private static long _lastTimestamp;
    private static ushort _sequence;
    private static readonly object Lock = new();
#endif

    public static Guid NewGuid()
    {
#if NET10_0
        return Guid.CreateVersion7();
#else
        return Create();
#endif
    }

#if !NET10_0
    private static Guid Create()
    {
        lock (Lock)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (timestamp <= _lastTimestamp)
            {
                // monotonicity within the same millisecond — counter in the low bits of randB
                _sequence++;
            }
            else
            {
                _lastTimestamp = timestamp;
                _sequence = 0;
            }

            // T-08: andthenand but — 74 and RandomNumberGenerator
            Span<byte> random = stackalloc byte[10];
            RandomNumberGenerator.Fill(random);

            Span<byte> bytes = stackalloc byte[16];
            // Guid and data1/data2 little-endian: and timestamp in mixed-endian
            // then in UUIDv7 but (RFC big-endian in inin)
            bytes[3] = (byte)(timestamp >> 40);
            bytes[2] = (byte)(timestamp >> 32);
            bytes[1] = (byte)(timestamp >> 24);
            bytes[0] = (byte)(timestamp >> 16);
            bytes[5] = (byte)(timestamp >> 8);
            bytes[4] = (byte)timestamp;

            // inand 7 + 12 and randA from andthen-andand (random[0..1])
            bytes[7] = (byte)(0x70 | (random[0] & 0x0F));
            bytes[6] = random[1];

            // inand 10xx + 62 and: andthen-and (random[2..9]) + and in and 16
            bytes[8] = (byte)(0x80 | (random[2] & 0x3F));
            bytes[9] = random[3];
            bytes[10] = random[4];
            bytes[11] = random[5];
            bytes[12] = random[6];
            bytes[13] = random[7];
            bytes[14] = (byte)(_sequence >> 8);
            bytes[15] = (byte)_sequence;

            return new Guid(bytes);
        }
    }
#endif
}
