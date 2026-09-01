namespace Mediana.Messaging;

/// <summary>
/// UUIDv7 (RFC 9562): time-ordered, index-friendly идентификатор сообщения (D10).
/// net10.0 — Guid.CreateVersion7(); netstandard2.1 — собственная реализация (D14).
/// </summary>
public static class GuidV7
{
#if !NET10_0
    private static long _lastTimestamp;
    private static ushort _sequence;
    private static readonly Random Rng = new();
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
    private static long NextInt64()
    {
        lock (Lock)
        {
            return (((long)Rng.Next()) << 31) | (uint)Rng.Next();
        }
    }

    private static Guid Create()
    {
        lock (Lock)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (timestamp <= _lastTimestamp)
            {
                // монотонность внутри миллисекунды — счётчик в младших битах randB
                _sequence++;
            }
            else
            {
                _lastTimestamp = timestamp;
                _sequence = 0;
            }

            Span<byte> bytes = stackalloc byte[16];
            // Guid хранит data1/data2 little-endian: пишем timestamp в mixed-endian раскладку,
            // чтобы строковая форма UUIDv7 была корректной (RFC big-endian в выводе)
            bytes[3] = (byte)(timestamp >> 40);
            bytes[2] = (byte)(timestamp >> 32);
            bytes[1] = (byte)(timestamp >> 24);
            bytes[0] = (byte)(timestamp >> 16);
            bytes[5] = (byte)(timestamp >> 8);
            bytes[4] = (byte)timestamp;

            // версия 7: RFC octet-6 соответствует Guid-layout байту 7
            var randA = (ushort)(Rng.Next(0, 0x1000) & 0x0FFF);
            bytes[7] = (byte)(0x70 | (randA >> 8));
            bytes[6] = (byte)randA;

            // вариант 10xx + 62 бита: случайность и счётчик в младших 16
            var randB = (ulong)NextInt64() & 0x3FFFFFFFFFFFFFFFUL;
            randB = (randB & ~0xFFFFUL) | _sequence;
            bytes[8] = (byte)(0x80 | (randB >> 56));
            bytes[9] = (byte)(randB >> 48);
            bytes[10] = (byte)(randB >> 40);
            bytes[11] = (byte)(randB >> 32);
            bytes[12] = (byte)(randB >> 24);
            bytes[13] = (byte)(randB >> 16);
            bytes[14] = (byte)(randB >> 8);
            bytes[15] = (byte)randB;

            return new Guid(bytes);
        }
    }
#endif
}
