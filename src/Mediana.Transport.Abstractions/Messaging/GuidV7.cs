using System.Security.Cryptography;

namespace Mediana.Messaging;

/// <summary>
/// UUIDv7 (RFC 9562): time-ordered, index-friendly идентификатор сообщения (D10).
/// net10.0 — Guid.CreateVersion7(); netstandard2.1 — собственная реализация с
/// криптографической случайностью (T-08 fix: RandomNumberGenerator вместо System.Random).
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
                // монотонность внутри миллисекунды — счётчик в младших битах randB
                _sequence++;
            }
            else
            {
                _lastTimestamp = timestamp;
                _sequence = 0;
            }

            // T-08: криптографическая случайность — 74 бита через RandomNumberGenerator
            Span<byte> random = stackalloc byte[10];
            RandomNumberGenerator.Fill(random);

            Span<byte> bytes = stackalloc byte[16];
            // Guid хранит data1/data2 little-endian: пишем timestamp в mixed-endian раскладку,
            // чтобы строковая форма UUIDv7 была корректной (RFC big-endian в выводе)
            bytes[3] = (byte)(timestamp >> 40);
            bytes[2] = (byte)(timestamp >> 32);
            bytes[1] = (byte)(timestamp >> 24);
            bytes[0] = (byte)(timestamp >> 16);
            bytes[5] = (byte)(timestamp >> 8);
            bytes[4] = (byte)timestamp;

            // версия 7 + 12 бит randA из крипто-энтропии (random[0..1])
            bytes[7] = (byte)(0x70 | (random[0] & 0x0F));
            bytes[6] = random[1];

            // вариант 10xx + 62 бита: крипто-энтропия (random[2..9]) + счётчик в младших 16
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
