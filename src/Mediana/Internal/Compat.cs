using System.Diagnostics.CodeAnalysis;

namespace Mediana.Internal;

/// <summary>Guard-проверки без зависимости от ThrowIfNull (net6+).</summary>
internal static class Guard
{
    [DoesNotReturn]
    internal static void ThrowNull(string paramName)
        => throw new ArgumentNullException(paramName);

    internal static void NotNull([NotNull] object? value, string paramName)
    {
        if (value is null)
        {
            ThrowNull(paramName);
        }
    }
}
