namespace Mediana.Generators;

/// <summary>Диагностики генератора Mediana.</summary>
public static class MedianaDiagnostics
{
    public const string DuplicateHandlerId = "MED001";

    public const string DuplicateHandlerTitle = "Duplicate message handler";

    public const string DuplicateHandlerMessage =
        "Message '{0}' already has a {1} handler '{2}'; a {1} must have exactly one handler";

    public const string Category = "Mediana";
}
