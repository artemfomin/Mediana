namespace Mediana;

/// <summary>Ошибка конфигурации графа сообщений (нет хендлера, дубликат, некорректная политика).</summary>
public class MediatorConfigurationException : Exception
{
    public MediatorConfigurationException(string message)
        : base(message)
    {
    }

    public MediatorConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Ошибка выполнения удалённого запроса: тип ошибки хоста-обработчика, сериализованные детали и fault-конверт.
/// Локальный Send исключение прокидывает как есть — этот тип только для remote-путей.
/// </summary>
public class RemoteExecutionException : Exception
{
    public RemoteExecutionException(string message, string? remoteErrorType, IReadOnlyDictionary<string, string?>? details)
        : base(message)
    {
        RemoteErrorType = remoteErrorType;
        Details = details ?? EmptyDetails;
    }

    public string? RemoteErrorType { get; }

    public IReadOnlyDictionary<string, string?> Details { get; }

    private static readonly IReadOnlyDictionary<string, string?> EmptyDetails =
        new Dictionary<string, string?>();
}

/// <summary>Таймаут удалённого запроса (request/reply поверх транспорта).</summary>
public class RemoteTimeoutException : TimeoutException
{
    public RemoteTimeoutException(string message)
        : base(message)
    {
    }
}
