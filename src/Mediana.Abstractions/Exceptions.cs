namespace Mediana;

/// <summary>(, , ).</summary>
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
/// : , fault-
/// Send remote-
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

/// <summary>(request/reply ).</summary>
public class RemoteTimeoutException : TimeoutException
{
    public RemoteTimeoutException(string message)
        : base(message)
    {
    }
}
