namespace Mediana;

/// <summary>Message graph configuration error (no handler, duplicate, invalid policy).</summary>
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
/// Remote execution error: the remote host's error type, serialized details, and fault envelope.
/// Local Send propagates exceptions as-is — this type is for remote paths only.
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

/// <summary>Remote query timeout (request/reply over transport).</summary>
public class RemoteTimeoutException : TimeoutException
{
    public RemoteTimeoutException(string message)
        : base(message)
    {
    }
}
