using Mediana.Handlers;
using Mediana.Messaging;

namespace Mediana.UnitTests.ScanTargets;

public sealed record ScanMsg(int V) : ICommand<int>;

/// <summary>Scan to : generic-and, abstract, interface.</summary>
public sealed record ScanEvent : IEvent;

public class GenericHandler<T> : ICommandHandler<ScanMsg, int>
{
    public ValueTask<int> Handle(ScanMsg c, CancellationToken ct) => new(c.V);
}

public abstract class AbstractScanHandler : ICommandHandler<ScanMsg, int>
{
    public abstract ValueTask<int> Handle(ScanMsg c, CancellationToken ct);
}

public interface IScanHandler : ICommandHandler<ScanMsg, int>;

public sealed class ConcreteScanHandler : ICommandHandler<ScanMsg, int>
{
    public ValueTask<int> Handle(ScanMsg c, CancellationToken ct) => new(c.V + 1);
}

public sealed class ScanEventHandler : IEventHandler<ScanEvent>
{
    public ValueTask Handle(ScanEvent e, CancellationToken ct) => default;
}

/// <summary>Generic-interface andonand and on (IsInterface + IsGenericTypeDefinition).</summary>
public interface IGenScanHandler<T> : ICommandHandler<ScanMsg, int>;

/// <summary>Abstract-generic andonand (IsAbstract + IsGenericTypeDefinition).</summary>
public abstract class AbstractGenericScanHandler<T> : ICommandHandler<ScanMsg, int>
{
    public abstract ValueTask<int> Handle(ScanMsg c, CancellationToken ct);
}
