using Mediana.Handlers;
using Mediana.Messaging;

namespace Mediana.UnitTests.ScanTargets;

public sealed record ScanMsg(int V) : ICommand<int>;

/// <summary>Scan должен ИГНОРИРОВАТЬ: generic-определение, abstract, interface.</summary>
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
