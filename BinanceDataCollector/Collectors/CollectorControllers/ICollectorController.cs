namespace BinanceDataCollector.Collectors.CollectorControllers;

internal interface ICollectorController
{
    Task<bool> PrepareAsync(CancellationToken ct = default);
    Task DispatchAsync(CancellationToken ct = default);
}
