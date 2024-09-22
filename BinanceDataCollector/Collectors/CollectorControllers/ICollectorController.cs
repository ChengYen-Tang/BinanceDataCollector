namespace BinanceDataCollector.Collectors.CollectorControllers;

internal interface ICollectorController
{
    Task GatherAsync(CancellationToken ct = default);
    Task ExportToCsvAsync(CancellationToken ct = default);
}
