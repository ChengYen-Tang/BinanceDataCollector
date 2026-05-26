namespace BinanceDataCollector.Collectors.CollectorControllers;

internal interface ICollectorController
{
    Task GatherAsync(CancellationToken ct = default);
    Task ExportToParquetAsync(CancellationToken ct = default);
}
