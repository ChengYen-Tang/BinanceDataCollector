using BinanceDataCollector.StorageControllers;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models;

namespace BinanceDataCollector.Collectors.CollectorControllers;

internal abstract class CollectorController<T, T1> : ICollectorController
    where T : class
    where T1 : BinanceKline
{
    protected readonly ILogger logger;
    protected readonly ProductionLine productionLine;
    protected const int year = -1;
    private readonly StorageController<T, T1> storageController;

    public CollectorController(ILogger logger, ProductionLine productionLine, StorageController<T, T1> storageController)
        => (this.logger, this.productionLine, this.storageController) = (logger, productionLine, storageController);

    public async Task GatherAsync(CancellationToken ct = default)
    {
        Result<List<T>> result = await storageController.UpdateMocketAsync(ct);
        if (result.IsFailed)
        {
            logger.LogError(result.Errors[0].Message);
            return;
        }

        foreach (T symbol in result.Value)
        {
            AsyncWorkItem<T> workItem = new(GetLastTimeAsync, symbol, ct);
            if (await productionLine.GetLastTimeChannel.Writer.WaitToWriteAsync(ct))
                await productionLine.GetLastTimeChannel.Writer.WriteAsync(workItem, ct);
        }

        await productionLine.DeleteKlineChannel.Writer.WriteAsync(DeleteOldKlines(ct), ct);
    }

    private async Task GetLastTimeAsync(T symbol, CancellationToken ct = default)
    {
        KlineInterval interval = KlineInterval.OneMinute;
        if (ct.IsCancellationRequested)
            return;
        DateTime startTime = await storageController.GetLastTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> workItem = new(GatherKlinesAsync, symbol, interval, startTime, ct);
        if (await productionLine.GatherKlineChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherKlineChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T1>> workItem = await storageController.UpdateKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertKlineChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertKlineChannel.Writer.WriteAsync(workItem, ct);
    }

    private AsyncWorkItem DeleteOldKlines(CancellationToken ct = default)
        => new(storageController.DeleteOldKlines, ct);

    public Task ExportToCsvAsync(CancellationToken ct = default)
        => storageController.ExportToCsvAsync(ct);
}

internal class SpotCollectorController : CollectorController<BinanceSymbolInfo, SpotBinanceKline>
{
    public SpotCollectorController(ILogger<SpotCollectorController> logger, ProductionLine productionLine, SpotStorageController storageController)
        : base(logger, productionLine, storageController) { }
}

internal class CoinFuturesCollectorController : CollectorController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline>
{
    public CoinFuturesCollectorController(ILogger<CoinFuturesCollectorController> logger, ProductionLine productionLine, CoinFuturesStorageController storageController)
        : base(logger, productionLine, storageController) { }

}

internal class UsdFuturesCollectorController : CollectorController<BinanceFuturesUsdtSymbolInfo, FuturesUsdtBinanceKline>
{
    public UsdFuturesCollectorController(ILogger<UsdFuturesCollectorController> logger, ProductionLine productionLine, UsdFuturesStorageController storageController)
        : base(logger, productionLine, storageController) { }
}
