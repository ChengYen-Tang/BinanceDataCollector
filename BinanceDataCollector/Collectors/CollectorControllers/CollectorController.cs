using BinanceDataCollector.StorageControllers;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models;

namespace BinanceDataCollector.Collectors.CollectorControllers;

internal abstract class CollectorController<T, T1, T2> : ICollectorController
    where T : class
    where T1 : BinanceKline
    where T2 : BinanceMarkIndexKline?
{
    protected readonly ILogger logger;
    protected readonly ProductionLine productionLine;
    protected const int year = -1;
    protected abstract bool IsEnable { get; }
    protected abstract bool IsFutures { get; }
    private readonly StorageController<T, T1, T2> storageController;

    public CollectorController(ILogger logger, ProductionLine productionLine, StorageController<T, T1, T2> storageController)
        => (this.logger, this.productionLine, this.storageController) = (logger, productionLine, storageController);

    public async Task GatherAsync(CancellationToken ct = default)
    {
        if (!IsEnable)
            return;
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
        if (!IsFutures)
            return;
        AsyncWorkItem<T, KlineInterval, DateTime> premiumIndexWorkItem = new(GatherPremiumIndexKlinesAsync, symbol, interval, startTime, ct);
        if (await productionLine.GatherKlineChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherKlineChannel.Writer.WriteAsync(premiumIndexWorkItem, ct);
    }

    private async Task GatherKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T1>> workItem = await storageController.UpdateKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertKlineChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertKlineChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T2>> workItem = await storageController.UpdatePremiumIndexKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertKlineChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertKlineChannel.Writer.WriteAsync(workItem, ct);
    }

    private AsyncWorkItem DeleteOldKlines(CancellationToken ct = default)
        => new(storageController.DeleteOldKlines, ct);

    public Task ExportToCsvAsync(CancellationToken ct = default)
    {
        if (!IsEnable)
            return Task.CompletedTask;
        return storageController.ExportToCsvAsync(ct);
    }
}

internal class SpotCollectorController(IConfiguration configuration, ILogger<SpotCollectorController> logger, ProductionLine productionLine, SpotStorageController storageController) : CollectorController<BinanceSymbolInfo, SpotBinanceKline, BinanceKline?>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:Spot:IsEnabled", true);
    protected override bool IsFutures => false;
}

internal class CoinFuturesCollectorController(IConfiguration configuration, ILogger<CoinFuturesCollectorController> logger, ProductionLine productionLine, CoinFuturesStorageController storageController) : CollectorController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:CoinFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}

internal class UsdFuturesCollectorController(IConfiguration configuration, ILogger<UsdFuturesCollectorController> logger, ProductionLine productionLine, UsdFuturesStorageController storageController) : CollectorController<BinanceFuturesUsdtSymbolInfo, FuturesUsdtBinanceKline, FuturesUsdtBinancePremiumIndexKline>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:UsdFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}
