using BinanceDataCollector.StorageControllers;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models;

namespace BinanceDataCollector.Collectors.CollectorControllers;

internal abstract class CollectorController<T, T1, T2, T3> : ICollectorController
    where T : class
    where T1 : BinanceKline
    where T2 : BinanceMarkIndexKline?
    where T3 : FuturesFundingRate
{
    protected readonly ILogger logger;
    protected readonly ProductionLine productionLine;
    protected const int year = -1;
    protected abstract bool IsEnable { get; }
    protected abstract bool IsFutures { get; }
    private readonly StorageController<T, T1, T2, T3> storageController;

    public CollectorController(ILogger logger, ProductionLine productionLine, StorageController<T, T1, T2, T3> storageController)
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

        await productionLine.DeleteChannel.Writer.WriteAsync(DeleteOldData(ct), ct);
    }

    private async Task GetLastTimeAsync(T symbol, CancellationToken ct = default)
    {
        KlineInterval interval = KlineInterval.OneMinute;
        if (ct.IsCancellationRequested)
            return;
        DateTime startTime = await storageController.GetLastTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> workItem = new(GatherKlinesAsync, symbol, interval, startTime, ct);
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(workItem, ct);
        if (!IsFutures)
            return;
        DateTime premiumIndexStartTime = await storageController.GetLastPremiumIndexTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> premiumIndexWorkItem = new(GatherPremiumIndexKlinesAsync, symbol, interval, premiumIndexStartTime, ct);
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(premiumIndexWorkItem, ct);

        DateTime fundingStartTime = await storageController.GetLastFundingTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> fundingWorkItem = new(GatherFundingRatesAsync, symbol, fundingStartTime, ct);
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(fundingWorkItem, ct);
    }

    private async Task GatherKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T1>> workItem = await storageController.UpdateKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T2>> workItem = await storageController.UpdatePremiumIndexKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T3>> workItem = await storageController.UpdateFundingRatesAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private AsyncWorkItem DeleteOldData(CancellationToken ct = default)
        => new(storageController.DeleteOldData, ct);

    public Task ExportToCsvAsync(CancellationToken ct = default)
    {
        if (!IsEnable)
            return Task.CompletedTask;
        return storageController.ExportToCsvAsync(ct);
    }
}

internal class SpotCollectorController(IConfiguration configuration, ILogger<SpotCollectorController> logger, ProductionLine productionLine, SpotStorageController storageController) : CollectorController<BinanceSymbolInfo, SpotBinanceKline, BinanceKline?, FuturesFundingRate>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:Spot:IsEnabled", true);
    protected override bool IsFutures => false;
}

internal class CoinFuturesCollectorController(IConfiguration configuration, ILogger<CoinFuturesCollectorController> logger, ProductionLine productionLine, CoinFuturesStorageController storageController) : CollectorController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline, FuturesCoinFundingRate>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:CoinFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}

internal class UsdFuturesCollectorController(IConfiguration configuration, ILogger<UsdFuturesCollectorController> logger, ProductionLine productionLine, UsdFuturesStorageController storageController) : CollectorController<BinanceFuturesUsdtSymbolInfo, FuturesUsdtBinanceKline, FuturesUsdtBinancePremiumIndexKline, FuturesUsdtFundingRate>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:UsdFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}
