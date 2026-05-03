using BinanceDataCollector.StorageControllers;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models;

namespace BinanceDataCollector.Collectors.CollectorControllers;

internal abstract class CollectorController<T, T1, T2, T3, T4, T5, T6, T7, T8, T9> : ICollectorController
    where T : class
    where T1 : BinanceKline
    where T2 : BinanceMarkIndexKline
    where T3 : BinanceMarkIndexKline
    where T4 : BinanceMarkIndexKline
    where T5 : FuturesFundingRate
    where T6 : FuturesOpenInterestHistory
    where T7 : FuturesLongShortRatio
    where T8 : FuturesLongShortRatio
    where T9 : FuturesLongShortRatio
{
    protected readonly ILogger logger;
    protected readonly ProductionLine productionLine;
    protected const int year = -1;
    protected abstract bool IsEnable { get; }
    protected abstract bool IsFutures { get; }
    private readonly StorageController<T, T1, T2, T3, T4, T5, T6, T7, T8, T9> storageController;

    public CollectorController(ILogger logger, ProductionLine productionLine, StorageController<T, T1, T2, T3, T4, T5, T6, T7, T8, T9> storageController)
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
            AsyncWorkItem<T> workItem = new(GetLastTimeAsync, symbol, ct, $"Step=GetLastTime, Symbol={symbol}");
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
        AsyncWorkItem<T, KlineInterval, DateTime> workItem = new(GatherKlinesAsync, symbol, interval, startTime, ct, BuildWorkItemDescription("Klines", symbol, interval, startTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(workItem, ct);
        if (!IsFutures)
            return;
        DateTime premiumIndexStartTime = await storageController.GetLastPremiumIndexTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> premiumIndexWorkItem = new(GatherPremiumIndexKlinesAsync, symbol, interval, premiumIndexStartTime, ct, BuildWorkItemDescription("PremiumIndexKlines", symbol, interval, premiumIndexStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(premiumIndexWorkItem, ct);

        DateTime indexPriceStartTime = await storageController.GetLastIndexPriceTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> indexPriceWorkItem = new(GatherIndexPriceKlinesAsync, symbol, interval, indexPriceStartTime, ct, BuildWorkItemDescription("IndexPriceKlines", symbol, interval, indexPriceStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(indexPriceWorkItem, ct);

        DateTime markPriceStartTime = await storageController.GetLastMarkPriceTimeAsync(symbol, interval, ct);
        AsyncWorkItem<T, KlineInterval, DateTime> markPriceWorkItem = new(GatherMarkPriceKlinesAsync, symbol, interval, markPriceStartTime, ct, BuildWorkItemDescription("MarkPriceKlines", symbol, interval, markPriceStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(markPriceWorkItem, ct);

        DateTime fundingStartTime = await storageController.GetLastFundingTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> fundingWorkItem = new(GatherFundingRatesAsync, symbol, fundingStartTime, ct, BuildWorkItemDescription("FundingRates", symbol, startTime: fundingStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(fundingWorkItem, ct);

        DateTime openInterestStartTime = await storageController.GetLastOpenInterestTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> openInterestWorkItem = new(GatherOpenInterestHistoriesAsync, symbol, openInterestStartTime, ct, BuildWorkItemDescription("OpenInterestHistories", symbol, startTime: openInterestStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(openInterestWorkItem, ct);

        DateTime topLongShortPositionStartTime = await storageController.GetLastTopLongShortPositionRatioTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> topLongShortPositionWorkItem = new(GatherTopLongShortPositionRatiosAsync, symbol, topLongShortPositionStartTime, ct, BuildWorkItemDescription("TopLongShortPositionRatios", symbol, startTime: topLongShortPositionStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(topLongShortPositionWorkItem, ct);

        DateTime topLongShortAccountStartTime = await storageController.GetLastTopLongShortAccountRatioTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> topLongShortAccountWorkItem = new(GatherTopLongShortAccountRatiosAsync, symbol, topLongShortAccountStartTime, ct, BuildWorkItemDescription("TopLongShortAccountRatios", symbol, startTime: topLongShortAccountStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(topLongShortAccountWorkItem, ct);

        DateTime globalLongShortAccountStartTime = await storageController.GetLastGlobalLongShortAccountRatioTimeAsync(symbol, ct);
        AsyncWorkItem<T, DateTime> globalLongShortAccountWorkItem = new(GatherGlobalLongShortAccountRatiosAsync, symbol, globalLongShortAccountStartTime, ct, BuildWorkItemDescription("GlobalLongShortAccountRatios", symbol, startTime: globalLongShortAccountStartTime));
        if (await productionLine.GatherChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.GatherChannel.Writer.WriteAsync(globalLongShortAccountWorkItem, ct);
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

    private async Task GatherIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T3>> workItem = await storageController.UpdateIndexPriceKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T4>> workItem = await storageController.UpdateMarkPriceKlinesAsync(symbol, interval, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T5>> workItem = await storageController.UpdateFundingRatesAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T6>> workItem = await storageController.UpdateOpenInterestHistoriesAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T7>> workItem = await storageController.UpdateTopLongShortPositionRatiosAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T8>> workItem = await storageController.UpdateTopLongShortAccountRatiosAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private async Task GatherGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        AsyncWorkItem<IList<T9>> workItem = await storageController.UpdateGlobalLongShortAccountRatiosAsync(symbol, startTime, ct);
        if (await productionLine.InsertChannel.Writer.WaitToWriteAsync(ct))
            await productionLine.InsertChannel.Writer.WriteAsync(workItem, ct);
    }

    private AsyncWorkItem DeleteOldData(CancellationToken ct = default)
        => new(storageController.DeleteOldData, ct, $"Step=DeleteOldData, Controller={GetType().Name}");

    private static string BuildWorkItemDescription(string dataType, T symbol, KlineInterval? interval = null, DateTime? startTime = null)
    {
        string description = $"DataType={dataType}, Symbol={symbol}";
        if (interval.HasValue)
            description += $", Interval={interval.Value}";
        if (startTime.HasValue)
            description += $", StartTime={startTime.Value:O}";
        return description;
    }

    public Task ExportToCsvAsync(CancellationToken ct = default)
    {
        if (!IsEnable)
            return Task.CompletedTask;
        return storageController.ExportToCsvAsync(ct);
    }
}

internal class SpotCollectorController(IConfiguration configuration, ILogger<SpotCollectorController> logger, ProductionLine productionLine, SpotStorageController storageController) : CollectorController<BinanceSymbolInfo, SpotBinanceKline, BinanceMarkIndexKline, BinanceMarkIndexKline, BinanceMarkIndexKline, FuturesFundingRate, FuturesOpenInterestHistory, FuturesLongShortRatio, FuturesLongShortRatio, FuturesLongShortRatio>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:Spot:IsEnabled", true);
    protected override bool IsFutures => false;
}

internal class CoinFuturesCollectorController(IConfiguration configuration, ILogger<CoinFuturesCollectorController> logger, ProductionLine productionLine, CoinFuturesStorageController storageController) : CollectorController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline, FuturesCoinBinanceIndexPriceKline, FuturesCoinBinanceMarkPriceKline, FuturesCoinFundingRate, FuturesCoinOpenInterestHistory, FuturesCoinTopLongShortPositionRatio, FuturesCoinTopLongShortAccountRatio, FuturesCoinGlobalLongShortAccountRatio>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:CoinFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}

internal class UsdFuturesCollectorController(IConfiguration configuration, ILogger<UsdFuturesCollectorController> logger, ProductionLine productionLine, UsdFuturesStorageController storageController) : CollectorController<BinanceFuturesUsdtSymbolInfo, FuturesUsdtBinanceKline, FuturesUsdtBinancePremiumIndexKline, FuturesUsdtBinanceIndexPriceKline, FuturesUsdtBinanceMarkPriceKline, FuturesUsdtFundingRate, FuturesUsdtOpenInterestHistory, FuturesUsdtTopLongShortPositionRatio, FuturesUsdtTopLongShortAccountRatio, FuturesUsdtGlobalLongShortAccountRatio>(logger, productionLine, storageController)
{
    protected override bool IsEnable => configuration.GetValue("Market:UsdFutures:IsEnabled", true);
    protected override bool IsFutures => true;
}
