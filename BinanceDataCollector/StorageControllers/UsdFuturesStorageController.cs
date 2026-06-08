using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels.Models.Storage;
using MarketDataBase = BinanceDataCollector.Collectors.BinanceMarketData.BaseMarketData;
using MarketDataDownloadBatch = BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch;
using MarketDataUsdFutures = BinanceDataCollector.Collectors.BinanceMarketData.UsdFutures;

namespace BinanceDataCollector.StorageControllers;

internal class UsdFuturesStorageController : StorageController<SymbolInfoCsv>
{
    private const string Market = "UsdFutures";
    private readonly UsdFutures usdFutures;
    private readonly MarketDataUsdFutures usdFuturesMarketData;

    public UsdFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<UsdFuturesStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (usdFutures, usdFuturesMarketData) = (new(client, configuration.GetSection("IgnoneCoins:UsdFutures").Get<string[]>() ?? []), new());

    protected override string MarketPathSegment => Market;
    protected override string SymbolInfoPath { get { return Path.Combine(RootSymbolInfoPath, "SymbolInfo.duckdb"); } }
    protected override string KlinePath { get { return Path.Combine(RootKlinePath, Market); } }
    protected override string PremiumIndexKlinePath { get { return Path.Combine(RootPremiumIndexKlinePath, Market); } }
    protected override string IndexPriceKlinePath { get { return Path.Combine(RootIndexPriceKlinePath, Market); } }
    protected override string MarkPriceKlinePath { get { return Path.Combine(RootMarkPriceKlinePath, Market); } }
    protected override string FundingRatePath { get { return Path.Combine(RootFundingRatePath, Market); } }
    protected override string OpenInterestPath { get { return Path.Combine(RootOpenInterestPath, Market); } }
    protected override string TopLongShortPositionRatioPath { get { return Path.Combine(RootTopLongShortPositionRatioPath, Market); } }
    protected override string TopLongShortAccountRatioPath { get { return Path.Combine(RootTopLongShortAccountRatioPath, Market); } }
    protected override string GlobalLongShortAccountRatioPath { get { return Path.Combine(RootGlobalLongShortAccountRatioPath, Market); } }
    protected override string TakerLongShortRatioPath { get { return Path.Combine(RootTakerLongShortRatioPath, Market); } }
    protected override string BasisPath { get { return Path.Combine(RootBasisPath, Market); } }
    protected override bool IsFutures => true;
    protected override AggTradesTimeUnit AggTradesTimeUnit => AggTradesTimeUnit.Milliseconds;
    protected override string GetSymbolName(SymbolInfoCsv symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(CancellationToken ct = default)
        => GetStoredSymbolNamesAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> currentSymbols, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await DeleteSymbolDatabasesAsync(
        [
            KlinePath,
            PremiumIndexKlinePath,
            IndexPriceKlinePath,
            MarkPriceKlinePath,
            FundingRatePath,
            OpenInterestPath,
            BasisPath,
            TopLongShortPositionRatioPath,
            TopLongShortAccountRatioPath,
            GlobalLongShortAccountRatioPath,
            TakerLongShortRatioPath,
        ], currentSymbols, ct);
        await DeleteAggTradesStorageAsync(currentSymbols, delistedSymbols, ct);
        await DeleteBookDepthStorageAsync(currentSymbols, ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        List<string> symbolNames = await GetStoredSymbolNamesAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            await DeleteSymbolRowsBeforeAsync(KlinePath, symbolName, nameof(Kline.OpenTime), ct);
            await DeleteSymbolRowsBeforeAsync(PremiumIndexKlinePath, symbolName, nameof(PremiumIndexKline.OpenTime), ct);
            await DeleteSymbolRowsBeforeAsync(IndexPriceKlinePath, symbolName, nameof(PremiumIndexKline.OpenTime), ct);
            await DeleteSymbolRowsBeforeAsync(MarkPriceKlinePath, symbolName, nameof(PremiumIndexKline.OpenTime), ct);
            await DeleteSymbolRowsBeforeAsync(FundingRatePath, symbolName, nameof(FundingRate.FundingTime), ct);
            await DeleteSymbolRowsBeforeAsync(OpenInterestPath, symbolName, nameof(OpenInterestHistory.Timestamp), ct);
            await DeleteSymbolRowsBeforeAsync(BasisPath, symbolName, nameof(FuturesBasisCsv.Timestamp), ct);
            await DeleteSymbolRowsBeforeAsync(TopLongShortPositionRatioPath, symbolName, nameof(LongShortRatioCsv.Timestamp), ct);
            await DeleteSymbolRowsBeforeAsync(TopLongShortAccountRatioPath, symbolName, nameof(LongShortRatioCsv.Timestamp), ct);
            await DeleteSymbolRowsBeforeAsync(GlobalLongShortAccountRatioPath, symbolName, nameof(LongShortRatioCsv.Timestamp), ct);
            await DeleteSymbolRowsBeforeAsync(TakerLongShortRatioPath, symbolName, nameof(TakerLongShortRatioCsv.Timestamp), ct);
            await DeleteOldAggTradesDataAsync(symbolName, ct);
            await DeleteOldBookDepthDataAsync(symbolName, ct);
        }
    }

    public override Task<DateTime> GetLastTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(KlinePath, symbol.Name, nameof(Kline.CloseTime), nameof(Kline), interval, null, ct);

    public override Task<DateTime> GetLastPremiumIndexTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(PremiumIndexKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), nameof(PremiumIndexKline), interval, null, ct);

    public override Task<DateTime> GetLastIndexPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(IndexPriceKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), "IndexPriceKline", interval, null, ct);

    public override Task<DateTime> GetLastMarkPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(MarkPriceKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), "MarkPriceKline", interval, null, ct);

    protected override async Task<Result<IReadOnlyList<Kline>>> GetKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await usdFutures.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok<IReadOnlyList<Kline>>(ConvertToModelRows<IBinanceKline, Kline>(result.Value, (kline, row) =>
        {
            row.OpenPrice = decimal.ToDouble(kline.OpenPrice);
            row.ClosePrice = decimal.ToDouble(kline.ClosePrice);
            row.HighPrice = decimal.ToDouble(kline.HighPrice);
            row.LowPrice = decimal.ToDouble(kline.LowPrice);
            row.Volume = decimal.ToDouble(kline.Volume);
            row.QuoteVolume = decimal.ToDouble(kline.QuoteVolume);
            row.TakerBuyBaseVolume = decimal.ToDouble(kline.TakerBuyBaseVolume);
            row.TakerBuyQuoteVolume = decimal.ToDouble(kline.TakerBuyQuoteVolume);
            row.TradeCount = kline.TradeCount;
            row.OpenTime = ToUnixMilliseconds(kline.OpenTime);
            row.CloseTime = ToUnixMilliseconds(kline.CloseTime);
        }));
    }

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(SymbolInfoCsv symbol, DateTime downloadStartTime, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadAggTradesAsync(symbol.Name, downloadStartTime, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override Task<Result<MarketDataDownloadBatch>> GetBookDepthAsync(SymbolInfoCsv symbol, DateTime downloadStartTime, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadBookDepthAsync(symbol.Name, downloadStartTime, GetMarketDataTempSymbolPath(MarketDataBase.BookDepthDataType, symbol.Name), ct);

    protected override async Task<Result<IReadOnlyList<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetPremiumIndexKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok<IReadOnlyList<PremiumIndexKline>>(ConvertToModelRows<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline, PremiumIndexKline>(result.Value, (kline, row) =>
        {
            row.OpenPrice = decimal.ToDouble(kline.OpenPrice);
            row.ClosePrice = decimal.ToDouble(kline.ClosePrice);
            row.HighPrice = decimal.ToDouble(kline.HighPrice);
            row.LowPrice = decimal.ToDouble(kline.LowPrice);
            row.OpenTime = ToUnixMilliseconds(kline.OpenTime);
            row.CloseTime = ToUnixMilliseconds(kline.CloseTime);
        }));
    }

    protected override async Task<Result<IReadOnlyList<PremiumIndexKline>>> GetIndexPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetIndexPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok<IReadOnlyList<PremiumIndexKline>>(ConvertToModelRows<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline, PremiumIndexKline>(result.Value, (kline, row) =>
        {
            row.OpenPrice = decimal.ToDouble(kline.OpenPrice);
            row.ClosePrice = decimal.ToDouble(kline.ClosePrice);
            row.HighPrice = decimal.ToDouble(kline.HighPrice);
            row.LowPrice = decimal.ToDouble(kline.LowPrice);
            row.OpenTime = ToUnixMilliseconds(kline.OpenTime);
            row.CloseTime = ToUnixMilliseconds(kline.CloseTime);
        }));
    }

    protected override async Task<Result<IReadOnlyList<PremiumIndexKline>>> GetMarkPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetMarkPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok<IReadOnlyList<PremiumIndexKline>>(ConvertToModelRows<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline, PremiumIndexKline>(result.Value, (kline, row) =>
        {
            row.OpenPrice = decimal.ToDouble(kline.OpenPrice);
            row.ClosePrice = decimal.ToDouble(kline.ClosePrice);
            row.HighPrice = decimal.ToDouble(kline.HighPrice);
            row.LowPrice = decimal.ToDouble(kline.LowPrice);
            row.OpenTime = ToUnixMilliseconds(kline.OpenTime);
            row.CloseTime = ToUnixMilliseconds(kline.CloseTime);
        }));
    }

    protected override async Task<Result<List<SymbolInfoCsv>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceFuturesUsdtSymbol>> result = await usdFutures.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        List<SymbolInfoCsv> markets = ConvertToMarketRows<BinanceFuturesUsdtSymbol, SymbolInfoCsv>(result.Value, (symbol, row) =>
        {
            row.Name = symbol.Name;
            row.BaseAsset = symbol.BaseAsset;
            row.BaseAssetPrecision = symbol.BaseAssetPrecision;
            row.QuoteAsset = symbol.QuoteAsset;
            row.ContractType = symbol.ContractType.ToString();
            row.DeliveryDate = ToUnixMilliseconds(symbol.DeliveryDate);
            row.LiquidationFee = decimal.ToDouble(symbol.LiquidationFee);
            row.ListingDate = ToUnixMilliseconds(symbol.ListingDate);
            row.MaintMarginPercent = decimal.ToDouble(symbol.MaintMarginPercent);
            row.MarginAsset = symbol.MarginAsset;
            row.MarketTakeBound = decimal.ToDouble(symbol.MarketTakeBound);
            row.RequiredMarginPercent = decimal.ToDouble(symbol.RequiredMarginPercent);
            row.OrderTypes = string.Join('|', symbol.OrderTypes);
            row.Pair = symbol.Pair;
            row.PricePrecision = symbol.PricePrecision;
            row.QuantityPrecision = symbol.QuantityPrecision;
            row.QuoteAssetPrecision = symbol.QuoteAssetPrecision;
            row.Status = symbol.Status.ToString();
            row.TimeInForce = string.Join('|', symbol.TimeInForce);
            row.TriggerProtect = decimal.ToDouble(symbol.TriggerProtect);
            row.UnderlyingType = symbol.UnderlyingType.ToString();
            row.UnderlyingSubType = string.Join('|', symbol.UnderlyingSubType);
        });

        return Result.Ok(markets);
    }

    public override Task<DateTime> GetLastFundingTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(FundingRatePath, symbol.Name, nameof(FundingRate.FundingTime), nameof(FundingRate), null, null, ct);

    protected override async Task<Result<IReadOnlyList<FundingRate>>> GetFundingRatesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesFundingRateHistory>> result = await usdFutures.GetFundingRatesAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<FundingRate>>(ConvertToModelRows<BinanceFuturesFundingRateHistory, FundingRate>(result.Value, (rate, row) =>
        {
            row.Rate = decimal.ToDouble(rate.FundingRate);
            row.FundingTime = ToUnixMilliseconds(rate.FundingTime);
            row.MarkPrice = rate.MarkPrice.HasValue ? decimal.ToDouble(rate.MarkPrice.Value) : null;
        }));
    }

    public override Task<DateTime> GetLastOpenInterestTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(OpenInterestPath, symbol.Name, nameof(OpenInterestHistory.Timestamp), nameof(OpenInterestHistory), null, null, ct);

    public override Task<DateTime> GetLastBasisTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(BasisPath, symbol.Name, nameof(FuturesBasisCsv.Timestamp), nameof(FuturesBasisCsv), null, null, ct);

    protected override async Task<Result<IReadOnlyList<OpenInterestHistory>>> GetOpenInterestHistoriesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesOpenInterestHistory>> result = await usdFutures.GetOpenInterestHistoryAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        List<OpenInterestHistory> openInterestHistories = ConvertToModelRows<BinanceFuturesOpenInterestHistory, OpenInterestHistory>(
            result.Value,
            item => item.Timestamp.HasValue,
            (item, row) =>
            {
                row.Timestamp = ToUnixMilliseconds(item.Timestamp!.Value);
                row.SumOpenInterest = decimal.ToDouble(item.SumOpenInterest);
                row.SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue);
            });
        return Result.Ok<IReadOnlyList<OpenInterestHistory>>(openInterestHistories);
    }

    public override Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TopLongShortPositionRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), "TopLongShortPositionRatios", null, null, ct);

    protected override async Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortPositionRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<LongShortRatioCsv>>(ConvertToModelRows<BinanceFuturesLongShortRatio, LongShortRatioCsv>(result.Value, item => item.Timestamp.HasValue, (item, row) =>
        {
            row.Timestamp = ToUnixMilliseconds(item.Timestamp!.Value);
            row.LongShortRatio = decimal.ToDouble(item.LongShortRatio);
            row.LongAccount = decimal.ToDouble(item.LongAccount);
            row.ShortAccount = decimal.ToDouble(item.ShortAccount);
        }));
    }

    public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TopLongShortAccountRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), "TopLongShortAccountRatios", null, null, ct);

    protected override async Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<LongShortRatioCsv>>(ConvertToModelRows<BinanceFuturesLongShortRatio, LongShortRatioCsv>(result.Value, item => item.Timestamp.HasValue, (item, row) =>
        {
            row.Timestamp = ToUnixMilliseconds(item.Timestamp!.Value);
            row.LongShortRatio = decimal.ToDouble(item.LongShortRatio);
            row.LongAccount = decimal.ToDouble(item.LongAccount);
            row.ShortAccount = decimal.ToDouble(item.ShortAccount);
        }));
    }

    public override Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(GlobalLongShortAccountRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), "GlobalLongShortAccountRatios", null, null, ct);

    public override Task<DateTime> GetLastTakerLongShortRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TakerLongShortRatioPath, symbol.Name, nameof(TakerLongShortRatioCsv.Timestamp), nameof(TakerLongShortRatioCsv), null, null, ct);

    protected override async Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetGlobalLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<LongShortRatioCsv>>(ConvertToModelRows<BinanceFuturesLongShortRatio, LongShortRatioCsv>(result.Value, item => item.Timestamp.HasValue, (item, row) =>
        {
            row.Timestamp = ToUnixMilliseconds(item.Timestamp!.Value);
            row.LongShortRatio = decimal.ToDouble(item.LongShortRatio);
            row.LongAccount = decimal.ToDouble(item.LongAccount);
            row.ShortAccount = decimal.ToDouble(item.ShortAccount);
        }));
    }

    protected override async Task<Result<IReadOnlyList<FuturesBasisCsv>>> GetBasisAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBasis>> result = await usdFutures.GetBasisAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<FuturesBasisCsv>>(ConvertToModelRows<BinanceFuturesBasis, FuturesBasisCsv>(result.Value, (item, row) =>
        {
            row.Timestamp = ToUnixMilliseconds(item.Timestamp);
            row.FuturesPrice = decimal.ToDouble(item.FuturesPrice);
            row.IndexPrice = decimal.ToDouble(item.IndexPrice);
            row.BasisValue = decimal.ToDouble(item.Basis);
            row.BasisRate = decimal.ToDouble(item.BasisRate);
            row.AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null;
        }));
    }

    protected override async Task<Result<IReadOnlyList<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBuySellVolumeRatio>> result = await usdFutures.GetTakerLongShortRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok<IReadOnlyList<TakerLongShortRatioCsv>>(ConvertToModelRows<BinanceFuturesBuySellVolumeRatio, TakerLongShortRatioCsv>(result.Value, item => item.Timestamp.HasValue, (item, row) =>
        {
            row.Timestamp = ToUnixMilliseconds(item.Timestamp!.Value);
            row.BuySellRatio = decimal.ToDouble(item.BuySellRatio);
            row.BuyVolume = decimal.ToDouble(item.BuyVolume);
            row.SellVolume = decimal.ToDouble(item.SellVolume);
            row.BuyVolumeValue = null;
            row.SellVolumeValue = null;
        }));
    }

}
