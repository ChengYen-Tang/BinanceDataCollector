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
    protected override string KlinePath { get { return Path.Combine(RootKlinePath, Market + ".duckdb"); } }
    protected override string PremiumIndexKlinePath { get { return Path.Combine(RootPremiumIndexKlinePath, Market + ".duckdb"); } }
    protected override string IndexPriceKlinePath { get { return Path.Combine(RootIndexPriceKlinePath, Market + ".duckdb"); } }
    protected override string MarkPriceKlinePath { get { return Path.Combine(RootMarkPriceKlinePath, Market + ".duckdb"); } }
    protected override string FundingRatePath { get { return Path.Combine(RootFundingRatePath, Market + ".duckdb"); } }
    protected override string OpenInterestPath { get { return Path.Combine(RootOpenInterestPath, Market + ".duckdb"); } }
    protected override string TopLongShortPositionRatioPath { get { return Path.Combine(RootTopLongShortPositionRatioPath, Market + ".duckdb"); } }
    protected override string TopLongShortAccountRatioPath { get { return Path.Combine(RootTopLongShortAccountRatioPath, Market + ".duckdb"); } }
    protected override string GlobalLongShortAccountRatioPath { get { return Path.Combine(RootGlobalLongShortAccountRatioPath, Market + ".duckdb"); } }
    protected override string TakerLongShortRatioPath { get { return Path.Combine(RootTakerLongShortRatioPath, Market + ".duckdb"); } }
    protected override string BasisPath { get { return Path.Combine(RootBasisPath, Market + ".duckdb"); } }
    protected override bool IsFutures => true;
    protected override AggTradesTimeUnit AggTradesTimeUnit => AggTradesTimeUnit.Milliseconds;
    protected override string GetSymbolName(SymbolInfoCsv symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(CancellationToken ct = default)
        => GetStoredSymbolNamesAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await DeleteSymbolTablesAsync(
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
        ], delistedSymbols, ct);
        await DeleteAggTradesStorageAsync(delistedSymbols, ct);
        await DeleteBookDepthStorageAsync(delistedSymbols, ct);
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
        => GetLastTimestampAsync(KlinePath, symbol.Name, nameof(Kline.CloseTime), null, ct);

    public override Task<DateTime> GetLastPremiumIndexTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(PremiumIndexKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), null, ct);

    public override Task<DateTime> GetLastIndexPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(IndexPriceKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), null, ct);

    public override Task<DateTime> GetLastMarkPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(MarkPriceKlinePath, symbol.Name, nameof(PremiumIndexKline.CloseTime), null, ct);

    protected override async Task<Result<List<Kline>>> GetKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await usdFutures.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(ConvertToModelRows(result.Value, kline => new Kline
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            Volume = decimal.ToDouble(kline.Volume),
            QuoteVolume = decimal.ToDouble(kline.QuoteVolume),
            TakerBuyBaseVolume = decimal.ToDouble(kline.TakerBuyBaseVolume),
            TakerBuyQuoteVolume = decimal.ToDouble(kline.TakerBuyQuoteVolume),
            TradeCount = kline.TradeCount,
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }));
    }

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(SymbolInfoCsv symbol, DateTime downloadStartTime, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadAggTradesAsync(symbol.Name, downloadStartTime, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override Task<Result<MarketDataDownloadBatch>> GetBookDepthAsync(SymbolInfoCsv symbol, DateTime downloadStartTime, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadBookDepthAsync(symbol.Name, downloadStartTime, GetMarketDataTempSymbolPath(MarketDataBase.BookDepthDataType, symbol.Name), ct);

    protected override async Task<Result<List<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetPremiumIndexKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(ConvertToModelRows(result.Value, kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }));
    }

    protected override async Task<Result<List<PremiumIndexKline>>> GetIndexPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetIndexPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(ConvertToModelRows(result.Value, kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }));
    }

    protected override async Task<Result<List<PremiumIndexKline>>> GetMarkPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetMarkPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(ConvertToModelRows(result.Value, kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }));
    }

    protected override async Task<Result<List<SymbolInfoCsv>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceFuturesUsdtSymbol>> result = await usdFutures.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        List<SymbolInfoCsv> markets = result.Value.AsParallel().Select(symbol => new SymbolInfoCsv
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            QuoteAsset = symbol.QuoteAsset,
            ContractType = symbol.ContractType.ToString(),
            DeliveryDate = ToUnixMilliseconds(symbol.DeliveryDate),
            LiquidationFee = decimal.ToDouble(symbol.LiquidationFee),
            ListingDate = ToUnixMilliseconds(symbol.ListingDate),
            MaintMarginPercent = decimal.ToDouble(symbol.MaintMarginPercent),
            MarginAsset = symbol.MarginAsset,
            MarketTakeBound = decimal.ToDouble(symbol.MarketTakeBound),
            RequiredMarginPercent = decimal.ToDouble(symbol.RequiredMarginPercent),
            OrderTypes = string.Join('|', symbol.OrderTypes),
            Pair = symbol.Pair,
            PricePrecision = symbol.PricePrecision,
            QuantityPrecision = symbol.QuantityPrecision,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            Status = symbol.Status.ToString(),
            TimeInForce = string.Join('|', symbol.TimeInForce),
            TriggerProtect = decimal.ToDouble(symbol.TriggerProtect),
            UnderlyingType = symbol.UnderlyingType.ToString(),
            UnderlyingSubType = string.Join('|', symbol.UnderlyingSubType)
        }).ToList();

        return Result.Ok(markets);
    }

    public override Task<DateTime> GetLastFundingTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(FundingRatePath, symbol.Name, nameof(FundingRate.FundingTime), null, ct);

    protected override async Task<Result<List<FundingRate>>> GetFundingRatesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesFundingRateHistory>> result = await usdFutures.GetFundingRatesAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, rate => new FundingRate
        {
            Rate = decimal.ToDouble(rate.FundingRate),
            FundingTime = ToUnixMilliseconds(rate.FundingTime),
            MarkPrice = rate.MarkPrice.HasValue ? decimal.ToDouble(rate.MarkPrice.Value) : null,
        }));
    }

    public override Task<DateTime> GetLastOpenInterestTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(OpenInterestPath, symbol.Name, nameof(OpenInterestHistory.Timestamp), null, ct);

    public override Task<DateTime> GetLastBasisTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(BasisPath, symbol.Name, nameof(FuturesBasisCsv.Timestamp), null, ct);

    protected override async Task<Result<List<OpenInterestHistory>>> GetOpenInterestHistoriesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesOpenInterestHistory>> result = await usdFutures.GetOpenInterestHistoryAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        List<OpenInterestHistory> openInterestHistories = ConvertToModelRows(
            result.Value,
            item => item.Timestamp.HasValue,
            item => new OpenInterestHistory
            {
                Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
                SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
                SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue)
            });
        return Result.Ok(openInterestHistories);
    }

    public override Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TopLongShortPositionRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), null, ct);

    protected override async Task<Result<List<LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortPositionRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, item => item.Timestamp.HasValue, item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }));
    }

    public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TopLongShortAccountRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), null, ct);

    protected override async Task<Result<List<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, item => item.Timestamp.HasValue, item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }));
    }

    public override Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(GlobalLongShortAccountRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), null, ct);

    public override Task<DateTime> GetLastTakerLongShortRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TakerLongShortRatioPath, symbol.Name, nameof(TakerLongShortRatioCsv.Timestamp), null, ct);

    protected override async Task<Result<List<LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetGlobalLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, item => item.Timestamp.HasValue, item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }));
    }

    protected override async Task<Result<List<FuturesBasisCsv>>> GetBasisAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBasis>> result = await usdFutures.GetBasisAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, item => new FuturesBasisCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp),
            FuturesPrice = decimal.ToDouble(item.FuturesPrice),
            IndexPrice = decimal.ToDouble(item.IndexPrice),
            BasisValue = decimal.ToDouble(item.Basis),
            BasisRate = decimal.ToDouble(item.BasisRate),
            AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null
        }));
    }

    protected override async Task<Result<List<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBuySellVolumeRatio>> result = await usdFutures.GetTakerLongShortRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(ConvertToModelRows(result.Value, item => item.Timestamp.HasValue, item => new TakerLongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            BuySellRatio = decimal.ToDouble(item.BuySellRatio),
            BuyVolume = decimal.ToDouble(item.BuyVolume),
            SellVolume = decimal.ToDouble(item.SellVolume),
            BuyVolumeValue = null,
            SellVolumeValue = null
        }));
    }

}
