using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels.Models.Storage;
using MarketDataBase = BinanceDataCollector.Collectors.BinanceMarketData.BaseMarketData;
using MarketDataDownloadBatch = BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch;
using SpotMarketData = BinanceDataCollector.Collectors.BinanceMarketData.Spot;
namespace BinanceDataCollector.StorageControllers;

internal class SpotStorageController : StorageController<SymbolInfoCsv>
{
    private const string Market = "Spot";
    private static readonly DateTime SpotAggTradesMicrosecondsStart = new(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);
    private readonly Spot spot;
    private readonly SpotMarketData spotMarketData;

    public SpotStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<SpotStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (spot, spotMarketData) = (new(client, configuration.GetSection("IgnoneCoins:Spot").Get<string[]>() ?? []), new());

    protected override string MarketPathSegment => Market;
    protected override string SymbolInfoPath { get { return Path.Combine(RootSymbolInfoPath, "SymbolInfo.duckdb"); } }
    protected override string KlinePath { get { return Path.Combine(RootKlinePath, Market + ".duckdb"); } }
    protected override string PremiumIndexKlinePath => throw new NotImplementedException();
    protected override string IndexPriceKlinePath => throw new NotSupportedException("Spot market does not support index price klines.");
    protected override string MarkPriceKlinePath => throw new NotSupportedException("Spot market does not support mark price klines.");
    protected override string FundingRatePath => throw new NotSupportedException("Spot market does not support funding rates.");
    protected override string OpenInterestPath => throw new NotSupportedException("Spot market does not support open interest histories.");
    protected override string TopLongShortPositionRatioPath => throw new NotSupportedException("Spot market does not support long/short ratios.");
    protected override string TopLongShortAccountRatioPath => throw new NotSupportedException("Spot market does not support long/short ratios.");
    protected override string GlobalLongShortAccountRatioPath => throw new NotSupportedException("Spot market does not support long/short ratios.");
    protected override string TakerLongShortRatioPath => throw new NotSupportedException("Spot market does not support taker long/short ratios.");
    protected override string BasisPath => throw new NotSupportedException("Spot market does not support basis.");
    protected override bool IsFutures => false;
    protected override AggTradesTimeUnit AggTradesTimeUnit => AggTradesTimeUnit.Microseconds;
    protected override string GetSymbolName(SymbolInfoCsv symbol)
        => symbol.Name;

    protected override AggTradesTimeUnit GetAggTradesTimeUnitForTimestamp(DateTime timestamp)
        => timestamp.ToUniversalTime() >= SpotAggTradesMicrosecondsStart
            ? AggTradesTimeUnit.Microseconds
            : AggTradesTimeUnit.Milliseconds;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(CancellationToken ct = default)
        => GetStoredSymbolNamesAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await DeleteSymbolTablesAsync([KlinePath], delistedSymbols, ct);
        await DeleteAggTradesStorageAsync(delistedSymbols, ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        List<string> symbolNames = await GetStoredSymbolNamesAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            await DeleteSymbolRowsBeforeAsync(KlinePath, symbolName, nameof(Kline.OpenTime), ct);
            await DeleteOldAggTradesDataAsync(symbolName, ct);
        }
    }

    public override Task<DateTime> GetLastTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => GetLastTimestampAsync(KlinePath, symbol.Name, nameof(Kline.CloseTime), null, ct);

    public override Task<DateTime> GetLastPremiumIndexTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support premium index klines.");

    public override Task<DateTime> GetLastIndexPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support index price klines.");

    public override Task<DateTime> GetLastMarkPriceTimeAsync(SymbolInfoCsv symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support mark price klines.");

    public override Task<DateTime> GetLastFundingTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support funding rates.");

    public override Task<DateTime> GetLastOpenInterestTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support open interest histories.");

    public override Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastTakerLongShortRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support taker long/short ratios.");

    public override Task<DateTime> GetLastBasisTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support basis.");

    protected override async Task<Result<IReadOnlyList<Kline>>> GetKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await spot.GetKlinesAsync(symbol.Name, interval, startTime, ct);
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
        => spotMarketData.DownloadAggTradesAsync(symbol.Name, downloadStartTime, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override Task<Result<MarketDataDownloadBatch>> GetBookDepthAsync(SymbolInfoCsv symbol, DateTime downloadStartTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support book depth market data.");

    protected override Task<Result<IReadOnlyList<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotImplementedException();

    protected override Task<Result<IReadOnlyList<PremiumIndexKline>>> GetIndexPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support index price klines.");

    protected override Task<Result<IReadOnlyList<PremiumIndexKline>>> GetMarkPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support mark price klines.");

    protected override Task<Result<IReadOnlyList<FundingRate>>> GetFundingRatesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support funding rates.");

    protected override Task<Result<IReadOnlyList<OpenInterestHistory>>> GetOpenInterestHistoriesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support open interest histories.");

    protected override Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<IReadOnlyList<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support taker long/short ratios.");

    protected override Task<Result<IReadOnlyList<FuturesBasisCsv>>> GetBasisAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support basis.");

    protected override async Task<Result<List<SymbolInfoCsv>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceSymbol>> result = await spot.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        List<SymbolInfoCsv> markets = ConvertToMarketRows<BinanceSymbol, SymbolInfoCsv>(result.Value, (symbol, row) =>
        {
            row.Name = symbol.Name;
            row.BaseAsset = symbol.BaseAsset;
            row.BaseAssetPrecision = symbol.BaseAssetPrecision;
            row.BaseFeePrecision = symbol.BaseFeePrecision;
            row.AllowTrailingStop = symbol.AllowTrailingStop;
            row.CancelReplaceAllowed = symbol.CancelReplaceAllowed;
            row.IcebergAllowed = symbol.IcebergAllowed;
            row.IsMarginTradingAllowed = symbol.IsMarginTradingAllowed;
            row.IsSpotTradingAllowed = symbol.IsSpotTradingAllowed;
            row.OCOAllowed = symbol.OCOAllowed;
            row.OrderTypes = string.Join('|', symbol.OrderTypes);
            row.QuoteAsset = symbol.QuoteAsset;
            row.QuoteAssetPrecision = symbol.QuoteAssetPrecision;
            row.QuoteFeePrecision = symbol.QuoteFeePrecision;
            row.Permissions = string.Join('|', symbol.Permissions);
            row.QuoteOrderQuantityMarketAllowed = symbol.QuoteOrderQuantityMarketAllowed;
            row.Status = symbol.Status.ToString();
        });

        return Result.Ok(markets);
    }

}
