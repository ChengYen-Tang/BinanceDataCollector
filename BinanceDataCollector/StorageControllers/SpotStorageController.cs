using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;
using BinanceDataCollector.Collectors.BinanceApi;
using MarketDataBase = BinanceDataCollector.Collectors.BinanceMarketData.BaseMarketData;
using MarketDataDownloadBatch = BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch;
using SpotMarketData = BinanceDataCollector.Collectors.BinanceMarketData.Spot;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using CryptoExchange.Net.Converters.SystemTextJson;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class SpotStorageController : StorageController<BinanceSymbolInfo, SpotBinanceKline, CollectorModels.Models.BinanceMarkIndexKline, CollectorModels.Models.BinanceMarkIndexKline, CollectorModels.Models.BinanceMarkIndexKline, FuturesFundingRate, FuturesOpenInterestHistory, FuturesLongShortRatio, FuturesLongShortRatio, FuturesLongShortRatio, FuturesTakerLongShortRatio, FuturesBasis>
{
    private const string Market = "Spot";
    private readonly Spot spot;
    private readonly SpotMarketData spotMarketData;

    public SpotStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<SpotStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (spot, spotMarketData) = (new(client, configuration.GetSection("IgnoneCoins:Spot").Get<string[]>() ?? []), new());

    protected override string MarketPathSegment => Market;
    protected override string SymbolInfoPath { get { return Path.Combine(RootSymbolInfoPath, Market); } }
    protected override string KlinePath { get { return Path.Combine(RootKlinePath, Market); } }
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
    private static IReadOnlyCollection<string> MarketDataTypes => [MarketDataBase.AggTradesDataType];

    protected override string GetSymbolName(BinanceSymbolInfo symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(BinanceDbContext db, CancellationToken ct = default)
        => db.BinanceSymbolInfos.AsNoTracking().Select(item => item.Name).ToListAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(BinanceDbContext db, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await db.DropShardingTablesAsync(delistedSymbols, [nameof(BinanceDbContext.SpotBinanceKlines)], LogDropStatus, ct);

        await db.BinanceSymbolInfos.Where(item => delistedSymbols.Contains(item.Name)).ExecuteDeleteAsync(ct);
        await DeleteMarketDataSymbolDirectoriesAsync(delistedSymbols, MarketDataTypes, ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        List<string> symbolNames = await db.BinanceSymbolInfos
            .AsNoTracking()
            .Select(s => s.Name)
            .ToListAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            await db.SpotBinanceKlines
                .Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName)
                .ExecuteDeleteAsync(ct);
            await DeleteOldAggTradesDataAsync(symbolName, ct);
        }
    }

    public override async Task<DateTime> GetLastTimeAsync(BinanceSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.SpotBinanceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.SpotBinanceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override Task<DateTime> GetLastPremiumIndexTimeAsync(BinanceSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support premium index klines.");

    public override Task<DateTime> GetLastIndexPriceTimeAsync(BinanceSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support index price klines.");

    public override Task<DateTime> GetLastMarkPriceTimeAsync(BinanceSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support mark price klines.");

    public override Task<DateTime> GetLastFundingTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support funding rates.");

    public override Task<DateTime> GetLastOpenInterestTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support open interest histories.");

    public override Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    public override Task<DateTime> GetLastTakerLongShortRatioTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support taker long/short ratios.");

    public override Task<DateTime> GetLastBasisTimeAsync(BinanceSymbolInfo symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support basis.");

    protected override async Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        string[] symbols = await db.BinanceSymbolInfos.AsNoTracking().Select(item => item.Name).ToArrayAsync(ct);
        if (symbols.Length == 0)
            return Result.Fail("No symbols found.");
        return Result.Ok(symbols);
    }

    protected override async Task<Result<SymbolInfoCsv[]>> GetCsvSymbolInfosAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        SymbolInfoCsv[] symbols = await db.BinanceSymbolInfos.AsNoTracking()
            .OrderBy(item => item.Name)
            .Select(item => new SymbolInfoCsv
            {
                Name = item.Name,
                Status = item.Status.ToString(),
                BaseAsset = item.BaseAsset,
                QuoteAsset = item.QuoteAsset,
                BaseAssetPrecision = item.BaseAssetPrecision,
                QuoteAssetPrecision = item.QuoteAssetPrecision,
                BaseFeePrecision = item.BaseFeePrecision,
                QuoteFeePrecision = item.QuoteFeePrecision,
                OrderTypes = string.Join('|', item.OrderTypes),
                Permissions = string.Join('|', item.Permissions),
                IcebergAllowed = item.IcebergAllowed,
                CancelReplaceAllowed = item.CancelReplaceAllowed,
                IsSpotTradingAllowed = item.IsSpotTradingAllowed,
                AllowTrailingStop = item.AllowTrailingStop,
                IsMarginTradingAllowed = item.IsMarginTradingAllowed,
                OCOAllowed = item.OCOAllowed,
                QuoteOrderQuantityMarketAllowed = item.QuoteOrderQuantityMarketAllowed
            })
            .ToArrayAsync(ct);
        if (symbols.Length == 0)
            return Result.Fail("No symbol infos found.");
        return Result.Ok(symbols);
    }

    protected override async Task<Result<Kline[]>> GetCsvKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        Kline[] klines = await db.SpotBinanceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new Kline
        {
            OpenTime = DateTimeConverter.ConvertToMilliseconds(item.OpenTime).Value,
            OpenPrice = item.OpenPrice,
            HighPrice = item.HighPrice,
            LowPrice = item.LowPrice,
            ClosePrice = item.ClosePrice,
            Volume = item.Volume,
            QuoteVolume = item.QuoteVolume,
            TakerBuyBaseVolume = item.TakerBuyBaseVolume,
            TakerBuyQuoteVolume = item.TakerBuyQuoteVolume,
            TradeCount = item.TradeCount,
            CloseTime = DateTimeConverter.ConvertToMilliseconds(item.CloseTime).Value
        }).ToArrayAsync(ct);
        if (klines.Length == 0)
            return Result.Fail("No klines found.");
        return Result.Ok(klines);
    }

    protected override async Task<Result<List<SpotBinanceKline>>> GetKlinesAsync(BinanceSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await spot.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new SpotBinanceKline()
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
            Interval = interval,
            OpenTime = kline.OpenTime,
            CloseTime = kline.CloseTime,
            SymbolInfo = symbol,
            SymbolInfoId = symbol.Name,
            Id = CombineKlineId(symbol.Name, interval, kline.CloseTime)
        }).ToList());
    }

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(BinanceSymbolInfo symbol, (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState, CancellationToken ct = default)
        => spotMarketData.DownloadAggTradesAsync(symbol.Name, syncState, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override Task<Result<List<CollectorModels.Models.BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(BinanceSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotImplementedException();

    protected override Task<Result<List<CollectorModels.Models.BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(BinanceSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support index price klines.");

    protected override Task<Result<List<CollectorModels.Models.BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(BinanceSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support mark price klines.");

    protected override Task<Result<List<FuturesFundingRate>>> GetFundingRatesAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support funding rates.");

    protected override Task<Result<List<FuturesOpenInterestHistory>>> GetOpenInterestHistoriesAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support open interest histories.");

    protected override Task<Result<List<FuturesLongShortRatio>>> GetTopLongShortPositionRatiosAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<List<FuturesLongShortRatio>>> GetTopLongShortAccountRatiosAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<List<FuturesLongShortRatio>>> GetGlobalLongShortAccountRatiosAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<List<FuturesTakerLongShortRatio>>> GetTakerLongShortRatiosAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support taker long/short ratios.");

    protected override Task<Result<List<FuturesBasis>>> GetBasisAsync(BinanceSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support basis.");

    protected override async Task<Result<List<BinanceSymbolInfo>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceSymbol>> result = await spot.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        List<BinanceSymbolInfo> markets = result.Value.AsParallel().Select(symbol => new BinanceSymbolInfo
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            BaseFeePrecision = symbol.BaseFeePrecision,
            AllowTrailingStop = symbol.AllowTrailingStop,
            CancelReplaceAllowed = symbol.CancelReplaceAllowed,
            IcebergAllowed = symbol.IcebergAllowed,
            IsMarginTradingAllowed = symbol.IsMarginTradingAllowed,
            IsSpotTradingAllowed = symbol.IsSpotTradingAllowed,
            OCOAllowed = symbol.OCOAllowed,
            OrderTypes = symbol.OrderTypes,
            QuoteAsset = symbol.QuoteAsset,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            QuoteFeePrecision = symbol.QuoteFeePrecision,
            Permissions = symbol.Permissions,
            QuoteOrderQuantityMarketAllowed = symbol.QuoteOrderQuantityMarketAllowed,
            Status = symbol.Status
        }).ToList();

        return Result.Ok(markets);
    }

    protected override Task<Result<PremiumIndexKline[]>> GetCsvPremiumIndexKlinesAsync(string symbol, CancellationToken ct = default)
        => throw new NotImplementedException();

    protected override Task<Result<PremiumIndexKline[]>> GetCsvIndexPriceKlinesAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support index price klines.");

    protected override Task<Result<PremiumIndexKline[]>> GetCsvMarkPriceKlinesAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support mark price klines.");

    protected override Task<Result<FundingRate[]>> GetCsvFundingRatesAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support funding rates.");

    protected override Task<Result<OpenInterestHistory[]>> GetCsvOpenInterestHistoriesAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support open interest histories.");

    protected override Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortPositionRatiosAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<LongShortRatioCsv[]>> GetCsvGlobalLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support long/short ratios.");

    protected override Task<Result<TakerLongShortRatioCsv[]>> GetCsvTakerLongShortRatiosAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support taker long/short ratios.");

    protected override Task<Result<FuturesBasisCsv[]>> GetCsvBasisAsync(string symbol, CancellationToken ct = default)
        => throw new NotSupportedException("Spot market does not support basis.");
}
