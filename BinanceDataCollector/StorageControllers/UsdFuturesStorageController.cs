using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using MarketDataBase = BinanceDataCollector.Collectors.BinanceMarketData.BaseMarketData;
using MarketDataDownloadBatch = BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch;
using MarketDataUsdFutures = BinanceDataCollector.Collectors.BinanceMarketData.UsdFutures;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using CryptoExchange.Net.Converters.SystemTextJson;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class UsdFuturesStorageController : StorageController<BinanceFuturesUsdtSymbolInfo, FuturesUsdtBinanceKline, FuturesUsdtBinancePremiumIndexKline, FuturesUsdtBinanceIndexPriceKline, FuturesUsdtBinanceMarkPriceKline, FuturesUsdtFundingRate, FuturesUsdtOpenInterestHistory, FuturesUsdtTopLongShortPositionRatio, FuturesUsdtTopLongShortAccountRatio, FuturesUsdtGlobalLongShortAccountRatio, FuturesUsdtTakerLongShortRatio, FuturesUsdtBasis>
{
    private const string Market = "UsdFutures";
    private readonly UsdFutures usdFutures;
    private readonly MarketDataUsdFutures usdFuturesMarketData;

    public UsdFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<UsdFuturesStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (usdFutures, usdFuturesMarketData) = (new(client, configuration.GetSection("IgnoneCoins:UsdFutures").Get<string[]>() ?? []), new());

    protected override string MarketPathSegment => Market;
    protected override string SymbolInfoPath { get { return Path.Combine(RootSymbolInfoPath, Market); } }
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
    private static IReadOnlyCollection<string> MarketDataTypes => [MarketDataBase.AggTradesDataType];

    protected override string GetSymbolName(BinanceFuturesUsdtSymbolInfo symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(BinanceDbContext db, CancellationToken ct = default)
        => db.BinanceFuturesUsdtSymbolInfos.AsNoTracking().Select(item => item.Name).ToListAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(BinanceDbContext db, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await db.DropShardingTablesAsync(delistedSymbols,
        [
            nameof(BinanceDbContext.FuturesUsdtBinanceKlines),
            nameof(BinanceDbContext.FuturesUsdtBinancePremiumIndexKlines),
            nameof(BinanceDbContext.FuturesUsdtBinanceIndexPriceKlines),
            nameof(BinanceDbContext.FuturesUsdtBinanceMarkPriceKlines),
            nameof(BinanceDbContext.FuturesUsdtFundingRates),
            nameof(BinanceDbContext.FuturesUsdtOpenInterestHistories),
            nameof(BinanceDbContext.FuturesUsdtBasisHistories),
            nameof(BinanceDbContext.FuturesUsdtTopLongShortPositionRatios),
            nameof(BinanceDbContext.FuturesUsdtTopLongShortAccountRatios),
            nameof(BinanceDbContext.FuturesUsdtGlobalLongShortAccountRatios),
            nameof(BinanceDbContext.FuturesUsdtTakerLongShortRatios),
        ], LogDropStatus, ct);
        
        await db.BinanceFuturesUsdtSymbolInfos.Where(item => delistedSymbols.Contains(item.Name)).ExecuteDeleteAsync(ct);
        await DeleteMarketDataSymbolDirectoriesAsync(delistedSymbols, MarketDataTypes, ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        List<string> symbolNames = await db.BinanceFuturesUsdtSymbolInfos
            .AsNoTracking()
            .Select(s => s.Name)
            .ToListAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            await db.FuturesUsdtBinanceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtBinancePremiumIndexKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtBinanceIndexPriceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtBinanceMarkPriceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtFundingRates.Where(item => item.FundingTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtOpenInterestHistories.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtBasisHistories.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtTopLongShortPositionRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtTopLongShortAccountRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtGlobalLongShortAccountRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesUsdtTakerLongShortRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await DeleteOldAggTradesDataAsync(symbolName, ct);
        }
    }

    public override async Task<DateTime> GetLastTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtBinanceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtBinanceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastPremiumIndexTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtBinancePremiumIndexKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtBinancePremiumIndexKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastIndexPriceTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtBinanceIndexPriceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtBinanceIndexPriceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastMarkPriceTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtBinanceMarkPriceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtBinanceMarkPriceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    protected override async Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        string[] symbols = await db.BinanceFuturesUsdtSymbolInfos.AsNoTracking().Select(item => item.Name).ToArrayAsync(ct);
        if (symbols.Length == 0)
            return Result.Fail("No symbols found.");
        return Result.Ok(symbols);
    }

    protected override async Task<Result<SymbolInfoCsv[]>> GetCsvSymbolInfosAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        SymbolInfoCsv[] symbols = await db.BinanceFuturesUsdtSymbolInfos.AsNoTracking()
            .OrderBy(item => item.Name)
            .Select(item => new SymbolInfoCsv
            {
                Name = item.Name,
                Status = item.Status.ToString(),
                BaseAsset = item.BaseAsset,
                QuoteAsset = item.QuoteAsset,
                MarginAsset = item.MarginAsset,
                Pair = item.Pair,
                BaseAssetPrecision = item.BaseAssetPrecision,
                QuoteAssetPrecision = item.QuoteAssetPrecision,
                PricePrecision = item.PricePrecision,
                QuantityPrecision = item.QuantityPrecision,
                ContractType = item.ContractType.ToString(),
                UnderlyingType = item.UnderlyingType.ToString(),
                UnderlyingSubType = string.Join('|', item.UnderlyingSubType),
                OrderTypes = string.Join('|', item.OrderTypes),
                TimeInForce = string.Join('|', item.TimeInForce),
                MaintMarginPercent = item.MaintMarginPercent,
                RequiredMarginPercent = item.RequiredMarginPercent,
                TriggerProtect = item.TriggerProtect,
                LiquidationFee = item.LiquidationFee,
                MarketTakeBound = item.MarketTakeBound,
                ListingDate = DateTimeConverter.ConvertToMilliseconds(item.ListingDate).Value,
                DeliveryDate = DateTimeConverter.ConvertToMilliseconds(item.DeliveryDate).Value
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
        Kline[] klines = await db.FuturesUsdtBinanceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new Kline
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

    protected override async Task<Result<PremiumIndexKline[]>> GetCsvIndexPriceKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        PremiumIndexKline[] klines = await db.FuturesUsdtBinanceIndexPriceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
        {
            OpenTime = DateTimeConverter.ConvertToMilliseconds(item.OpenTime).Value,
            OpenPrice = item.OpenPrice,
            HighPrice = item.HighPrice,
            LowPrice = item.LowPrice,
            ClosePrice = item.ClosePrice,
            CloseTime = DateTimeConverter.ConvertToMilliseconds(item.CloseTime).Value
        }).ToArrayAsync(ct);
        if (klines.Length == 0)
            return Result.Fail("No klines found.");
        return Result.Ok(klines);
    }

    protected override async Task<Result<PremiumIndexKline[]>> GetCsvMarkPriceKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        PremiumIndexKline[] klines = await db.FuturesUsdtBinanceMarkPriceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
        {
            OpenTime = DateTimeConverter.ConvertToMilliseconds(item.OpenTime).Value,
            OpenPrice = item.OpenPrice,
            HighPrice = item.HighPrice,
            LowPrice = item.LowPrice,
            ClosePrice = item.ClosePrice,
            CloseTime = DateTimeConverter.ConvertToMilliseconds(item.CloseTime).Value
        }).ToArrayAsync(ct);
        if (klines.Length == 0)
            return Result.Fail("No klines found.");
        return Result.Ok(klines);
    }

    protected override async Task<Result<List<FuturesUsdtBinanceKline>>> GetKlinesAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await usdFutures.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesUsdtBinanceKline()
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

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(BinanceFuturesUsdtSymbolInfo symbol, (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadAggTradesAsync(symbol.Name, syncState, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override async Task<Result<List<FuturesUsdtBinancePremiumIndexKline>>> GetPremiumIndexKlinesAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetPremiumIndexKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesUsdtBinancePremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            Interval = interval,
            OpenTime = kline.OpenTime,
            CloseTime = kline.CloseTime,
            SymbolInfo = symbol,
            SymbolInfoId = symbol.Name,
            Id = CombineKlineId(symbol.Name, interval, kline.CloseTime)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesUsdtBinanceIndexPriceKline>>> GetIndexPriceKlinesAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetIndexPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesUsdtBinanceIndexPriceKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            Interval = interval,
            OpenTime = kline.OpenTime,
            CloseTime = kline.CloseTime,
            SymbolInfo = symbol,
            SymbolInfoId = symbol.Name,
            Id = CombineKlineId(symbol.Name, interval, kline.CloseTime)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesUsdtBinanceMarkPriceKline>>> GetMarkPriceKlinesAsync(BinanceFuturesUsdtSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetMarkPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesUsdtBinanceMarkPriceKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            Interval = interval,
            OpenTime = kline.OpenTime,
            CloseTime = kline.CloseTime,
            SymbolInfo = symbol,
            SymbolInfoId = symbol.Name,
            Id = CombineKlineId(symbol.Name, interval, kline.CloseTime)
        }).ToList());
    }

    protected override async Task<Result<List<BinanceFuturesUsdtSymbolInfo>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceFuturesUsdtSymbol>> result = await usdFutures.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        List<BinanceFuturesUsdtSymbolInfo> markets = result.Value.AsParallel().Select(symbol => new BinanceFuturesUsdtSymbolInfo
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            QuoteAsset = symbol.QuoteAsset,
            ContractType = symbol.ContractType,
            DeliveryDate = symbol.DeliveryDate,
            LiquidationFee = decimal.ToDouble(symbol.LiquidationFee),
            ListingDate = symbol.ListingDate,
            MaintMarginPercent = decimal.ToDouble(symbol.MaintMarginPercent),
            MarginAsset = symbol.MarginAsset,
            MarketTakeBound = decimal.ToDouble(symbol.MarketTakeBound),
            RequiredMarginPercent = decimal.ToDouble(symbol.RequiredMarginPercent),
            OrderTypes = symbol.OrderTypes,
            Pair = symbol.Pair,
            PricePrecision = symbol.PricePrecision,
            QuantityPrecision = symbol.QuantityPrecision,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            Status = symbol.Status,
            TimeInForce = symbol.TimeInForce,
            TriggerProtect = decimal.ToDouble(symbol.TriggerProtect),
            UnderlyingType = symbol.UnderlyingType,
            UnderlyingSubType = symbol.UnderlyingSubType,
            SettlePlan = decimal.ToDouble(symbol.SettlePlan),
        }).ToList();

        return Result.Ok(markets);
    }

    protected override async Task<Result<PremiumIndexKline[]>> GetCsvPremiumIndexKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        PremiumIndexKline[] klines = await db.FuturesUsdtBinancePremiumIndexKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
        {
            OpenTime = DateTimeConverter.ConvertToMilliseconds(item.OpenTime).Value,
            OpenPrice = item.OpenPrice,
            HighPrice = item.HighPrice,
            LowPrice = item.LowPrice,
            ClosePrice = item.ClosePrice,
            CloseTime = DateTimeConverter.ConvertToMilliseconds(item.CloseTime).Value
        }).ToArrayAsync(ct);
        if (klines.Length == 0)
            return Result.Fail("No klines found.");
        return Result.Ok(klines);
    }

    public override async Task<DateTime> GetLastFundingTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        string symbolName = symbol.Name;
        return (await db.FuturesUsdtFundingRates.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbolName, ct))
            ? await db.FuturesUsdtFundingRates.AsNoTracking().Where(item => item.SymbolInfoId == symbolName).MaxAsync(item => item.FundingTime, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesUsdtFundingRate>>> GetFundingRatesAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesFundingRateHistory>> result = await usdFutures.GetFundingRatesAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(rate => new FuturesUsdtFundingRate
        {
            FundingRate = decimal.ToDouble(rate.FundingRate),
            FundingTime = rate.FundingTime,
            MarkPrice = rate.MarkPrice.HasValue ? decimal.ToDouble(rate.MarkPrice.Value) : null,
            SymbolInfoId = symbolName,
            Id = CombineFundingRateId(symbolName, rate.FundingTime)
        }).ToList());
    }

    public override async Task<DateTime> GetLastOpenInterestTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtOpenInterestHistories.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtOpenInterestHistories.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastBasisTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtBasisHistories.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtBasisHistories.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesUsdtOpenInterestHistory>>> GetOpenInterestHistoriesAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesOpenInterestHistory>> result = await usdFutures.GetOpenInterestHistoryAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        List<FuturesUsdtOpenInterestHistory> openInterestHistories = result.Value
            .Where(item => item.Timestamp.HasValue)
            .AsParallel()
            .Select(item => new FuturesUsdtOpenInterestHistory
            {
                Timestamp = item.Timestamp!.Value,
                SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
                SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue),
                SymbolInfoId = symbolName,
                Id = CombineOpenInterestId(symbolName, item.Timestamp.Value)
            }).ToList();
        return Result.Ok(openInterestHistories);
    }

    public override async Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtTopLongShortPositionRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtTopLongShortPositionRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesUsdtTopLongShortPositionRatio>>> GetTopLongShortPositionRatiosAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortPositionRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesUsdtTopLongShortPositionRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    public override async Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtTopLongShortAccountRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtTopLongShortAccountRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesUsdtTopLongShortAccountRatio>>> GetTopLongShortAccountRatiosAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesUsdtTopLongShortAccountRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    public override async Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtGlobalLongShortAccountRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtGlobalLongShortAccountRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastTakerLongShortRatioTimeAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesUsdtTakerLongShortRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesUsdtTakerLongShortRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesUsdtGlobalLongShortAccountRatio>>> GetGlobalLongShortAccountRatiosAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetGlobalLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesUsdtGlobalLongShortAccountRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesUsdtBasis>>> GetBasisAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBasis>> result = await usdFutures.GetBasisAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(item => new FuturesUsdtBasis
        {
            Timestamp = item.Timestamp,
            FuturesPrice = decimal.ToDouble(item.FuturesPrice),
            IndexPrice = decimal.ToDouble(item.IndexPrice),
            BasisValue = decimal.ToDouble(item.Basis),
            BasisRate = decimal.ToDouble(item.BasisRate),
            AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null,
            SymbolInfoId = symbolName,
            Id = CombineOpenInterestId(symbolName, item.Timestamp)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesUsdtTakerLongShortRatio>>> GetTakerLongShortRatiosAsync(BinanceFuturesUsdtSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBuySellVolumeRatio>> result = await usdFutures.GetTakerLongShortRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesUsdtTakerLongShortRatio
        {
            Timestamp = item.Timestamp!.Value,
            BuySellRatio = decimal.ToDouble(item.BuySellRatio),
            BuyVolume = decimal.ToDouble(item.BuyVolume),
            SellVolume = decimal.ToDouble(item.SellVolume),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    protected override async Task<Result<FundingRate[]>> GetCsvFundingRatesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        FundingRate[] rates = await db.FuturesUsdtFundingRates.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.FundingTime)
            .Select(item => new FundingRate
            {
                FundingTime = DateTimeConverter.ConvertToMilliseconds(item.FundingTime).Value,
                Rate = item.FundingRate,
                MarkPrice = item.MarkPrice
            }).ToArrayAsync(ct);
        if (rates.Length == 0)
            return Result.Fail("No funding rates found.");
        return Result.Ok(rates);
    }

    protected override async Task<Result<OpenInterestHistory[]>> GetCsvOpenInterestHistoriesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        OpenInterestHistory[] openInterestHistories = await db.FuturesUsdtOpenInterestHistories.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new OpenInterestHistory
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                SumOpenInterest = item.SumOpenInterest,
                SumOpenInterestValue = item.SumOpenInterestValue
            }).ToArrayAsync(ct);

        if (openInterestHistories.Length == 0)
            return Result.Fail("No open interest histories found.");
        return Result.Ok(openInterestHistories);
    }

    protected override async Task<Result<FuturesBasisCsv[]>> GetCsvBasisAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        FuturesBasisCsv[] histories = await db.FuturesUsdtBasisHistories.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new FuturesBasisCsv
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                FuturesPrice = item.FuturesPrice,
                IndexPrice = item.IndexPrice,
                BasisValue = item.BasisValue,
                BasisRate = item.BasisRate,
                AnnualizedBasisRate = item.AnnualizedBasisRate
            }).ToArrayAsync(ct);
        if (histories.Length == 0)
            return Result.Fail("No basis histories found.");
        return Result.Ok(histories);
    }

    protected override async Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortPositionRatiosAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        LongShortRatioCsv[] ratios = await db.FuturesUsdtTopLongShortPositionRatios.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new LongShortRatioCsv
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                LongShortRatio = item.LongShortRatio,
                LongAccount = item.LongAccount,
                ShortAccount = item.ShortAccount
            }).ToArrayAsync(ct);
        if (ratios.Length == 0)
            return Result.Fail("No top long short position ratios found.");
        return Result.Ok(ratios);
    }

    protected override async Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        LongShortRatioCsv[] ratios = await db.FuturesUsdtTopLongShortAccountRatios.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new LongShortRatioCsv
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                LongShortRatio = item.LongShortRatio,
                LongAccount = item.LongAccount,
                ShortAccount = item.ShortAccount
            }).ToArrayAsync(ct);
        if (ratios.Length == 0)
            return Result.Fail("No top long short account ratios found.");
        return Result.Ok(ratios);
    }

    protected override async Task<Result<LongShortRatioCsv[]>> GetCsvGlobalLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        LongShortRatioCsv[] ratios = await db.FuturesUsdtGlobalLongShortAccountRatios.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new LongShortRatioCsv
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                LongShortRatio = item.LongShortRatio,
                LongAccount = item.LongAccount,
                ShortAccount = item.ShortAccount
            }).ToArrayAsync(ct);
        if (ratios.Length == 0)
            return Result.Fail("No global long short account ratios found.");
        return Result.Ok(ratios);
    }

    protected override async Task<Result<TakerLongShortRatioCsv[]>> GetCsvTakerLongShortRatiosAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        TakerLongShortRatioCsv[] ratios = await db.FuturesUsdtTakerLongShortRatios.AsNoTracking()
            .Where(item => item.SymbolInfoId == symbol)
            .OrderBy(item => item.Timestamp)
            .Select(item => new TakerLongShortRatioCsv
            {
                Timestamp = DateTimeConverter.ConvertToMilliseconds(item.Timestamp).Value,
                BuySellRatio = item.BuySellRatio,
                BuyVolume = item.BuyVolume,
                SellVolume = item.SellVolume,
                BuyVolumeValue = item.BuyVolumeValue,
                SellVolumeValue = item.SellVolumeValue
            }).ToArrayAsync(ct);
        if (ratios.Length == 0)
            return Result.Fail("No taker long short ratios found.");
        return Result.Ok(ratios);
    }

}
