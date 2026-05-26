using Binance.Net.Interfaces.Clients;
using Binance.Net.Enums;
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
    private static IReadOnlyCollection<string> MarketDataTypes => [MarketDataBase.AggTradesDataType];

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
        await DeleteMarketDataSymbolDirectoriesAsync(delistedSymbols, MarketDataTypes, ct);
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

    protected override async Task<Result<List<Kline>>> GetKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await usdFutures.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new Kline
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
        }).ToList());
    }

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(SymbolInfoCsv symbol, (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState, CancellationToken ct = default)
        => usdFuturesMarketData.DownloadAggTradesAsync(symbol.Name, syncState, GetMarketDataTempSymbolPath(MarketDataBase.AggTradesDataType, symbol.Name), ct);

    protected override async Task<Result<List<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetPremiumIndexKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }).ToList());
    }

    protected override async Task<Result<List<PremiumIndexKline>>> GetIndexPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetIndexPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }).ToList());
    }

    protected override async Task<Result<List<PremiumIndexKline>>> GetMarkPriceKlinesAsync(SymbolInfoCsv symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await usdFutures.GetMarkPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new PremiumIndexKline()
        {
            OpenPrice = decimal.ToDouble(kline.OpenPrice),
            ClosePrice = decimal.ToDouble(kline.ClosePrice),
            HighPrice = decimal.ToDouble(kline.HighPrice),
            LowPrice = decimal.ToDouble(kline.LowPrice),
            OpenTime = ToUnixMilliseconds(kline.OpenTime),
            CloseTime = ToUnixMilliseconds(kline.CloseTime)
        }).ToList());
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

    public override Task<DateTime> GetLastFundingTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(FundingRatePath, symbol.Name, nameof(FundingRate.FundingTime), null, ct);

    protected override async Task<Result<List<FundingRate>>> GetFundingRatesAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesFundingRateHistory>> result = await usdFutures.GetFundingRatesAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(rate => new FundingRate
        {
            Rate = decimal.ToDouble(rate.FundingRate),
            FundingTime = ToUnixMilliseconds(rate.FundingTime),
            MarkPrice = rate.MarkPrice.HasValue ? decimal.ToDouble(rate.MarkPrice.Value) : null,
        }).ToList());
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

        List<OpenInterestHistory> openInterestHistories = result.Value
            .Where(item => item.Timestamp.HasValue)
            .AsParallel()
            .Select(item => new OpenInterestHistory
            {
                Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
                SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
                SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue)
            }).ToList();
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

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }).ToList());
    }

    public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(SymbolInfoCsv symbol, CancellationToken ct = default)
        => GetLastTimestampAsync(TopLongShortAccountRatioPath, symbol.Name, nameof(LongShortRatioCsv.Timestamp), null, ct);

    protected override async Task<Result<List<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await usdFutures.GetTopLongShortAccountRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }).ToList());
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

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesBasisCsv>>> GetBasisAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBasis>> result = await usdFutures.GetBasisAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(item => new FuturesBasisCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp),
            FuturesPrice = decimal.ToDouble(item.FuturesPrice),
            IndexPrice = decimal.ToDouble(item.IndexPrice),
            BasisValue = decimal.ToDouble(item.Basis),
            BasisRate = decimal.ToDouble(item.BasisRate),
            AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null
        }).ToList());
    }

    protected override async Task<Result<List<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(SymbolInfoCsv symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBuySellVolumeRatio>> result = await usdFutures.GetTakerLongShortRatioAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new TakerLongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            BuySellRatio = decimal.ToDouble(item.BuySellRatio),
            BuyVolume = decimal.ToDouble(item.BuyVolume),
            SellVolume = decimal.ToDouble(item.SellVolume),
            BuyVolumeValue = null,
            SellVolumeValue = null
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
