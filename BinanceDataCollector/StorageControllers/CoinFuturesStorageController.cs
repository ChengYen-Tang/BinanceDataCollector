using Binance.Net.Interfaces.Clients;
using Binance.Net.Enums;
using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using MarketDataDownloadBatch = BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch;
using MarketDataCoinFutures = BinanceDataCollector.Collectors.BinanceMarketData.CoinFutures;
using BinanceDataCollector.WorkItems;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using CryptoExchange.Net.Converters.SystemTextJson;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class CoinFuturesStorageController : StorageController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline, FuturesCoinBinanceIndexPriceKline, FuturesCoinBinanceMarkPriceKline, FuturesCoinFundingRate, FuturesCoinOpenInterestHistory, FuturesCoinTopLongShortPositionRatio, FuturesCoinTopLongShortAccountRatio, FuturesCoinGlobalLongShortAccountRatio, FuturesCoinTakerLongShortRatio, FuturesCoinBasis>
{
    private const string Market = "CoinFutures";
    private readonly CoinFutures coinFutures;
    private readonly MarketDataCoinFutures coinFuturesMarketData;

    public CoinFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<CoinFuturesStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (coinFutures, coinFuturesMarketData) = (new(client, configuration.GetSection("IgnoneCoins:CoinFutures").Get<string[]>() ?? []), new());

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
    private static IReadOnlyCollection<string> MarketDataTypes => ["AggTrades"];

    protected override string GetSymbolName(BinanceFuturesCoinSymbolInfo symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(BinanceDbContext db, CancellationToken ct = default)
        => db.BinanceFuturesCoinSymbolInfos.AsNoTracking().Select(item => item.Name).ToListAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(BinanceDbContext db, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await db.DropShardingTablesAsync(delistedSymbols,
        [
            nameof(BinanceDbContext.FuturesCoinBinanceKlines),
            nameof(BinanceDbContext.FuturesCoinBinancePremiumIndexKlines),
            nameof(BinanceDbContext.FuturesCoinBinanceIndexPriceKlines),
            nameof(BinanceDbContext.FuturesCoinBinanceMarkPriceKlines),
            nameof(BinanceDbContext.FuturesCoinFundingRates),
            nameof(BinanceDbContext.FuturesCoinOpenInterestHistories),
            nameof(BinanceDbContext.FuturesCoinBasisHistories),
            nameof(BinanceDbContext.FuturesCoinTopLongShortPositionRatios),
            nameof(BinanceDbContext.FuturesCoinTopLongShortAccountRatios),
            nameof(BinanceDbContext.FuturesCoinGlobalLongShortAccountRatios),
            nameof(BinanceDbContext.FuturesCoinTakerLongShortRatios),
        ], LogDropStatus, ct);

        await db.BinanceFuturesCoinSymbolInfos.Where(item => delistedSymbols.Contains(item.Name)).ExecuteDeleteAsync(ct);
        await DeleteMarketDataSymbolDirectoriesAsync(delistedSymbols, MarketDataTypes, ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        List<string> symbolNames = await db.BinanceFuturesCoinSymbolInfos
            .AsNoTracking()
            .Select(s => s.Name)
            .ToListAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            await db.FuturesCoinBinanceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinBinancePremiumIndexKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinBinanceIndexPriceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinBinanceMarkPriceKlines.Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinFundingRates.Where(item => item.FundingTime < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinOpenInterestHistories.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinBasisHistories.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinTopLongShortPositionRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinTopLongShortAccountRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinGlobalLongShortAccountRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
            await db.FuturesCoinTakerLongShortRatios.Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName).ExecuteDeleteAsync(ct);
        }
    }

    public override async Task<DateTime> GetLastTimeAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBinanceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBinanceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastPremiumIndexTimeAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBinancePremiumIndexKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBinancePremiumIndexKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastIndexPriceTimeAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBinanceIndexPriceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBinanceIndexPriceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastMarkPriceTimeAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBinanceMarkPriceKlines.AsNoTracking().AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBinanceMarkPriceKlines.AsNoTracking().Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
    }

    protected override async Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        string[] symbols = await db.BinanceFuturesCoinSymbolInfos.AsNoTracking().Select(item => item.Name).ToArrayAsync(ct);
        if (symbols.Length == 0)
            return Result.Fail("No symbols found.");
        return Result.Ok(symbols);
    }

    protected override async Task<Result<SymbolInfoCsv[]>> GetCsvSymbolInfosAsync(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        SymbolInfoCsv[] symbols = await db.BinanceFuturesCoinSymbolInfos.AsNoTracking()
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
        Kline[] klines = await db.FuturesCoinBinanceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new Kline
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
        PremiumIndexKline[] klines = await db.FuturesCoinBinanceIndexPriceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
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
        PremiumIndexKline[] klines = await db.FuturesCoinBinanceMarkPriceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
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

    protected override async Task<Result<List<FuturesCoinBinanceKline>>> GetKlinesAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<IBinanceKline>> result = await coinFutures.GetKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesCoinBinanceKline()
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

    protected override Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
        => coinFuturesMarketData.DownloadAggTradesAsync(symbol.Name, startTime, GetMarketDataTempSymbolPath("AggTrades", symbol.Name), ct);

    public override async Task<AsyncWorkItem<MarketDataDownloadBatch>> UpdateAggTradesAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", "AggTrades", symbol, startTime);
        Result<MarketDataDownloadBatch> result = await GetAggTradesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", "AggTrades", symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure("AggTrades", symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<MarketDataDownloadBatch>(InsertAggTradesAsync, CreateEmptyMarketDataDownloadBatch(symbol.Name), ct);
        }

        return new AsyncWorkItem<MarketDataDownloadBatch>(InsertAggTradesAsync, result.Value, ct);
    }

    protected override Task InsertAggTradesAsync(MarketDataDownloadBatch batch, CancellationToken ct = default)
    {
        if (batch.Files.Count == 0)
            return Task.CompletedTask;

        logger.LogDebug("AggTrades temp batch ready. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}", batch.MarketPathSegment, batch.Symbol, batch.Files.Count);
        return Task.CompletedTask;
    }

    private static MarketDataDownloadBatch CreateEmptyMarketDataDownloadBatch(string symbol)
        => new()
        {
            MarketPathSegment = Market,
            DataType = "aggTrades",
            Symbol = symbol,
            Files = [],
        };

    public override async Task<DateTime> GetLastFundingTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        string symbolName = symbol.Name;
        return (await db.FuturesCoinFundingRates.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbolName, ct))
            ? await db.FuturesCoinFundingRates.AsNoTracking().Where(item => item.SymbolInfoId == symbolName).MaxAsync(item => item.FundingTime, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesCoinFundingRate>>> GetFundingRatesAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesFundingRateHistory>> result = await coinFutures.GetFundingRatesAsync(symbolName, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(rate => new FuturesCoinFundingRate
        {
            FundingRate = decimal.ToDouble(rate.FundingRate),
            FundingTime = rate.FundingTime,
            MarkPrice = rate.MarkPrice.HasValue ? decimal.ToDouble(rate.MarkPrice.Value) : null,
            SymbolInfoId = symbolName,
            Id = CombineFundingRateId(symbolName, rate.FundingTime)
        }).ToList());
    }

    public override async Task<DateTime> GetLastOpenInterestTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinOpenInterestHistories.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinOpenInterestHistories.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastBasisTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBasisHistories.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBasisHistories.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesCoinOpenInterestHistory>>> GetOpenInterestHistoriesAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesCoinOpenInterestHistory>> result = await coinFutures.GetOpenInterestHistoryAsync(symbol.Pair, symbol.ContractType ?? ContractType.Perpetual, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        List<FuturesCoinOpenInterestHistory> openInterestHistories = result.Value
            .Where(item => item.Timestamp.HasValue)
            .AsParallel()
            .Select(item => new FuturesCoinOpenInterestHistory
            {
                Timestamp = item.Timestamp!.Value,
                SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
                SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue),
                SymbolInfoId = symbolName,
                Id = CombineOpenInterestId(symbolName, item.Timestamp.Value)
            }).ToList();
        return Result.Ok(openInterestHistories);
    }

    public override async Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinTopLongShortPositionRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinTopLongShortPositionRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesCoinTopLongShortPositionRatio>>> GetTopLongShortPositionRatiosAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await coinFutures.GetTopLongShortPositionRatioAsync(symbol.Pair, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesCoinTopLongShortPositionRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    public override async Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinTopLongShortAccountRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinTopLongShortAccountRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesCoinTopLongShortAccountRatio>>> GetTopLongShortAccountRatiosAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await coinFutures.GetTopLongShortAccountRatioAsync(symbol.Pair, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesCoinTopLongShortAccountRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    public override async Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinGlobalLongShortAccountRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinGlobalLongShortAccountRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    public override async Task<DateTime> GetLastTakerLongShortRatioTimeAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinTakerLongShortRatios.AsNoTracking().AnyAsync(item => item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinTakerLongShortRatios.AsNoTracking().Where(item => item.SymbolInfoId == symbol.Name).MaxAsync(item => item.Timestamp, ct)
            : yearsReserved;
    }

    protected override async Task<Result<List<FuturesCoinGlobalLongShortAccountRatio>>> GetGlobalLongShortAccountRatiosAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesLongShortRatio>> result = await coinFutures.GetGlobalLongShortAccountRatioAsync(symbol.Pair, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp.HasValue).AsParallel().Select(item => new FuturesCoinGlobalLongShortAccountRatio
        {
            Timestamp = item.Timestamp!.Value,
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp.Value)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesCoinBasis>>> GetBasisAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesBasis>> result = await coinFutures.GetBasisAsync(symbol.Pair, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.AsParallel().Select(item => new FuturesCoinBasis
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

    protected override async Task<Result<List<FuturesCoinTakerLongShortRatio>>> GetTakerLongShortRatiosAsync(BinanceFuturesCoinSymbolInfo symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = symbol.Name;
        Result<List<BinanceFuturesCoinBuySellVolumeRatio>> result = await coinFutures.GetTakerLongShortRatioAsync(symbol.Pair, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        return Result.Ok(result.Value.Where(item => item.Timestamp != DateTime.MinValue).AsParallel().Select(item => new FuturesCoinTakerLongShortRatio
        {
            Timestamp = item.Timestamp,
            BuySellRatio = item.TakerSellVolume == 0 ? null : decimal.ToDouble(item.TakerBuyVolume / item.TakerSellVolume),
            BuyVolume = decimal.ToDouble(item.TakerBuyVolume),
            SellVolume = decimal.ToDouble(item.TakerSellVolume),
            BuyVolumeValue = decimal.ToDouble(item.TakerBuyVolumeValue),
            SellVolumeValue = decimal.ToDouble(item.TakerSellVolumeValue),
            SymbolInfoId = symbolName,
            Id = CombineLongShortRatioId(symbolName, item.Timestamp)
        }).ToList());
    }

    protected override async Task<Result<List<FuturesCoinBinancePremiumIndexKline>>> GetPremiumIndexKlinesAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await coinFutures.GetPremiumIndexKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesCoinBinancePremiumIndexKline()
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

    protected override async Task<Result<List<FuturesCoinBinanceIndexPriceKline>>> GetIndexPriceKlinesAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await coinFutures.GetIndexPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesCoinBinanceIndexPriceKline()
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

    protected override async Task<Result<List<FuturesCoinBinanceMarkPriceKline>>> GetMarkPriceKlinesAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>> result = await coinFutures.GetMarkPriceKlinesAsync(symbol.Name, interval, startTime, ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        return Result.Ok(result.Value.AsParallel().Select(kline => new FuturesCoinBinanceMarkPriceKline()
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

    protected override async Task<Result<List<BinanceFuturesCoinSymbolInfo>>> GetMarketAsync(CancellationToken ct = default)
    {
        Result<IEnumerable<BinanceFuturesCoinSymbol>> result = await coinFutures.GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);
        List<BinanceFuturesCoinSymbolInfo> markets = result.Value.AsParallel().Select(symbol => new BinanceFuturesCoinSymbolInfo
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
            ContractSize = symbol.ContractSize,
            EqualQuantityPrecision = symbol.EqualQuantityPrecision
        }).ToList();

        return Result.Ok(markets);
    }

    protected override async Task<Result<PremiumIndexKline[]>> GetCsvPremiumIndexKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        PremiumIndexKline[] klines = await db.FuturesCoinBinancePremiumIndexKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new PremiumIndexKline
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

    protected override async Task<Result<FundingRate[]>> GetCsvFundingRatesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        FundingRate[] rates = await db.FuturesCoinFundingRates.AsNoTracking()
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
        OpenInterestHistory[] openInterestHistories = await db.FuturesCoinOpenInterestHistories.AsNoTracking()
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
        FuturesBasisCsv[] histories = await db.FuturesCoinBasisHistories.AsNoTracking()
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
        LongShortRatioCsv[] ratios = await db.FuturesCoinTopLongShortPositionRatios.AsNoTracking()
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
        LongShortRatioCsv[] ratios = await db.FuturesCoinTopLongShortAccountRatios.AsNoTracking()
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
        LongShortRatioCsv[] ratios = await db.FuturesCoinGlobalLongShortAccountRatios.AsNoTracking()
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
        TakerLongShortRatioCsv[] ratios = await db.FuturesCoinTakerLongShortRatios.AsNoTracking()
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
