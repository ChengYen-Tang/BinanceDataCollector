using Binance.Net.Interfaces.Clients;
using Binance.Net.Enums;
using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using CryptoExchange.Net.Converters.SystemTextJson;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class CoinFuturesStorageController : StorageController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline, FuturesCoinBinanceIndexPriceKline, FuturesCoinBinanceMarkPriceKline, FuturesCoinFundingRate, FuturesCoinOpenInterestHistory, FuturesCoinTopLongShortPositionRatio, FuturesCoinTopLongShortAccountRatio, FuturesCoinGlobalLongShortAccountRatio>
{
    private readonly CoinFutures coinFutures;

    public CoinFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<CoinFuturesStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (coinFutures) = (new(client, configuration.GetSection("IgnoneCoins:CoinFutures").Get<string[]>() ?? []));

    protected override string KlinePath { get { return Path.Combine(RootKlinePath, "CoinFutures"); } }
    protected override string PremiumIndexKlinePath { get { return Path.Combine(RootPremiumIndexKlinePath, "CoinFutures"); } }
    protected override string IndexPriceKlinePath { get { return Path.Combine(RootIndexPriceKlinePath, "CoinFutures"); } }
    protected override string MarkPriceKlinePath { get { return Path.Combine(RootMarkPriceKlinePath, "CoinFutures"); } }
    protected override string FundingRatePath { get { return Path.Combine(RootFundingRatePath, "CoinFutures"); } }
    protected override string OpenInterestPath { get { return Path.Combine(RootOpenInterestPath, "CoinFutures"); } }
    protected override string TopLongShortPositionRatioPath { get { return Path.Combine(RootTopLongShortPositionRatioPath, "CoinFutures"); } }
    protected override string TopLongShortAccountRatioPath { get { return Path.Combine(RootTopLongShortAccountRatioPath, "CoinFutures"); } }
    protected override string GlobalLongShortAccountRatioPath { get { return Path.Combine(RootGlobalLongShortAccountRatioPath, "CoinFutures"); } }
    protected override bool IsFutures => true;

    protected override string GetSymbolName(BinanceFuturesCoinSymbolInfo symbol)
        => symbol.Name;

    protected override Task<List<string>> GetExistingSymbolNamesAsync(BinanceDbContext db, CancellationToken ct = default)
        => db.BinanceFuturesCoinSymbolInfos.AsNoTracking().Select(item => item.Name).ToListAsync(ct);

    protected override async Task DeleteDelistedSymbolsAsync(BinanceDbContext db, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await db.DropShardingTablesAsync(delistedSymbols,
        [
            typeof(FuturesCoinBinanceKline).Name,
            typeof(FuturesCoinBinancePremiumIndexKline).Name,
            typeof(FuturesCoinBinanceIndexPriceKline).Name,
            typeof(FuturesCoinBinanceMarkPriceKline).Name,
            typeof(FuturesCoinFundingRate).Name,
            typeof(FuturesCoinOpenInterestHistory).Name,
            typeof(FuturesCoinTopLongShortPositionRatio).Name,
            typeof(FuturesCoinTopLongShortAccountRatio).Name,
            typeof(FuturesCoinGlobalLongShortAccountRatio).Name,
        ], LogDropStatus, ct);

        await db.BinanceFuturesCoinSymbolInfos.Where(item => delistedSymbols.Contains(item.Name)).ExecuteDeleteAsync(ct);
    }

    public override async Task DeleteOldData(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;

        // 只取得 symbol 名稱，不載入完整的 entity
        List<string> symbolNames = await db.BinanceFuturesCoinSymbolInfos
            .AsNoTracking()
            .Select(s => s.Name)
            .ToListAsync(ct);

        foreach (string symbolName in symbolNames)
        {
            // 只查詢必要欄位：Id 和 SymbolInfoId (Sharding Key)
            var klineMinimalData = await db.FuturesCoinBinanceKlines
                .AsNoTracking()
                .Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var premiumIndexMinimalData = await db.FuturesCoinBinancePremiumIndexKlines
                .AsNoTracking()
                .Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var indexPriceMinimalData = await db.FuturesCoinBinanceIndexPriceKlines
                .AsNoTracking()
                .Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var markPriceMinimalData = await db.FuturesCoinBinanceMarkPriceKlines
                .AsNoTracking()
                .Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var fundingRateMinimalData = await db.FuturesCoinFundingRates
                .AsNoTracking()
                .Where(item => item.FundingTime < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var openInterestMinimalData = await db.FuturesCoinOpenInterestHistories
                .AsNoTracking()
                .Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var topLongShortPositionRatioMinimalData = await db.FuturesCoinTopLongShortPositionRatios
                .AsNoTracking()
                .Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var topLongShortAccountRatioMinimalData = await db.FuturesCoinTopLongShortAccountRatios
                .AsNoTracking()
                .Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            var globalLongShortAccountRatioMinimalData = await db.FuturesCoinGlobalLongShortAccountRatios
                .AsNoTracking()
                .Where(item => item.Timestamp < yearsReserved && item.SymbolInfoId == symbolName)
                .Select(item => new { item.Id, item.SymbolInfoId })
                .ToArrayAsync(ct);

            // 轉換為只包含必要欄位的 entity
            FuturesCoinBinanceKline[] klines = [.. klineMinimalData.Select(x => new FuturesCoinBinanceKline
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinBinancePremiumIndexKline[] premiumIndexKlines = [.. premiumIndexMinimalData.Select(x => new FuturesCoinBinancePremiumIndexKline
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinBinanceIndexPriceKline[] indexPriceKlines = [.. indexPriceMinimalData.Select(x => new FuturesCoinBinanceIndexPriceKline
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinBinanceMarkPriceKline[] markPriceKlines = [.. markPriceMinimalData.Select(x => new FuturesCoinBinanceMarkPriceKline
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinFundingRate[] fundingRates = [.. fundingRateMinimalData.Select(x => new FuturesCoinFundingRate
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinOpenInterestHistory[] openInterestHistories = [.. openInterestMinimalData.Select(x => new FuturesCoinOpenInterestHistory
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinTopLongShortPositionRatio[] topLongShortPositionRatios = [.. topLongShortPositionRatioMinimalData.Select(x => new FuturesCoinTopLongShortPositionRatio
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinTopLongShortAccountRatio[] topLongShortAccountRatios = [.. topLongShortAccountRatioMinimalData.Select(x => new FuturesCoinTopLongShortAccountRatio
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            FuturesCoinGlobalLongShortAccountRatio[] globalLongShortAccountRatios = [.. globalLongShortAccountRatioMinimalData.Select(x => new FuturesCoinGlobalLongShortAccountRatio
            {
                Id = x.Id,
                SymbolInfoId = x.SymbolInfoId
            })];

            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            Dictionary<DbContext, IEnumerable<FuturesCoinBinanceKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
            Dictionary<DbContext, IEnumerable<FuturesCoinBinancePremiumIndexKline>> bulkShardingEnumerablePremiumIndex = db.BulkShardingTableEnumerable(premiumIndexKlines);
            Dictionary<DbContext, IEnumerable<FuturesCoinBinanceIndexPriceKline>> bulkShardingEnumerableIndexPrice = db.BulkShardingTableEnumerable(indexPriceKlines);
            Dictionary<DbContext, IEnumerable<FuturesCoinBinanceMarkPriceKline>> bulkShardingEnumerableMarkPrice = db.BulkShardingTableEnumerable(markPriceKlines);
            Dictionary<DbContext, IEnumerable<FuturesCoinFundingRate>> bulkShardingEnumerableFundingRates = db.BulkShardingTableEnumerable(fundingRates);
            Dictionary<DbContext, IEnumerable<FuturesCoinOpenInterestHistory>> bulkShardingEnumerableOpenInterest = db.BulkShardingTableEnumerable(openInterestHistories);
            Dictionary<DbContext, IEnumerable<FuturesCoinTopLongShortPositionRatio>> bulkShardingEnumerableTopLongShortPositionRatios = db.BulkShardingTableEnumerable(topLongShortPositionRatios);
            Dictionary<DbContext, IEnumerable<FuturesCoinTopLongShortAccountRatio>> bulkShardingEnumerableTopLongShortAccountRatios = db.BulkShardingTableEnumerable(topLongShortAccountRatios);
            Dictionary<DbContext, IEnumerable<FuturesCoinGlobalLongShortAccountRatio>> bulkShardingEnumerableGlobalLongShortAccountRatios = db.BulkShardingTableEnumerable(globalLongShortAccountRatios);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinanceKline>> item in bulkShardingEnumerable)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinancePremiumIndexKline>> item in bulkShardingEnumerablePremiumIndex)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinanceIndexPriceKline>> item in bulkShardingEnumerableIndexPrice)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinanceMarkPriceKline>> item in bulkShardingEnumerableMarkPrice)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinFundingRate>> item in bulkShardingEnumerableFundingRates)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinOpenInterestHistory>> item in bulkShardingEnumerableOpenInterest)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinTopLongShortPositionRatio>> item in bulkShardingEnumerableTopLongShortPositionRatios)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinTopLongShortAccountRatio>> item in bulkShardingEnumerableTopLongShortAccountRatios)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinGlobalLongShortAccountRatio>> item in bulkShardingEnumerableGlobalLongShortAccountRatios)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            await transaction.CommitAsync(ct);
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
}
