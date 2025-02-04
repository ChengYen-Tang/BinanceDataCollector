using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using CryptoExchange.Net.Converters.JsonNet;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class CoinFuturesStorageController : StorageController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline, FuturesCoinBinancePremiumIndexKline>
{
    private readonly CoinFutures coinFutures;

    public CoinFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<CoinFuturesStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (coinFutures) = (new(client, configuration.GetSection("IgnoneCoins:CoinFutures").Get<string[]>() ?? []));

    protected override string KlinePath { get { return Path.Combine(RootKlinePath, "CoinFutures"); } }
    protected override string PremiumIndexKlinePath { get { return Path.Combine(RootPremiumIndexKlinePath, "CoinFutures"); } }
    protected override bool IsFutures => true;

    public override async Task DeleteOldKlines(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        foreach (BinanceFuturesCoinSymbolInfo symbol in await db.BinanceFuturesCoinSymbolInfos.AsNoTracking().ToArrayAsync(ct))
        {
            FuturesCoinBinanceKline[] klines = await db.FuturesCoinBinanceKlines.AsNoTracking().Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbol.Name).ToArrayAsync(ct);
            FuturesCoinBinancePremiumIndexKline[] premiumIndexKlines = await db.FuturesCoinBinancePremiumIndexKlines.AsNoTracking().Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbol.Name).ToArrayAsync(ct);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            Dictionary<DbContext, IEnumerable<FuturesCoinBinanceKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
            Dictionary<DbContext, IEnumerable<FuturesCoinBinancePremiumIndexKline>> bulkShardingEnumerablePremiumIndex = db.BulkShardingTableEnumerable(premiumIndexKlines);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinanceKline>> item in bulkShardingEnumerable)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinancePremiumIndexKline>> item in bulkShardingEnumerablePremiumIndex)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
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
}
