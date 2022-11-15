using Binance.Net.Objects.Models.Futures;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.ShardingCore;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class CoinFuturesStorageController : StorageController<BinanceFuturesCoinSymbolInfo, FuturesCoinBinanceKline>
{
    private readonly CoinFutures coinFutures;

    public CoinFuturesStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<CoinFuturesStorageController> logger, BinanceClient client)
        : base(serviceProvider, logger) => (coinFutures) = (new(client, configuration.GetSection("IgnoneCoins:CoinFutures").Get<string[]>() ?? Array.Empty<string>()));

    public override async Task DeleteOldKlines(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        FuturesCoinBinanceKline[] klines = await db.FuturesCoinBinanceKlines.Where(item => item.OpenTime < yearsReserved).ToArrayAsync(ct);
        using IDbContextTransaction transaction = db.Database.BeginTransaction();
        Dictionary<DbContext, IEnumerable<FuturesCoinBinanceKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
        foreach (KeyValuePair<DbContext, IEnumerable<FuturesCoinBinanceKline>> item in bulkShardingEnumerable)
            await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
        transaction.Commit();
    }

    public override async Task<DateTime> GetLastTimeAsync(BinanceFuturesCoinSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        return (await db.FuturesCoinBinanceKlines.AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.FuturesCoinBinanceKlines.Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
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
}
