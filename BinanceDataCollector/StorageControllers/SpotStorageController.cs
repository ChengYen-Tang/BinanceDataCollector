using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class SpotStorageController : StorageController<BinanceSymbolInfo, SpotBinanceKline, BinanceKline?>
{
    private readonly Spot spot;

    public SpotStorageController(IConfiguration configuration, IServiceProvider serviceProvider, ILogger<SpotStorageController> logger, IBinanceRestClient client)
        : base(serviceProvider, logger) => (spot) = (new(client, configuration.GetSection("IgnoneCoins:Spot").Get<string[]>() ?? []));

    protected override string KlinePath { get { return Path.Combine(RootKlinePath, "Spot"); } }
    protected override string PremiumIndexKlinePath => throw new NotImplementedException();
    protected override bool IsFutures => false;

    public override async Task DeleteOldKlines(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        foreach (BinanceSymbolInfo symbol in await db.BinanceSymbolInfos.AsNoTracking().ToArrayAsync(ct))
        {
            SpotBinanceKline[] klines = await db.SpotBinanceKlines.AsNoTracking().Where(item => item.OpenTime < yearsReserved && item.SymbolInfoId == symbol.Name).ToArrayAsync(ct);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            Dictionary<DbContext, IEnumerable<SpotBinanceKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
            foreach (KeyValuePair<DbContext, IEnumerable<SpotBinanceKline>> item in bulkShardingEnumerable)
                await item.Key.BulkDeleteAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
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

    protected override async Task<Result<Kline[]>> GetCsvKlinesAsync(string symbol, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        Kline[] klines = await db.SpotBinanceKlines.AsNoTracking().Where(item => item.SymbolInfoId == symbol).OrderBy(item => item.OpenTime).Select(item => new Kline
        {
            OpenTime = item.OpenTime,
            OpenPrice = item.OpenPrice,
            HighPrice = item.HighPrice,
            LowPrice = item.LowPrice,
            ClosePrice = item.ClosePrice,
            Volume = item.Volume,
            QuoteVolume = item.QuoteVolume,
            TakerBuyBaseVolume = item.TakerBuyBaseVolume,
            TakerBuyQuoteVolume = item.TakerBuyQuoteVolume,
            TradeCount = item.TradeCount,
            CloseTime = item.CloseTime
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

    protected override Task<Result<List<BinanceKline?>>> GetPremiumIndexKlinesAsync(BinanceSymbolInfo symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => throw new NotImplementedException();

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
}
