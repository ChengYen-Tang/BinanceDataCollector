﻿using Binance.Net.Objects.Models.Spot;
using BinanceDataCollector.Collectors.BinanceApi;
using CollectorModels;
using CollectorModels.Models;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal class SpotStorageController : StorageController<BinanceSymbolInfo, SpotBinanceKline>
{
    private readonly Spot spot;
    
    public SpotStorageController(IServiceProvider serviceProvider, ILogger<SpotStorageController> logger, BinanceClient client)
        : base(serviceProvider, logger) => (spot) = (new(client));

    public override async Task DeleteOldKlines(CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        SpotBinanceKline[] klines = await db.SpotBinanceKlines.Where(item => item.OpenTime < yearsReserved).ToArrayAsync(ct);
        using IDbContextTransaction transaction = db.Database.BeginTransaction();
        await db.BulkDeleteAsync(klines, bulkConfig, cancellationToken: ct);
        transaction.Commit();
    }

    public override async Task<DateTime> GetLastTimeAsync(BinanceSymbolInfo symbol, KlineInterval interval, CancellationToken ct = default)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        var a = (await db.SpotBinanceKlines.AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct));
        return (await db.SpotBinanceKlines.AnyAsync(item => item.Interval == interval && item.SymbolInfoId == symbol.Name, ct))
            ? await db.SpotBinanceKlines.Where(item => item.Interval == interval && item.SymbolInfoId == symbol.Name).MaxAsync(item => item.CloseTime, ct)
            : yearsReserved;
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
            IceBergAllowed = symbol.IceBergAllowed,
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
}