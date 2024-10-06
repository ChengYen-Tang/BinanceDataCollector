using Binance.Net.Enums;
using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

public class BinanceMarkIndexKline
{
    /// <summary>
    /// The time this candlestick opened
    /// </summary>
    public DateTime OpenTime { get; set; }

    /// <summary>
    /// The price at which this candlestick opened
    /// </summary>
    public double OpenPrice { get; set; }

    /// <summary>
    /// The highest price in this candlestick
    /// </summary>
    public double HighPrice { get; set; }

    /// <summary>
    /// The lowest price in this candlestick
    /// </summary>
    public double LowPrice { get; set; }

    /// <summary>
    /// The price at which this candlestick closed
    /// </summary>
    public double ClosePrice { get; set; }

    /// <summary>
    /// The close time of this candlestick
    /// </summary>
    public DateTime CloseTime { get; set; }

    public KlineInterval Interval { get; set; }

    [Key]
    public string Id { get; set; }

    public string SymbolInfoId { get; set; }
}

public class BinanceKline : BinanceMarkIndexKline
{
    /// <summary>
    /// The volume traded during this candlestick
    /// </summary>
    public double Volume { get; set; }

    /// <summary>
    /// The volume traded during this candlestick in the asset form
    /// </summary>
    public double QuoteVolume { get; set; }

    /// <summary>
    /// The amount of trades in this candlestick
    /// </summary>
    public int TradeCount { get; set; }

    /// <summary>
    /// Taker buy base asset volume
    /// </summary>
    public double TakerBuyBaseVolume { get; set; }

    /// <summary>
    /// Taker buy quote asset volume
    /// </summary>
    public double TakerBuyQuoteVolume { get; set; }
}

[Index(nameof(CloseTime))]
[Index(nameof(OpenTime))]
[Index(nameof(Interval))]
[Index(nameof(CloseTime), nameof(Interval))]
[Index(nameof(OpenTime), nameof(Interval))]
public class SpotBinanceKline : BinanceKline
{
    public BinanceSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(CloseTime))]
[Index(nameof(OpenTime))]
[Index(nameof(Interval))]
[Index(nameof(CloseTime), nameof(Interval))]
[Index(nameof(OpenTime), nameof(Interval))]
public class FuturesUsdtBinanceKline : BinanceKline
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(CloseTime))]
[Index(nameof(OpenTime))]
[Index(nameof(Interval))]
[Index(nameof(CloseTime), nameof(Interval))]
[Index(nameof(OpenTime), nameof(Interval))]
public class FuturesUsdtBinancePremiumIndexKline : BinanceMarkIndexKline
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(CloseTime))]
[Index(nameof(OpenTime))]
[Index(nameof(Interval))]
[Index(nameof(CloseTime), nameof(Interval))]
[Index(nameof(OpenTime), nameof(Interval))]
public class FuturesCoinBinanceKline : BinanceKline
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(CloseTime))]
[Index(nameof(OpenTime))]
[Index(nameof(Interval))]
[Index(nameof(CloseTime), nameof(Interval))]
[Index(nameof(OpenTime), nameof(Interval))]
public class FuturesCoinBinancePremiumIndexKline : BinanceMarkIndexKline
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}
