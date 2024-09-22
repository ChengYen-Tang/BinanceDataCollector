using System;

namespace CollectorModels.Models.Csv;

public class Kline
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
    /// The volume traded during this candlestick
    /// </summary>
    public double Volume { get; set; }

    /// <summary>
    /// The close time of this candlestick
    /// </summary>
    public DateTime CloseTime { get; set; }

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
