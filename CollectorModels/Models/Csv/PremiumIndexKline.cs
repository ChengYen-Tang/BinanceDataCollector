﻿using Magicodes.ExporterAndImporter.Core;
using System;

namespace CollectorModels.Models.Csv;

public class PremiumIndexKline
{
    /// <summary>
    /// The time this candlestick opened
    /// </summary>
    [ExporterHeader(Format = "s")]
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
    [ExporterHeader(Format = "s")]
    public DateTime CloseTime { get; set; }
}