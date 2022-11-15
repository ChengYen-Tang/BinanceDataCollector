using System.Collections.Generic;
using System;
using Binance.Net.Enums;
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace CollectorModels.Models;

public class BinanceFuturesSymbolInfo
{
    /// <summary>
    /// Contract type
    /// </summary>
    public ContractType? ContractType { get; set; }
    /// <summary>
    /// The maintenance margin percent
    /// </summary>
    public double MaintMarginPercent { get; set; }
    /// <summary>
    /// The price Precision
    /// </summary>
    public int PricePrecision { get; set; }
    /// <summary>
    /// The quantity precision
    /// </summary>
    public int QuantityPrecision { get; set; }
    /// <summary>
    /// The required margin percent
    /// </summary>
    public double RequiredMarginPercent { get; set; }
    /// <summary>
    /// The base asset
    /// </summary>
    public string BaseAsset { get; set; }
    /// <summary>
    /// Margin asset
    /// </summary>
    public string MarginAsset { get; set; }
    /// <summary>
    /// The quote asset
    /// </summary>
    public string QuoteAsset { get; set; }
    /// <summary>
    /// The precision of the base asset
    /// </summary>
    public int BaseAssetPrecision { get; set; }
    /// <summary>
    /// The precision of the quote asset
    /// </summary>
    public int QuoteAssetPrecision { get; set; }
    /// <summary>
    /// Allowed order types
    /// </summary>
    public IEnumerable<FuturesOrderType> OrderTypes { get; set; }
    /// <summary>
    /// The symbol
    /// </summary>
    [Key]
    public string Name { get; set; }
    /// <summary>
    /// Pair
    /// </summary>
    public string Pair { get; set; }
    /// <summary>
    /// Delivery Date
    /// </summary>
    public DateTime DeliveryDate { get; set; }
    /// <summary>
    /// Delivery Date
    /// </summary>
    public DateTime ListingDate { get; set; }
    /// <summary>
    /// Trigger protect
    /// </summary>
    public double TriggerProtect { get; set; }
    /// <summary>
    /// Currently Empty
    /// </summary>
    public UnderlyingType UnderlyingType { get; set; }
    /// <summary>
    /// Sub types
    /// </summary>
    public IEnumerable<string> UnderlyingSubType { get; set; }

    /// <summary>
    /// Liquidation fee
    /// </summary>
    public double LiquidationFee { get; set; }
    /// <summary>
    /// The max price difference rate (from mark price) a market order can make
    /// </summary>
    public double MarketTakeBound { get; set; }

    /// <summary>
    /// Allowed order time in force
    /// </summary>
    public IEnumerable<TimeInForce> TimeInForce { get; set; }

    /// <summary>
    /// The status of the symbol
    /// </summary>
    public SymbolStatus Status { get; set; }

    public override string ToString()
        => Name;
}
