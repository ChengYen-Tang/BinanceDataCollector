using Binance.Net.Enums;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

[Index(nameof(BaseAsset))]
[Index(nameof(QuoteAsset))]
[Index(nameof(BaseAsset), nameof(QuoteAsset))]
public class BinanceSymbolInfo
{
    /// <summary>
    /// The symbol
    /// </summary>
    [Key]
    public string Name { get; set; }
    /// <summary>
    /// The status of the symbol
    /// </summary>
    public SymbolStatus Status { get; set; }
    /// <summary>
    /// The base asset
    /// </summary>
    public string BaseAsset { get; set; }
    /// <summary>
    /// The precision of the base asset
    /// </summary>
    public int BaseAssetPrecision { get; set; }
    /// <summary>
    /// The quote asset
    /// </summary>
    public string QuoteAsset { get; set; }
    /// <summary>
    /// The precision of the quote asset
    /// </summary>
    public int QuoteAssetPrecision { get; set; }

    /// <summary>
    /// Allowed order types
    /// </summary>
    public IEnumerable<SpotOrderType> OrderTypes { get; set; }
    /// <summary>
    /// Ice berg orders allowed
    /// </summary>
    public bool IceBergAllowed { get; set; }
    /// <summary>
    /// Cancel replace allowed
    /// </summary>
    public bool CancelReplaceAllowed { get; set; }
    /// <summary>
    /// Spot trading orders allowed
    /// </summary>
    public bool IsSpotTradingAllowed { get; set; }
    /// <summary>
    /// Trailling stop orders are allowed
    /// </summary>
    public bool AllowTrailingStop { get; set; }
    /// <summary>
    /// Margin trading orders allowed
    /// </summary>
    public bool IsMarginTradingAllowed { get; set; }
    /// <summary>
    /// If OCO(One Cancels Other) orders are allowed
    /// </summary>
    public bool OCOAllowed { get; set; }
    /// <summary>
    /// Whether or not it is allowed to specify the quantity of a market order in the quote asset
    /// </summary>
    public bool QuoteOrderQuantityMarketAllowed { get; set; }
    /// <summary>
    /// The precision of the base asset fee
    /// </summary>
    public int BaseFeePrecision { get; set; }
    /// <summary>
    /// The precision of the quote asset fee
    /// </summary>
    public int QuoteFeePrecision { get; set; }
    /// <summary>
    /// Permissions types
    /// </summary>
    public IEnumerable<AccountType> Permissions { get; set; }

    public List<SpotBinanceKline> BinanceKlines { get; set; }
}
