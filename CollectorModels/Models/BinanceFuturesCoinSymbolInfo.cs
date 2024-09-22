using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;

namespace CollectorModels.Models;

[Index(nameof(BaseAsset))]
[Index(nameof(QuoteAsset))]
[Index(nameof(BaseAsset), nameof(QuoteAsset))]
public class BinanceFuturesCoinSymbolInfo : BinanceFuturesSymbolInfo
{
    /// <summary>
    /// Contract size
    /// </summary>
    public int ContractSize { get; set; }

    /// <summary>
    /// Equal quantity precision
    /// </summary>
    public int EqualQuantityPrecision { get; set; }

    public List<FuturesCoinBinanceKline> BinanceKlines { get; set; }
    public List<FuturesCoinBinancePremiumIndexKline> BinancePremiumIndexKlines { get; set; }
}
