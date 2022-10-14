using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;

namespace CollectorModels.Models;

[Index(nameof(BaseAsset))]
[Index(nameof(QuoteAsset))]
[Index(nameof(BaseAsset), nameof(QuoteAsset))]
public class BinanceFuturesUsdtSymbolInfo : BinanceFuturesSymbolInfo
{
    /// <summary>
    /// The status of the symbol
    /// </summary>
    public double SettlePlan { get; set; }

    public List<FuturesUsdtBinanceKline> BinanceKlines { get; set; }
}
