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
    public List<FuturesCoinBinanceIndexPriceKline> BinanceIndexPriceKlines { get; set; }
    public List<FuturesCoinBinanceMarkPriceKline> BinanceMarkPriceKlines { get; set; }
    public List<FuturesCoinFundingRate> FundingRates { get; set; }
    public List<FuturesCoinOpenInterestHistory> OpenInterestHistories { get; set; }
    public List<FuturesCoinTopLongShortPositionRatio> TopLongShortPositionRatios { get; set; }
    public List<FuturesCoinTopLongShortAccountRatio> TopLongShortAccountRatios { get; set; }
    public List<FuturesCoinGlobalLongShortAccountRatio> GlobalLongShortAccountRatios { get; set; }
}
