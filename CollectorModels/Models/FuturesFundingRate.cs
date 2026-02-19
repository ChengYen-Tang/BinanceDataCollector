using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

public class FuturesFundingRate
{
    [Key]
    public string Id { get; set; }

    public DateTime FundingTime { get; set; }

    public double FundingRate { get; set; }

    public double? MarkPrice { get; set; }

    public string SymbolInfoId { get; set; }
}

[Index(nameof(FundingTime))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(FundingTime), nameof(SymbolInfoId))]
public class FuturesUsdtFundingRate : FuturesFundingRate
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(FundingTime))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(FundingTime), nameof(SymbolInfoId))]
public class FuturesCoinFundingRate : FuturesFundingRate
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}
