using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

public class FuturesOpenInterestHistory
{
    [Key]
    public string Id { get; set; }

    public DateTime Timestamp { get; set; }

    public double SumOpenInterest { get; set; }

    public double SumOpenInterestValue { get; set; }

    public string SymbolInfoId { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtOpenInterestHistory : FuturesOpenInterestHistory
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinOpenInterestHistory : FuturesOpenInterestHistory
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}
