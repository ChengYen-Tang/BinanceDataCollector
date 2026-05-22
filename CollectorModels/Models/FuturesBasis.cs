using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

public abstract class FuturesBasis
{
    [Key]
    public string Id { get; set; }

    public DateTime Timestamp { get; set; }

    public double FuturesPrice { get; set; }

    public double IndexPrice { get; set; }

    public double BasisValue { get; set; }

    public double BasisRate { get; set; }

    public double? AnnualizedBasisRate { get; set; }

    public string SymbolInfoId { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtBasis : FuturesBasis
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinBasis : FuturesBasis
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}
