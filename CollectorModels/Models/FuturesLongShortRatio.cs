using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;

namespace CollectorModels.Models;

public abstract class FuturesLongShortRatio
{
    [Key]
    public string Id { get; set; }

    public DateTime Timestamp { get; set; }

    public double LongShortRatio { get; set; }

    public double LongAccount { get; set; }

    public double ShortAccount { get; set; }

    public string SymbolInfoId { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtTopLongShortPositionRatio : FuturesLongShortRatio
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinTopLongShortPositionRatio : FuturesLongShortRatio
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtTopLongShortAccountRatio : FuturesLongShortRatio
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinTopLongShortAccountRatio : FuturesLongShortRatio
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtGlobalLongShortAccountRatio : FuturesLongShortRatio
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinGlobalLongShortAccountRatio : FuturesLongShortRatio
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}

public abstract class FuturesTakerLongShortRatio
{
    [Key]
    public string Id { get; set; }

    public DateTime Timestamp { get; set; }

    public double? BuySellRatio { get; set; }

    public double BuyVolume { get; set; }

    public double SellVolume { get; set; }

    public double? BuyVolumeValue { get; set; }

    public double? SellVolumeValue { get; set; }

    public string SymbolInfoId { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesUsdtTakerLongShortRatio : FuturesTakerLongShortRatio
{
    public BinanceFuturesUsdtSymbolInfo SymbolInfo { get; set; }
}

[Index(nameof(Timestamp))]
[Index(nameof(SymbolInfoId))]
[Index(nameof(Timestamp), nameof(SymbolInfoId))]
public class FuturesCoinTakerLongShortRatio : FuturesTakerLongShortRatio
{
    public BinanceFuturesCoinSymbolInfo SymbolInfo { get; set; }
}
