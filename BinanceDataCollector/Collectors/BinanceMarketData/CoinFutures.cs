namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class CoinFutures : BaseMarketData
{
    protected override string MarketPathSegment => "CoinFutures";
    protected override string MarketDataRemotePathSegment => "futures/cm";

    public override Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(
        string symbol,
        (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState,
        string tempSymbolPath,
        CancellationToken ct = default)
        => DownloadAsync(AggTradesDataType, symbol, syncState, tempSymbolPath, ct);
}
