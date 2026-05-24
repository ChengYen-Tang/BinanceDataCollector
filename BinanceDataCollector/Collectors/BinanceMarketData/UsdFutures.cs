namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class UsdFutures : BaseMarketData
{
    private const string AggTradesDataType = "aggTrades";

    protected override string MarketPathSegment => "UsdFutures";
    protected override string MarketDataRemotePathSegment => "futures/um";

    public override Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(string symbol, DateTime startTime, string tempSymbolPath, CancellationToken ct = default)
        => DownloadAsync(AggTradesDataType, symbol, startTime, tempSymbolPath, ct);
}
