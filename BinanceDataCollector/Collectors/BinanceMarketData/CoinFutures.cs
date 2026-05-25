namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class CoinFutures : BaseMarketData
{
    protected override string MarketPathSegment => "CoinFutures";
    protected override string MarketDataRemotePathSegment => "futures/cm";

    public override Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(string symbol, DateTime startTime, string tempSymbolPath, CancellationToken ct = default)
        => DownloadAsync(AggTradesDataType, symbol, startTime, tempSymbolPath, ct);
}
