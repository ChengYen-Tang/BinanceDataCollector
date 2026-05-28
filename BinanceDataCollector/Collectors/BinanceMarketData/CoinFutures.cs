namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class CoinFutures : BaseMarketData
{
    protected override string MarketPathSegment => "CoinFutures";
    protected override string MarketDataRemotePathSegment => "futures/cm";

    public override Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(
        string symbol,
        DateTime downloadStartTime,
        string tempSymbolPath,
        CancellationToken ct = default)
        => DownloadAsync(AggTradesDataType, symbol, downloadStartTime, tempSymbolPath, ct);

    public override Task<Result<MarketDataDownloadBatch>> DownloadBookDepthAsync(
        string symbol,
        DateTime downloadStartTime,
        string tempSymbolPath,
        CancellationToken ct = default)
        => DownloadAsync(BookDepthDataType, symbol, downloadStartTime, tempSymbolPath, ct);
}
