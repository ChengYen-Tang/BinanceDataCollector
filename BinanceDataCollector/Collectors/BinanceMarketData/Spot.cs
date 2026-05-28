namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class Spot : BaseMarketData
{
    protected override string MarketPathSegment => "Spot";
    protected override string MarketDataRemotePathSegment => "spot";

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
        => throw new NotSupportedException("Spot market does not support book depth market data.");
}
