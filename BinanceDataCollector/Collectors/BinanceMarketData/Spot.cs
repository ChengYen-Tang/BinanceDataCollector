namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class Spot : BaseMarketData
{
    private const string AggTradesDataType = "aggTrades";

    protected override string MarketPathSegment => "Spot";
    protected override string MarketDataRemotePathSegment => "spot";

    public override Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(string symbol, DateTime startTime, string tempSymbolPath, CancellationToken ct = default)
        => DownloadAsync(AggTradesDataType, symbol, startTime, tempSymbolPath, ct);
}
