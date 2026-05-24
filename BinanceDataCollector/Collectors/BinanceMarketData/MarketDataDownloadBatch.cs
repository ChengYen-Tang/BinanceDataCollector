namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal sealed class MarketDataDownloadBatch
{
    public required string MarketPathSegment { get; init; }
    public required string DataType { get; init; }
    public required string Symbol { get; init; }
    public required IReadOnlyList<MarketDataDownloadFile> Files { get; init; }
}

internal sealed class MarketDataDownloadFile
{
    public required string DataType { get; init; }
    public required string Symbol { get; init; }
    public required string Period { get; init; }
    public required string FileName { get; init; }
    public required string RelativeZipPath { get; init; }
    public required string RelativeChecksumPath { get; init; }
    public required string TempZipPath { get; init; }
    public required string TempChecksumPath { get; init; }
}
