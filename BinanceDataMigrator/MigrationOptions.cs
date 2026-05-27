public sealed class MigrationOptions
{
    public const string SectionName = "Migration";

    public string StorageRootPath { get; set; } = "DataStorage";

    public int BatchSize { get; set; } = 10000;

    public int MaxParallelSymbols { get; set; } = 4;

    public MarketOptions Markets { get; set; } = new();

    public sealed class MarketOptions
    {
        public bool Spot { get; set; } = true;

        public bool CoinFutures { get; set; } = true;

        public bool UsdFutures { get; set; } = true;
    }
}
