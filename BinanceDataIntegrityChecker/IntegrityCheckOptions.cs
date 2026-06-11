namespace BinanceDataIntegrityChecker;

public sealed class IntegrityCheckOptions
{
    public const string SectionName = "IntegrityCheck";

    public string RootFolder { get; set; } = "DataStorage";

    public int MaxMissingDaysToLog { get; set; } = 20;

    public int MaxDegreeOfParallelism { get; set; } = Math.Max(1, Environment.ProcessorCount);

    public DataTypeOptions DataTypes { get; set; } = new();

    public sealed class DataTypeOptions
    {
        public bool AggTrades { get; set; } = true;

        public bool BookDepth { get; set; } = true;
    }
}
