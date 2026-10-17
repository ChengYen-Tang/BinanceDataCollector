namespace BinanceDataCollector;

// Models use epoch milliseconds; only the persisted primary key uses these units.
internal sealed record DuckDbTimeKey(string LogicalColumn, string PhysicalColumn, long Unit, long Offset)
{
    public static DuckDbTimeKey? ForColumn(string dbPath, string column)
    {
        string? marketFolder = Path.GetDirectoryName(Path.GetFullPath(dbPath));
        string? series = Path.GetFileName(marketFolder is null ? null : Path.GetDirectoryName(marketFolder));
        return (series, column) switch
        {
            ("Kline" or "PremiumIndexKline" or "IndexPriceKline" or "MarkPriceKline", "CloseTime")
                => new(column, "CloseTimeMinute", 60_000, 59_999),
            ("OpenInterestHistory" or "Basis" or "GlobalLongShortAccountRatio"
                or "TopLongShortAccountRatio" or "TopLongShortPositionRatio" or "TakerLongShortRatio", "Timestamp")
                => new(column, "Timestamp5Minute", 300_000, 0),
            _ => null
        };
    }

    public long Encode(long milliseconds)
    {
        Int128 delta = (Int128)milliseconds - Offset;
        if (delta % Unit != 0)
            throw new InvalidDataException($"{LogicalColumn}={milliseconds} is not aligned to {Unit} ms with offset {Offset}; refusing lossy time-key encoding.");
        return checked((long)(delta / Unit));
    }

    public long Decode(long key) => checked((long)((Int128)key * Unit + Offset));

    // x < threshold is equivalent to encoded(x) < ceil((threshold-offset)/unit),
    // including thresholds between grid points and times before the Unix epoch.
    public long ExclusiveUpperBound(long threshold)
    {
        Int128 delta = (Int128)threshold - Offset;
        Int128 quotient = delta / Unit;
        return checked((long)(quotient + (delta % Unit > 0 ? 1 : 0)));
    }

    public string EncodeSql(string column) => $"((({column}::HUGEINT - {Offset}) // {Unit})::BIGINT)";

    public string DecodeSql(string column) => $"(({column}::HUGEINT * {Unit} + {Offset})::BIGINT)";
}
