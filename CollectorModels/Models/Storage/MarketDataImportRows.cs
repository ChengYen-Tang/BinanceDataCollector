namespace CollectorModels.Models.Storage;

public readonly record struct AggTradeImportRow(
    long ParquetAggTradeId,
    double Price,
    double Quantity,
    long FirstTradeId,
    long LastTradeId,
    long TransactTime,
    bool IsBuyerMaker);

public readonly record struct BookDepthImportRow(
    long SnapshotTime,
    decimal Percentage,
    double Depth,
    double Notional);

public readonly record struct BookDepthRowKey(long SnapshotTime, decimal Percentage);
