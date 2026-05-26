internal sealed record DatasetIntegrityPolicy<TRecord>(
    string DataType,
    Func<TRecord, long> KeySelector,
    TimeSpan ExpectedInterval,
    TimeSpan JitterTolerance,
    int OverlapIntervals,
    TimeSpan? ApiLookbackLimit = null);

internal sealed record TimeSeriesGap(
    DateTime PreviousTimestamp,
    DateTime NextTimestamp,
    DateTime RepairStartTime,
    DateTime RepairEndTime,
    int EstimatedMissingCount);
