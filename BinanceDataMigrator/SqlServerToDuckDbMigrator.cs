using BinanceDataCollector;
using Binance.Net.Enums;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Storage;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Data.Common;

internal sealed class SqlServerToDuckDbMigrator(
    BinanceDbContext dbContext,
    BinanceRepairDownloader repairDownloader,
    IOptions<MigrationOptions> options,
    ILogger<SqlServerToDuckDbMigrator> logger)
{
    private static readonly DatasetIntegrityPolicy<Kline> OneMinuteKlinePolicy =
        new("Kline", item => item.CloseTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), 2);

    private static readonly DatasetIntegrityPolicy<PremiumIndexKline> OneMinutePremiumIndexKlinePolicy =
        new("PremiumIndexKline", item => item.CloseTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), 2);

    private static readonly DatasetIntegrityPolicy<PremiumIndexKline> OneMinuteIndexPriceKlinePolicy =
        new("IndexPriceKline", item => item.CloseTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), 2);

    private static readonly DatasetIntegrityPolicy<PremiumIndexKline> OneMinuteMarkPriceKlinePolicy =
        new("MarkPriceKline", item => item.CloseTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), 2);

    private static readonly DatasetIntegrityPolicy<FundingRate> FundingRatePolicy =
        new("FundingRate", item => item.FundingTime, TimeSpan.FromHours(8), TimeSpan.FromMinutes(10), 1);

    private static readonly DatasetIntegrityPolicy<OpenInterestHistory> FiveMinuteOpenInterestPolicy =
        new("OpenInterestHistory", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private static readonly DatasetIntegrityPolicy<FuturesBasisCsv> FiveMinuteBasisPolicy =
        new("Basis", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private static readonly DatasetIntegrityPolicy<LongShortRatioCsv> FiveMinuteTopLongShortPositionRatioPolicy =
        new("TopLongShortPositionRatio", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private static readonly DatasetIntegrityPolicy<LongShortRatioCsv> FiveMinuteTopLongShortAccountRatioPolicy =
        new("TopLongShortAccountRatio", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private static readonly DatasetIntegrityPolicy<LongShortRatioCsv> FiveMinuteGlobalLongShortAccountRatioPolicy =
        new("GlobalLongShortAccountRatio", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private static readonly DatasetIntegrityPolicy<TakerLongShortRatioCsv> FiveMinuteTakerLongShortRatioPolicy =
        new("TakerLongShortRatio", item => item.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1), 2, TimeSpan.FromDays(29));

    private readonly MigrationOptions options = options.Value;

    public async Task RunAsync(CancellationToken ct = default)
    {
        ValidateOptions();

        Directory.CreateDirectory(options.StorageRootPath);

        if (options.Markets.Spot)
            await MigrateSpotAsync(ct);

        if (options.Markets.CoinFutures)
            await MigrateCoinFuturesAsync(ct);

        if (options.Markets.UsdFutures)
            await MigrateUsdFuturesAsync(ct);
    }

    private void ValidateOptions()
    {
        if (string.IsNullOrWhiteSpace(options.StorageRootPath))
            throw new InvalidOperationException("Migration:StorageRootPath is required.");

        if (options.BatchSize <= 0)
            throw new InvalidOperationException("Migration:BatchSize must be greater than 0.");
    }

    private async Task MigrateSpotAsync(CancellationToken ct)
    {
        List<BinanceSymbolInfo> symbols = await dbContext.BinanceSymbolInfos
            .AsNoTracking()
            .OrderBy(item => item.Name)
            .ToListAsync(ct);

        logger.LogInformation("Migrating Spot market. SymbolCount: {Count}", symbols.Count);

        await DuckDbStorageHelper.ReplaceTableAsync(
            GetSymbolInfoDbPath(),
            "Spot",
            symbols.Select(MapSpotSymbolInfo).ToArray(),
            nameof(SymbolInfoCsv.Name),
            recreateTable: true,
            ct);

        string klineDbPath = GetMarketDbPath("Kline", "Spot");
        foreach (BinanceSymbolInfo symbol in symbols)
        {
            await MigrateShardedTableAsync(
                klineDbPath,
                "SpotBinanceKlines",
                symbol.Name,
                nameof(Kline.CloseTime),
                CreateKline,
                OneMinuteKlinePolicy,
                (repairStart, repairEnd, token) => repairDownloader.GetSpotKlinesAsync(symbol.Name, repairStart, repairEnd, token),
                ct);
        }
    }

    private async Task MigrateCoinFuturesAsync(CancellationToken ct)
    {
        List<BinanceFuturesCoinSymbolInfo> symbols = await dbContext.BinanceFuturesCoinSymbolInfos
            .AsNoTracking()
            .OrderBy(item => item.Name)
            .ToListAsync(ct);

        logger.LogInformation("Migrating CoinFutures market. SymbolCount: {Count}", symbols.Count);

        await DuckDbStorageHelper.ReplaceTableAsync(
            GetSymbolInfoDbPath(),
            "CoinFutures",
            symbols.Select(MapCoinFuturesSymbolInfo).ToArray(),
            nameof(SymbolInfoCsv.Name),
            recreateTable: true,
            ct);

        foreach (BinanceFuturesCoinSymbolInfo symbol in symbols)
            await MigrateCoinFuturesSymbolAsync(symbol, ct);
    }

    private async Task MigrateUsdFuturesAsync(CancellationToken ct)
    {
        List<BinanceFuturesUsdtSymbolInfo> symbols = await dbContext.BinanceFuturesUsdtSymbolInfos
            .AsNoTracking()
            .OrderBy(item => item.Name)
            .ToListAsync(ct);

        logger.LogInformation("Migrating UsdFutures market. SymbolCount: {Count}", symbols.Count);

        await DuckDbStorageHelper.ReplaceTableAsync(
            GetSymbolInfoDbPath(),
            "UsdFutures",
            symbols.Select(MapUsdFuturesSymbolInfo).ToArray(),
            nameof(SymbolInfoCsv.Name),
            recreateTable: true,
            ct);

        foreach (BinanceFuturesUsdtSymbolInfo symbol in symbols)
            await MigrateUsdFuturesSymbolAsync(symbol, ct);
    }

    private async Task MigrateCoinFuturesSymbolAsync(BinanceFuturesCoinSymbolInfo symbol, CancellationToken ct)
    {
        string market = "CoinFutures";
        string pair = GetPair(symbol);
        ContractType contractType = GetContractType(symbol);

        await MigrateShardedTableAsync(
            GetMarketDbPath("Kline", market),
            "FuturesCoinBinanceKlines",
            symbol.Name,
            nameof(Kline.CloseTime),
            CreateKline,
            OneMinuteKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("PremiumIndexKline", market),
            "FuturesCoinBinancePremiumIndexKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinutePremiumIndexKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesPremiumIndexKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("IndexPriceKline", market),
            "FuturesCoinBinanceIndexPriceKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinuteIndexPriceKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesIndexPriceKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("MarkPriceKline", market),
            "FuturesCoinBinanceMarkPriceKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinuteMarkPriceKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesMarkPriceKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("FundingRate", market),
            "FuturesCoinFundingRates",
            symbol.Name,
            nameof(FundingRate.FundingTime),
            CreateFundingRate,
            FundingRatePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesFundingRatesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("OpenInterestHistory", market),
            "FuturesCoinOpenInterestHistories",
            symbol.Name,
            nameof(OpenInterestHistory.Timestamp),
            CreateOpenInterestHistory,
            FiveMinuteOpenInterestPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesOpenInterestHistoriesAsync(pair, contractType, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("Basis", market),
            "FuturesCoinBasisHistories",
            symbol.Name,
            nameof(FuturesBasisCsv.Timestamp),
            CreateFuturesBasis,
            FiveMinuteBasisPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesBasisAsync(pair, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TopLongShortPositionRatio", market),
            "FuturesCoinTopLongShortPositionRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteTopLongShortPositionRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesTopLongShortPositionRatiosAsync(pair, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TopLongShortAccountRatio", market),
            "FuturesCoinTopLongShortAccountRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteTopLongShortAccountRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesTopLongShortAccountRatiosAsync(pair, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("GlobalLongShortAccountRatio", market),
            "FuturesCoinGlobalLongShortAccountRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteGlobalLongShortAccountRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesGlobalLongShortAccountRatiosAsync(pair, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TakerLongShortRatio", market),
            "FuturesCoinTakerLongShortRatios",
            symbol.Name,
            nameof(TakerLongShortRatioCsv.Timestamp),
            CreateTakerLongShortRatio,
            FiveMinuteTakerLongShortRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetCoinFuturesTakerLongShortRatiosAsync(pair, repairStart, repairEnd, token),
            ct);
    }

    private async Task MigrateUsdFuturesSymbolAsync(BinanceFuturesUsdtSymbolInfo symbol, CancellationToken ct)
    {
        string market = "UsdFutures";

        await MigrateShardedTableAsync(
            GetMarketDbPath("Kline", market),
            "FuturesUsdtBinanceKlines",
            symbol.Name,
            nameof(Kline.CloseTime),
            CreateKline,
            OneMinuteKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("PremiumIndexKline", market),
            "FuturesUsdtBinancePremiumIndexKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinutePremiumIndexKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesPremiumIndexKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("IndexPriceKline", market),
            "FuturesUsdtBinanceIndexPriceKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinuteIndexPriceKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesIndexPriceKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("MarkPriceKline", market),
            "FuturesUsdtBinanceMarkPriceKlines",
            symbol.Name,
            nameof(PremiumIndexKline.CloseTime),
            CreatePremiumIndexKline,
            OneMinuteMarkPriceKlinePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesMarkPriceKlinesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("FundingRate", market),
            "FuturesUsdtFundingRates",
            symbol.Name,
            nameof(FundingRate.FundingTime),
            CreateFundingRate,
            FundingRatePolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesFundingRatesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("OpenInterestHistory", market),
            "FuturesUsdtOpenInterestHistories",
            symbol.Name,
            nameof(OpenInterestHistory.Timestamp),
            CreateOpenInterestHistory,
            FiveMinuteOpenInterestPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesOpenInterestHistoriesAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("Basis", market),
            "FuturesUsdtBasisHistories",
            symbol.Name,
            nameof(FuturesBasisCsv.Timestamp),
            CreateFuturesBasis,
            FiveMinuteBasisPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesBasisAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TopLongShortPositionRatio", market),
            "FuturesUsdtTopLongShortPositionRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteTopLongShortPositionRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesTopLongShortPositionRatiosAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TopLongShortAccountRatio", market),
            "FuturesUsdtTopLongShortAccountRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteTopLongShortAccountRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesTopLongShortAccountRatiosAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("GlobalLongShortAccountRatio", market),
            "FuturesUsdtGlobalLongShortAccountRatios",
            symbol.Name,
            nameof(LongShortRatioCsv.Timestamp),
            CreateLongShortRatio,
            FiveMinuteGlobalLongShortAccountRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesGlobalLongShortAccountRatiosAsync(symbol.Name, repairStart, repairEnd, token),
            ct);

        await MigrateShardedTableAsync(
            GetMarketDbPath("TakerLongShortRatio", market),
            "FuturesUsdtTakerLongShortRatios",
            symbol.Name,
            nameof(TakerLongShortRatioCsv.Timestamp),
            CreateTakerLongShortRatio,
            FiveMinuteTakerLongShortRatioPolicy,
            (repairStart, repairEnd, token) => repairDownloader.GetUsdFuturesTakerLongShortRatiosAsync(symbol.Name, repairStart, repairEnd, token),
            ct);
    }

    private async Task MigrateShardedTableAsync<TRecord>(
        string duckDbPath,
        string logicalTableName,
        string symbolName,
        string keyColumn,
        Func<DbDataReader, TRecord> map,
        DatasetIntegrityPolicy<TRecord>? policy,
        Func<DateTime, DateTime, CancellationToken, Task<List<TRecord>>>? repairAsync,
        CancellationToken ct)
        where TRecord : class
    {
        ct.ThrowIfCancellationRequested();

        string physicalTableName = $"{logicalTableName}_{symbolName}";
        if (!await TableExistsAsync(physicalTableName, ct))
        {
            logger.LogDebug("Skip missing SQL table. Table: {Table}", physicalTableName);
            return;
        }

        List<TRecord> sqlRows = await LoadRowsAsync(physicalTableName, keyColumn, map, ct);
        List<TRecord> finalRows = policy is null
            ? sqlRows
            : await ValidateAndRepairRowsAsync(symbolName, sqlRows, policy, repairAsync, ct);

        await DuckDbStorageHelper.ReplaceTableAsync(
            duckDbPath,
            symbolName,
            finalRows,
            keyColumn,
            recreateTable: false,
            ct);

        logger.LogInformation(
            "Migrated table. SqlTable: {SqlTable}, DuckDb: {DuckDb}, Symbol: {Symbol}, SqlRowCount: {SqlRowCount}, FinalRowCount: {FinalRowCount}",
            physicalTableName,
            duckDbPath,
            symbolName,
            sqlRows.Count,
            finalRows.Count);
    }

    private async Task<List<TRecord>> ValidateAndRepairRowsAsync<TRecord>(
        string symbolName,
        IReadOnlyCollection<TRecord> sqlRows,
        DatasetIntegrityPolicy<TRecord> policy,
        Func<DateTime, DateTime, CancellationToken, Task<List<TRecord>>>? repairAsync,
        CancellationToken ct)
        where TRecord : class
    {
        List<TRecord> normalizedRows = DeduplicateAndOrder(sqlRows, policy.KeySelector);
        List<TimeSeriesGap> initialGaps = FindGaps(normalizedRows, policy);
        if (initialGaps.Count == 0)
            return normalizedRows;

        LogDetectedGaps(symbolName, policy.DataType, "GapDetected", initialGaps);

        if (repairAsync is null)
            return normalizedRows;

        List<TRecord> repairedRows = [];
        foreach (TimeSeriesGap gap in MergeGaps(initialGaps))
        {
            ct.ThrowIfCancellationRequested();

            DateTime? apiLookbackFloor = policy.ApiLookbackLimit.HasValue
                ? DateTime.Today.Subtract(policy.ApiLookbackLimit.Value)
                : null;

            if (apiLookbackFloor.HasValue && gap.RepairEndTime < apiLookbackFloor.Value)
            {
                logger.LogWarning(
                    "RepairSkippedByLookbackLimit. DataType: {DataType}, Symbol: {Symbol}, RepairStart: {RepairStart}, RepairEnd: {RepairEnd}, LookbackFloor: {LookbackFloor}, EstimatedMissingCount: {EstimatedMissingCount}",
                    policy.DataType,
                    symbolName,
                    gap.RepairStartTime,
                    gap.RepairEndTime,
                    apiLookbackFloor.Value,
                    gap.EstimatedMissingCount);
                continue;
            }

            DateTime repairStart = gap.RepairStartTime;
            if (apiLookbackFloor.HasValue && repairStart < apiLookbackFloor.Value)
            {
                logger.LogInformation(
                    "RepairClampedByLookbackLimit. DataType: {DataType}, Symbol: {Symbol}, OriginalRepairStart: {OriginalRepairStart}, ClampedRepairStart: {ClampedRepairStart}, RepairEnd: {RepairEnd}",
                    policy.DataType,
                    symbolName,
                    repairStart,
                    apiLookbackFloor.Value,
                    gap.RepairEndTime);
                repairStart = apiLookbackFloor.Value;
            }

            logger.LogWarning(
                "RepairRequested. DataType: {DataType}, Symbol: {Symbol}, RepairStart: {RepairStart}, RepairEnd: {RepairEnd}, EstimatedMissingCount: {EstimatedMissingCount}",
                policy.DataType,
                symbolName,
                repairStart,
                gap.RepairEndTime,
                gap.EstimatedMissingCount);

            try
            {
                List<TRecord> repairBatch = await repairAsync(repairStart, gap.RepairEndTime, ct);
                repairedRows.AddRange(repairBatch);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "RepairAttemptFailed. DataType: {DataType}, Symbol: {Symbol}, RepairStart: {RepairStart}, RepairEnd: {RepairEnd}",
                    policy.DataType,
                    symbolName,
                    repairStart,
                    gap.RepairEndTime);
            }
        }

        List<TRecord> mergedRows = DeduplicateAndOrder(sqlRows.Concat(repairedRows), policy.KeySelector);
        List<TimeSeriesGap> remainingGaps = FindGaps(mergedRows, policy);

        if (remainingGaps.Count == 0)
        {
            logger.LogInformation(
                "RepairRecovered. DataType: {DataType}, Symbol: {Symbol}, AddedRowCount: {AddedRowCount}",
                policy.DataType,
                symbolName,
                repairedRows.Count);
        }
        else
        {
            LogDetectedGaps(symbolName, policy.DataType, "RepairStillMissing", remainingGaps);
        }

        return mergedRows;
    }

    private void LogDetectedGaps(string symbolName, string dataType, string eventName, IReadOnlyCollection<TimeSeriesGap> gaps)
    {
        foreach (TimeSeriesGap gap in gaps)
        {
            logger.LogWarning(
                "{EventName}. DataType: {DataType}, Symbol: {Symbol}, PreviousTimestamp: {PreviousTimestamp}, NextTimestamp: {NextTimestamp}, RepairStart: {RepairStart}, RepairEnd: {RepairEnd}, EstimatedMissingCount: {EstimatedMissingCount}",
                eventName,
                dataType,
                symbolName,
                gap.PreviousTimestamp,
                gap.NextTimestamp,
                gap.RepairStartTime,
                gap.RepairEndTime,
                gap.EstimatedMissingCount);
        }
    }

    private static List<TimeSeriesGap> FindGaps<TRecord>(IReadOnlyList<TRecord> rows, DatasetIntegrityPolicy<TRecord> policy)
    {
        List<TimeSeriesGap> gaps = [];
        if (rows.Count < 2)
            return gaps;

        long expectedStepMs = (long)policy.ExpectedInterval.TotalMilliseconds;
        long toleranceMs = (long)policy.JitterTolerance.TotalMilliseconds;
        long overlapMs = expectedStepMs * policy.OverlapIntervals;

        for (int i = 1; i < rows.Count; i++)
        {
            long previousKey = policy.KeySelector(rows[i - 1]);
            long nextKey = policy.KeySelector(rows[i]);
            long deltaMs = nextKey - previousKey;
            if (deltaMs <= expectedStepMs + toleranceMs)
                continue;

            DateTime previousTime = FromUnixMilliseconds(previousKey);
            DateTime nextTime = FromUnixMilliseconds(nextKey);
            int estimatedMissingCount = Math.Max(1, (int)(deltaMs / expectedStepMs) - 1);
            gaps.Add(new TimeSeriesGap(
                previousTime,
                nextTime,
                previousTime.AddMilliseconds(-overlapMs),
                nextTime.AddMilliseconds(overlapMs),
                estimatedMissingCount));
        }

        return gaps;
    }

    private static List<TimeSeriesGap> MergeGaps(IReadOnlyList<TimeSeriesGap> gaps)
    {
        if (gaps.Count <= 1)
            return [.. gaps];

        List<TimeSeriesGap> ordered = [.. gaps.OrderBy(item => item.RepairStartTime)];
        List<TimeSeriesGap> merged = [];
        TimeSeriesGap current = ordered[0];

        for (int i = 1; i < ordered.Count; i++)
        {
            TimeSeriesGap next = ordered[i];
            if (next.RepairStartTime <= current.RepairEndTime)
            {
                current = current with
                {
                    RepairEndTime = next.RepairEndTime > current.RepairEndTime ? next.RepairEndTime : current.RepairEndTime,
                    EstimatedMissingCount = current.EstimatedMissingCount + next.EstimatedMissingCount
                };
                continue;
            }

            merged.Add(current);
            current = next;
        }

        merged.Add(current);
        return merged;
    }

    private async Task<List<TRecord>> LoadRowsAsync<TRecord>(
        string physicalTableName,
        string keyColumn,
        Func<DbDataReader, TRecord> map,
        CancellationToken ct)
    {
        List<TRecord> rows = [];

        await using SqlConnection connection = new(GetConnectionString());
        await connection.OpenAsync(ct);

        await using SqlCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM [dbo].[{EscapeSqlIdentifier(physicalTableName)}] ORDER BY [{EscapeSqlIdentifier(keyColumn)}];";

        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(map(reader));

        return rows;
    }

    private async Task<bool> TableExistsAsync(string physicalTableName, CancellationToken ct)
    {
        await using SqlConnection connection = new(GetConnectionString());
        await connection.OpenAsync(ct);

        await using SqlCommand command = connection.CreateCommand();
        command.CommandText = "SELECT CASE WHEN OBJECT_ID(@tableName, N'U') IS NULL THEN 0 ELSE 1 END;";
        command.Parameters.AddWithValue("@tableName", $"[dbo].[{physicalTableName}]");

        object? value = await command.ExecuteScalarAsync(ct);
        return value is not null && Convert.ToInt32(value) == 1;
    }

    private string GetConnectionString()
        => dbContext.Database.GetConnectionString()
            ?? throw new InvalidOperationException("Database connection string is not configured.");

    private string GetSymbolInfoDbPath()
        => Path.Combine(options.StorageRootPath, "SymbolInfo.duckdb");

    private string GetMarketDbPath(string dataType, string market)
        => Path.Combine(options.StorageRootPath, dataType, market + ".duckdb");

    private static List<TRecord> DeduplicateAndOrder<TRecord>(IEnumerable<TRecord> rows, Func<TRecord, long> keySelector)
        where TRecord : class
        => [.. rows
            .GroupBy(keySelector)
            .Select(group => group.Last())
            .OrderBy(keySelector)];

    private static SymbolInfoCsv MapSpotSymbolInfo(BinanceSymbolInfo symbol)
        => new()
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            BaseFeePrecision = symbol.BaseFeePrecision,
            AllowTrailingStop = symbol.AllowTrailingStop,
            CancelReplaceAllowed = symbol.CancelReplaceAllowed,
            IcebergAllowed = symbol.IcebergAllowed,
            IsMarginTradingAllowed = symbol.IsMarginTradingAllowed,
            IsSpotTradingAllowed = symbol.IsSpotTradingAllowed,
            OCOAllowed = symbol.OCOAllowed,
            OrderTypes = JoinValues(symbol.OrderTypes),
            QuoteAsset = symbol.QuoteAsset,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            QuoteFeePrecision = symbol.QuoteFeePrecision,
            Permissions = JoinValues(symbol.Permissions),
            QuoteOrderQuantityMarketAllowed = symbol.QuoteOrderQuantityMarketAllowed,
            Status = symbol.Status.ToString()
        };

    private static SymbolInfoCsv MapCoinFuturesSymbolInfo(BinanceFuturesCoinSymbolInfo symbol)
        => new()
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            QuoteAsset = symbol.QuoteAsset,
            ContractType = symbol.ContractType?.ToString(),
            DeliveryDate = ToUnixMilliseconds(symbol.DeliveryDate),
            LiquidationFee = symbol.LiquidationFee,
            ListingDate = ToUnixMilliseconds(symbol.ListingDate),
            MaintMarginPercent = symbol.MaintMarginPercent,
            MarginAsset = symbol.MarginAsset,
            MarketTakeBound = symbol.MarketTakeBound,
            RequiredMarginPercent = symbol.RequiredMarginPercent,
            OrderTypes = JoinValues(symbol.OrderTypes),
            Pair = symbol.Pair,
            PricePrecision = symbol.PricePrecision,
            QuantityPrecision = symbol.QuantityPrecision,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            Status = symbol.Status.ToString(),
            TimeInForce = JoinValues(symbol.TimeInForce),
            TriggerProtect = symbol.TriggerProtect,
            UnderlyingType = symbol.UnderlyingType.ToString(),
            UnderlyingSubType = JoinValues(symbol.UnderlyingSubType)
        };

    private static SymbolInfoCsv MapUsdFuturesSymbolInfo(BinanceFuturesUsdtSymbolInfo symbol)
        => new()
        {
            Name = symbol.Name,
            BaseAsset = symbol.BaseAsset,
            BaseAssetPrecision = symbol.BaseAssetPrecision,
            QuoteAsset = symbol.QuoteAsset,
            ContractType = symbol.ContractType?.ToString(),
            DeliveryDate = ToUnixMilliseconds(symbol.DeliveryDate),
            LiquidationFee = symbol.LiquidationFee,
            ListingDate = ToUnixMilliseconds(symbol.ListingDate),
            MaintMarginPercent = symbol.MaintMarginPercent,
            MarginAsset = symbol.MarginAsset,
            MarketTakeBound = symbol.MarketTakeBound,
            RequiredMarginPercent = symbol.RequiredMarginPercent,
            OrderTypes = JoinValues(symbol.OrderTypes),
            Pair = symbol.Pair,
            PricePrecision = symbol.PricePrecision,
            QuantityPrecision = symbol.QuantityPrecision,
            QuoteAssetPrecision = symbol.QuoteAssetPrecision,
            Status = symbol.Status.ToString(),
            TimeInForce = JoinValues(symbol.TimeInForce),
            TriggerProtect = symbol.TriggerProtect,
            UnderlyingType = symbol.UnderlyingType.ToString(),
            UnderlyingSubType = JoinValues(symbol.UnderlyingSubType)
        };

    private static Kline CreateKline(DbDataReader reader)
        => new()
        {
            OpenTime = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("OpenTime"))),
            OpenPrice = reader.GetDouble(reader.GetOrdinal("OpenPrice")),
            HighPrice = reader.GetDouble(reader.GetOrdinal("HighPrice")),
            LowPrice = reader.GetDouble(reader.GetOrdinal("LowPrice")),
            ClosePrice = reader.GetDouble(reader.GetOrdinal("ClosePrice")),
            Volume = reader.GetDouble(reader.GetOrdinal("Volume")),
            CloseTime = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("CloseTime"))),
            QuoteVolume = reader.GetDouble(reader.GetOrdinal("QuoteVolume")),
            TradeCount = reader.GetInt32(reader.GetOrdinal("TradeCount")),
            TakerBuyBaseVolume = reader.GetDouble(reader.GetOrdinal("TakerBuyBaseVolume")),
            TakerBuyQuoteVolume = reader.GetDouble(reader.GetOrdinal("TakerBuyQuoteVolume"))
        };

    private static PremiumIndexKline CreatePremiumIndexKline(DbDataReader reader)
        => new()
        {
            OpenTime = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("OpenTime"))),
            OpenPrice = reader.GetDouble(reader.GetOrdinal("OpenPrice")),
            HighPrice = reader.GetDouble(reader.GetOrdinal("HighPrice")),
            LowPrice = reader.GetDouble(reader.GetOrdinal("LowPrice")),
            ClosePrice = reader.GetDouble(reader.GetOrdinal("ClosePrice")),
            CloseTime = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("CloseTime")))
        };

    private static FundingRate CreateFundingRate(DbDataReader reader)
        => new()
        {
            FundingTime = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("FundingTime"))),
            Rate = reader.GetDouble(reader.GetOrdinal("FundingRate")),
            MarkPrice = reader.IsDBNull(reader.GetOrdinal("MarkPrice")) ? null : reader.GetDouble(reader.GetOrdinal("MarkPrice"))
        };

    private static OpenInterestHistory CreateOpenInterestHistory(DbDataReader reader)
        => new()
        {
            Timestamp = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("Timestamp"))),
            SumOpenInterest = reader.GetDouble(reader.GetOrdinal("SumOpenInterest")),
            SumOpenInterestValue = reader.GetDouble(reader.GetOrdinal("SumOpenInterestValue"))
        };

    private static LongShortRatioCsv CreateLongShortRatio(DbDataReader reader)
        => new()
        {
            Timestamp = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("Timestamp"))),
            LongShortRatio = reader.GetDouble(reader.GetOrdinal("LongShortRatio")),
            LongAccount = reader.GetDouble(reader.GetOrdinal("LongAccount")),
            ShortAccount = reader.GetDouble(reader.GetOrdinal("ShortAccount"))
        };

    private static TakerLongShortRatioCsv CreateTakerLongShortRatio(DbDataReader reader)
        => new()
        {
            Timestamp = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("Timestamp"))),
            BuySellRatio = reader.IsDBNull(reader.GetOrdinal("BuySellRatio")) ? null : reader.GetDouble(reader.GetOrdinal("BuySellRatio")),
            BuyVolume = reader.GetDouble(reader.GetOrdinal("BuyVolume")),
            SellVolume = reader.GetDouble(reader.GetOrdinal("SellVolume")),
            BuyVolumeValue = reader.IsDBNull(reader.GetOrdinal("BuyVolumeValue")) ? null : reader.GetDouble(reader.GetOrdinal("BuyVolumeValue")),
            SellVolumeValue = reader.IsDBNull(reader.GetOrdinal("SellVolumeValue")) ? null : reader.GetDouble(reader.GetOrdinal("SellVolumeValue"))
        };

    private static FuturesBasisCsv CreateFuturesBasis(DbDataReader reader)
        => new()
        {
            Timestamp = ToUnixMilliseconds(reader.GetDateTime(reader.GetOrdinal("Timestamp"))),
            FuturesPrice = reader.GetDouble(reader.GetOrdinal("FuturesPrice")),
            IndexPrice = reader.GetDouble(reader.GetOrdinal("IndexPrice")),
            BasisValue = reader.GetDouble(reader.GetOrdinal("BasisValue")),
            BasisRate = reader.GetDouble(reader.GetOrdinal("BasisRate")),
            AnnualizedBasisRate = reader.IsDBNull(reader.GetOrdinal("AnnualizedBasisRate")) ? null : reader.GetDouble(reader.GetOrdinal("AnnualizedBasisRate"))
        };

    private static string GetPair(BinanceFuturesSymbolInfo symbol)
        => string.IsNullOrWhiteSpace(symbol.Pair) ? symbol.Name : symbol.Pair;

    private static ContractType GetContractType(BinanceFuturesSymbolInfo symbol)
        => symbol.ContractType ?? ContractType.Perpetual;

    private static DateTime FromUnixMilliseconds(long value)
        => DateTimeOffset.FromUnixTimeMilliseconds(value).UtcDateTime;

    private static long ToUnixMilliseconds(DateTime value)
        => new DateTimeOffset(value).ToUnixTimeMilliseconds();

    private static string JoinValues<T>(IEnumerable<T>? values)
        => values is null ? string.Empty : string.Join('|', values);

    private static string EscapeSqlIdentifier(string identifier)
        => identifier.Replace("]", "]]", StringComparison.Ordinal);
}
