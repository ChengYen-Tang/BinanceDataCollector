using BinanceDataCollector.Collectors.BinanceMarketData;
using BinanceDataCollector.WorkItems;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using Parquet;
using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;

namespace BinanceDataCollector.StorageControllers;

internal sealed record SymbolRows<T>(string SymbolName, IList<T> Rows);

internal abstract class StorageController<T>
    where T : class
{
    protected readonly IServiceProvider serviceProvider;
    protected readonly ILogger logger;
    protected readonly DateTime yearsReserved;
    protected static string DataPath = DuckDbStorageArchiveHelper.StorageRootPath;
    protected static string MarketDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "BinanceMarketData");
    protected static string MarketDataTempPath = Path.Combine(DuckDbStorageArchiveHelper.TmpPath, "BinanceMarketData");
    protected static string RootKlinePath = Path.Combine(DataPath, "Kline");
    protected static string RootPremiumIndexKlinePath = Path.Combine(DataPath, "PremiumIndexKline");
    protected static string RootIndexPriceKlinePath = Path.Combine(DataPath, "IndexPriceKline");
    protected static string RootMarkPriceKlinePath = Path.Combine(DataPath, "MarkPriceKline");
    protected static string RootFundingRatePath = Path.Combine(DataPath, "FundingRate");
    protected static string RootOpenInterestPath = Path.Combine(DataPath, "OpenInterestHistory");
    protected static string RootTopLongShortPositionRatioPath = Path.Combine(DataPath, "TopLongShortPositionRatio");
    protected static string RootTopLongShortAccountRatioPath = Path.Combine(DataPath, "TopLongShortAccountRatio");
    protected static string RootGlobalLongShortAccountRatioPath = Path.Combine(DataPath, "GlobalLongShortAccountRatio");
    protected static string RootTakerLongShortRatioPath = Path.Combine(DataPath, "TakerLongShortRatio");
    protected static string RootBasisPath = Path.Combine(DataPath, "Basis");
    protected static string RootSymbolInfoPath = DataPath;
    protected abstract string MarketPathSegment { get; }
    protected abstract string SymbolInfoPath { get; }
    protected abstract string KlinePath { get; }
    protected abstract string PremiumIndexKlinePath { get; }
    protected abstract string IndexPriceKlinePath { get; }
    protected abstract string MarkPriceKlinePath { get; }
    protected abstract string FundingRatePath { get; }
    protected abstract string OpenInterestPath { get; }
    protected abstract string TopLongShortPositionRatioPath { get; }
    protected abstract string TopLongShortAccountRatioPath { get; }
    protected abstract string GlobalLongShortAccountRatioPath { get; }
    protected abstract string TakerLongShortRatioPath { get; }
    protected abstract string BasisPath { get; }
    protected abstract bool IsFutures { get; }

    public StorageController(IServiceProvider serviceProvider, ILogger logger)
        => (this.serviceProvider, this.logger, yearsReserved) = (serviceProvider, logger, DateTime.Today.AddYears(-3));

    public async Task<Result<List<T>>> UpdateMocketAsync(CancellationToken ct = default)
    {
        Result<List<T>> result = await GetMarketAsync(ct);
        if (result.IsFailed)
            return Result.Fail(result.Errors);

        try
        {
            string[] currentSymbols = [.. result.Value.Select(GetSymbolName)];
            List<string> existingSymbols = await GetExistingSymbolNamesAsync(ct);
            string[] delistedSymbols = [.. existingSymbols.Except(currentSymbols)];

            logger.LogInformation("Start syncing {SymbolType}. MarketCount: {MarketCount}, DelistedCount: {DelistedCount}", typeof(T).Name, result.Value.Count, delistedSymbols.Length);

            Stopwatch upsertStopwatch = Stopwatch.StartNew();
            await DuckDbStorageHelper.ReplaceTableAsync(
                SymbolInfoPath,
                MarketPathSegment,
                result.Value.Cast<SymbolInfoCsv>().ToArray(),
                nameof(SymbolInfoCsv.Name),
                true,
                ct);
            upsertStopwatch.Stop();
            logger.LogInformation("Finish upserting {SymbolType}. Cost: {ElapsedMs}ms", typeof(T).Name, upsertStopwatch.ElapsedMilliseconds);

            if (delistedSymbols.Length > 0)
            {
                Stopwatch deleteStopwatch = Stopwatch.StartNew();
                await DeleteDelistedSymbolsAsync(delistedSymbols, ct);
                deleteStopwatch.Stop();
                logger.LogDebug("Finish deleting delisted {SymbolType}. Count: {DelistedCount}, Cost: {ElapsedMs}ms", typeof(T).Name, delistedSymbols.Length, deleteStopwatch.ElapsedMilliseconds);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Sync {SymbolType} failed", typeof(T).Name);
            return Result.Fail(ex.Message);
        }
        return Result.Ok(result.Value);
    }

    public async Task<AsyncWorkItem<SymbolRows<Kline>>> UpdateKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(Kline), symbol, interval, startTime);
        Result<List<Kline>> result = await GetKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(Kline), symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(Kline), symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, new SymbolRows<Kline>(symbolName, []), ct);
        }

        return new(InsertKlinesAsync, new SymbolRows<Kline>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<PremiumIndexKline>>> UpdateIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        Result<List<PremiumIndexKline>> result = await GetIndexPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(PremiumIndexKline), symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertIndexPriceKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, []), ct);
        }

        return new(InsertIndexPriceKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<PremiumIndexKline>>> UpdateMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        Result<List<PremiumIndexKline>> result = await GetMarkPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(PremiumIndexKline), symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertMarkPriceKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, []), ct);
        }

        return new(InsertMarkPriceKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<PremiumIndexKline>>> UpdatePremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        Result<List<PremiumIndexKline>> result = await GetPremiumIndexKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(PremiumIndexKline), symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(PremiumIndexKline), symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertPremiumIndexKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, []), ct);
        }

        return new(InsertPremiumIndexKlinesAsync, new SymbolRows<PremiumIndexKline>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<FundingRate>>> UpdateFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(FundingRate), symbol, startTime);
        Result<List<FundingRate>> result = await GetFundingRatesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(FundingRate), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(FundingRate), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<FundingRate>>(InsertFundingRatesAsync, new SymbolRows<FundingRate>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<FundingRate>>(InsertFundingRatesAsync, new SymbolRows<FundingRate>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<OpenInterestHistory>>> UpdateOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(OpenInterestHistory), symbol, startTime);
        Result<List<OpenInterestHistory>> result = await GetOpenInterestHistoriesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(OpenInterestHistory), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(OpenInterestHistory), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<OpenInterestHistory>>(InsertOpenInterestHistoriesAsync, new SymbolRows<OpenInterestHistory>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<OpenInterestHistory>>(InsertOpenInterestHistoriesAsync, new SymbolRows<OpenInterestHistory>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<LongShortRatioCsv>>> UpdateTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        Result<List<LongShortRatioCsv>> result = await GetTopLongShortPositionRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(LongShortRatioCsv), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortPositionRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortPositionRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<LongShortRatioCsv>>> UpdateTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        Result<List<LongShortRatioCsv>> result = await GetTopLongShortAccountRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(LongShortRatioCsv), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortAccountRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortAccountRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<LongShortRatioCsv>>> UpdateGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        Result<List<LongShortRatioCsv>> result = await GetGlobalLongShortAccountRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(LongShortRatioCsv), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(LongShortRatioCsv), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertGlobalLongShortAccountRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertGlobalLongShortAccountRatiosAsync, new SymbolRows<LongShortRatioCsv>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<TakerLongShortRatioCsv>>> UpdateTakerLongShortRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(TakerLongShortRatioCsv), symbol, startTime);
        Result<List<TakerLongShortRatioCsv>> result = await GetTakerLongShortRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(TakerLongShortRatioCsv), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(TakerLongShortRatioCsv), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<TakerLongShortRatioCsv>>(InsertTakerLongShortRatiosAsync, new SymbolRows<TakerLongShortRatioCsv>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<TakerLongShortRatioCsv>>(InsertTakerLongShortRatiosAsync, new SymbolRows<TakerLongShortRatioCsv>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<SymbolRows<FuturesBasisCsv>>> UpdateBasisAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(FuturesBasisCsv), symbol, startTime);
        Result<List<FuturesBasisCsv>> result = await GetBasisAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", nameof(FuturesBasisCsv), symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(nameof(FuturesBasisCsv), symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<SymbolRows<FuturesBasisCsv>>(InsertBasisAsync, new SymbolRows<FuturesBasisCsv>(symbolName, []), ct);
        }

        return new AsyncWorkItem<SymbolRows<FuturesBasisCsv>>(InsertBasisAsync, new SymbolRows<FuturesBasisCsv>(symbolName, result.Value), ct);
    }

    public async Task<AsyncWorkItem<MarketDataDownloadBatch?>> UpdateAggTradesAsync(
        T symbol,
        (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState,
        CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}, MonthlyLatestPeriodStart: {MonthlyLatestPeriodStart}, DailyLatestPeriodStart: {DailyLatestPeriodStart}",
            BaseMarketData.AggTradesDataType, symbol, syncState.DownloadStartTime, syncState.MonthlyLatestPeriodStart, syncState.DailyLatestPeriodStart);
        Result<MarketDataDownloadBatch> result = await GetAggTradesAsync(symbol, syncState, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}, MonthlyLatestPeriodStart: {MonthlyLatestPeriodStart}, DailyLatestPeriodStart: {DailyLatestPeriodStart}",
            BaseMarketData.AggTradesDataType, symbol, syncState.DownloadStartTime, syncState.MonthlyLatestPeriodStart, syncState.DailyLatestPeriodStart);
        if (result.IsFailed)
        {
            LogSyncFailure(BaseMarketData.AggTradesDataType, symbol, result.Errors[0].Message, startTime: syncState.DownloadStartTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertAggTradesAsync, null, ct);
        }

        return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertAggTradesAsync, result.Value, ct);
    }

    protected void LogSyncFailure(string dataType, T symbol, string message, KlineInterval? interval = null, DateTime? startTime = null)
        => logger.LogError("Sync failed. DataType: {DataType}, Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}, Message: {Message}",
            dataType, symbol, interval, startTime, message);

    protected void LogParquetExportFailure(string dataType, string symbol, string message)
        => logger.LogError("Parquet export failed. DataType: {DataType}, Symbol: {Symbol}, Message: {Message}",
            dataType, symbol, message);

    protected virtual ParquetExportOptions GetMetadataParquetExportOptions()
        => new()
        {
            CompressionMethod = CompressionMethod.Snappy,
            RowGroupSize = 1_024,
        };

    protected virtual ParquetExportOptions GetLargeTimeSeriesParquetExportOptions()
        => new()
        {
            CompressionMethod = CompressionMethod.Zstd,
            RowGroupSize = 100_000,
        };

    protected virtual ParquetExportOptions GetMediumTimeSeriesParquetExportOptions()
        => new()
        {
            CompressionMethod = CompressionMethod.Zstd,
            RowGroupSize = 20_000,
        };

    protected TEntity[] DeduplicateByKey<TEntity, TKey>(IList<TEntity> entities, Func<TEntity, TKey> getKey)
        where TKey : notnull
    {
        if (entities.Count <= 1)
            return [.. entities];

        Dictionary<TKey, TEntity> uniqueEntities = [];
        foreach (TEntity entity in entities)
            uniqueEntities[getKey(entity)] = entity;

        if (uniqueEntities.Count != entities.Count)
            logger.LogWarning("Deduplicated insert batch. DataType: {DataType}, OriginalCount: {OriginalCount}, UniqueCount: {UniqueCount}",
                typeof(TEntity).Name, entities.Count, uniqueEntities.Count);

        return [.. uniqueEntities.Values];
    }

    public async Task ExportToParquetAsync(CancellationToken ct = default)
    {
        logger.LogInformation("Start parquet export. StorageController: {StorageController}", GetType().Name);
        Result<string[]> symbolNamesResult = await GetAllSymbolNamesAsync(ct);
        if (symbolNamesResult.IsFailed)
        {
            logger.LogError(symbolNamesResult.Errors[0].Message);
            return;
        }

        logger.LogInformation("Parquet export symbols loaded. StorageController: {StorageController}, SymbolCount: {SymbolCount}", GetType().Name, symbolNamesResult.Value.Length);

        if (Directory.Exists(SymbolInfoPath))
            Directory.Delete(SymbolInfoPath, true);
        Directory.CreateDirectory(SymbolInfoPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, "SymbolInfo");

        Result<SymbolInfoCsv[]> symbolInfosResult = await GetCsvSymbolInfosAsync(ct);
        if (symbolInfosResult.IsFailed)
            logger.LogError(symbolInfosResult.Errors[0].Message);
        else
        {
            string symbolInfoPath = Path.Combine(SymbolInfoPath, "symbols.parquet");
            await ParquetExportHelper.ExportAsync(symbolInfoPath, symbolInfosResult.Value, GetMetadataParquetExportOptions(), ct);
        }

        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, "SymbolInfo");

        if (Directory.Exists(KlinePath))
            Directory.Delete(KlinePath, true);
        Directory.CreateDirectory(KlinePath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(Kline));


        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<Kline[]> klinesResult = await GetCsvKlinesAsync(symbol, ct);
            if (klinesResult.IsFailed)
            {
                LogParquetExportFailure(nameof(Kline), symbol, klinesResult.Errors[0].Message);
                return;
            }

            string path = Path.Combine(KlinePath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(path, klinesResult.Value, GetLargeTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(Kline));

        if (!IsFutures)
        {
            logger.LogInformation("Finish parquet export. StorageController: {StorageController}", GetType().Name);
            return;
        }

        if (Directory.Exists(PremiumIndexKlinePath))
            Directory.Delete(PremiumIndexKlinePath, true);
        Directory.CreateDirectory(PremiumIndexKlinePath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(PremiumIndexKline));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> premiumIndexKlinesResult = await GetCsvPremiumIndexKlinesAsync(symbol, ct);
            if (premiumIndexKlinesResult.IsFailed)
            {
                LogParquetExportFailure(nameof(PremiumIndexKline), symbol, premiumIndexKlinesResult.Errors[0].Message);
                return;
            }

            string premiumIndexPath = Path.Combine(PremiumIndexKlinePath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(premiumIndexPath, premiumIndexKlinesResult.Value, GetLargeTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(PremiumIndexKline));

        if (Directory.Exists(IndexPriceKlinePath))
            Directory.Delete(IndexPriceKlinePath, true);
        Directory.CreateDirectory(IndexPriceKlinePath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: IndexPriceKline", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> indexPriceKlinesResult = await GetCsvIndexPriceKlinesAsync(symbol, ct);
            if (indexPriceKlinesResult.IsFailed)
            {
                LogParquetExportFailure("IndexPriceKline", symbol, indexPriceKlinesResult.Errors[0].Message);
                return;
            }

            string indexPricePath = Path.Combine(IndexPriceKlinePath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(indexPricePath, indexPriceKlinesResult.Value, GetLargeTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: IndexPriceKline", GetType().Name);

        if (Directory.Exists(MarkPriceKlinePath))
            Directory.Delete(MarkPriceKlinePath, true);
        Directory.CreateDirectory(MarkPriceKlinePath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: MarkPriceKline", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> markPriceKlinesResult = await GetCsvMarkPriceKlinesAsync(symbol, ct);
            if (markPriceKlinesResult.IsFailed)
            {
                LogParquetExportFailure("MarkPriceKline", symbol, markPriceKlinesResult.Errors[0].Message);
                return;
            }

            string markPricePath = Path.Combine(MarkPriceKlinePath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(markPricePath, markPriceKlinesResult.Value, GetLargeTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: MarkPriceKline", GetType().Name);

        if (Directory.Exists(FundingRatePath))
            Directory.Delete(FundingRatePath, true);
        Directory.CreateDirectory(FundingRatePath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(FundingRate));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<FundingRate[]> fundingRateResult = await GetCsvFundingRatesAsync(symbol, ct);
            if (fundingRateResult.IsFailed)
            {
                LogParquetExportFailure(nameof(FundingRate), symbol, fundingRateResult.Errors[0].Message);
                return;
            }

            string fundingRatePath = Path.Combine(FundingRatePath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(fundingRatePath, fundingRateResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(FundingRate));

        if (Directory.Exists(OpenInterestPath))
            Directory.Delete(OpenInterestPath, true);
        Directory.CreateDirectory(OpenInterestPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(OpenInterestHistory));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<OpenInterestHistory[]> openInterestResult = await GetCsvOpenInterestHistoriesAsync(symbol, ct);
            if (openInterestResult.IsFailed)
            {
                LogParquetExportFailure(nameof(OpenInterestHistory), symbol, openInterestResult.Errors[0].Message);
                return;
            }

            string openInterestPath = Path.Combine(OpenInterestPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(openInterestPath, openInterestResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(OpenInterestHistory));

        if (Directory.Exists(BasisPath))
            Directory.Delete(BasisPath, true);
        Directory.CreateDirectory(BasisPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: Basis", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<FuturesBasisCsv[]> basisResult = await GetCsvBasisAsync(symbol, ct);
            if (basisResult.IsFailed)
            {
                LogParquetExportFailure("Basis", symbol, basisResult.Errors[0].Message);
                return;
            }

            string basisPath = Path.Combine(BasisPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(basisPath, basisResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: Basis", GetType().Name);

        if (Directory.Exists(TopLongShortPositionRatioPath))
            Directory.Delete(TopLongShortPositionRatioPath, true);
        Directory.CreateDirectory(TopLongShortPositionRatioPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: TopLongShortPositionRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvTopLongShortPositionRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogParquetExportFailure("TopLongShortPositionRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TopLongShortPositionRatioPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(ratioPath, ratioResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: TopLongShortPositionRatio", GetType().Name);

        if (Directory.Exists(TopLongShortAccountRatioPath))
            Directory.Delete(TopLongShortAccountRatioPath, true);
        Directory.CreateDirectory(TopLongShortAccountRatioPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: TopLongShortAccountRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvTopLongShortAccountRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogParquetExportFailure("TopLongShortAccountRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TopLongShortAccountRatioPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(ratioPath, ratioResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: TopLongShortAccountRatio", GetType().Name);

        if (Directory.Exists(GlobalLongShortAccountRatioPath))
            Directory.Delete(GlobalLongShortAccountRatioPath, true);
        Directory.CreateDirectory(GlobalLongShortAccountRatioPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: GlobalLongShortAccountRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvGlobalLongShortAccountRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogParquetExportFailure("GlobalLongShortAccountRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(GlobalLongShortAccountRatioPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(ratioPath, ratioResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: GlobalLongShortAccountRatio", GetType().Name);

        if (Directory.Exists(TakerLongShortRatioPath))
            Directory.Delete(TakerLongShortRatioPath, true);
        Directory.CreateDirectory(TakerLongShortRatioPath);
        logger.LogInformation("Start parquet export section. StorageController: {StorageController}, DataType: TakerLongShortRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<TakerLongShortRatioCsv[]> ratioResult = await GetCsvTakerLongShortRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogParquetExportFailure("TakerLongShortRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TakerLongShortRatioPath, $"{symbol}.parquet");
            await ParquetExportHelper.ExportAsync(ratioPath, ratioResult.Value, GetMediumTimeSeriesParquetExportOptions(), ct);
        });
        logger.LogInformation("Finish parquet export section. StorageController: {StorageController}, DataType: TakerLongShortRatio", GetType().Name);
        logger.LogInformation("Finish parquet export. StorageController: {StorageController}", GetType().Name);
    }

    protected async Task InsertKlinesAsync(SymbolRows<Kline> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        Kline[] uniqueKlines = DeduplicateByKey(batch.Rows, item => item.CloseTime);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(Kline), uniqueKlines.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(KlinePath, batch.SymbolName, uniqueKlines, nameof(Kline.CloseTime), ct);
            logger.LogDebug($"Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertPremiumIndexKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        PremiumIndexKline[] uniqueKlines = DeduplicateByKey(batch.Rows, item => item.CloseTime);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(PremiumIndexKline), uniqueKlines.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(PremiumIndexKlinePath, batch.SymbolName, uniqueKlines, nameof(PremiumIndexKline.CloseTime), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertIndexPriceKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        PremiumIndexKline[] uniqueKlines = DeduplicateByKey(batch.Rows, item => item.CloseTime);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(PremiumIndexKline), uniqueKlines.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(IndexPriceKlinePath, batch.SymbolName, uniqueKlines, nameof(PremiumIndexKline.CloseTime), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertMarkPriceKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        PremiumIndexKline[] uniqueKlines = DeduplicateByKey(batch.Rows, item => item.CloseTime);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(PremiumIndexKline), uniqueKlines.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(MarkPriceKlinePath, batch.SymbolName, uniqueKlines, nameof(PremiumIndexKline.CloseTime), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected Task InsertTopLongShortPositionRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(TopLongShortPositionRatioPath, batch, ct);

    protected Task InsertTopLongShortAccountRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(TopLongShortAccountRatioPath, batch, ct);

    protected Task InsertGlobalLongShortAccountRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(GlobalLongShortAccountRatioPath, batch, ct);

    private async Task InsertLongShortRatiosAsync(string dbPath, SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        LongShortRatioCsv[] uniqueRatios = DeduplicateByKey(batch.Rows, item => item.Timestamp);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(LongShortRatioCsv), uniqueRatios.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(dbPath, batch.SymbolName, uniqueRatios, nameof(LongShortRatioCsv.Timestamp), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertTakerLongShortRatiosAsync(SymbolRows<TakerLongShortRatioCsv> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        TakerLongShortRatioCsv[] uniqueRatios = DeduplicateByKey(batch.Rows, item => item.Timestamp);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(TakerLongShortRatioCsv), uniqueRatios.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(TakerLongShortRatioPath, batch.SymbolName, uniqueRatios, nameof(TakerLongShortRatioCsv.Timestamp), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertBasisAsync(SymbolRows<FuturesBasisCsv> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        FuturesBasisCsv[] uniqueHistories = DeduplicateByKey(batch.Rows, item => item.Timestamp);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(FuturesBasisCsv), uniqueHistories.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(BasisPath, batch.SymbolName, uniqueHistories, nameof(FuturesBasisCsv.Timestamp), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertOpenInterestHistoriesAsync(SymbolRows<OpenInterestHistory> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        OpenInterestHistory[] uniqueOpenInterestHistories = DeduplicateByKey(batch.Rows, item => item.Timestamp);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(OpenInterestHistory), uniqueOpenInterestHistories.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(OpenInterestPath, batch.SymbolName, uniqueOpenInterestHistories, nameof(OpenInterestHistory.Timestamp), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected async Task InsertFundingRatesAsync(SymbolRows<FundingRate> batch, CancellationToken ct = default)
    {
        if (!batch.Rows.Any())
            return;
        FundingRate[] uniqueRates = DeduplicateByKey(batch.Rows, item => item.FundingTime);
        try
        {
            logger.LogDebug("Start inserting {DataType}. Count: {Count}", nameof(FundingRate), uniqueRates.Length);
            await DuckDbStorageHelper.UpsertRowsAsync(FundingRatePath, batch.SymbolName, uniqueRates, nameof(FundingRate.FundingTime), ct);
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
        }
    }

    protected virtual async Task InsertAggTradesAsync(MarketDataDownloadBatch? batch, CancellationToken ct = default)
    {
        if (batch is null || batch.Files.Count == 0)
            return;

        string symbolPath = GetMarketDataSymbolPath(batch.DataType, batch.Symbol);
        Directory.CreateDirectory(symbolPath);
        logger.LogDebug("Start persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}", batch.MarketPathSegment, batch.Symbol, batch.Files.Count);

        foreach (MarketDataDownloadFile file in batch.Files)
        {
            ct.ThrowIfCancellationRequested();

            string periodDirectory = GetAggTradesPeriodDirectoryName(file);
            string periodRootDirectory = Path.Combine(symbolPath, GetPeriodFolderName(file.Period));
            string destinationDirectory = Path.Combine(periodRootDirectory, periodDirectory);
            string stagingDirectory = destinationDirectory + ".__staging";

            if (Directory.Exists(stagingDirectory))
                Directory.Delete(stagingDirectory, true);

            Directory.CreateDirectory(stagingDirectory);
            try
            {
                await ExtractArchiveWithChecksumsAsync(file.TempZipPath, stagingDirectory, ct);
                await File.WriteAllTextAsync(Path.Combine(stagingDirectory, "_SUCCESS"), string.Empty, ct);

                if (Directory.Exists(destinationDirectory))
                    Directory.Delete(destinationDirectory, true);

                Directory.CreateDirectory(periodRootDirectory);
                Directory.Move(stagingDirectory, destinationDirectory);
                DeleteFileIfExists(file.TempZipPath);
                DeleteFileIfExists(file.TempChecksumPath);
                DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempZipPath));
                DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempChecksumPath));
            }
            catch (OperationCanceledException)
            {
                if (Directory.Exists(stagingDirectory))
                    Directory.Delete(stagingDirectory, true);
                throw;
            }
            catch (Exception ex)
            {
                if (Directory.Exists(stagingDirectory))
                    Directory.Delete(stagingDirectory, true);
                logger.LogError(ex,
                    "Persist aggTrades archive failed. Market: {Market}, Symbol: {Symbol}, Period: {Period}, FileName: {FileName}",
                    batch.MarketPathSegment, batch.Symbol, file.Period, file.FileName);
            }
        }

        logger.LogDebug("Finish persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}", batch.MarketPathSegment, batch.Symbol, batch.Files.Count);
    }

    public virtual Task<(DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart)> GetLastAggTradesAsync(T symbol, CancellationToken ct = default)
    {
        string symbolPath = GetMarketDataSymbolPath(BaseMarketData.AggTradesDataType, GetSymbolName(symbol));
        DateTime? lastMonthlyDate = GetLastCompletedPeriod(Path.Combine(symbolPath, "Monthly"), "yyyy-MM");
        DateTime? lastDailyDate = GetLastCompletedPeriod(Path.Combine(symbolPath, "Daily"), "yyyy-MM-dd");
        return Task.FromResult((yearsReserved, lastMonthlyDate, lastDailyDate));
    }

    protected async Task<DateTime> GetLastTimestampAsync(
        string dbPath,
        string symbolName,
        string columnName,
        IReadOnlyDictionary<string, object?>? filters = null,
        CancellationToken ct = default)
    {
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, symbolName, columnName, filters, ct);
        return latest.HasValue
            ? DateTimeOffset.FromUnixTimeMilliseconds(latest.Value).UtcDateTime
            : yearsReserved;
    }

    protected async Task DeleteSymbolRowsBeforeAsync(string dbPath, string symbolName, string columnName, CancellationToken ct = default)
        => await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, symbolName, columnName, new DateTimeOffset(yearsReserved).ToUnixTimeMilliseconds(), ct);

    protected Task<List<string>> GetStoredSymbolNamesAsync(CancellationToken ct = default)
        => DuckDbStorageHelper.GetStringValuesAsync(SymbolInfoPath, MarketPathSegment, "Name", ct);

    protected async Task DeleteSymbolTablesAsync(IEnumerable<string> dbPaths, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        foreach (string dbPath in dbPaths)
        {
            foreach (string symbolName in delistedSymbols)
            {
                ct.ThrowIfCancellationRequested();
                await DuckDbStorageHelper.DropTableIfExistsAsync(dbPath, symbolName, ct);
            }
        }
    }

    protected static long ToUnixMilliseconds(DateTime value)
        => new DateTimeOffset(value).ToUnixTimeMilliseconds();

    public abstract Task<DateTime> GetLastTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastPremiumIndexTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastIndexPriceTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastMarkPriceTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastFundingTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastOpenInterestTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastTakerLongShortRatioTimeAsync(T symbol, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastBasisTimeAsync(T symbol, CancellationToken ct = default);

    public abstract Task DeleteOldData(CancellationToken ct = default);

    protected abstract string GetSymbolName(T symbol);
    protected abstract Task<List<string>> GetExistingSymbolNamesAsync(CancellationToken ct = default);
    protected abstract Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default);

    protected abstract Task<Result<List<T>>> GetMarketAsync(CancellationToken ct = default);

    protected abstract Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(
        T symbol,
        (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState,
        CancellationToken ct = default);
    protected abstract Task<Result<List<Kline>>> GetKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<PremiumIndexKline>>> GetIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<PremiumIndexKline>>> GetMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<FundingRate>>> GetFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<OpenInterestHistory>>> GetOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<FuturesBasisCsv>>> GetBasisAsync(T symbol, DateTime startTime, CancellationToken ct = default);

    protected abstract Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default);
    protected abstract Task<Result<SymbolInfoCsv[]>> GetCsvSymbolInfosAsync(CancellationToken ct = default);

    protected abstract Task<Result<Kline[]>> GetCsvKlinesAsync(string symbol, CancellationToken ct = default);

    protected abstract Task<Result<PremiumIndexKline[]>> GetCsvPremiumIndexKlinesAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<PremiumIndexKline[]>> GetCsvIndexPriceKlinesAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<PremiumIndexKline[]>> GetCsvMarkPriceKlinesAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<FundingRate[]>> GetCsvFundingRatesAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<OpenInterestHistory[]>> GetCsvOpenInterestHistoriesAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortPositionRatiosAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<LongShortRatioCsv[]>> GetCsvTopLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<LongShortRatioCsv[]>> GetCsvGlobalLongShortAccountRatiosAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<TakerLongShortRatioCsv[]>> GetCsvTakerLongShortRatiosAsync(string symbol, CancellationToken ct = default);
    protected abstract Task<Result<FuturesBasisCsv[]>> GetCsvBasisAsync(string symbol, CancellationToken ct = default);

    protected void LogDropStatus(ShardingExtension.ShardingTableDropResult result)
    {
        if (result.ExistedBeforeDrop && result.ExistsAfterDrop)
            logger.LogError("Table cleanup status. Table: {Table}, Missing: 0, Dropped: 0, Failed: 1", result.EscapedTableName);
        else if (result.DroppedSuccessfully)
            logger.LogInformation("Table cleanup status. Table: {Table}, Missing: 0, Dropped: 1, Failed: 0", result.EscapedTableName);
        else
            logger.LogInformation("Table cleanup status. Table: {Table}, Missing: 1, Dropped: 0, Failed: 0", result.EscapedTableName);
    }

    protected Task DeleteMarketDataSymbolDirectoriesAsync(IReadOnlyCollection<string> delistedSymbols, IReadOnlyCollection<string> marketDataTypes, CancellationToken ct = default)
    {
        if (delistedSymbols.Count == 0 || marketDataTypes.Count == 0)
            return Task.CompletedTask;

        foreach (string dataType in marketDataTypes)
        {
            string marketPath = GetMarketDataMarketPath(dataType);
            if (!Directory.Exists(marketPath))
                continue;

            foreach (string symbol in delistedSymbols)
            {
                ct.ThrowIfCancellationRequested();

                string symbolPath = Path.Combine(marketPath, symbol);
                if (Directory.Exists(symbolPath))
                    Directory.Delete(symbolPath, true);
            }
        }

        return Task.CompletedTask;
    }

    protected Task DeleteOldAggTradesDataAsync(string symbolName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        string symbolPath = GetMarketDataSymbolPath(BaseMarketData.AggTradesDataType, symbolName);
        string monthlyRootPath = Path.Combine(symbolPath, "Monthly");
        string dailyRootPath = Path.Combine(symbolPath, "Daily");

        DeleteOverlappedAggTradesDailyDirectories(monthlyRootPath, dailyRootPath, ct);
        DeleteExpiredAggTradesDirectories(monthlyRootPath, dailyRootPath, ct);

        DeleteDirectoryIfEmpty(monthlyRootPath);
        DeleteDirectoryIfEmpty(dailyRootPath);
        DeleteDirectoryIfEmpty(symbolPath);

        return Task.CompletedTask;
    }

    private void DeleteOverlappedAggTradesDailyDirectories(string monthlyRootPath, string dailyRootPath, CancellationToken ct)
    {
        if (!Directory.Exists(monthlyRootPath) || !Directory.Exists(dailyRootPath))
            return;

        foreach (string monthlyDirectory in Directory.EnumerateDirectories(monthlyRootPath))
        {
            ct.ThrowIfCancellationRequested();

            string monthName = Path.GetFileName(monthlyDirectory);
            if (!File.Exists(Path.Combine(monthlyDirectory, "_SUCCESS")))
                continue;

            if (!DateTime.TryParseExact(monthName, "yyyy-MM", null, System.Globalization.DateTimeStyles.None, out DateTime month))
                continue;

            foreach (string dailyDirectory in Directory.EnumerateDirectories(dailyRootPath, $"{month:yyyy-MM}-*"))
            {
                ct.ThrowIfCancellationRequested();
                Directory.Delete(dailyDirectory, true);
            }
        }
    }

    private void DeleteExpiredAggTradesDirectories(string monthlyRootPath, string dailyRootPath, CancellationToken ct)
    {
        DateTime reservedDate = yearsReserved.Date;

        if (Directory.Exists(monthlyRootPath))
        {
            foreach (string monthlyDirectory in Directory.EnumerateDirectories(monthlyRootPath))
            {
                ct.ThrowIfCancellationRequested();

                string monthName = Path.GetFileName(monthlyDirectory);
                if (!DateTime.TryParseExact(monthName, "yyyy-MM", null, System.Globalization.DateTimeStyles.None, out DateTime month))
                    continue;

                DateTime monthEnd = new DateTime(month.Year, month.Month, DateTime.DaysInMonth(month.Year, month.Month));
                if (monthEnd < reservedDate)
                    Directory.Delete(monthlyDirectory, true);
            }
        }

        if (!Directory.Exists(dailyRootPath))
            return;

        foreach (string dailyDirectory in Directory.EnumerateDirectories(dailyRootPath))
        {
            ct.ThrowIfCancellationRequested();

            string dayName = Path.GetFileName(dailyDirectory);
            if (!DateTime.TryParseExact(dayName, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out DateTime day))
                continue;

            if (day < reservedDate)
                Directory.Delete(dailyDirectory, true);
        }
    }

    private string GetMarketDataMarketPath(string dataType)
        => Path.Combine(MarketDataPath, dataType, MarketPathSegment);

    protected string GetMarketDataSymbolPath(string dataType, string symbol)
        => Path.Combine(GetMarketDataMarketPath(dataType), symbol);

    protected string GetMarketDataTempSymbolPath(string dataType, string symbol)
        => Path.Combine(MarketDataTempPath, dataType, MarketPathSegment, symbol);

    private static string GetPeriodFolderName(string period)
        => period.Equals("monthly", StringComparison.OrdinalIgnoreCase) ? "Monthly"
        : period.Equals("daily", StringComparison.OrdinalIgnoreCase) ? "Daily"
        : throw new InvalidDataException($"Unsupported market data period: {period}");

    private static string GetAggTradesPeriodDirectoryName(MarketDataDownloadFile file)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(file.FileName);
        string prefix = $"{file.Symbol}-{file.DataType}-";
        if (!fileNameWithoutExtension.StartsWith(prefix, StringComparison.Ordinal))
            throw new InvalidDataException($"Invalid market data file name: {file.FileName}");

        return fileNameWithoutExtension[prefix.Length..];
    }

    private static DateTime? GetLastCompletedPeriod(string rootDirectory, string format)
    {
        if (!Directory.Exists(rootDirectory))
            return null;

        DateTime? latest = null;
        foreach (string directory in Directory.EnumerateDirectories(rootDirectory))
        {
            string periodName = Path.GetFileName(directory);
            if (!File.Exists(Path.Combine(directory, "_SUCCESS")))
                continue;

            if (!DateTime.TryParseExact(periodName, format, null, System.Globalization.DateTimeStyles.None, out DateTime parsed))
                continue;

            if (!latest.HasValue || parsed > latest.Value)
                latest = parsed;
        }

        return latest;
    }

    private static async Task ExtractArchiveWithChecksumsAsync(string archivePath, string destinationDirectory, CancellationToken ct)
    {
        int extractedFileCount = 0;
        using ZipArchive archive = ZipFile.OpenRead(archivePath);
        foreach (ZipArchiveEntry entry in archive.Entries)
        {
            ct.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(entry.Name))
                continue;

            string entryPath = Path.Combine(destinationDirectory, entry.Name);
            await using (Stream source = entry.Open())
            await using (FileStream destination = File.Create(entryPath))
                await source.CopyToAsync(destination, ct);

            string checksum = await ComputeSha256Async(entryPath, ct);
            await File.WriteAllTextAsync(Path.Combine(destinationDirectory, entry.Name + ".CHECKSUM"), $"{checksum}  {entry.Name}", ct);
            extractedFileCount++;
        }

        if (extractedFileCount == 0)
            throw new InvalidDataException($"Archive does not contain any files: {archivePath}");
    }

    private static async Task<string> ComputeSha256Async(string path, CancellationToken ct)
    {
        await using FileStream stream = File.OpenRead(path);
        byte[] hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static void DeleteDirectoryIfEmpty(string? path)
    {
        while (!string.IsNullOrWhiteSpace(path) && Directory.Exists(path) && !Directory.EnumerateFileSystemEntries(path).Any())
        {
            string? parentPath = Path.GetDirectoryName(path);
            Directory.Delete(path);
            path = parentPath;
        }
    }

    protected static string CombineKlineId(string symbol, KlineInterval interval, DateTime closeTime)
        => $"{symbol}-{interval}-{closeTime:s}";

    protected static string CombineFundingRateId(string symbol, DateTime fundingTime)
        => $"{symbol}-{fundingTime:s}";

    protected static string CombineOpenInterestId(string symbol, DateTime timestamp)
        => $"{symbol}-{timestamp:s}";

    protected static string CombineLongShortRatioId(string symbol, DateTime timestamp)
        => $"{symbol}-{timestamp:s}";
}
