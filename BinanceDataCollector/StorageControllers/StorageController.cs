using BinanceDataCollector.Collectors.BinanceMarketData;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models.Storage;
using DuckDB.NET.Data;
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
    private static readonly string BasePath = AppDomain.CurrentDomain.BaseDirectory;
    protected static string DataPath = DuckDbStorageArchiveHelper.StorageRootPath;
    protected static string MarketDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "BinanceMarketData");
    protected static string MarketDataTempPath = Path.Combine(BasePath, "Tmp", "BinanceMarketData");
    protected static string RootKlinePath = Path.Combine(DataPath, "Kline");
    protected static string RootPremiumIndexKlinePath = Path.Combine(DataPath, "PremiumIndexKline");
    protected static string RootIndexPriceKlinePath = Path.Combine(DataPath, "IndexPriceKline");
    protected static string RootMarkPriceKlinePath = Path.Combine(DataPath, "MarkPriceKline");
    protected static string RootFundingRatePath = Path.Combine(DataPath, "FundingRate");
    protected static string RootOpenInterestPath = Path.Combine(DataPath, "OpenInterestHistory");
    protected static string RootBookDepthPath = Path.Combine(DataPath, BaseMarketData.BookDepthDataType);
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
        DateTime downloadStartTime,
        CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}",
            BaseMarketData.AggTradesDataType, symbol, downloadStartTime);
        Result<MarketDataDownloadBatch> result = await GetAggTradesAsync(symbol, downloadStartTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}",
            BaseMarketData.AggTradesDataType, symbol, downloadStartTime);
        if (result.IsFailed)
        {
            LogSyncFailure(BaseMarketData.AggTradesDataType, symbol, result.Errors[0].Message, startTime: downloadStartTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertAggTradesAsync, null, ct);
        }

        return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertAggTradesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<MarketDataDownloadBatch?>> UpdateBookDepthAsync(
        T symbol,
        DateTime downloadStartTime,
        CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}",
            BaseMarketData.BookDepthDataType, symbol, downloadStartTime);
        Result<MarketDataDownloadBatch> result = await GetBookDepthAsync(symbol, downloadStartTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}",
            BaseMarketData.BookDepthDataType, symbol, downloadStartTime);
        if (result.IsFailed)
        {
            LogSyncFailure(BaseMarketData.BookDepthDataType, symbol, result.Errors[0].Message, startTime: downloadStartTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertBookDepthAsync, null, ct);
        }

        return new AsyncWorkItem<MarketDataDownloadBatch?>(InsertBookDepthAsync, result.Value, ct);
    }

    protected void LogSyncFailure(string dataType, T symbol, string message, KlineInterval? interval = null, DateTime? startTime = null)
        => logger.LogError("Sync failed. DataType: {DataType}, Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}, Message: {Message}",
            dataType, symbol, interval, startTime, message);

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
        ct.ThrowIfCancellationRequested();

        await EnsureAggTradesStorageMigratedAsync(batch.Symbol, ct);
        string databasePath = GetAggTradesDatabasePath();
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);

        logger.LogDebug("Start persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);

        using DuckDBConnection connection = OpenDuckDbConnection(databasePath);
        EnsureAggTradesTable(connection, batch.Symbol);

        MarketDataDownloadFile[] sortedFiles = [.. batch.Files.OrderBy(GetMarketDataDownloadFileSortKey)];
        foreach (MarketDataDownloadFile file in sortedFiles)
        {
            string extractionDirectory = file.TempZipPath + ".__extract";

            if (Directory.Exists(extractionDirectory))
                Directory.Delete(extractionDirectory, true);
            Directory.CreateDirectory(extractionDirectory);
            try
            {
                string[] extractedCsvPaths = await ExtractArchiveEntriesAsync(file.TempZipPath, extractionDirectory, ct);
                foreach (string csvPath in extractedCsvPaths.OrderBy(GetMarketDataCsvSortKey))
                    ReplaceAggTradesTailFromCsv(connection, batch.Symbol, csvPath);

                ExecuteDuckDbNonQuery(connection, "CHECKPOINT;");
                DeleteFileIfExists(file.TempZipPath);
                DeleteFileIfExists(file.TempChecksumPath);
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempZipPath)!);
            }
            catch (OperationCanceledException)
            {
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                throw;
            }
            catch (Exception ex)
            {
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                logger.LogError(ex,
                    "Persist aggTrades archive failed. Market: {Market}, Symbol: {Symbol}, Period: {Period}, FileName: {FileName}",
                    batch.MarketPathSegment, batch.Symbol, file.Period, file.FileName);
            }
        }

        logger.LogDebug("Finish persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
    }

    protected virtual async Task InsertBookDepthAsync(MarketDataDownloadBatch? batch, CancellationToken ct = default)
    {
        if (batch is null || batch.Files.Count == 0)
            return;
        ct.ThrowIfCancellationRequested();

        string databasePath = GetBookDepthDatabasePath();
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);

        logger.LogDebug("Start persisting bookDepth batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);

        using DuckDBConnection connection = OpenDuckDbConnection(databasePath);
        EnsureBookDepthTable(connection, batch.Symbol);

        MarketDataDownloadFile[] sortedFiles = [.. batch.Files.OrderBy(GetMarketDataDownloadFileSortKey)];
        foreach (MarketDataDownloadFile file in sortedFiles)
        {
            string extractionDirectory = file.TempZipPath + ".__extract";

            if (Directory.Exists(extractionDirectory))
                Directory.Delete(extractionDirectory, true);
            Directory.CreateDirectory(extractionDirectory);
            try
            {
                string[] extractedCsvPaths = await ExtractArchiveEntriesAsync(file.TempZipPath, extractionDirectory, ct);
                foreach (string csvPath in extractedCsvPaths.OrderBy(GetMarketDataCsvSortKey))
                    ReplaceBookDepthTailFromCsv(connection, batch.Symbol, csvPath);

                ExecuteDuckDbNonQuery(connection, "CHECKPOINT;");
                DeleteFileIfExists(file.TempZipPath);
                DeleteFileIfExists(file.TempChecksumPath);
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempZipPath)!);
            }
            catch (OperationCanceledException)
            {
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                throw;
            }
            catch (Exception ex)
            {
                if (Directory.Exists(extractionDirectory))
                    Directory.Delete(extractionDirectory, true);
                logger.LogError(ex,
                    "Persist bookDepth archive failed. Market: {Market}, Symbol: {Symbol}, Period: {Period}, FileName: {FileName}",
                    batch.MarketPathSegment, batch.Symbol, file.Period, file.FileName);
            }
        }

        logger.LogDebug("Finish persisting bookDepth batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
    }

    public virtual async Task<DateTime> GetLastAggTradesAsync(T symbol, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        await EnsureAggTradesStorageMigratedAsync(symbolName, ct);

        string databasePath = GetAggTradesDatabasePath();
        if (!File.Exists(databasePath))
            return yearsReserved;

        DateTime? latest = await DuckDbStorageHelper.GetMaxDateTimeAsync(databasePath, symbolName, "transact_time", null, ct);
        return latest ?? yearsReserved;
    }

    public virtual async Task<DateTime> GetLastBookDepthAsync(T symbol, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        string databasePath = GetBookDepthDatabasePath();
        if (!File.Exists(databasePath))
            return yearsReserved;

        DateTime? latest = await DuckDbStorageHelper.GetMaxDateTimeAsync(databasePath, symbolName, "snapshot_time", null, ct);
        return latest ?? yearsReserved;
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
        DateTime downloadStartTime,
        CancellationToken ct = default);
    protected abstract Task<Result<MarketDataDownloadBatch>> GetBookDepthAsync(
        T symbol,
        DateTime downloadStartTime,
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

        string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbolName);
        if (Directory.Exists(legacySymbolPath))
            Directory.Delete(legacySymbolPath, true);

        string databasePath = GetAggTradesDatabasePath();
        if (!File.Exists(databasePath))
            return Task.CompletedTask;

        return DeleteOldAggTradesRowsAsync(databasePath, symbolName, ct);
    }

    protected async Task DeleteAggTradesStorageAsync(IReadOnlyCollection<string> symbols, CancellationToken ct = default)
    {
        foreach (string symbol in symbols)
        {
            ct.ThrowIfCancellationRequested();

            await DuckDbStorageHelper.DropTableIfExistsAsync(GetAggTradesDatabasePath(), symbol, ct);

            string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbol);
            if (Directory.Exists(legacySymbolPath))
                Directory.Delete(legacySymbolPath, true);
        }
    }

    protected Task DeleteOldBookDepthDataAsync(string symbolName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        string databasePath = GetBookDepthDatabasePath();
        if (!File.Exists(databasePath))
            return Task.CompletedTask;

        return DeleteOldBookDepthRowsAsync(databasePath, symbolName, ct);
    }

    protected async Task DeleteBookDepthStorageAsync(IReadOnlyCollection<string> symbols, CancellationToken ct = default)
    {
        foreach (string symbol in symbols)
        {
            ct.ThrowIfCancellationRequested();
            await DuckDbStorageHelper.DropTableIfExistsAsync(GetBookDepthDatabasePath(), symbol, ct);
        }
    }

    private async Task EnsureAggTradesStorageMigratedAsync(string symbolName, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbolName);
        if (!Directory.Exists(legacySymbolPath))
            return;

        string databasePath = GetAggTradesDatabasePath();
        DateTime? existingLatest = File.Exists(databasePath)
            ? await DuckDbStorageHelper.GetMaxDateTimeAsync(databasePath, symbolName, "transact_time", null, ct)
            : null;
        if (existingLatest.HasValue)
        {
            Directory.Delete(legacySymbolPath, true);
            return;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);

        logger.LogInformation("Start migrating aggTrades legacy storage. Market: {Market}, Symbol: {Symbol}, LegacyPath: {LegacyPath}, DatabasePath: {DatabasePath}",
            MarketPathSegment, symbolName, legacySymbolPath, databasePath);

        try
        {
            string[] csvPaths = Directory
                .EnumerateFiles(legacySymbolPath, "*.csv", SearchOption.AllDirectories)
                .OrderBy(GetMarketDataCsvSortKey)
                .ToArray();

            if (csvPaths.Length == 0)
            {
                Directory.Delete(legacySymbolPath, true);
                return;
            }

            if (File.Exists(databasePath))
            {
                using DuckDBConnection connection = OpenDuckDbConnection(databasePath);
                EnsureAggTradesTable(connection, symbolName);
                foreach (string csvPath in csvPaths)
                    AppendAggTradesFromCsv(connection, symbolName, csvPath);
                ExecuteDuckDbNonQuery(connection, "CHECKPOINT;");
            }
            else
            {
                string stagingDatabasePath = databasePath + ".__staging";
                DeleteFileIfExists(stagingDatabasePath);
                using DuckDBConnection connection = OpenDuckDbConnection(stagingDatabasePath);
                EnsureAggTradesTable(connection, symbolName);
                foreach (string csvPath in csvPaths)
                    AppendAggTradesFromCsv(connection, symbolName, csvPath);

                ExecuteDuckDbNonQuery(connection, "CHECKPOINT;");
                connection.Close();
                File.Move(stagingDatabasePath, databasePath);
            }

            Directory.Delete(legacySymbolPath, true);
            logger.LogInformation("Finish migrating aggTrades legacy storage. Market: {Market}, Symbol: {Symbol}, ImportedFiles: {ImportedFiles}",
                MarketPathSegment, symbolName, csvPaths.Length);
        }
        catch
        {
            DeleteFileIfExists(databasePath + ".__staging");
            throw;
        }
    }

    private string GetAggTradesDatabasePath()
        => Path.Combine(DataPath, BaseMarketData.AggTradesDataType, $"{MarketPathSegment}.duckdb");

    private string GetBookDepthDatabasePath()
        => Path.Combine(DataPath, BaseMarketData.BookDepthDataType, $"{MarketPathSegment}.duckdb");

    private string GetLegacyAggTradesSymbolPath(string symbol)
        => GetMarketDataSymbolPath(BaseMarketData.AggTradesDataType, symbol);

    private static DuckDBConnection OpenDuckDbConnection(string databasePath)
    {
        DuckDBConnection connection = new($"Data Source={databasePath}");
        connection.Open();
        return connection;
    }

    private static void EnsureAggTradesTable(DuckDBConnection connection, string tableName)
        => ExecuteDuckDbNonQuery(connection, $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (
                parquetagg_trade_id BIGINT,
                price DOUBLE,
                quantity DOUBLE,
                first_trade_id BIGINT,
                last_trade_id BIGINT,
                transact_time TIMESTAMP,
                is_buyer_maker BOOLEAN
            );
            """);

    private static void EnsureBookDepthTable(DuckDBConnection connection, string tableName)
        => ExecuteDuckDbNonQuery(connection, $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (
                snapshot_time TIMESTAMP,
                percentage DECIMAL(10,2),
                depth DOUBLE,
                notional DOUBLE
            );
            """);

    private static void ReplaceAggTradesTailFromCsv(DuckDBConnection connection, string tableName, string csvPath)
    {
        BuildAggTradesStagingTable(connection, csvPath);

        DateTime? replaceFrom = ExecuteDuckDbScalar(connection, "SELECT MIN(transact_time) FROM staging_agg_trades_deduped;") switch
        {
            null => null,
            DBNull => null,
            object value => Convert.ToDateTime(value)
        };

        if (!replaceFrom.HasValue)
        {
            DropAggTradesStagingTables(connection);
            return;
        }

        using DuckDBTransaction transaction = connection.BeginTransaction();

        using (DuckDBCommand deleteCommand = connection.CreateCommand())
        {
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE transact_time >= $replace_from;";
            deleteCommand.Parameters.Add(new DuckDBParameter("replace_from", replaceFrom.Value));
            deleteCommand.ExecuteNonQuery();
        }

        using (DuckDBCommand insertCommand = connection.CreateCommand())
        {
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"""
                INSERT INTO {QuoteIdentifier(tableName)}
                SELECT parquetagg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
                FROM staging_agg_trades_deduped
                ORDER BY transact_time, parquetagg_trade_id;
                """;
            insertCommand.ExecuteNonQuery();
        }

        transaction.Commit();
        DropAggTradesStagingTables(connection);
    }

    private static void AppendAggTradesFromCsv(DuckDBConnection connection, string tableName, string csvPath)
    {
        BuildAggTradesStagingTable(connection, csvPath);

        using DuckDBCommand insertCommand = connection.CreateCommand();
        insertCommand.CommandText = $"""
            INSERT INTO {QuoteIdentifier(tableName)}
            SELECT parquetagg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
            FROM staging_agg_trades_deduped
            ORDER BY transact_time, parquetagg_trade_id;
            """;
        insertCommand.ExecuteNonQuery();

        DropAggTradesStagingTables(connection);
    }

    private static void ReplaceBookDepthTailFromCsv(DuckDBConnection connection, string tableName, string csvPath)
    {
        BuildBookDepthStagingTable(connection, csvPath);

        DateTime? replaceFrom = ExecuteDuckDbScalar(connection, "SELECT MIN(snapshot_time) FROM staging_book_depth_deduped;") switch
        {
            null => null,
            DBNull => null,
            object value => Convert.ToDateTime(value)
        };

        if (!replaceFrom.HasValue)
        {
            DropBookDepthStagingTables(connection);
            return;
        }

        using DuckDBTransaction transaction = connection.BeginTransaction();

        using (DuckDBCommand deleteCommand = connection.CreateCommand())
        {
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE snapshot_time >= $replace_from;";
            deleteCommand.Parameters.Add(new DuckDBParameter("replace_from", replaceFrom.Value));
            deleteCommand.ExecuteNonQuery();
        }

        using (DuckDBCommand insertCommand = connection.CreateCommand())
        {
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"""
                INSERT INTO {QuoteIdentifier(tableName)}
                SELECT snapshot_time, percentage, depth, notional
                FROM staging_book_depth_deduped
                ORDER BY snapshot_time, percentage;
                """;
            insertCommand.ExecuteNonQuery();
        }

        transaction.Commit();
        DropBookDepthStagingTables(connection);
    }

    private static void BuildAggTradesStagingTable(DuckDBConnection connection, string csvPath)
    {
        DropAggTradesStagingTables(connection);

        string csvLiteral = ToSqlStringLiteral(csvPath);
        ExecuteDuckDbNonQuery(connection, $"""
            CREATE TEMP TABLE staging_agg_trades AS
            SELECT
                CAST(column0 AS BIGINT) AS parquetagg_trade_id,
                CAST(column1 AS DOUBLE) AS price,
                CAST(column2 AS DOUBLE) AS quantity,
                CAST(column3 AS BIGINT) AS first_trade_id,
                CAST(column4 AS BIGINT) AS last_trade_id,
                make_timestamp_ms(CAST(column5 AS BIGINT)) AS transact_time,
                CAST(column6 AS BOOLEAN) AS is_buyer_maker
            FROM read_csv({csvLiteral}, header = false, all_varchar = true);
            """);

        ExecuteDuckDbNonQuery(connection, """
            CREATE TEMP TABLE staging_agg_trades_deduped AS
            SELECT parquetagg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
            FROM (
                SELECT
                    parquetagg_trade_id,
                    price,
                    quantity,
                    first_trade_id,
                    last_trade_id,
                    transact_time,
                    is_buyer_maker,
                    ROW_NUMBER() OVER (PARTITION BY parquetagg_trade_id ORDER BY transact_time DESC, parquetagg_trade_id DESC) AS row_num
                FROM staging_agg_trades
            )
            WHERE row_num = 1;
            """);
    }

    private static void BuildBookDepthStagingTable(DuckDBConnection connection, string csvPath)
    {
        DropBookDepthStagingTables(connection);

        string csvLiteral = ToSqlStringLiteral(csvPath);
        ExecuteDuckDbNonQuery(connection, $"""
            CREATE TEMP TABLE staging_book_depth AS
            SELECT
                CAST("timestamp" AS TIMESTAMP) AS snapshot_time,
                CAST(percentage AS DECIMAL(10,2)) AS percentage,
                CAST(depth AS DOUBLE) AS depth,
                CAST(notional AS DOUBLE) AS notional
            FROM read_csv({csvLiteral}, header = true, all_varchar = true);
            """);

        ExecuteDuckDbNonQuery(connection, """
            CREATE TEMP TABLE staging_book_depth_deduped AS
            SELECT snapshot_time, percentage, depth, notional
            FROM (
                SELECT
                    snapshot_time,
                    percentage,
                    depth,
                    notional,
                    ROW_NUMBER() OVER (PARTITION BY snapshot_time, percentage ORDER BY snapshot_time DESC, percentage DESC) AS row_num
                FROM staging_book_depth
            )
            WHERE row_num = 1;
            """);
    }

    private static void DropAggTradesStagingTables(DuckDBConnection connection)
        => ExecuteDuckDbNonQuery(connection, """
            DROP TABLE IF EXISTS staging_agg_trades_deduped;
            DROP TABLE IF EXISTS staging_agg_trades;
            """);

    private static void DropBookDepthStagingTables(DuckDBConnection connection)
        => ExecuteDuckDbNonQuery(connection, """
            DROP TABLE IF EXISTS staging_book_depth_deduped;
            DROP TABLE IF EXISTS staging_book_depth;
            """);

    private static object? ExecuteDuckDbScalar(DuckDBConnection connection, string commandText)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        return command.ExecuteScalar();
    }

    private static void ExecuteDuckDbNonQuery(DuckDBConnection connection, string commandText)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        command.ExecuteNonQuery();
    }

    private static async Task<string[]> ExtractArchiveEntriesAsync(string archivePath, string destinationDirectory, CancellationToken ct)
    {
        List<string> extractedPaths = [];
        using ZipArchive archive = ZipFile.OpenRead(archivePath);
        foreach (ZipArchiveEntry entry in archive.Entries)
        {
            ct.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(entry.Name))
                continue;

            string destinationPath = Path.Combine(destinationDirectory, entry.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);

            await using Stream source = entry.Open();
            await using FileStream destination = File.Create(destinationPath);
            await source.CopyToAsync(destination, ct);
            extractedPaths.Add(destinationPath);
        }

        if (extractedPaths.Count == 0)
            throw new InvalidDataException($"Archive does not contain any files: {archivePath}");

        return [.. extractedPaths];
    }

    private static DateTime GetMarketDataDownloadFileSortKey(MarketDataDownloadFile file)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(file.FileName);
        string prefix = $"{file.Symbol}-{file.DataType}-";
        if (!fileNameWithoutExtension.StartsWith(prefix, StringComparison.Ordinal))
            return DateTime.MaxValue;

        string period = fileNameWithoutExtension[prefix.Length..];
        if (DateTime.TryParseExact(period, "yyyy-MM", null, System.Globalization.DateTimeStyles.None, out DateTime month))
            return month;
        if (DateTime.TryParseExact(period, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out DateTime day))
            return day;
        return DateTime.MaxValue;
    }

    private static DateTime GetMarketDataCsvSortKey(string csvPath)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(csvPath);
        int separatorIndex = fileNameWithoutExtension.LastIndexOf('-');
        if (separatorIndex < 0)
            return DateTime.MaxValue;

        string period = fileNameWithoutExtension[(fileNameWithoutExtension.LastIndexOf('-', separatorIndex - 1) + 1)..];
        if (DateTime.TryParseExact(period, "yyyy-MM", null, System.Globalization.DateTimeStyles.None, out DateTime month))
            return month;
        if (DateTime.TryParseExact(period, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out DateTime day))
            return day;
        return DateTime.MaxValue;
    }

    private static string ToSqlStringLiteral(string value)
        => $"'{value.Replace("'", "''")}'";

    private async Task DeleteOldAggTradesRowsAsync(string databasePath, string tableName, CancellationToken ct)
    {
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(databasePath, tableName, "transact_time", yearsReserved, ct);
        DateTime? latest = await DuckDbStorageHelper.GetMaxDateTimeAsync(databasePath, tableName, "transact_time", null, ct);
        if (!latest.HasValue)
            await DuckDbStorageHelper.DropTableIfExistsAsync(databasePath, tableName, ct);
    }

    private async Task DeleteOldBookDepthRowsAsync(string databasePath, string tableName, CancellationToken ct)
    {
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(databasePath, tableName, "snapshot_time", yearsReserved, ct);
        DateTime? latest = await DuckDbStorageHelper.GetMaxDateTimeAsync(databasePath, tableName, "snapshot_time", null, ct);
        if (!latest.HasValue)
            await DuckDbStorageHelper.DropTableIfExistsAsync(databasePath, tableName, ct);
    }

    private static string QuoteIdentifier(string value)
        => "\"" + value.Replace("\"", "\"\"") + "\"";

    private string GetMarketDataMarketPath(string dataType)
        => Path.Combine(MarketDataPath, dataType, MarketPathSegment);

    protected string GetMarketDataSymbolPath(string dataType, string symbol)
        => Path.Combine(GetMarketDataMarketPath(dataType), symbol);

    protected string GetMarketDataTempSymbolPath(string dataType, string symbol)
        => Path.Combine(MarketDataTempPath, dataType, MarketPathSegment, symbol);

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

}
