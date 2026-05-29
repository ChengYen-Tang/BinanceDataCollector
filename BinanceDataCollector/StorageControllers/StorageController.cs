using BinanceDataCollector.Collectors.BinanceMarketData;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models.Storage;
using System.Diagnostics;
using System.IO.Compression;

namespace BinanceDataCollector.StorageControllers;

internal sealed class SymbolRows<T>(string symbolName, List<T> rows, int poolInitialCapacity) : IDisposable
{
    public string SymbolName { get; } = symbolName;
    public List<T> Rows { get; } = rows;

    public void Dispose()
        => PooledObjectHelper.ReturnList(Rows, poolInitialCapacity);
}

internal enum AggTradesTimeUnit
{
    Milliseconds,
    Microseconds,
}

internal abstract class StorageController<T>
    where T : class
{
    private const long AggTradesMicrosecondsBoundary = 1_000_000_000_000_000L;
    private const int BinanceApiModelRowsInitialCapacity = 1_500;
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
    protected abstract AggTradesTimeUnit AggTradesTimeUnit { get; }

    public StorageController(IServiceProvider serviceProvider, ILogger logger)
        => (this.serviceProvider, this.logger, yearsReserved) = (serviceProvider, logger, DateTime.Today.AddYears(-3));

    protected static List<TModel> RentModelRows<TModel>()
        => PooledObjectHelper.RentList<TModel>(BinanceApiModelRowsInitialCapacity);

    protected static SymbolRows<TModel> CreateSymbolRows<TModel>(string symbolName, List<TModel> rows)
        => new(symbolName, rows, BinanceApiModelRowsInitialCapacity);

    protected static SymbolRows<TModel> CreateEmptySymbolRows<TModel>(string symbolName)
        => CreateSymbolRows(symbolName, RentModelRows<TModel>());

    protected static List<TModel> ConvertToModelRows<TSource, TModel>(IEnumerable<TSource> source, Func<TSource, TModel> map)
    {
        List<TModel> rows = RentModelRows<TModel>();
        foreach (TSource item in source)
            rows.Add(map(item));
        return rows;
    }

    protected static List<TModel> ConvertToModelRows<TSource, TModel>(
        IEnumerable<TSource> source,
        Func<TSource, bool> predicate,
        Func<TSource, TModel> map)
    {
        List<TModel> rows = RentModelRows<TModel>();
        foreach (TSource item in source)
        {
            if (predicate(item))
                rows.Add(map(item));
        }

        return rows;
    }

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
            return new(InsertKlinesAsync, CreateEmptySymbolRows<Kline>(symbolName), ct);
        }

        return new(InsertKlinesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new(InsertIndexPriceKlinesAsync, CreateEmptySymbolRows<PremiumIndexKline>(symbolName), ct);
        }

        return new(InsertIndexPriceKlinesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new(InsertMarkPriceKlinesAsync, CreateEmptySymbolRows<PremiumIndexKline>(symbolName), ct);
        }

        return new(InsertMarkPriceKlinesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new(InsertPremiumIndexKlinesAsync, CreateEmptySymbolRows<PremiumIndexKline>(symbolName), ct);
        }

        return new(InsertPremiumIndexKlinesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<FundingRate>>(InsertFundingRatesAsync, CreateEmptySymbolRows<FundingRate>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<FundingRate>>(InsertFundingRatesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<OpenInterestHistory>>(InsertOpenInterestHistoriesAsync, CreateEmptySymbolRows<OpenInterestHistory>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<OpenInterestHistory>>(InsertOpenInterestHistoriesAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortPositionRatiosAsync, CreateEmptySymbolRows<LongShortRatioCsv>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortPositionRatiosAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortAccountRatiosAsync, CreateEmptySymbolRows<LongShortRatioCsv>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertTopLongShortAccountRatiosAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertGlobalLongShortAccountRatiosAsync, CreateEmptySymbolRows<LongShortRatioCsv>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<LongShortRatioCsv>>(InsertGlobalLongShortAccountRatiosAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<TakerLongShortRatioCsv>>(InsertTakerLongShortRatiosAsync, CreateEmptySymbolRows<TakerLongShortRatioCsv>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<TakerLongShortRatioCsv>>(InsertTakerLongShortRatiosAsync, CreateSymbolRows(symbolName, result.Value), ct);
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
            return new AsyncWorkItem<SymbolRows<FuturesBasisCsv>>(InsertBasisAsync, CreateEmptySymbolRows<FuturesBasisCsv>(symbolName), ct);
        }

        return new AsyncWorkItem<SymbolRows<FuturesBasisCsv>>(InsertBasisAsync, CreateSymbolRows(symbolName, result.Value), ct);
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

    protected async Task InsertDeduplicatedRowsAsync<TEntity, TKey>(
        string dbPath,
        SymbolRows<TEntity> batch,
        string keyColumn,
        Func<TEntity, TKey> getKey,
        CancellationToken ct = default)
        where TKey : notnull
    {
        List<TEntity>? uniqueRows = null;
        Dictionary<TKey, TEntity>? uniqueEntities = null;
        try
        {
            if (batch.Rows.Count == 0)
                return;

            IReadOnlyList<TEntity> rows = batch.Rows;
            if (batch.Rows.Count > 1)
            {
                uniqueEntities = PooledObjectHelper.RentDictionary<TKey, TEntity>(batch.Rows.Count);
                foreach (TEntity entity in batch.Rows)
                    uniqueEntities[getKey(entity)] = entity;

                if (uniqueEntities.Count != batch.Rows.Count)
                {
                    logger.LogWarning("Deduplicated insert batch. DataType: {DataType}, OriginalCount: {OriginalCount}, UniqueCount: {UniqueCount}",
                        typeof(TEntity).Name, batch.Rows.Count, uniqueEntities.Count);
                    uniqueRows = RentModelRows<TEntity>();
                    uniqueRows.AddRange(uniqueEntities.Values);
                    rows = uniqueRows;
                }
            }

            try
            {
                logger.LogDebug("Start inserting {DataType}. Count: {Count}", typeof(TEntity).Name, rows.Count);
                await DuckDbStorageHelper.UpsertRowsAsync(dbPath, batch.SymbolName, rows, keyColumn, ct);
                logger.LogDebug("Finish inserting.");
            }
            catch (Exception ex)
            {
                logger.LogError("Symbol: {Symbol}, Message: {Message}", batch.SymbolName, ex.Message);
            }
        }
        finally
        {
            if (uniqueRows is not null)
                PooledObjectHelper.ReturnList(uniqueRows, BinanceApiModelRowsInitialCapacity);
            if (uniqueEntities is not null)
                PooledObjectHelper.ReturnDictionary(uniqueEntities, batch.Rows.Count);
            batch.Dispose();
        }
    }

    protected Task InsertKlinesAsync(SymbolRows<Kline> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(KlinePath, batch, nameof(Kline.CloseTime), item => item.CloseTime, ct);

    protected Task InsertPremiumIndexKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(PremiumIndexKlinePath, batch, nameof(PremiumIndexKline.CloseTime), item => item.CloseTime, ct);

    protected Task InsertIndexPriceKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(IndexPriceKlinePath, batch, nameof(PremiumIndexKline.CloseTime), item => item.CloseTime, ct);

    protected Task InsertMarkPriceKlinesAsync(SymbolRows<PremiumIndexKline> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(MarkPriceKlinePath, batch, nameof(PremiumIndexKline.CloseTime), item => item.CloseTime, ct);

    protected Task InsertTopLongShortPositionRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(TopLongShortPositionRatioPath, batch, ct);

    protected Task InsertTopLongShortAccountRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(TopLongShortAccountRatioPath, batch, ct);

    protected Task InsertGlobalLongShortAccountRatiosAsync(SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertLongShortRatiosAsync(GlobalLongShortAccountRatioPath, batch, ct);

    private Task InsertLongShortRatiosAsync(string dbPath, SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(dbPath, batch, nameof(LongShortRatioCsv.Timestamp), item => item.Timestamp, ct);

    protected Task InsertTakerLongShortRatiosAsync(SymbolRows<TakerLongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(TakerLongShortRatioPath, batch, nameof(TakerLongShortRatioCsv.Timestamp), item => item.Timestamp, ct);

    protected Task InsertBasisAsync(SymbolRows<FuturesBasisCsv> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(BasisPath, batch, nameof(FuturesBasisCsv.Timestamp), item => item.Timestamp, ct);

    protected Task InsertOpenInterestHistoriesAsync(SymbolRows<OpenInterestHistory> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(OpenInterestPath, batch, nameof(OpenInterestHistory.Timestamp), item => item.Timestamp, ct);

    protected Task InsertFundingRatesAsync(SymbolRows<FundingRate> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(FundingRatePath, batch, nameof(FundingRate.FundingTime), item => item.FundingTime, ct);

    protected virtual async Task InsertAggTradesAsync(MarketDataDownloadBatch? batch, CancellationToken ct = default)
    {
        if (batch is null || batch.Files.Count == 0)
            return;
        ct.ThrowIfCancellationRequested();

        await EnsureAggTradesStorageMigratedAsync(batch.Symbol, ct);
        string databasePath = GetAggTradesDatabasePath();
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);
        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, batch.Symbol, AggTradesMicrosecondsBoundary, ct);

        logger.LogDebug("Start persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);

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
                {
                    LogUnexpectedAggTradesTimeUnitIfNeeded(csvPath);
                    await DuckDbStorageHelper.ReplaceAggTradesTailFromCsvAsync(
                        databasePath,
                        batch.Symbol,
                        csvPath,
                        GetAggTradesTimeUnitForTimestamp(GetMarketDataCsvSortKey(csvPath)) == AggTradesTimeUnit.Microseconds,
                        ct);
                }

                await DuckDbStorageHelper.CheckpointAsync(databasePath, ct);
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
                    await DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(databasePath, batch.Symbol, csvPath, ct);

                await DuckDbStorageHelper.CheckpointAsync(databasePath, ct);
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

        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, symbolName, AggTradesMicrosecondsBoundary, ct);
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, symbolName, "transact_time", null, ct);
        return latest.HasValue ? FromAggTradesUnixTime(latest.Value) : yearsReserved;
    }

    public virtual async Task<DateTime> GetLastBookDepthAsync(T symbol, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        string databasePath = GetBookDepthDatabasePath();
        if (!File.Exists(databasePath))
            return yearsReserved;

        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, symbolName, "snapshot_time", null, ct);
        return latest.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(latest.Value).UtcDateTime : yearsReserved;
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
        long? existingLatest = File.Exists(databasePath)
            ? await DuckDbStorageHelper.GetMaxInt64Async(databasePath, symbolName, "transact_time", null, ct)
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

            foreach (string csvPath in csvPaths)
            {
                LogUnexpectedAggTradesTimeUnitIfNeeded(csvPath);
                await DuckDbStorageHelper.AppendAggTradesFromCsvAsync(
                    databasePath,
                    symbolName,
                    csvPath,
                    GetAggTradesTimeUnitForTimestamp(GetMarketDataCsvSortKey(csvPath)) == AggTradesTimeUnit.Microseconds,
                    ct);
            }

            await DuckDbStorageHelper.CheckpointAsync(databasePath, ct);

            Directory.Delete(legacySymbolPath, true);
            logger.LogInformation("Finish migrating aggTrades legacy storage. Market: {Market}, Symbol: {Symbol}, ImportedFiles: {ImportedFiles}",
                MarketPathSegment, symbolName, csvPaths.Length);
        }
        catch
        {
            throw;
        }
    }

    private string GetAggTradesDatabasePath()
        => Path.Combine(DataPath, BaseMarketData.AggTradesDataType, $"{MarketPathSegment}.duckdb");

    private string GetBookDepthDatabasePath()
        => Path.Combine(DataPath, BaseMarketData.BookDepthDataType, $"{MarketPathSegment}.duckdb");

    private string GetLegacyAggTradesSymbolPath(string symbol)
        => GetMarketDataSymbolPath(BaseMarketData.AggTradesDataType, symbol);

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
        if (TryParseMarketDataPeriodAsUtc(period, "yyyy-MM", out DateTime month))
            return month;
        if (TryParseMarketDataPeriodAsUtc(period, "yyyy-MM-dd", out DateTime day))
            return day;
        return DateTime.MaxValue;
    }

    private static DateTime GetMarketDataCsvSortKey(string csvPath)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(csvPath);
        string period = ExtractMarketDataPeriod(fileNameWithoutExtension);
        if (TryParseMarketDataPeriodAsUtc(period, "yyyy-MM", out DateTime month))
            return month;
        if (TryParseMarketDataPeriodAsUtc(period, "yyyy-MM-dd", out DateTime day))
            return day;
        return DateTime.MaxValue;
    }

    private static string ExtractMarketDataPeriod(string fileNameWithoutExtension)
    {
        string[] segments = fileNameWithoutExtension.Split('-');
        if (segments.Length >= 5
            && segments[^3].Length == 4
            && segments[^2].Length == 2
            && segments[^1].Length == 2)
        {
            return $"{segments[^3]}-{segments[^2]}-{segments[^1]}";
        }

        if (segments.Length >= 4
            && segments[^2].Length == 4
            && segments[^1].Length == 2)
        {
            return $"{segments[^2]}-{segments[^1]}";
        }

        return string.Empty;
    }

    private static bool TryParseMarketDataPeriodAsUtc(string value, string format, out DateTime parsed)
    {
        if (DateTime.TryParseExact(
            value,
            format,
            null,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out parsed))
        {
            parsed = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            return true;
        }

        parsed = default;
        return false;
    }

    private async Task DeleteOldAggTradesRowsAsync(string databasePath, string tableName, CancellationToken ct)
    {
        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, tableName, AggTradesMicrosecondsBoundary, ct);
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(
            databasePath,
            tableName,
            "transact_time",
            ToAggTradesUnixTime(yearsReserved),
            ct);

        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, tableName, "transact_time", null, ct);
        if (!latest.HasValue)
            await DuckDbStorageHelper.DropTableIfExistsAsync(databasePath, tableName, ct);
    }

    protected virtual AggTradesTimeUnit GetAggTradesTimeUnitForTimestamp(DateTime timestamp)
        => AggTradesTimeUnit;

    protected long ToAggTradesUnixTime(DateTime value)
        => checked((value.ToUniversalTime() - DateTime.UnixEpoch).Ticks / 10);

    protected DateTime FromAggTradesUnixTime(long value)
        => DateTime.UnixEpoch.AddTicks(checked(value * 10));

    private void LogUnexpectedAggTradesTimeUnitIfNeeded(string csvPath)
    {
        try
        {
            string? dataLine = File.ReadLines(csvPath).FirstOrDefault(static line => !string.IsNullOrWhiteSpace(line));
            if (string.IsNullOrWhiteSpace(dataLine))
                return;

            string[] columns = dataLine.Split(',');
            if (columns.Length < 6 || !long.TryParse(columns[5], out long rawTime))
                return;

            DateTime csvPeriodStart = GetMarketDataCsvSortKey(csvPath);
            AggTradesTimeUnit expectedTimeUnit = GetAggTradesTimeUnitForTimestamp(csvPeriodStart);
            int digitLength = rawTime.ToString().TrimStart('-').Length;
            bool matchesExpected = expectedTimeUnit switch
            {
                AggTradesTimeUnit.Milliseconds => digitLength is >= 13 and <= 14,
                AggTradesTimeUnit.Microseconds => digitLength is >= 16 and <= 17,
                _ => true,
            };

            if (!matchesExpected)
            {
                logger.LogWarning(
                    "Unexpected aggTrades time unit shape. Market: {Market}, ExpectedUnit: {ExpectedUnit}, Digits: {Digits}, CsvPath: {CsvPath}, RawValue: {RawValue}",
                    MarketPathSegment,
                    expectedTimeUnit,
                    digitLength,
                    csvPath,
                    rawTime);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to inspect aggTrades time unit. Market: {Market}, CsvPath: {CsvPath}", MarketPathSegment, csvPath);
        }
    }

    private async Task DeleteOldBookDepthRowsAsync(string databasePath, string tableName, CancellationToken ct)
    {
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(databasePath, tableName, "snapshot_time", ToUnixMilliseconds(yearsReserved), ct);
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, tableName, "snapshot_time", null, ct);
        if (!latest.HasValue)
            await DuckDbStorageHelper.DropTableIfExistsAsync(databasePath, tableName, ct);
    }

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
