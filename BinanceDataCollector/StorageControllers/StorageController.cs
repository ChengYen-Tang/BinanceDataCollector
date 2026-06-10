using BinanceDataCollector.Collectors.BinanceMarketData;
using BinanceDataCollector.WorkItems;
using CollectorModels.Models.Storage;
using System.Diagnostics;
using System.IO.Compression;

namespace BinanceDataCollector.StorageControllers;

internal sealed class SymbolRows<T>(string symbolName, IReadOnlyList<T> rows) : IDisposable
{
    public string SymbolName { get; } = symbolName;
    public IReadOnlyList<T> Rows { get; } = rows;

    public void Dispose() { }
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
    private const string SymbolDataTableName = "data";
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

    protected static SymbolRows<TModel> CreateSymbolRows<TModel>(string symbolName, IReadOnlyList<TModel> rows)
        => new(symbolName, rows);

    protected static SymbolRows<TModel> CreateEmptySymbolRows<TModel>(string symbolName)
        => CreateSymbolRows(symbolName, new List<TModel>());

    protected static List<TModel> ConvertToModelRows<TSource, TModel>(IEnumerable<TSource> source, Action<TSource, TModel> map)
        where TModel : class, new()
        => [.. source
            .AsParallel()
            .AsOrdered()
            .Select(item =>
            {
                TModel row = new();
                map(item, row);
                return row;
            })];

    protected static List<TModel> ConvertToModelRows<TSource, TModel>(
        IEnumerable<TSource> source,
        Func<TSource, bool> predicate,
        Action<TSource, TModel> map)
        where TModel : class, new()
        => [.. source
            .AsParallel()
            .AsOrdered()
            .Where(predicate)
            .Select(item =>
            {
                TModel row = new();
                map(item, row);
                return row;
            })];

    protected static List<TModel> ConvertToMarketRows<TSource, TModel>(IEnumerable<TSource> source, Action<TSource, TModel> map)
        where TModel : class, new()
        => ConvertToModelRows(source, map);

    public async Task<Result<List<T>>> FetchMarketAsync(CancellationToken ct = default)
    {
        Result<List<T>> result = await GetMarketAsync(ct);
        if (result.IsFailed)
        {
            LogMarketSyncFailure(result.Errors[0].Message);
            return Result.Fail(result.Errors);
        }

        return Result.Ok(result.Value);
    }

    public async Task<Result> ApplyMarketAsync(IReadOnlyCollection<T> markets, CancellationToken ct = default)
    {
        try
        {
            string[] currentSymbols = [.. markets.Select(GetSymbolName)];
            List<string> existingSymbols = await GetExistingSymbolNamesAsync(ct);
            string[] delistedSymbols = [.. existingSymbols.Except(currentSymbols)];

            logger.LogInformation("Start syncing {SymbolType}. MarketCount: {MarketCount}, DelistedCount: {DelistedCount}", typeof(T).Name, markets.Count, delistedSymbols.Length);

            Stopwatch upsertStopwatch = Stopwatch.StartNew();
            await DuckDbStorageHelper.ReplaceTableAsync(
                SymbolInfoPath,
                MarketPathSegment,
                markets.Cast<SymbolInfoCsv>().ToArray(),
                nameof(SymbolInfoCsv.Name),
                true,
                ct);
            upsertStopwatch.Stop();
            logger.LogInformation("Finish upserting {SymbolType}. Cost: {ElapsedMs}ms", typeof(T).Name, upsertStopwatch.ElapsedMilliseconds);

            await MigrateLegacySharedDatabasesAsync(ct);

            Stopwatch deleteStopwatch = Stopwatch.StartNew();
            await DeleteDelistedSymbolsAsync(currentSymbols, delistedSymbols, ct);
            deleteStopwatch.Stop();
            logger.LogDebug("Finish deleting stale {SymbolType} storage. DelistedCount: {DelistedCount}, Cost: {ElapsedMs}ms", typeof(T).Name, delistedSymbols.Length, deleteStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Sync {SymbolType} failed", typeof(T).Name);
            return Result.Fail(ex.Message);
        }
        return Result.Ok();
    }

    public async Task<AsyncWorkItem<SymbolRows<Kline>>> UpdateKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", nameof(Kline), symbol, interval, startTime);
        Result<IReadOnlyList<Kline>> result = await GetKlinesAsync(symbol, interval, startTime, ct);
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
        Result<IReadOnlyList<PremiumIndexKline>> result = await GetIndexPriceKlinesAsync(symbol, interval, startTime, ct);
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
        Result<IReadOnlyList<PremiumIndexKline>> result = await GetMarkPriceKlinesAsync(symbol, interval, startTime, ct);
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
        Result<IReadOnlyList<PremiumIndexKline>> result = await GetPremiumIndexKlinesAsync(symbol, interval, startTime, ct);
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
        Result<IReadOnlyList<FundingRate>> result = await GetFundingRatesAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<OpenInterestHistory>> result = await GetOpenInterestHistoriesAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<LongShortRatioCsv>> result = await GetTopLongShortPositionRatiosAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<LongShortRatioCsv>> result = await GetTopLongShortAccountRatiosAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<LongShortRatioCsv>> result = await GetGlobalLongShortAccountRatiosAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<TakerLongShortRatioCsv>> result = await GetTakerLongShortRatiosAsync(symbol, startTime, ct);
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
        Result<IReadOnlyList<FuturesBasisCsv>> result = await GetBasisAsync(symbol, startTime, ct);
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

    protected void LogMarketSyncFailure(string message)
        => logger.LogError("Sync market failed. Market: {Market}, SymbolType: {SymbolType}, Message: {Message}",
            MarketPathSegment, typeof(T).Name, message);

    protected async Task InsertDeduplicatedRowsAsync<TEntity, TKey>(
        string dbFolderPath,
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
                uniqueEntities = new Dictionary<TKey, TEntity>(batch.Rows.Count);
                foreach (TEntity entity in batch.Rows)
                    uniqueEntities[getKey(entity)] = entity;

                if (uniqueEntities.Count != batch.Rows.Count)
                {
                    logger.LogWarning("Deduplicated insert batch. DataType: {DataType}, OriginalCount: {OriginalCount}, UniqueCount: {UniqueCount}",
                        typeof(TEntity).Name, batch.Rows.Count, uniqueEntities.Count);
                    uniqueRows = new(BinanceApiModelRowsInitialCapacity);
                    uniqueRows.AddRange(uniqueEntities.Values);
                    rows = uniqueRows;
                }
            }

            try
            {
                string databasePath = GetSymbolDatabasePath(dbFolderPath, batch.SymbolName);
                logger.LogDebug("Start inserting {DataType}. Count: {Count}", typeof(TEntity).Name, rows.Count);
                await DuckDbStorageHelper.UpsertRowsAsync(databasePath, SymbolDataTableName, rows, keyColumn, ct);
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

    private Task InsertLongShortRatiosAsync(string dbFolderPath, SymbolRows<LongShortRatioCsv> batch, CancellationToken ct = default)
        => InsertDeduplicatedRowsAsync(dbFolderPath, batch, nameof(LongShortRatioCsv.Timestamp), item => item.Timestamp, ct);

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
        string databasePath = GetAggTradesDatabasePath(batch.Symbol);
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);
        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, SymbolDataTableName, AggTradesMicrosecondsBoundary, ct);

        logger.LogDebug("Start persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);

        string tempSymbolPath = GetMarketDataTempSymbolPath(BaseMarketData.AggTradesDataType, batch.Symbol);
        MarketDataDownloadFile[] sortedFiles = [.. batch.Files.OrderBy(GetMarketDataDownloadFileSortKey)];
        bool symbolDbModified = false;
        bool shouldExitEarly = false;
        try
        {
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
                            SymbolDataTableName,
                            csvPath,
                            GetAggTradesCsvSchema(),
                            GetAggTradesTimeUnitForTimestamp(GetMarketDataCsvSortKey(csvPath)) == AggTradesTimeUnit.Microseconds,
                            DuckDbStorageHelper.DatabaseInitializationProfile.LargeRowGroup,
                            ct);
                        symbolDbModified = true;

                        if (ct.IsCancellationRequested)
                        {
                            shouldExitEarly = true;
                            break;
                        }
                    }

                    DeleteFileIfExists(file.TempZipPath);
                    DeleteFileIfExists(file.TempChecksumPath);
                    if (Directory.Exists(extractionDirectory))
                        Directory.Delete(extractionDirectory, true);
                    DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempZipPath)!);
                }
                catch
                {
                    if (Directory.Exists(extractionDirectory))
                        Directory.Delete(extractionDirectory, true);
                    throw;
                }

                if (shouldExitEarly)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            shouldExitEarly = true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Persist aggTrades archive failed. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
                batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
        }
        finally
        {
            if (symbolDbModified)
                await DuckDbStorageHelper.CheckpointAsync(databasePath, CancellationToken.None);
        }

        if (shouldExitEarly)
            return;

        DeleteDirectoryIfExists(tempSymbolPath);
        logger.LogDebug("Finish persisting aggTrades batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
    }

    protected virtual async Task InsertBookDepthAsync(MarketDataDownloadBatch? batch, CancellationToken ct = default)
    {
        if (batch is null || batch.Files.Count == 0)
            return;
        ct.ThrowIfCancellationRequested();

        string databasePath = GetBookDepthDatabasePath(batch.Symbol);
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);

        logger.LogDebug("Start persisting bookDepth batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);

        string tempSymbolPath = GetMarketDataTempSymbolPath(BaseMarketData.BookDepthDataType, batch.Symbol);
        MarketDataDownloadFile[] sortedFiles = [.. batch.Files.OrderBy(GetMarketDataDownloadFileSortKey)];
        bool symbolDbModified = false;
        bool shouldExitEarly = false;
        try
        {
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
                        await DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(
                            databasePath,
                            SymbolDataTableName,
                            csvPath,
                            DuckDbStorageHelper.DatabaseInitializationProfile.LargeRowGroup,
                            ct);
                        symbolDbModified = true;

                        if (ct.IsCancellationRequested)
                        {
                            shouldExitEarly = true;
                            break;
                        }
                    }

                    DeleteFileIfExists(file.TempZipPath);
                    DeleteFileIfExists(file.TempChecksumPath);
                    if (Directory.Exists(extractionDirectory))
                        Directory.Delete(extractionDirectory, true);
                    DeleteDirectoryIfEmpty(Path.GetDirectoryName(file.TempZipPath)!);
                }
                catch
                {
                    if (Directory.Exists(extractionDirectory))
                        Directory.Delete(extractionDirectory, true);
                    throw;
                }

                if (shouldExitEarly)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            shouldExitEarly = true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Persist bookDepth archive failed. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
                batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
        }
        finally
        {
            if (symbolDbModified)
                await DuckDbStorageHelper.CheckpointAsync(databasePath, CancellationToken.None);
        }

        if (shouldExitEarly)
            return;

        DeleteDirectoryIfExists(tempSymbolPath);
        logger.LogDebug("Finish persisting bookDepth batch. Market: {Market}, Symbol: {Symbol}, FileCount: {FileCount}, DatabasePath: {DatabasePath}",
            batch.MarketPathSegment, batch.Symbol, batch.Files.Count, databasePath);
    }

    private AggTradesCsvSchema GetAggTradesCsvSchema()
        => IsFutures ? AggTradesCsvSchema.Futures : AggTradesCsvSchema.Spot;

    public virtual async Task<DateTime> GetLastAggTradesAsync(T symbol, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        await EnsureAggTradesStorageMigratedAsync(symbolName, ct);

        string databasePath = GetAggTradesDatabasePath(symbolName);
        if (!File.Exists(databasePath))
            return yearsReserved;

        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, SymbolDataTableName, AggTradesMicrosecondsBoundary, ct);
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, "transact_time", null, ct);
        return latest.HasValue ? FromAggTradesUnixTime(latest.Value) : yearsReserved;
    }

    public virtual async Task<DateTime> GetLastBookDepthAsync(T symbol, CancellationToken ct = default)
    {
        string symbolName = GetSymbolName(symbol);
        string databasePath = GetBookDepthDatabasePath(symbolName);
        if (!File.Exists(databasePath))
            return yearsReserved;

        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, "snapshot_time", null, ct);
        return latest.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(latest.Value).UtcDateTime : yearsReserved;
    }

    protected async Task<DateTime> GetLastTimestampAsync(
        string dbFolderPath,
        string symbolName,
        string columnName,
        string dataType,
        KlineInterval? interval = null,
        IReadOnlyDictionary<string, object?>? filters = null,
        CancellationToken ct = default)
    {
        try
        {
            string databasePath = GetSymbolDatabasePath(dbFolderPath, symbolName);
            long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, columnName, filters, ct);
            return latest.HasValue
                ? DateTimeOffset.FromUnixTimeMilliseconds(latest.Value).UtcDateTime
                : yearsReserved;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Get last time failed. DataType: {DataType}, Symbol: {Symbol}, Interval: {Interval}, Column: {Column}, DatabasePath: {DatabasePath}",
                dataType, symbolName, interval, columnName, GetSymbolDatabasePath(dbFolderPath, symbolName));
            throw;
        }
    }

    protected async Task DeleteSymbolRowsBeforeAsync(string dbFolderPath, string symbolName, string columnName, CancellationToken ct = default)
    {
        string dbPath = GetSymbolDatabasePath(dbFolderPath, symbolName);
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, SymbolDataTableName, columnName, new DateTimeOffset(yearsReserved).ToUnixTimeMilliseconds(), ct);
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, SymbolDataTableName, columnName, null, ct);
        if (!latest.HasValue)
            DeleteDatabaseFileIfExists(dbPath);
    }

    protected Task<List<string>> GetStoredSymbolNamesAsync(CancellationToken ct = default)
        => DuckDbStorageHelper.GetStringValuesAsync(SymbolInfoPath, MarketPathSegment, "Name", ct);

    protected async Task DeleteSymbolDatabasesAsync(IEnumerable<string> folderPaths, IReadOnlyCollection<string> currentSymbols, CancellationToken ct = default)
    {
        HashSet<string> currentSymbolSet = new(currentSymbols, StringComparer.OrdinalIgnoreCase);
        foreach (string folderPath in folderPaths)
        {
            foreach (string dbPath in GetStaleSymbolDatabasePaths(folderPath, currentSymbolSet))
            {
                ct.ThrowIfCancellationRequested();
                DeleteDatabaseFileIfExists(dbPath);
            }
        }
    }

    protected List<string> GetStaleSymbolDatabasePaths(string folderPath, IReadOnlySet<string> currentSymbols)
        => !Directory.Exists(folderPath)
            ? []
            : [.. Directory
                .EnumerateFiles(folderPath, "*.duckdb", SearchOption.TopDirectoryOnly)
                .Where(path => !currentSymbols.Contains(Path.GetFileNameWithoutExtension(path)))];

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
    protected abstract Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> currentSymbols, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default);

    protected abstract Task<Result<List<T>>> GetMarketAsync(CancellationToken ct = default);

    protected abstract Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(
        T symbol,
        DateTime downloadStartTime,
        CancellationToken ct = default);
    protected abstract Task<Result<MarketDataDownloadBatch>> GetBookDepthAsync(
        T symbol,
        DateTime downloadStartTime,
        CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<Kline>>> GetKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<PremiumIndexKline>>> GetPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<PremiumIndexKline>>> GetIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<PremiumIndexKline>>> GetMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<FundingRate>>> GetFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<OpenInterestHistory>>> GetOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<IReadOnlyList<FuturesBasisCsv>>> GetBasisAsync(T symbol, DateTime startTime, CancellationToken ct = default);

    protected Task DeleteOldAggTradesDataAsync(string symbolName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbolName);
        if (Directory.Exists(legacySymbolPath))
            Directory.Delete(legacySymbolPath, true);

        string databasePath = GetAggTradesDatabasePath(symbolName);
        if (!File.Exists(databasePath))
            return Task.CompletedTask;

        return DeleteOldAggTradesRowsAsync(databasePath, symbolName, ct);
    }

    protected async Task DeleteAggTradesStorageAsync(IReadOnlyCollection<string> currentSymbols, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default)
    {
        await DeleteSymbolDatabasesAsync([GetAggTradesStorageFolderPath()], currentSymbols, ct);

        foreach (string symbol in delistedSymbols)
        {
            ct.ThrowIfCancellationRequested();
            string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbol);
            if (Directory.Exists(legacySymbolPath))
                Directory.Delete(legacySymbolPath, true);
        }
    }

    protected Task DeleteOldBookDepthDataAsync(string symbolName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        string databasePath = GetBookDepthDatabasePath(symbolName);
        if (!File.Exists(databasePath))
            return Task.CompletedTask;

        return DeleteOldBookDepthRowsAsync(databasePath, symbolName, ct);
    }

    protected Task DeleteBookDepthStorageAsync(IReadOnlyCollection<string> currentSymbols, CancellationToken ct = default)
        => DeleteSymbolDatabasesAsync([GetBookDepthStorageFolderPath()], currentSymbols, ct);

    private async Task EnsureAggTradesStorageMigratedAsync(string symbolName, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        string legacySymbolPath = GetLegacyAggTradesSymbolPath(symbolName);
        if (!Directory.Exists(legacySymbolPath))
            return;

        string databasePath = GetAggTradesDatabasePath(symbolName);
        long? existingLatest = File.Exists(databasePath)
            ? await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, "transact_time", null, ct)
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
                    SymbolDataTableName,
                    csvPath,
                    GetAggTradesCsvSchema(),
                    GetAggTradesTimeUnitForTimestamp(GetMarketDataCsvSortKey(csvPath)) == AggTradesTimeUnit.Microseconds,
                    DuckDbStorageHelper.DatabaseInitializationProfile.LargeRowGroup,
                    ct);
            }

            Directory.Delete(legacySymbolPath, true);
            logger.LogInformation("Finish migrating aggTrades legacy storage. Market: {Market}, Symbol: {Symbol}, ImportedFiles: {ImportedFiles}",
                MarketPathSegment, symbolName, csvPaths.Length);
        }
        catch
        {
            throw;
        }
    }

    private string GetAggTradesStorageFolderPath()
        => Path.Combine(DataPath, BaseMarketData.AggTradesDataType, MarketPathSegment);

    private string GetAggTradesDatabasePath(string symbol)
        => GetSymbolDatabasePath(GetAggTradesStorageFolderPath(), symbol);

    private string GetBookDepthStorageFolderPath()
        => Path.Combine(DataPath, BaseMarketData.BookDepthDataType, MarketPathSegment);

    private string GetBookDepthDatabasePath(string symbol)
        => GetSymbolDatabasePath(GetBookDepthStorageFolderPath(), symbol);

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
        await DuckDbStorageHelper.NormalizeAggTradesStoredTimeAsync(databasePath, SymbolDataTableName, AggTradesMicrosecondsBoundary, ct);
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(
            databasePath,
            SymbolDataTableName,
            "transact_time",
            ToAggTradesUnixTime(yearsReserved),
            ct);

        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, "transact_time", null, ct);
        if (!latest.HasValue)
            DeleteDatabaseFileIfExists(databasePath);
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
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(databasePath, SymbolDataTableName, "snapshot_time", ToUnixMilliseconds(yearsReserved), ct);
        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(databasePath, SymbolDataTableName, "snapshot_time", null, ct);
        if (!latest.HasValue)
            DeleteDatabaseFileIfExists(databasePath);
    }

    private async Task MigrateLegacySharedDatabasesAsync(CancellationToken ct)
    {
        foreach ((string folderPath, DuckDbStorageHelper.DatabaseInitializationProfile profile) in GetLegacySharedDatabaseFolders())
            await MigrateLegacySharedDatabaseAsync(folderPath, profile, ct);
    }

    private IEnumerable<(string FolderPath, DuckDbStorageHelper.DatabaseInitializationProfile Profile)> GetLegacySharedDatabaseFolders()
    {
        yield return (KlinePath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);

        if (IsFutures)
        {
            yield return (PremiumIndexKlinePath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (IndexPriceKlinePath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (MarkPriceKlinePath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (FundingRatePath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (OpenInterestPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (TopLongShortPositionRatioPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (TopLongShortAccountRatioPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (GlobalLongShortAccountRatioPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (TakerLongShortRatioPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (BasisPath, DuckDbStorageHelper.DatabaseInitializationProfile.Default);
            yield return (GetBookDepthStorageFolderPath(), DuckDbStorageHelper.DatabaseInitializationProfile.LargeRowGroup);
        }

        yield return (GetAggTradesStorageFolderPath(), DuckDbStorageHelper.DatabaseInitializationProfile.LargeRowGroup);
    }

    private async Task MigrateLegacySharedDatabaseAsync(
        string marketFolderPath,
        DuckDbStorageHelper.DatabaseInitializationProfile profile,
        CancellationToken ct)
    {
        string legacyDatabasePath = GetLegacyMarketDatabasePath(marketFolderPath);
        if (!File.Exists(legacyDatabasePath))
            return;

        List<string> tableNames = await DuckDbStorageHelper.GetTableNamesAsync(legacyDatabasePath, ct);
        if (tableNames.Count == 0)
        {
            DeleteDatabaseFileIfExists(legacyDatabasePath);
            return;
        }

        logger.LogInformation(
            "Start migrating legacy shared DuckDB. Market: {Market}, LegacyDatabasePath: {LegacyDatabasePath}, TableCount: {TableCount}",
            MarketPathSegment,
            legacyDatabasePath,
            tableNames.Count);

        foreach (string symbolName in tableNames)
        {
            ct.ThrowIfCancellationRequested();
            string targetDatabasePath = GetSymbolDatabasePath(marketFolderPath, symbolName);
            await DuckDbStorageHelper.CopyTableAsync(
                legacyDatabasePath,
                symbolName,
                targetDatabasePath,
                SymbolDataTableName,
                profile,
                ct);
        }

        DeleteDatabaseFileIfExists(legacyDatabasePath);

        logger.LogInformation(
            "Finish migrating legacy shared DuckDB. Market: {Market}, LegacyDatabasePath: {LegacyDatabasePath}, MigratedSymbols: {MigratedSymbols}",
            MarketPathSegment,
            legacyDatabasePath,
            tableNames.Count);
    }

    private static string GetSymbolDatabasePath(string folderPath, string symbol)
        => Path.Combine(folderPath, $"{symbol}.duckdb");

    private static string GetLegacyMarketDatabasePath(string marketFolderPath)
        => Path.Combine(Path.GetDirectoryName(marketFolderPath)!, Path.GetFileName(marketFolderPath) + ".duckdb");

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

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
    }

    private static void DeleteDatabaseFileIfExists(string databasePath)
    {
        if (!File.Exists(databasePath))
            return;

        File.Delete(databasePath);
        DeleteDirectoryIfEmpty(Path.GetDirectoryName(databasePath));
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
