using BinanceDataCollector.Collectors.BinanceMarketData;
using BinanceDataCollector.WorkItems;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using EFCore.BulkExtensions;
using Magicodes.ExporterAndImporter.Csv;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;

namespace BinanceDataCollector.StorageControllers;

internal abstract class StorageController<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>
    where T : class
    where T1 : BinanceKline
    where T2 : BinanceMarkIndexKline
    where T3 : BinanceMarkIndexKline
    where T4 : BinanceMarkIndexKline
    where T5 : FuturesFundingRate
    where T6 : FuturesOpenInterestHistory
    where T7 : FuturesLongShortRatio
    where T8 : FuturesLongShortRatio
    where T9 : FuturesLongShortRatio
    where T10 : FuturesTakerLongShortRatio
    where T11 : FuturesBasis
{
    protected readonly IServiceProvider serviceProvider;
    protected readonly ILogger logger;
    protected readonly DateTime yearsReserved;
    protected readonly static BulkConfig bulkConfig = new() { UseTempDB = true, BatchSize = 14400 };
    protected static string DataPath = CsvExportArchiveHelper.WorkRootPath;
    protected static string MarketDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "BinanceMarketData");
    protected static string MarketDataTempPath = Path.Combine(CsvExportArchiveHelper.TmpPath, "BinanceMarketData");
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
    protected static string RootSymbolInfoPath = Path.Combine(DataPath, "SymbolInfo");
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

        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            string[] currentSymbols = [.. result.Value.Select(GetSymbolName)];
            List<string> existingSymbols = await GetExistingSymbolNamesAsync(db, ct);
            string[] delistedSymbols = [.. existingSymbols.Except(currentSymbols)];

            logger.LogInformation("Start syncing {SymbolType}. MarketCount: {MarketCount}, DelistedCount: {DelistedCount}", typeof(T).Name, result.Value.Count, delistedSymbols.Length);

            Stopwatch upsertStopwatch = Stopwatch.StartNew();
            using (IDbContextTransaction transaction = db.Database.BeginTransaction())
            {
                await db.BulkInsertOrUpdateAsync(result.Value, bulkConfig, cancellationToken: ct);
                transaction.Commit();
            }
            upsertStopwatch.Stop();
            logger.LogInformation("Finish upserting {SymbolType}. Cost: {ElapsedMs}ms", typeof(T).Name, upsertStopwatch.ElapsedMilliseconds);

            if (delistedSymbols.Length > 0)
            {
                Stopwatch deleteStopwatch = Stopwatch.StartNew();
                await DeleteDelistedSymbolsAsync(db, delistedSymbols, ct);
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

    public async Task<AsyncWorkItem<IList<T1>>> UpdateKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T1).Name, symbol, interval, startTime);
        Result<List<T1>> result = await GetKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T1).Name, symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T1).Name, symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T3>>> UpdateIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T3).Name, symbol, interval, startTime);
        Result<List<T3>> result = await GetIndexPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T3).Name, symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T3).Name, symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T4>>> UpdateMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T4).Name, symbol, interval, startTime);
        Result<List<T4>> result = await GetMarkPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T4).Name, symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T4).Name, symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T2>>> UpdatePremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T2).Name, symbol, interval, startTime);
        Result<List<T2>> result = await GetPremiumIndexKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, Interval: {Interval}, StartTime: {StartTime}", typeof(T2).Name, symbol, interval, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T2).Name, symbol, result.Errors[0].Message, interval, startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T5>>> UpdateFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T5).Name, symbol, startTime);
        Result<List<T5>> result = await GetFundingRatesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T5).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T5).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T5>>(InsertFundingRatesAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T5>>(InsertFundingRatesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T6>>> UpdateOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T6).Name, symbol, startTime);
        Result<List<T6>> result = await GetOpenInterestHistoriesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T6).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T6).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T6>>(InsertOpenInterestHistoriesAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T6>>(InsertOpenInterestHistoriesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T7>>> UpdateTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T7).Name, symbol, startTime);
        Result<List<T7>> result = await GetTopLongShortPositionRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T7).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T7).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T7>>(InsertLongShortRatiosAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T7>>(InsertLongShortRatiosAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T8>>> UpdateTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T8).Name, symbol, startTime);
        Result<List<T8>> result = await GetTopLongShortAccountRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T8).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T8).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T8>>(InsertLongShortRatiosAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T8>>(InsertLongShortRatiosAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T9>>> UpdateGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T9).Name, symbol, startTime);
        Result<List<T9>> result = await GetGlobalLongShortAccountRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T9).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T9).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T9>>(InsertLongShortRatiosAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T9>>(InsertLongShortRatiosAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T10>>> UpdateTakerLongShortRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T10).Name, symbol, startTime);
        Result<List<T10>> result = await GetTakerLongShortRatiosAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T10).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T10).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T10>>(InsertTakerLongShortRatiosAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T10>>(InsertTakerLongShortRatiosAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T11>>> UpdateBasisAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug("Start getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T11).Name, symbol, startTime);
        Result<List<T11>> result = await GetBasisAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting {DataType}. Symbol: {Symbol}, StartTime: {StartTime}", typeof(T11).Name, symbol, startTime);
        if (result.IsFailed)
        {
            LogSyncFailure(typeof(T11).Name, symbol, result.Errors[0].Message, startTime: startTime);
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T11>>(InsertBasisAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T11>>(InsertBasisAsync, result.Value, ct);
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

    protected void LogCsvExportFailure(string dataType, string symbol, string message)
        => logger.LogError("Csv export failed. DataType: {DataType}, Symbol: {Symbol}, Message: {Message}",
            dataType, symbol, message);

    protected TEntity[] DeduplicateById<TEntity>(IList<TEntity> entities, Func<TEntity, string> getId)
    {
        if (entities.Count <= 1)
            return [.. entities];

        Dictionary<string, TEntity> uniqueEntities = [];
        foreach (TEntity entity in entities)
            uniqueEntities[getId(entity)] = entity;

        if (uniqueEntities.Count != entities.Count)
            logger.LogWarning("Deduplicated insert batch. DataType: {DataType}, OriginalCount: {OriginalCount}, UniqueCount: {UniqueCount}",
                typeof(TEntity).Name, entities.Count, uniqueEntities.Count);

        return [.. uniqueEntities.Values];
    }

    public async Task ExportToCsvAsync(CancellationToken ct = default)
    {
        logger.LogInformation("Start csv export. StorageController: {StorageController}", GetType().Name);
        Result<string[]> symbolNamesResult = await GetAllSymbolNamesAsync(ct);
        if (symbolNamesResult.IsFailed)
        {
            logger.LogError(symbolNamesResult.Errors[0].Message);
            return;
        }

        logger.LogInformation("Csv export symbols loaded. StorageController: {StorageController}, SymbolCount: {SymbolCount}", GetType().Name, symbolNamesResult.Value.Length);

        if (Directory.Exists(SymbolInfoPath))
            Directory.Delete(SymbolInfoPath, true);
        Directory.CreateDirectory(SymbolInfoPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, "SymbolInfo");

        Result<SymbolInfoCsv[]> symbolInfosResult = await GetCsvSymbolInfosAsync(ct);
        if (symbolInfosResult.IsFailed)
            logger.LogError(symbolInfosResult.Errors[0].Message);
        else
        {
            string symbolInfoPath = Path.Combine(SymbolInfoPath, "symbols.csv");
            CsvExporter exporter = new();
            await exporter.Export(symbolInfoPath, symbolInfosResult.Value);
        }

        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, "SymbolInfo");

        if (Directory.Exists(KlinePath))
            Directory.Delete(KlinePath, true);
        Directory.CreateDirectory(KlinePath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(Kline));


        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<Kline[]> klinesResult = await GetCsvKlinesAsync(symbol, ct);
            if (klinesResult.IsFailed)
            {
                LogCsvExportFailure(nameof(Kline), symbol, klinesResult.Errors[0].Message);
                return;
            }

            string path = Path.Combine(KlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(path, klinesResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(Kline));

        if (!IsFutures)
        {
            logger.LogInformation("Finish csv export. StorageController: {StorageController}", GetType().Name);
            return;
        }

        if (Directory.Exists(PremiumIndexKlinePath))
            Directory.Delete(PremiumIndexKlinePath, true);
        Directory.CreateDirectory(PremiumIndexKlinePath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(PremiumIndexKline));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> premiumIndexKlinesResult = await GetCsvPremiumIndexKlinesAsync(symbol, ct);
            if (premiumIndexKlinesResult.IsFailed)
            {
                LogCsvExportFailure(nameof(PremiumIndexKline), symbol, premiumIndexKlinesResult.Errors[0].Message);
                return;
            }

            string premiumIndexPath = Path.Combine(PremiumIndexKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(premiumIndexPath, premiumIndexKlinesResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(PremiumIndexKline));

        if (Directory.Exists(IndexPriceKlinePath))
            Directory.Delete(IndexPriceKlinePath, true);
        Directory.CreateDirectory(IndexPriceKlinePath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: IndexPriceKline", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> indexPriceKlinesResult = await GetCsvIndexPriceKlinesAsync(symbol, ct);
            if (indexPriceKlinesResult.IsFailed)
            {
                LogCsvExportFailure("IndexPriceKline", symbol, indexPriceKlinesResult.Errors[0].Message);
                return;
            }

            string indexPricePath = Path.Combine(IndexPriceKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(indexPricePath, indexPriceKlinesResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: IndexPriceKline", GetType().Name);

        if (Directory.Exists(MarkPriceKlinePath))
            Directory.Delete(MarkPriceKlinePath, true);
        Directory.CreateDirectory(MarkPriceKlinePath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: MarkPriceKline", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> markPriceKlinesResult = await GetCsvMarkPriceKlinesAsync(symbol, ct);
            if (markPriceKlinesResult.IsFailed)
            {
                LogCsvExportFailure("MarkPriceKline", symbol, markPriceKlinesResult.Errors[0].Message);
                return;
            }

            string markPricePath = Path.Combine(MarkPriceKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(markPricePath, markPriceKlinesResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: MarkPriceKline", GetType().Name);

        if (Directory.Exists(FundingRatePath))
            Directory.Delete(FundingRatePath, true);
        Directory.CreateDirectory(FundingRatePath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(FundingRate));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<FundingRate[]> fundingRateResult = await GetCsvFundingRatesAsync(symbol, ct);
            if (fundingRateResult.IsFailed)
            {
                LogCsvExportFailure(nameof(FundingRate), symbol, fundingRateResult.Errors[0].Message);
                return;
            }

            string fundingRatePath = Path.Combine(FundingRatePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(fundingRatePath, fundingRateResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(FundingRate));

        if (Directory.Exists(OpenInterestPath))
            Directory.Delete(OpenInterestPath, true);
        Directory.CreateDirectory(OpenInterestPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(OpenInterestHistory));

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<OpenInterestHistory[]> openInterestResult = await GetCsvOpenInterestHistoriesAsync(symbol, ct);
            if (openInterestResult.IsFailed)
            {
                LogCsvExportFailure(nameof(OpenInterestHistory), symbol, openInterestResult.Errors[0].Message);
                return;
            }

            string openInterestPath = Path.Combine(OpenInterestPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(openInterestPath, openInterestResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: {DataType}", GetType().Name, nameof(OpenInterestHistory));

        if (Directory.Exists(BasisPath))
            Directory.Delete(BasisPath, true);
        Directory.CreateDirectory(BasisPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: Basis", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<FuturesBasisCsv[]> basisResult = await GetCsvBasisAsync(symbol, ct);
            if (basisResult.IsFailed)
            {
                LogCsvExportFailure("Basis", symbol, basisResult.Errors[0].Message);
                return;
            }

            string basisPath = Path.Combine(BasisPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(basisPath, basisResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: Basis", GetType().Name);

        if (Directory.Exists(TopLongShortPositionRatioPath))
            Directory.Delete(TopLongShortPositionRatioPath, true);
        Directory.CreateDirectory(TopLongShortPositionRatioPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: TopLongShortPositionRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvTopLongShortPositionRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogCsvExportFailure("TopLongShortPositionRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TopLongShortPositionRatioPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(ratioPath, ratioResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: TopLongShortPositionRatio", GetType().Name);

        if (Directory.Exists(TopLongShortAccountRatioPath))
            Directory.Delete(TopLongShortAccountRatioPath, true);
        Directory.CreateDirectory(TopLongShortAccountRatioPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: TopLongShortAccountRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvTopLongShortAccountRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogCsvExportFailure("TopLongShortAccountRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TopLongShortAccountRatioPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(ratioPath, ratioResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: TopLongShortAccountRatio", GetType().Name);

        if (Directory.Exists(GlobalLongShortAccountRatioPath))
            Directory.Delete(GlobalLongShortAccountRatioPath, true);
        Directory.CreateDirectory(GlobalLongShortAccountRatioPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: GlobalLongShortAccountRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<LongShortRatioCsv[]> ratioResult = await GetCsvGlobalLongShortAccountRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogCsvExportFailure("GlobalLongShortAccountRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(GlobalLongShortAccountRatioPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(ratioPath, ratioResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: GlobalLongShortAccountRatio", GetType().Name);

        if (Directory.Exists(TakerLongShortRatioPath))
            Directory.Delete(TakerLongShortRatioPath, true);
        Directory.CreateDirectory(TakerLongShortRatioPath);
        logger.LogInformation("Start csv export section. StorageController: {StorageController}, DataType: TakerLongShortRatio", GetType().Name);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<TakerLongShortRatioCsv[]> ratioResult = await GetCsvTakerLongShortRatiosAsync(symbol, ct);
            if (ratioResult.IsFailed)
            {
                LogCsvExportFailure("TakerLongShortRatio", symbol, ratioResult.Errors[0].Message);
                return;
            }

            string ratioPath = Path.Combine(TakerLongShortRatioPath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(ratioPath, ratioResult.Value);
        });
        logger.LogInformation("Finish csv export section. StorageController: {StorageController}, DataType: TakerLongShortRatio", GetType().Name);
        logger.LogInformation("Finish csv export. StorageController: {StorageController}", GetType().Name);
    }

    protected async Task InsertKlinesAsync<TKline>(IList<TKline> klines, CancellationToken ct = default)
        where TKline : BinanceMarkIndexKline
    {
        if (!klines.Any())
            return;
        TKline[] uniqueKlines = DeduplicateById(klines, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(TKline).Name} Count: {uniqueKlines.Length}...");
            Dictionary<DbContext, IEnumerable<TKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueKlines);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<TKline>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug($"Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {klines[0].SymbolInfoId}, Interval: {klines[0].Interval}, Message: {ex.Message}");
        }
    }

    protected async Task InsertLongShortRatiosAsync<TLongShortRatio>(IList<TLongShortRatio> ratios, CancellationToken ct = default)
        where TLongShortRatio : FuturesLongShortRatio
    {
        if (!ratios.Any())
            return;
        TLongShortRatio[] uniqueRatios = DeduplicateById(ratios, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(TLongShortRatio).Name} Count: {uniqueRatios.Length}...");
            Dictionary<DbContext, IEnumerable<TLongShortRatio>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueRatios);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<TLongShortRatio>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {ratios[0].SymbolInfoId}, Message: {ex.Message}");
        }
    }

    protected async Task InsertTakerLongShortRatiosAsync(IList<T10> ratios, CancellationToken ct = default)
    {
        if (!ratios.Any())
            return;
        T10[] uniqueRatios = DeduplicateById(ratios, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T10).Name} Count: {uniqueRatios.Length}...");
            Dictionary<DbContext, IEnumerable<T10>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueRatios);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<T10>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {ratios[0].SymbolInfoId}, Message: {ex.Message}");
        }
    }

    protected async Task InsertBasisAsync(IList<T11> histories, CancellationToken ct = default)
    {
        if (!histories.Any())
            return;
        T11[] uniqueHistories = DeduplicateById(histories, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T11).Name} Count: {uniqueHistories.Length}...");
            Dictionary<DbContext, IEnumerable<T11>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueHistories);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<T11>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {histories[0].SymbolInfoId}, Message: {ex.Message}");
        }
    }

    protected async Task InsertOpenInterestHistoriesAsync(IList<T6> openInterestHistories, CancellationToken ct = default)
    {
        if (!openInterestHistories.Any())
            return;
        T6[] uniqueOpenInterestHistories = DeduplicateById(openInterestHistories, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T6).Name} Count: {uniqueOpenInterestHistories.Length}...");
            Dictionary<DbContext, IEnumerable<T6>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueOpenInterestHistories);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<T6>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {openInterestHistories[0].SymbolInfoId}, Message: {ex.Message}");
        }
    }

    protected async Task InsertFundingRatesAsync(IList<T5> rates, CancellationToken ct = default)
    {
        if (!rates.Any())
            return;
        T5[] uniqueRates = DeduplicateById(rates, item => item.Id);
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T5).Name} Count: {uniqueRates.Length}...");
            Dictionary<DbContext, IEnumerable<T5>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(uniqueRates);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<T5>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug("Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {rates[0].SymbolInfoId}, Message: {ex.Message}");
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
    protected abstract Task<List<string>> GetExistingSymbolNamesAsync(BinanceDbContext db, CancellationToken ct = default);
    protected abstract Task DeleteDelistedSymbolsAsync(BinanceDbContext db, IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default);

    protected abstract Task<Result<List<T>>> GetMarketAsync(CancellationToken ct = default);

    protected abstract Task<Result<MarketDataDownloadBatch>> GetAggTradesAsync(
        T symbol,
        (DateTime DownloadStartTime, DateTime? MonthlyLatestPeriodStart, DateTime? DailyLatestPeriodStart) syncState,
        CancellationToken ct = default);
    protected abstract Task<Result<List<T1>>> GetKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T2>>> GetPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T3>>> GetIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T4>>> GetMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T5>>> GetFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T6>>> GetOpenInterestHistoriesAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T7>>> GetTopLongShortPositionRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T8>>> GetTopLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T9>>> GetGlobalLongShortAccountRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T10>>> GetTakerLongShortRatiosAsync(T symbol, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T11>>> GetBasisAsync(T symbol, DateTime startTime, CancellationToken ct = default);

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
