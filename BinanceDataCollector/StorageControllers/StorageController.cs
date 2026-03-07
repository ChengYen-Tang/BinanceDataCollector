using BinanceDataCollector.WorkItems;
using CollectorModels;
using CollectorModels.Models;
using CollectorModels.Models.Csv;
using CollectorModels.ShardingCore;
using EFCore.BulkExtensions;
using Magicodes.ExporterAndImporter.Csv;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal abstract class StorageController<T, T1, T2, T3, T4, T5>
    where T : class
    where T1 : BinanceKline
    where T2 : BinanceMarkIndexKline
    where T3 : BinanceMarkIndexKline
    where T4 : BinanceMarkIndexKline
    where T5 : FuturesFundingRate
{
    protected readonly IServiceProvider serviceProvider;
    protected readonly ILogger logger;
    protected readonly DateTime yearsReserved;
    protected readonly static BulkConfig bulkConfig = new() { UseTempDB = true, BatchSize = 14400 };
    protected static string DataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Data");
    protected static string RootKlinePath = Path.Combine(DataPath, "Kline");
    protected static string RootPremiumIndexKlinePath = Path.Combine(DataPath, "PremiumIndexKline");
    protected static string RootIndexPriceKlinePath = Path.Combine(DataPath, "IndexPriceKline");
    protected static string RootMarkPriceKlinePath = Path.Combine(DataPath, "MarkPriceKline");
    protected static string RootFundingRatePath = Path.Combine(DataPath, "FundingRate");
    protected abstract string KlinePath { get; }
    protected abstract string PremiumIndexKlinePath { get; }
    protected abstract string IndexPriceKlinePath { get; }
    protected abstract string MarkPriceKlinePath { get; }
    protected abstract string FundingRatePath { get; }
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
            logger.LogDebug($"Start updating {typeof(T).Name} Count: {result.Value.Count}...");
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            await db.BulkInsertOrUpdateOrDeleteAsync(result.Value, bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug($"Finish updating.");
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
        return Result.Ok(result.Value);
    }

    public async Task<AsyncWorkItem<IList<T1>>> UpdateKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug($"Start getting {typeof(T1).Name} {symbol} {interval} {startTime}...");
        Result<List<T1>> result = await GetKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug($"Finish getting.");
        if (result.IsFailed)
        {
            logger.LogError($"Symbol:{symbol}, Interval: {interval}, Message: {result.Errors[0].Message}");
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T3>>> UpdateIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug($"Start getting {typeof(T3).Name} IndexPrice {symbol} {interval} {startTime}...");
        Result<List<T3>> result = await GetIndexPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting.");
        if (result.IsFailed)
        {
            logger.LogError($"Symbol:{symbol}, Interval: {interval}, Message: {result.Errors[0].Message}");
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T4>>> UpdateMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug($"Start getting {typeof(T4).Name} MarkPrice {symbol} {interval} {startTime}...");
        Result<List<T4>> result = await GetMarkPriceKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug("Finish getting.");
        if (result.IsFailed)
        {
            logger.LogError($"Symbol:{symbol}, Interval: {interval}, Message: {result.Errors[0].Message}");
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T2>>> UpdatePremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug($"Start getting {typeof(T2).Name} {symbol} {interval} {startTime}...");
        Result<List<T2>> result = await GetPremiumIndexKlinesAsync(symbol, interval, startTime, ct);
        logger.LogDebug($"Finish getting.");
        if (result.IsFailed)
        {
            logger.LogError($"Symbol:{symbol}, Interval: {interval}, Message: {result.Errors[0].Message}");
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new(InsertKlinesAsync, [], ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    public async Task<AsyncWorkItem<IList<T5>>> UpdateFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default)
    {
        logger.LogDebug($"Start getting {typeof(T5).Name} {symbol} {startTime}...");
        Result<List<T5>> result = await GetFundingRatesAsync(symbol, startTime, ct);
        logger.LogDebug("Finish getting.");
        if (result.IsFailed)
        {
            logger.LogError($"Symbol:{symbol}, Message: {result.Errors[0].Message}");
            if (result.Errors[0].Message != "Invalid symbol.")
                await Task.Delay(30 * 60 * 1000, ct);
            return new AsyncWorkItem<IList<T5>>(InsertFundingRatesAsync, [], ct);
        }

        return new AsyncWorkItem<IList<T5>>(InsertFundingRatesAsync, result.Value, ct);
    }

    public async Task ExportToCsvAsync(CancellationToken ct = default)
    {
        Result<string[]> symbolNamesResult = await GetAllSymbolNamesAsync(ct);
        if (symbolNamesResult.IsFailed)
        {
            logger.LogError(symbolNamesResult.Errors[0].Message);
            return;
        }

        if (Directory.Exists(KlinePath))
            Directory.Delete(KlinePath, true);
        Directory.CreateDirectory(KlinePath);


        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<Kline[]> klinesResult = await GetCsvKlinesAsync(symbol, ct);
            if (klinesResult.IsFailed)
            {
                logger.LogError(klinesResult.Errors[0].Message);
                return;
            }

            string path = Path.Combine(KlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(path, klinesResult.Value);
        });

        if (!IsFutures)
            return;

        if (Directory.Exists(PremiumIndexKlinePath))
            Directory.Delete(PremiumIndexKlinePath, true);
        Directory.CreateDirectory(PremiumIndexKlinePath);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> premiumIndexKlinesResult = await GetCsvPremiumIndexKlinesAsync(symbol, ct);
            if (premiumIndexKlinesResult.IsFailed)
            {
                logger.LogError(premiumIndexKlinesResult.Errors[0].Message);
                return;
            }

            string premiumIndexPath = Path.Combine(PremiumIndexKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(premiumIndexPath, premiumIndexKlinesResult.Value);
        });

        if (Directory.Exists(IndexPriceKlinePath))
            Directory.Delete(IndexPriceKlinePath, true);
        Directory.CreateDirectory(IndexPriceKlinePath);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> indexPriceKlinesResult = await GetCsvIndexPriceKlinesAsync(symbol, ct);
            if (indexPriceKlinesResult.IsFailed)
            {
                logger.LogError(indexPriceKlinesResult.Errors[0].Message);
                return;
            }

            string indexPricePath = Path.Combine(IndexPriceKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(indexPricePath, indexPriceKlinesResult.Value);
        });

        if (Directory.Exists(MarkPriceKlinePath))
            Directory.Delete(MarkPriceKlinePath, true);
        Directory.CreateDirectory(MarkPriceKlinePath);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<PremiumIndexKline[]> markPriceKlinesResult = await GetCsvMarkPriceKlinesAsync(symbol, ct);
            if (markPriceKlinesResult.IsFailed)
            {
                logger.LogError(markPriceKlinesResult.Errors[0].Message);
                return;
            }

            string markPricePath = Path.Combine(MarkPriceKlinePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(markPricePath, markPriceKlinesResult.Value);
        });

        if (Directory.Exists(FundingRatePath))
            Directory.Delete(FundingRatePath, true);
        Directory.CreateDirectory(FundingRatePath);

        await Parallel.ForEachAsync(symbolNamesResult.Value, ct, async (symbol, ct) =>
        {
            Result<FundingRate[]> fundingRateResult = await GetCsvFundingRatesAsync(symbol, ct);
            if (fundingRateResult.IsFailed)
            {
                logger.LogError(fundingRateResult.Errors[0].Message);
                return;
            }

            string fundingRatePath = Path.Combine(FundingRatePath, $"{symbol}.csv");
            CsvExporter exporter = new();
            await exporter.Export(fundingRatePath, fundingRateResult.Value);
        });
    }

    protected async Task InsertKlinesAsync<TKline>(IList<TKline> klines, CancellationToken ct = default)
        where TKline : BinanceMarkIndexKline
    {
        if (!klines.Any())
            return;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(TKline).Name} Count: {klines.Count}...");
            Dictionary<DbContext, IEnumerable<TKline>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
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

    protected async Task InsertFundingRatesAsync(IList<T5> rates, CancellationToken ct = default)
    {
        if (!rates.Any())
            return;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T5).Name} Count: {rates.Count}...");
            Dictionary<DbContext, IEnumerable<T5>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(rates);
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

    public abstract Task<DateTime> GetLastTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public abstract Task<DateTime> GetLastPremiumIndexTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);
    public virtual Task<DateTime> GetLastIndexPriceTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default)
        => Task.FromResult(yearsReserved);
    public virtual Task<DateTime> GetLastMarkPriceTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default)
        => Task.FromResult(yearsReserved);
    public abstract Task<DateTime> GetLastFundingTimeAsync(T symbol, CancellationToken ct = default);

    public abstract Task DeleteOldData(CancellationToken ct = default);

    protected abstract Task<Result<List<T>>> GetMarketAsync(CancellationToken ct = default);

    protected abstract Task<Result<List<T1>>> GetKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected abstract Task<Result<List<T2>>> GetPremiumIndexKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    protected virtual Task<Result<List<T3>>> GetIndexPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => Task.FromResult(Result.Fail<List<T3>>("Not supported."));
    protected virtual Task<Result<List<T4>>> GetMarkPriceKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        => Task.FromResult(Result.Fail<List<T4>>("Not supported."));
    protected abstract Task<Result<List<T5>>> GetFundingRatesAsync(T symbol, DateTime startTime, CancellationToken ct = default);

    protected abstract Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default);

    protected abstract Task<Result<Kline[]>> GetCsvKlinesAsync(string symbol, CancellationToken ct = default);

    protected abstract Task<Result<PremiumIndexKline[]>> GetCsvPremiumIndexKlinesAsync(string symbol, CancellationToken ct = default);
    protected virtual Task<Result<PremiumIndexKline[]>> GetCsvIndexPriceKlinesAsync(string symbol, CancellationToken ct = default)
        => Task.FromResult(Result.Fail<PremiumIndexKline[]>("Not supported."));
    protected virtual Task<Result<PremiumIndexKline[]>> GetCsvMarkPriceKlinesAsync(string symbol, CancellationToken ct = default)
        => Task.FromResult(Result.Fail<PremiumIndexKline[]>("Not supported."));
    protected abstract Task<Result<FundingRate[]>> GetCsvFundingRatesAsync(string symbol, CancellationToken ct = default);

    protected static string CombineKlineId(string symbol, KlineInterval interval, DateTime closeTime)
        => $"{symbol}-{interval}-{closeTime:s}";

    protected static string CombineFundingRateId(string symbol, DateTime fundingTime)
        => $"{symbol}-{fundingTime:s}";
}
