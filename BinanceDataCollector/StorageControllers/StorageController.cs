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

internal abstract class StorageController<T, T1>
    where T : class
    where T1 : BinanceKline
{
    protected readonly IServiceProvider serviceProvider;
    protected readonly ILogger logger;
    protected readonly DateTime yearsReserved;
    protected readonly static BulkConfig bulkConfig = new() { UseTempDB = true, BatchSize = 14400 };
    protected static string DataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Data");
    protected static string RootKlinePath = Path.Combine(DataPath, "Kline");
    protected abstract string KlinePath { get; }

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

    public async Task ExportToCsvAsync(CancellationToken ct = default)
    {
        Result<string[]> symbolNamesResult = await GetAllSymbolNamesAsync(ct);
        if (symbolNamesResult.IsFailed)
        {
            logger.LogError(symbolNamesResult.Errors[0].Message);
            return;
        }

        if (Directory.Exists(KlinePath))
            Directory.Delete(KlinePath);
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
    }

    protected async Task InsertKlinesAsync(IList<T1> klines, CancellationToken ct = default)
    {
        if (!klines.Any())
            return;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            logger.LogDebug($"Start inserting {typeof(T1).Name} Count: {klines.Count}...");
            Dictionary<DbContext, IEnumerable<T1>> bulkShardingEnumerable = db.BulkShardingTableEnumerable(klines);
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            foreach (KeyValuePair<DbContext, IEnumerable<T1>> item in bulkShardingEnumerable)
                await item.Key.BulkInsertOrUpdateAsync(item.Value.ToArray(), bulkConfig, cancellationToken: ct);
            transaction.Commit();
            logger.LogDebug($"Finish inserting.");
        }
        catch (Exception ex)
        {
            logger.LogError($"Symbol: {klines[0].SymbolInfoId}, Interval: {klines[0].Interval}, Message: {ex.Message}");
        }
    }

    public abstract Task<DateTime> GetLastTimeAsync(T symbol, KlineInterval interval, CancellationToken ct = default);

    public abstract Task DeleteOldKlines(CancellationToken ct = default);

    protected abstract Task<Result<List<T>>> GetMarketAsync(CancellationToken ct = default);

    protected abstract Task<Result<List<T1>>> GetKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);

    protected abstract Task<Result<string[]>> GetAllSymbolNamesAsync(CancellationToken ct = default);

    protected abstract Task<Result<Kline[]>> GetCsvKlinesAsync(string symbol, CancellationToken ct = default);

    protected static string CombineKlineId(string symbol, KlineInterval interval, DateTime closeTime)
        => $"{symbol}-{interval}-{closeTime:s}";
}
