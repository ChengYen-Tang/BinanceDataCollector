using BinanceDataCollector.WorkItems;
using CollectorModels;
using CollectorModels.Models;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore.Storage;

namespace BinanceDataCollector.StorageControllers;

internal abstract class StorageController<T, T1>
    where T : class
    where T1 : BinanceKline
{
    protected readonly IServiceProvider serviceProvider;
    protected readonly ILogger logger;
    protected readonly DateTime yearsReserved;
    protected readonly static BulkConfig bulkConfig = new() { UseTempDB= true, BatchSize = 14400 };

    public StorageController(IServiceProvider serviceProvider, ILogger logger)
        => (this.serviceProvider, this.logger, yearsReserved) = (serviceProvider, logger, DateTime.Today.AddYears(-1));

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
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            await db.BulkInsertOrUpdateOrDeleteAsync(result.Value, bulkConfig, cancellationToken: ct);
            transaction.Commit();
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
        return Result.Ok(result.Value);
    }

    public async Task<AsyncWorkItem<IList<T1>>> UpdateKlinesAsync(T symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        Result<List<T1>> result = await GetKlinesAsync(symbol, interval, startTime, ct);
        if (result.IsFailed)
        {
            logger.LogError(result.Errors[0].Message);
            return new(InsertKlinesAsync, new List<T1>(), ct);
        }

        return new(InsertKlinesAsync, result.Value, ct);
    }

    private async Task InsertKlinesAsync(IList<T1> klines, CancellationToken ct = default)
    {
        if (!klines.Any())
            return;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider service = scope.ServiceProvider;
        using BinanceDbContext db = service.GetService<BinanceDbContext>()!;
        try
        {
            using IDbContextTransaction transaction = db.Database.BeginTransaction();
            await db.BulkInsertAsync(klines, bulkConfig, cancellationToken: ct);
            transaction.Commit();
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

    protected string CombineKlineId(string symbol, KlineInterval interval, DateTime closeTime)
        => $"{symbol}-{interval}-{closeTime:s}";
}
