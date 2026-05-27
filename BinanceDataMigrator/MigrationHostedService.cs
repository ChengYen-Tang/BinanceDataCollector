using Microsoft.Extensions.Options;

internal sealed class MigrationHostedService(
    SqlServerToDuckDbMigrator migrator,
    IHostApplicationLifetime hostApplicationLifetime,
    ILogger<MigrationHostedService> logger,
    IOptions<MigrationOptions> options) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            MigrationOptions currentOptions = options.Value;
            logger.LogInformation(
                "Start SQL Server -> DuckDB migration. StorageRootPath: {StorageRootPath}, BatchSize: {BatchSize}, MaxParallelSymbols: {MaxParallelSymbols}",
                currentOptions.StorageRootPath,
                currentOptions.BatchSize,
                currentOptions.MaxParallelSymbols);

            await migrator.RunAsync(stoppingToken);

            logger.LogInformation("SQL Server -> DuckDB migration completed.");
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning("SQL Server -> DuckDB migration canceled.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "SQL Server -> DuckDB migration failed.");
        }
        finally
        {
            hostApplicationLifetime.StopApplication();
        }
    }
}
