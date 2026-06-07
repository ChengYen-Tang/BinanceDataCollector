namespace BinanceDataIntegrityChecker;

public sealed class IntegrityCheckHostedService(
    MarketDataIntegrityChecker checker,
    IHostApplicationLifetime applicationLifetime,
    ILogger<IntegrityCheckHostedService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            checker.Run(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning("DuckDB integrity check was cancelled.");
        }
        finally
        {
            applicationLifetime.StopApplication();
        }

        return Task.CompletedTask;
    }
}
