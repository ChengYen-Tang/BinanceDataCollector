using BinanceDataCollector.Collectors.CollectorControllers;
using CollectorModels;
using Hangfire;
using Hangfire.InMemory;
using Microsoft.EntityFrameworkCore;

namespace BinanceDataCollector;

internal class Worker(ILogger<Worker> logger, IServiceProvider serviceProvider, HangfireJob hangfireJob, ProductionLine productionLine) : IHostedService
{
    private readonly CancellationTokenSource cts = new();
    private BackgroundJobServer backgroundJobServer = null!;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider scopeServiceProvider = scope.ServiceProvider;
        using BinanceDbContext db = scopeServiceProvider.GetRequiredService<BinanceDbContext>();
        _ = db.Model;
        if (db.Database.GetPendingMigrations().Any())
            db.Database.Migrate();
        _ = db.Model;
        productionLine.Start();
        GlobalConfiguration.Configuration
                .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                .UseColouredConsoleLogProvider()
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UseActivator(new HangfireActivator(serviceProvider))
                .UseInMemoryStorage(new InMemoryStorageOptions
                {
                    MaxExpirationTime = TimeSpan.FromDays(30)
                });

        RecurringJob.AddOrUpdate("Sync", () => hangfireJob.RunJob(cts.Token), Cron.Daily, new() { TimeZone = TimeZoneInfo.Utc });
        RecurringJob.TriggerJob("Sync");
        backgroundJobServer = new BackgroundJobServer();

        logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        productionLine.Stop();
        backgroundJobServer.Dispose();
        cts.Cancel();
        logger.LogInformation("Worker stopped at: {time}", DateTimeOffset.Now);
        return Task.CompletedTask;
    }
}

public class HangfireActivator(IServiceProvider serviceProvider) : JobActivator
{
    public override object ActivateJob(Type jobType)
    {
        return serviceProvider.GetService(jobType)!;
    }
}

public class HangfireJob
{
    private readonly ILogger<HangfireJob> logger;
    private readonly IServiceProvider serviceProvider;

    private static volatile bool isRunning = false;

    public HangfireJob(ILogger<HangfireJob> logger, IServiceProvider serviceProvider) =>
        (this.logger, this.serviceProvider) = (logger, serviceProvider);

    public async Task RunJob(CancellationToken ct)
    {
        if (isRunning)
            return;
        logger.LogInformation("HangfireJob running at: {time}", DateTimeOffset.Now);
        isRunning = true;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider scopeServiceProvider = scope.ServiceProvider;
        ICollectorController[] controllers = scopeServiceProvider.GetServices<ICollectorController>().ToArray();
        foreach (ICollectorController controller in controllers)
            await controller.GatherAsync(ct);
        ProductionLine productionLine = scopeServiceProvider.GetService<ProductionLine>()!;
        logger.LogInformation("Waiting for production line to finish at: {time}", DateTimeOffset.Now);
        productionLine.Wait();
        logger.LogInformation("Production line finished at: {time}", DateTimeOffset.Now);
        productionLine.ResetEvent();
        foreach (ICollectorController controller in controllers)
        {
            logger.LogInformation("Start exporting csv. Controller: {Controller}, Time: {time}", controller.GetType().Name, DateTimeOffset.Now);
            await controller.ExportToCsvAsync(ct);
            logger.LogInformation("Finish exporting csv. Controller: {Controller}, Time: {time}", controller.GetType().Name, DateTimeOffset.Now);
        }
        isRunning = false;
        logger.LogInformation("HangfireJob stopped at: {time}", DateTimeOffset.Now);
    }
}
