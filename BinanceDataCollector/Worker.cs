using BinanceDataCollector.Collectors.CollectorControllers;
using Hangfire;
using Hangfire.InMemory;

namespace BinanceDataCollector;

internal class Worker(ILogger<Worker> logger, IServiceProvider serviceProvider, HangfireJob hangfireJob, ProductionLine productionLine) : IHostedService
{
    private readonly CancellationTokenSource cts = new();
    private BackgroundJobServer backgroundJobServer = null!;

    public Task StartAsync(CancellationToken cancellationToken)
    {
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

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        hangfireJob.RequestStop();
        cts.Cancel();
        productionLine.Stop();
        await hangfireJob.WaitForIdleAsync(CancellationToken.None);
        backgroundJobServer.Dispose();
        await DuckDbStorageHelper.CheckpointDirtyDatabasesAsync(cancellationToken);
        logger.LogInformation("Worker stopped at: {time}", DateTimeOffset.Now);
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
    private readonly Lock runStateLock = new();

    private bool isRunning;
    private bool isStopping;
    private TaskCompletionSource idleTaskCompletionSource = CreateCompletedIdleTaskCompletionSource();

    public HangfireJob(ILogger<HangfireJob> logger, IServiceProvider serviceProvider) =>
        (this.logger, this.serviceProvider) = (logger, serviceProvider);

    public async Task RunJob(CancellationToken ct)
    {
        lock (runStateLock)
        {
            if (isRunning || isStopping)
                return;

            isRunning = true;
            idleTaskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        logger.LogInformation("HangfireJob running at: {time}", DateTimeOffset.Now);
        try
        {
            using IServiceScope scope = serviceProvider.CreateScope();
            IServiceProvider scopeServiceProvider = scope.ServiceProvider;
            ProductionLine productionLine = scopeServiceProvider.GetService<ProductionLine>()!;
            productionLine.ResetEvent();
            ICollectorController[] controllers = scopeServiceProvider.GetServices<ICollectorController>().ToArray();

            bool allPrepared = true;
            foreach (ICollectorController controller in controllers)
                allPrepared &= await controller.PrepareAsync(ct);

            if (!allPrepared)
            {
                logger.LogWarning("One or more markets failed during prepare phase. Dispatch skipped at: {time}", DateTimeOffset.Now);
                return;
            }

            foreach (ICollectorController controller in controllers)
                await controller.DispatchAsync(ct);
            logger.LogInformation("Waiting for production line to finish at: {time}", DateTimeOffset.Now);
            bool productionLineCompleted = productionLine.Wait(ct);
            if (!productionLineCompleted)
            {
                logger.LogInformation("Production line wait canceled at: {time}", DateTimeOffset.Now);
                return;
            }
            logger.LogInformation("Production line finished at: {time}", DateTimeOffset.Now);

            await PackageDuckDbArchiveAsync(ct);
        }
        finally
        {
            lock (runStateLock)
            {
                isRunning = false;
                idleTaskCompletionSource.TrySetResult();
            }
            logger.LogInformation("HangfireJob stopped at: {time}", DateTimeOffset.Now);
        }
    }

    public Task WaitForIdleAsync(CancellationToken ct = default)
    {
        Task waitTask;
        lock (runStateLock)
            waitTask = idleTaskCompletionSource.Task;

        return waitTask.WaitAsync(ct);
    }

    public void RequestStop()
    {
        lock (runStateLock)
            isStopping = true;
    }

    private async Task PackageDuckDbArchiveAsync(CancellationToken ct)
    {
        lock (runStateLock)
        {
            if (isStopping)
                return;
        }

        ct.ThrowIfCancellationRequested();
        logger.LogInformation("Start packaging DuckDB archive at: {time}", DateTimeOffset.Now);
        await DuckDbStorageHelper.CheckpointDirtyDatabasesAsync(CancellationToken.None);
        bool packaged = await DuckDbStorageArchiveHelper.FinalizeArchiveAsync(logger, ct);
        if (packaged)
            logger.LogInformation("Finish packaging DuckDB archive at: {time}", DateTimeOffset.Now);
    }

    private static TaskCompletionSource CreateCompletedIdleTaskCompletionSource()
    {
        TaskCompletionSource taskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        taskCompletionSource.TrySetResult();
        return taskCompletionSource;
    }
}
