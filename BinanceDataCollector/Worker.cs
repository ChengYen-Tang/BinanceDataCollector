using BinanceDataCollector.Collectors.CollectorControllers;
using Hangfire;
using ShardingCore;

namespace BinanceDataCollector;

internal class Worker : IHostedService
{
    private readonly ILogger<Worker> logger;
    private readonly IServiceProvider serviceProvider;
    private readonly HangfireJob hangfireJob;
    private readonly CancellationTokenSource cts;
    private readonly ProductionLine productionLine;
    private BackgroundJobServer backgroundJobServer = null!;

    public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider, HangfireJob hangfireJob, ProductionLine productionLine)
    {
        serviceProvider.UseAutoShardingCreate();
        serviceProvider.UseAutoTryCompensateTable();
        this.logger = logger;
        this.serviceProvider = serviceProvider;
        this.hangfireJob = hangfireJob;
        this.productionLine = productionLine;
        cts = new();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        productionLine.Start();
        GlobalConfiguration.Configuration
                .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                .UseColouredConsoleLogProvider()
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UseActivator(new HangfireActivator(serviceProvider))
                .UseInMemoryStorage();

        //BackgroundJob.Enqueue(() => hangfireJob.RunJob(cts.Token));
        RecurringJob.AddOrUpdate(() => hangfireJob.RunJob(cts.Token), "30 0 * * *");
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

public class HangfireActivator : JobActivator
{
    private readonly IServiceProvider serviceProvider;

    public HangfireActivator(IServiceProvider serviceProvider) =>
        this.serviceProvider = serviceProvider;

    public override object ActivateJob(Type jobType)
    {
        return serviceProvider.GetService(jobType)!;
    }
}

public class HangfireJob
{
    private readonly IServiceProvider serviceProvider;

    private static volatile bool isRunning = false;
    
    public HangfireJob(IServiceProvider serviceProvider) =>
        this.serviceProvider = serviceProvider;

    public async Task RunJob(CancellationToken ct)
    {
        if (isRunning)
            return;
        isRunning = true;
        using IServiceScope scope = serviceProvider.CreateScope();
        IServiceProvider scopeServiceProvider = scope.ServiceProvider;
        ICollectorController[] controllers = scopeServiceProvider.GetServices<ICollectorController>().ToArray();
        foreach (ICollectorController controller in controllers)
            await controller.GatherAsync(ct);
        ProductionLine productionLine = scopeServiceProvider.GetService<ProductionLine>()!;
        productionLine.Wait();
        isRunning = false;
    }
}
