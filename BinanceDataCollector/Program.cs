using BinanceDataCollector;
using BinanceDataCollector.Collectors.CollectorControllers;
using BinanceDataCollector.StorageControllers;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        string outputTemplate = "{Timestamp:o} {RequestId,13} [{Level:u3}] [{SourceContext} {Method}] {Message} ({EventId:x8}){NewLine}{Exception}";
        Dictionary<string, LogLevel> levelOverrides = new() { { "Default", LogLevel.Information }, { "Microsoft.Hosting.Lifetime", LogLevel.Information }, { "System.Net.Http.HttpClient", LogLevel.Error }, };
#if DEBUG
        logging.SetMinimumLevel(LogLevel.Debug);
        logging.AddFile("Logs/{Date}.txt", LogLevel.Debug, levelOverrides, outputTemplate: outputTemplate);
#else
        logging.SetMinimumLevel(LogLevel.Information);
        logging.AddFile("Logs/{Date}.txt", LogLevel.Information, levelOverrides, outputTemplate: outputTemplate);
#endif
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton<HangfireJob>();
        services.AddSingleton<ProductionLine>();

        services.AddBinance(x =>
        {
            x.Rest.OutputOriginalData = true;
            x.Rest.RateLimiterEnabled = true;
            x.Rest.RateLimitingBehaviour = RateLimitingBehaviour.Wait;
            x.Socket.OutputOriginalData = true;
            x.Socket.RateLimiterEnabled = true;
            x.Socket.RateLimitingBehaviour = RateLimitingBehaviour.Wait;
        });
        services.AddScoped<ICollectorController, SpotCollectorController>();
        services.AddScoped<ICollectorController, CoinFuturesCollectorController>();
        services.AddScoped<ICollectorController, UsdFuturesCollectorController>();
        services.AddScoped<SpotStorageController>();
        services.AddScoped<CoinFuturesStorageController>();
        services.AddScoped<UsdFuturesStorageController>();

        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
