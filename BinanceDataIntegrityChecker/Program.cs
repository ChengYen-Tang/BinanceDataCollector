using BinanceDataIntegrityChecker;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        string outputTemplate = "{Timestamp:o} [{Level:u3}] [{SourceContext}] {Message}{NewLine}{Exception}";
        Dictionary<string, LogLevel> levelOverrides = new()
        {
            { "Default", LogLevel.Information },
            { "Microsoft.Hosting.Lifetime", LogLevel.Warning }
        };

        logging.ClearProviders();
        logging.AddSimpleConsole(options =>
        {
            options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
            options.SingleLine = true;
        });
        logging.AddFile("Logs/IntegrityCheck-{Date}.txt", LogLevel.Information, levelOverrides, outputTemplate: outputTemplate);
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.Configure<IntegrityCheckOptions>(hostContext.Configuration.GetSection(IntegrityCheckOptions.SectionName));
        services.AddSingleton<MarketDataIntegrityChecker>();
        services.AddHostedService<IntegrityCheckHostedService>();
    })
    .Build();

await host.RunAsync();
