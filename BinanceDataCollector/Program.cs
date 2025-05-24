using BinanceDataCollector;
using BinanceDataCollector.Collectors.CollectorControllers;
using BinanceDataCollector.StorageControllers;
using CollectorModels;
using CollectorModels.ShardingCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using ShardingCore;

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
                x.Rest.RateLimiterEnabled = true;
                x.Rest.RateLimitingBehaviour = RateLimitingBehaviour.Wait;
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

        services.AddShardingDbContext<BinanceDbContext>()
                .UseRouteConfig(op =>
                {
                    op.AddShardingTableRoute<SpotBinanceKlineVirtualTableRoute>();
                    op.AddShardingTableRoute<FuturesUsdtBinanceKlineVirtualTableRoute>();
                    op.AddShardingTableRoute<FuturesUsdtBinancePremiumIndexKlineTableRoute>();
                    op.AddShardingTableRoute<FuturesCoinBinanceKlineVirtualTableRoute>();
                    op.AddShardingTableRoute<FuturesCoinBinancePremiumIndexKlineTableRoute>();
                }).UseConfig(op =>
                {
                    op.ThrowIfQueryRouteNotMatch = false;
                    op.UseShardingQuery((connStr, builder) =>
                    {
                        //connStr is delegate input param
                        builder.UseSqlServer(connStr, opts => { opts.CommandTimeout((int)TimeSpan.FromMinutes(180).TotalSeconds); });
                    });
                    op.UseShardingTransaction((connection, builder) =>
                    {
                        //connection is delegate input param
                        builder.UseSqlServer(connection, opts => { opts.CommandTimeout((int)TimeSpan.FromMinutes(180).TotalSeconds); });
                    });
                    //use your data base connection string
                    op.AddDefaultDataSource("DefaultConnection",
                        hostContext.Configuration["ConnectionStrings:DefaultConnection"]);
                    op.UseShardingMigrationConfigure(b =>
                    {
                        b.ReplaceService<IMigrationsSqlGenerator, ShardingSqlServerMigrationsSqlGenerator>();
                    });
                }).AddShardingCore();
    })
    .Build();

await host.RunAsync();
