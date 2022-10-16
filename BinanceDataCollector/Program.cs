using BinanceDataCollector;
using BinanceDataCollector.Collectors.CollectorControllers;
using BinanceDataCollector.StorageControllers;
using CollectorModels;
using CollectorModels.ShardingCore;
using Microsoft.EntityFrameworkCore;
using ShardingCore;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton<HangfireJob>();
        services.AddSingleton<ProductionLine>();

        services.AddScoped<BinanceClient>();
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
                    op.AddShardingTableRoute<FuturesCoinBinanceKlineVirtualTableRoute>();
                }).UseConfig(op =>
                {
                    op.ThrowIfQueryRouteNotMatch = false;
                    op.UseShardingQuery((connStr, builder) =>
                    {
                        //connStr is delegate input param
                        builder.UseSqlServer(connStr);
                    });
                    op.UseShardingTransaction((connection, builder) =>
                    {
                        //connection is delegate input param
                        builder.UseSqlServer(connection, opts => opts.CommandTimeout((int)TimeSpan.FromMinutes(30).TotalSeconds));
                    });
                    //use your data base connection string
                    op.AddDefaultDataSource(Guid.NewGuid().ToString("n"),
                        hostContext.Configuration["ConnectionStrings:DefaultConnection"]);
                }).AddShardingCore();
    })
    .Build();

await host.RunAsync();
