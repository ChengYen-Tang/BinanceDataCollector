using Binance.Net.Interfaces.Clients;
using CollectorModels;
using Microsoft.EntityFrameworkCore;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddSimpleConsole(options =>
        {
            options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
            options.SingleLine = true;
        });
    })
    .ConfigureServices((hostContext, services) =>
    {
        string connectionString = hostContext.Configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("ConnectionStrings:DefaultConnection is required.");

        services.Configure<MigrationOptions>(hostContext.Configuration.GetSection(MigrationOptions.SectionName));
        services.AddDbContext<BinanceDbContext>(options => options.UseSqlServer(
            connectionString,
            sqlOptions => sqlOptions.CommandTimeout(MigrationTimeouts.SqlServerCommandTimeoutSeconds)));
        services.AddSingleton<IBinanceRestClient>(_ => BinanceRepairDownloader.CreateClient());
        services.AddSingleton<BinanceRepairDownloader>();
        services.AddSingleton<SqlServerToDuckDbMigrator>();
        services.AddHostedService<MigrationHostedService>();
    })
    .Build();

await host.RunAsync();
