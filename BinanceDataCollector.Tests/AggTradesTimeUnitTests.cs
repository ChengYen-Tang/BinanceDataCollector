using BinanceDataCollector.StorageControllers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;

namespace BinanceDataCollector.Tests;

[TestClass]
public sealed class AggTradesTimeUnitTests
{
    [TestMethod]
    public void SpotAggTradesPeriodBoundaries_UseUtcWhenChoosingSourceTimeUnit()
    {
        TestStorageController controller = new();

        DateTime decemberMonthly = ParseCsvPeriod(@"C:\tmp\BTCUSDT-aggTrades-2024-12.csv");
        DateTime januaryMonthly = ParseCsvPeriod(@"C:\tmp\BTCUSDT-aggTrades-2025-01.csv");
        DateTime januaryFirstDaily = ParseCsvPeriod(@"C:\tmp\BTCUSDT-aggTrades-2025-01-01.csv");

        Assert.AreEqual(DateTimeKind.Utc, decemberMonthly.Kind);
        Assert.AreEqual(DateTimeKind.Utc, januaryMonthly.Kind);
        Assert.AreEqual(DateTimeKind.Utc, januaryFirstDaily.Kind);

        Assert.AreEqual(AggTradesTimeUnit.Milliseconds, controller.GetTimeUnit(decemberMonthly));
        Assert.AreEqual(AggTradesTimeUnit.Microseconds, controller.GetTimeUnit(januaryMonthly));
        Assert.AreEqual(AggTradesTimeUnit.Microseconds, controller.GetTimeUnit(januaryFirstDaily));
    }

    private static DateTime ParseCsvPeriod(string csvPath)
    {
        MethodInfo method = typeof(StorageController<object>)
            .GetMethod("GetMarketDataCsvSortKey", BindingFlags.Static | BindingFlags.NonPublic)!;
        return (DateTime)method.Invoke(null, [csvPath])!;
    }

    private sealed class TestStorageController : StorageController<object>
    {
        private static readonly DateTime SpotAggTradesMicrosecondsStart = new(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        public TestStorageController()
            : base(new ServiceCollection().BuildServiceProvider(), NullLogger.Instance)
        {
        }

        public AggTradesTimeUnit GetTimeUnit(DateTime timestamp)
            => GetAggTradesTimeUnitForTimestamp(timestamp);

        protected override AggTradesTimeUnit GetAggTradesTimeUnitForTimestamp(DateTime timestamp)
            => timestamp >= SpotAggTradesMicrosecondsStart
                ? AggTradesTimeUnit.Microseconds
                : AggTradesTimeUnit.Milliseconds;

        protected override string MarketPathSegment => "Spot";
        protected override string SymbolInfoPath => throw new NotSupportedException();
        protected override string KlinePath => throw new NotSupportedException();
        protected override string PremiumIndexKlinePath => throw new NotSupportedException();
        protected override string IndexPriceKlinePath => throw new NotSupportedException();
        protected override string MarkPriceKlinePath => throw new NotSupportedException();
        protected override string FundingRatePath => throw new NotSupportedException();
        protected override string OpenInterestPath => throw new NotSupportedException();
        protected override string TopLongShortPositionRatioPath => throw new NotSupportedException();
        protected override string TopLongShortAccountRatioPath => throw new NotSupportedException();
        protected override string GlobalLongShortAccountRatioPath => throw new NotSupportedException();
        protected override string TakerLongShortRatioPath => throw new NotSupportedException();
        protected override string BasisPath => throw new NotSupportedException();
        protected override bool IsFutures => false;
        protected override AggTradesTimeUnit AggTradesTimeUnit => AggTradesTimeUnit.Microseconds;
        public override Task<DateTime> GetLastTimeAsync(object symbol, Binance.Net.Enums.KlineInterval interval, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastPremiumIndexTimeAsync(object symbol, Binance.Net.Enums.KlineInterval interval, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastIndexPriceTimeAsync(object symbol, Binance.Net.Enums.KlineInterval interval, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastMarkPriceTimeAsync(object symbol, Binance.Net.Enums.KlineInterval interval, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastFundingTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastOpenInterestTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastTopLongShortPositionRatioTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastTopLongShortAccountRatioTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastGlobalLongShortAccountRatioTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastTakerLongShortRatioTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task<DateTime> GetLastBasisTimeAsync(object symbol, CancellationToken ct = default) => throw new NotSupportedException();
        public override Task DeleteOldData(CancellationToken ct = default) => throw new NotSupportedException();
        protected override string GetSymbolName(object symbol) => throw new NotSupportedException();
        protected override Task<List<string>> GetExistingSymbolNamesAsync(CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task DeleteDelistedSymbolsAsync(IReadOnlyCollection<string> delistedSymbols, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<object>>> GetMarketAsync(CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch>> GetAggTradesAsync(object symbol, DateTime downloadStartTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<BinanceDataCollector.Collectors.BinanceMarketData.MarketDataDownloadBatch>> GetBookDepthAsync(object symbol, DateTime downloadStartTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.Kline>>> GetKlinesAsync(object symbol, Binance.Net.Enums.KlineInterval interval, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.PremiumIndexKline>>> GetPremiumIndexKlinesAsync(object symbol, Binance.Net.Enums.KlineInterval interval, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.PremiumIndexKline>>> GetIndexPriceKlinesAsync(object symbol, Binance.Net.Enums.KlineInterval interval, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.PremiumIndexKline>>> GetMarkPriceKlinesAsync(object symbol, Binance.Net.Enums.KlineInterval interval, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.FundingRate>>> GetFundingRatesAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.OpenInterestHistory>>> GetOpenInterestHistoriesAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.LongShortRatioCsv>>> GetTopLongShortPositionRatiosAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.LongShortRatioCsv>>> GetTopLongShortAccountRatiosAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.LongShortRatioCsv>>> GetGlobalLongShortAccountRatiosAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.TakerLongShortRatioCsv>>> GetTakerLongShortRatiosAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
        protected override Task<FluentResults.Result<List<CollectorModels.Models.Storage.FuturesBasisCsv>>> GetBasisAsync(object symbol, DateTime startTime, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
