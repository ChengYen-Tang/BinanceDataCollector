using Binance.Net.Enums;
using BinanceDataCollector.Collectors.BinanceApi;

namespace BinanceDataCollector.Tests;

[TestClass]
public sealed class BinanceApiPaginationTests
{
    [TestMethod]
    public void GetNextKlineStartTime_UsesFullIntervalOverlap()
    {
        DateTime lastCloseTime = new(2026, 05, 28, 12, 34, 59, DateTimeKind.Utc);

        DateTime nextOneMinute = BaseTrade<object>.GetNextKlineStartTime(lastCloseTime, KlineInterval.OneMinute);
        DateTime nextFiveMinutes = BaseTrade<object>.GetNextKlineStartTime(lastCloseTime, KlineInterval.FiveMinutes);
        DateTime nextEightHours = BaseTrade<object>.GetNextKlineStartTime(lastCloseTime, KlineInterval.EightHour);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 33, 59, DateTimeKind.Utc), nextOneMinute);
        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 29, 59, DateTimeKind.Utc), nextFiveMinutes);
        Assert.AreEqual(new DateTime(2026, 05, 28, 4, 34, 59, DateTimeKind.Utc), nextEightHours);
    }

    [TestMethod]
    public void GetNextFundingRateStartTime_UsesEightHourOverlap()
    {
        DateTime lastFundingTime = new(2026, 05, 28, 16, 0, 0, DateTimeKind.Utc);

        DateTime nextStartTime = BaseTrade<object>.GetNextFundingRateStartTime(lastFundingTime);

        Assert.AreEqual(new DateTime(2026, 05, 28, 8, 0, 0, DateTimeKind.Utc), nextStartTime);
    }

    [TestMethod]
    public void GetNextRestrictedStartTime_AdvancesAfterLastTimestamp()
    {
        DateTime currentStartTime = new(2026, 05, 28, 12, 30, 00, DateTimeKind.Utc);
        DateTime lastTimestamp = new(2026, 05, 28, 12, 34, 59, DateTimeKind.Utc);

        DateTime nextStartTime = BaseTrade<object>.GetNextRestrictedStartTime(currentStartTime, lastTimestamp);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 39, 59, DateTimeKind.Utc), nextStartTime);
    }

    [TestMethod]
    public void GetNextRestrictedStartTime_AdvancesWhenLastTimestampDoesNotProgress()
    {
        DateTime currentStartTime = new(2026, 05, 28, 12, 30, 00, DateTimeKind.Utc);
        DateTime lastTimestamp = currentStartTime.AddMinutes(-5);

        DateTime nextStartTime = BaseTrade<object>.GetNextRestrictedStartTime(currentStartTime, lastTimestamp);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 35, 00, DateTimeKind.Utc), nextStartTime);
    }

    [TestMethod]
    public void GetRestrictedRequestStartTime_UsesFiveMinuteOverlap()
    {
        DateTime currentStartTime = new(2026, 05, 28, 12, 30, 00, DateTimeKind.Utc);

        DateTime requestStartTime = TestBaseTrade.GetRestrictedRequestStartTimeForTest(currentStartTime);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 25, 00, DateTimeKind.Utc), requestStartTime);
    }

    private sealed class TestBaseTrade : BaseTrade<object>
    {
        public TestBaseTrade() : base(null!)
        {
        }

        public static DateTime GetRestrictedRequestStartTimeForTest(DateTime currentStartTime)
            => GetRestrictedRequestStartTime(currentStartTime);

        public override Task<FluentResults.Result<List<Binance.Net.Interfaces.IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotSupportedException();

        public override Task<FluentResults.Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotSupportedException();

        public override Task<FluentResults.Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotSupportedException();

        public override Task<FluentResults.Result<List<Binance.Net.Objects.Models.Spot.BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotSupportedException();

        public override Task<FluentResults.Result<IEnumerable<object>>> GetMarketAsync(CancellationToken ct = default)
            => throw new NotSupportedException();
    }
}
