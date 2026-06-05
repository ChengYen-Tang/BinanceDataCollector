using Binance.Net.Enums;
using BinanceDataCollector.Collectors.BinanceApi;

namespace BinanceDataCollector.Tests;

[TestClass]
public sealed class BinanceApiPaginationTests
{
    [TestMethod]
    public void GetNextKlineStartTime_AdvancesWithInterval()
    {
        DateTime currentStartTime = new(2026, 05, 28, 12, 30, 00, DateTimeKind.Utc);
        DateTime lastCloseTime = new(2026, 05, 28, 12, 34, 59, DateTimeKind.Utc);

        DateTime nextOneMinute = BaseTrade<object>.GetNextKlineStartTime(currentStartTime, lastCloseTime, KlineInterval.OneMinute);
        DateTime nextFiveMinutes = BaseTrade<object>.GetNextKlineStartTime(currentStartTime, lastCloseTime, KlineInterval.FiveMinutes);
        DateTime nextEightHours = BaseTrade<object>.GetNextKlineStartTime(currentStartTime, lastCloseTime, KlineInterval.EightHour);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 35, 59, DateTimeKind.Utc), nextOneMinute);
        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 39, 59, DateTimeKind.Utc), nextFiveMinutes);
        Assert.AreEqual(new DateTime(2026, 05, 28, 20, 34, 59, DateTimeKind.Utc), nextEightHours);
    }

    [TestMethod]
    public void GetNextKlineStartTime_AdvancesWhenLastCloseTimeDoesNotProgress()
    {
        DateTime currentStartTime = new(2026, 05, 28, 12, 30, 00, DateTimeKind.Utc);
        DateTime lastCloseTime = currentStartTime.AddMinutes(-5);

        DateTime nextStartTime = BaseTrade<object>.GetNextKlineStartTime(currentStartTime, lastCloseTime, KlineInterval.FiveMinutes);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 35, 00, DateTimeKind.Utc), nextStartTime);
    }

    [TestMethod]
    public void GetNextFundingRateStartTime_AdvancesWithEightHourInterval()
    {
        DateTime currentStartTime = new(2026, 05, 28, 8, 0, 0, DateTimeKind.Utc);
        DateTime lastFundingTime = new(2026, 05, 28, 16, 0, 0, DateTimeKind.Utc);

        DateTime nextStartTime = BaseTrade<object>.GetNextFundingRateStartTime(currentStartTime, lastFundingTime);

        Assert.AreEqual(new DateTime(2026, 05, 29, 0, 0, 0, DateTimeKind.Utc), nextStartTime);
    }

    [TestMethod]
    public void GetNextFundingRateStartTime_AdvancesWhenLastTimestampDoesNotProgress()
    {
        DateTime currentStartTime = new(2026, 05, 28, 8, 0, 0, DateTimeKind.Utc);
        DateTime lastFundingTime = currentStartTime;

        DateTime nextStartTime = BaseTrade<object>.GetNextFundingRateStartTime(currentStartTime, lastFundingTime);

        Assert.AreEqual(new DateTime(2026, 05, 28, 16, 0, 0, DateTimeKind.Utc), nextStartTime);
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

    [TestMethod]
    public void GetRequestStartTime_UsesProvidedIntervalWithoutClamp()
    {
        DateTime currentStartTime = new(2000, 01, 02, 0, 0, 00, DateTimeKind.Utc);

        DateTime requestStartTime = TestBaseTrade.GetRequestStartTimeForTest(currentStartTime, TimeSpan.FromDays(1));

        Assert.AreEqual(new DateTime(2000, 01, 01, 0, 0, 00, DateTimeKind.Utc), requestStartTime);
    }

    private sealed class TestBaseTrade : BaseTrade<object>
    {
        public TestBaseTrade() : base(null!)
        {
        }

        public static DateTime GetRestrictedRequestStartTimeForTest(DateTime currentStartTime)
            => GetRestrictedRequestStartTime(currentStartTime);

        public static DateTime GetRequestStartTimeForTest(DateTime currentStartTime, TimeSpan interval)
            => GetRequestStartTime(currentStartTime, interval);

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
