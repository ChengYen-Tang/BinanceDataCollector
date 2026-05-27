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
    public void GetNextRestrictedStartTime_UsesFiveMinuteOverlap()
    {
        DateTime lastTimestamp = new(2026, 05, 28, 12, 34, 59, DateTimeKind.Utc);

        DateTime nextStartTime = BaseTrade<object>.GetNextRestrictedStartTime(lastTimestamp);

        Assert.AreEqual(new DateTime(2026, 05, 28, 12, 29, 59, DateTimeKind.Utc), nextStartTime);
    }
}
