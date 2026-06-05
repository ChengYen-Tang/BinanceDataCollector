using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal abstract class BaseTrade<T>(IBinanceRestClient client)
{
    private static readonly TimeSpan MaxRestrictedApiLookback = TimeSpan.FromDays(29);
    private static readonly TimeSpan RestrictedApiPageWindow = TimeSpan.FromMinutes(499 * 5);
    private static readonly TimeSpan RestrictedApiEndTimePadding = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan RestrictedApiInterval = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan FundingRateInterval = TimeSpan.FromHours(8);

    protected readonly IBinanceRestClient client = client;

    protected static DateTime ClampRestrictedStartTime(DateTime startTime)
    {
        DateTime minStartTime = DateTime.Today.Subtract(MaxRestrictedApiLookback);
        return startTime < minStartTime ? minStartTime : startTime;
    }

    protected static DateTime GetRestrictedEndTime(DateTime startTime, DateTime overallEndTime)
    {
        DateTime requestEndTime = startTime.Add(RestrictedApiPageWindow);
        DateTime effectiveOverallEndTime = overallEndTime.Add(RestrictedApiEndTimePadding);
        return requestEndTime < effectiveOverallEndTime ? requestEndTime : effectiveOverallEndTime;
    }

    protected static DateTime GetRestrictedRequestStartTime(DateTime currentStartTime)
        => ClampRestrictedStartTime(currentStartTime.Subtract(RestrictedApiInterval));

    protected static DateTime GetRequestStartTime(DateTime currentStartTime, TimeSpan interval)
        => currentStartTime.Subtract(interval);

    internal static DateTime GetNextKlineStartTime(DateTime currentStartTime, DateTime lastCloseTime, KlineInterval interval)
        => GetNextRestrictedStartTime(currentStartTime, lastCloseTime, GetKlineIntervalSpan(interval));

    internal static DateTime GetNextFundingRateStartTime(DateTime currentStartTime, DateTime lastFundingTime)
        => GetNextRestrictedStartTime(currentStartTime, lastFundingTime, FundingRateInterval);

    internal static DateTime GetNextRestrictedStartTime(DateTime currentStartTime, DateTime lastTimestamp)
        => GetNextRestrictedStartTime(currentStartTime, lastTimestamp, RestrictedApiInterval);

    internal static DateTime GetNextRestrictedStartTime(DateTime currentStartTime, DateTime lastTimestamp, TimeSpan interval)
    {
        DateTime nextTimestampStartTime = lastTimestamp.Add(interval);
        DateTime nextProgressStartTime = currentStartTime.Add(interval);
        return nextTimestampStartTime > nextProgressStartTime ? nextTimestampStartTime : nextProgressStartTime;
    }

    protected static TimeSpan GetKlineIntervalSpan(KlineInterval interval)
        => interval switch
        {
            KlineInterval.OneSecond => TimeSpan.FromSeconds(1),
            KlineInterval.OneMinute => TimeSpan.FromMinutes(1),
            KlineInterval.ThreeMinutes => TimeSpan.FromMinutes(3),
            KlineInterval.FiveMinutes => TimeSpan.FromMinutes(5),
            KlineInterval.FifteenMinutes => TimeSpan.FromMinutes(15),
            KlineInterval.ThirtyMinutes => TimeSpan.FromMinutes(30),
            KlineInterval.OneHour => TimeSpan.FromHours(1),
            KlineInterval.TwoHour => TimeSpan.FromHours(2),
            KlineInterval.FourHour => TimeSpan.FromHours(4),
            KlineInterval.SixHour => TimeSpan.FromHours(6),
            KlineInterval.EightHour => TimeSpan.FromHours(8),
            KlineInterval.TwelveHour => TimeSpan.FromHours(12),
            KlineInterval.OneDay => TimeSpan.FromDays(1),
            KlineInterval.ThreeDay => TimeSpan.FromDays(3),
            KlineInterval.OneWeek => TimeSpan.FromDays(7),
            KlineInterval.OneMonth => TimeSpan.FromDays(31),
            _ => throw new ArgumentOutOfRangeException(nameof(interval), interval, "Unsupported kline interval.")
        };

    public abstract Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<IEnumerable<T>>> GetMarketAsync(CancellationToken ct = default);
}
