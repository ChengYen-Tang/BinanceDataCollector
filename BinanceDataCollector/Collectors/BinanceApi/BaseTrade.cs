using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal abstract class BaseTrade<T>(IBinanceRestClient client)
{
    private static readonly TimeSpan MaxRestrictedApiLookback = TimeSpan.FromDays(29);
    private static readonly TimeSpan RestrictedApiPageWindow = TimeSpan.FromMinutes(499 * 5);
    private static readonly TimeSpan RestrictedApiEndTimePadding = TimeSpan.FromMinutes(5);

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

    public abstract Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<IEnumerable<T>>> GetMarketAsync(CancellationToken ct = default);
}
