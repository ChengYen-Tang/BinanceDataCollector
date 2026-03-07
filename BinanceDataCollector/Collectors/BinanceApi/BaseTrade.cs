using Binance.Net.Interfaces.Clients;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal abstract class BaseTrade<T>(IBinanceRestClient client)
{
    protected readonly IBinanceRestClient client = client;

    public abstract Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<IBinanceKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<IBinanceKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<IBinanceKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<IEnumerable<T>>> GetMarketAsync(CancellationToken ct = default);
}
