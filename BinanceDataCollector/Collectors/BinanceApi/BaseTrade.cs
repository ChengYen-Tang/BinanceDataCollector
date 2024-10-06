using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal abstract class BaseTrade<T>(IBinanceRestClient client)
{
    protected readonly IBinanceRestClient client = client;

    public abstract Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<List<BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<IEnumerable<T>>> GetMarketAsync(CancellationToken ct = default);
}
