namespace BinanceDataCollector.Collectors.BinanceApi;

internal abstract class BaseTrade<T>
{
    protected readonly BinanceClient client;

    public BaseTrade(BinanceClient client)
        => this.client = client;

    public abstract Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default);
    public abstract Task<Result<IEnumerable<T>>> GetMarketAsync(CancellationToken ct = default);
}
