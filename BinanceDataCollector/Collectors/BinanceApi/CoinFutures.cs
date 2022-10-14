using Binance.Net.Objects.Models.Futures;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class CoinFutures : BaseTrade<BinanceFuturesCoinSymbol>
{
    public CoinFutures(BinanceClient client)
        : base(client) { }

    public override async Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<IBinanceKline> klines = new();
        while (startTime < endTime)
        {
            WebCallResult<IEnumerable<IBinanceKline>> result = await client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1000, ct);
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Last().CloseTime;
            klines.AddRange(result.Data);
            await Task.Delay(500, ct);
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<IEnumerable<BinanceFuturesCoinSymbol>>> GetMarketAsync(CancellationToken ct = default)
    {
        WebCallResult<BinanceFuturesCoinExchangeInfo> result = await client.CoinFuturesApi.ExchangeData.GetExchangeInfoAsync(ct);
        if (!result.Success)
            return Result.Fail(result.Error!.Message);
        return Result.Ok(result.Data.Symbols);
    }
}
