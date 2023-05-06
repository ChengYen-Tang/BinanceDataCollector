using Binance.Net.Objects.Models.Futures;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class CoinFutures : BaseTrade<BinanceFuturesCoinSymbol>
{
    private readonly string[] ignoneCoins;
    public CoinFutures(BinanceClient client, string[] ignoneCoins)
        : base(client) => this.ignoneCoins = ignoneCoins;

    public override async Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<IBinanceKline> klines = new();
        while (startTime < endTime)
        {
            WebCallResult<IEnumerable<IBinanceKline>> result;
            try
            {
                result = (endTime - startTime).Days < 200 ?
                await client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1000, ct) :
                await client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, startTime.AddDays(200), 1000, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Any() ? result.Data.Last().CloseTime : startTime.AddDays(200);
            klines.AddRange(result.Data);
            await Task.Delay(500, ct);
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<IEnumerable<BinanceFuturesCoinSymbol>>> GetMarketAsync(CancellationToken ct = default)
    {
        WebCallResult<BinanceFuturesCoinExchangeInfo> result;
        try
        {
            result = await client.CoinFuturesApi.ExchangeData.GetExchangeInfoAsync(ct);
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
        if (!result.Success)
            return Result.Fail(result.Error!.Message);
        return Result.Ok(result.Data.Symbols.Where(x => !ignoneCoins.Contains(x.Name) && x.UnderlyingType == UnderlyingType.Coin));
    }
}
