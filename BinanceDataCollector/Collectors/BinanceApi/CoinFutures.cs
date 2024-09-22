using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class CoinFutures(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceFuturesCoinSymbol>(client)
{
    public override async Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<IBinanceKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<IEnumerable<IBinanceKline>> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Any() ? result.Data.Last().CloseTime : startTime.AddDays(200);
            klines.AddRange(result.Data);
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<IEnumerable<BinanceFuturesCoinSymbol>>> GetMarketAsync(CancellationToken ct = default)
    {
        WebCallResult<BinanceFuturesCoinExchangeInfo> result;
        try
        {
            result = await base.client.CoinFuturesApi.ExchangeData.GetExchangeInfoAsync(ct);
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
        if (!result.Success)
            return Result.Fail(result.Error!.Message);
        return Result.Ok(result.Data.Symbols.Where(x => !ignoneCoins.Any(ic => x.Name.Contains(ic) && !x.Name.Contains("1000") && !x.Name.Contains('_')) && x.UnderlyingType == UnderlyingType.Coin && x.ContractType == ContractType.Perpetual && x.Status == SymbolStatus.Trading));
    }
}
