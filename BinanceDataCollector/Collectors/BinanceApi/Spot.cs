using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi
{
    internal class Spot(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceSymbol>(client)
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
                    result = await base.client.SpotApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
                }
                catch (Exception ex)
                {
                    return Result.Fail(ex.Message);
                }
                if (!result.Success)
                    return Result.Fail(result.Error!.Message);
                if (!result.Data!.Any())
                    break;
                startTime = result.Data.Last().CloseTime;
                klines.AddRange(result.Data);
            }
            return Result.Ok(klines);
        }

        public override async Task<Result<IEnumerable<BinanceSymbol>>> GetMarketAsync(CancellationToken ct = default)
        {
            WebCallResult<BinanceExchangeInfo> result;
            try
            {
                result = await base.client.SpotApi.ExchangeData.GetExchangeInfoAsync(ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            return Result.Ok(result.Data.Symbols.Where(x => !ignoneCoins.Any(ic => x.Name.Contains(ic)) && !x.Name.Contains("1000") && !x.Name.Contains('_') && x.Status == SymbolStatus.Trading));
        }
    }
}
