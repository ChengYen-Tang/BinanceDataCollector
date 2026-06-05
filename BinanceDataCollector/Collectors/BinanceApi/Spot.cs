using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Spot;
using ApiKline = Binance.Net.Interfaces.IBinanceKline;

namespace BinanceDataCollector.Collectors.BinanceApi
{
    internal class Spot(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceSymbol>(client)
    {
        public override async Task<Result<List<ApiKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
        {
            TimeSpan intervalSpan = GetKlineIntervalSpan(interval);
            DateTime cursor = startTime;
            DateTime overallEndTime = DateTime.Today;
            List<ApiKline> klines = [];
            while (cursor < overallEndTime)
            {
                DateTime requestStartTime = GetRequestStartTime(cursor, intervalSpan);
                WebCallResult<ApiKline[]> result;
                try
                {
                    result = await base.client.SpotApi.ExchangeData.GetKlinesAsync(symbol, interval, requestStartTime, overallEndTime.Add(intervalSpan), 1500, ct);
                }
                catch (Exception ex)
                {
                    return Result.Fail(ex.Message);
                }
                if (!result.Success)
                    return Result.Fail(result.Error!.Message);
                ApiKline[] validData = [.. result.Data!.Where(item => item.CloseTime >= cursor)];
                cursor = validData.Length != 0 ? GetNextKlineStartTime(cursor, validData.Last().CloseTime, interval) : cursor.AddDays(200);
                klines.AddRange(validData);
            }
            return Result.Ok(klines);
        }

        public override Task<Result<List<BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotImplementedException();

        public override Task<Result<List<BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotImplementedException();

        public override Task<Result<List<BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
            => throw new NotImplementedException();

        public override async Task<Result<IEnumerable<BinanceSymbol>>> GetMarketAsync(CancellationToken ct = default)
        {
            WebCallResult<BinanceExchangeInfo> result;
            try
            {
                result = await base.client.SpotApi.ExchangeData.GetExchangeInfoAsync(ct: ct);
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
