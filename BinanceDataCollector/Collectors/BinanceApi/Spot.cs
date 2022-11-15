using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi
{
    internal class Spot : BaseTrade<BinanceSymbol>
    {
        private readonly string[] ignoneCoins;
        public Spot(BinanceClient client, string[] ignoneCoins)
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
                    result = await client.SpotApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1000, ct);
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
                await Task.Delay(500, ct);
            }
            return Result.Ok(klines);
        }

        public override async Task<Result<IEnumerable<BinanceSymbol>>> GetMarketAsync(CancellationToken ct = default)
        {
            WebCallResult<BinanceExchangeInfo> result = await client.SpotApi.ExchangeData.GetExchangeInfoAsync(ct);
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            return Result.Ok(result.Data.Symbols.Where(x => !ignoneCoins.Contains(x.Name)));
        }
    }
}
