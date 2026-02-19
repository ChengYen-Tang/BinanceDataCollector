using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class UsdFutures(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceFuturesUsdtSymbol>(client)
{
    public override async Task<Result<List<IBinanceKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<IBinanceKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<IBinanceKline[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? result.Data.Last().CloseTime : startTime.AddDays(200);
            klines.AddRange(result.Data);
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<List<BinanceMarkIndexKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<BinanceMarkIndexKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceMarkIndexKline[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetPremiumIndexKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? result.Data.Last().CloseTime : startTime.AddDays(200);
            klines.AddRange(result.Data);
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<IEnumerable<BinanceFuturesUsdtSymbol>>> GetMarketAsync(CancellationToken ct = default)
    {
        WebCallResult<BinanceFuturesUsdtExchangeInfo> result;
        try
        {
            result = await base.client.UsdFuturesApi.ExchangeData.GetExchangeInfoAsync(ct);
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
        if (!result.Success)
            return Result.Fail(result.Error!.Message);
        return Result.Ok(result.Data.Symbols.Where(x => !ignoneCoins.Any(ic => x.Name.Contains(ic)) && !x.Name.Contains("1000") && !x.Name.Contains('_') && x.UnderlyingType == UnderlyingType.Coin && x.ContractType == ContractType.Perpetual && x.Status == SymbolStatus.Trading));
    }

    public async Task<Result<List<BinanceFuturesFundingRateHistory>>> GetFundingRatesAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesFundingRateHistory> fundingRates = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesFundingRateHistory[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetFundingRatesAsync(symbol, startTime, endTime, 499, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            if (result.Data.Length == 0)
            {
                startTime = startTime.AddDays(200);
                continue;
            }

            fundingRates.AddRange(result.Data);
            DateTime lastFundingTime = result.Data.Last().FundingTime;
            if (lastFundingTime <= startTime)
                break;
            startTime = lastFundingTime.AddMilliseconds(1);
        }
        return Result.Ok(fundingRates);
    }
}
