using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using ApiKline = Binance.Net.Interfaces.IBinanceKline;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class UsdFutures(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceFuturesUsdtSymbol>(client)
{
    public override async Task<Result<List<ApiKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<ApiKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<ApiKline[]> result;
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

    public override async Task<Result<List<ApiKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<ApiKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<ApiKline[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetIndexPriceKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
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

    public override async Task<Result<List<ApiKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<ApiKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceMarkIndexKline[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetMarkPriceKlinesAsync(symbol, interval, 1500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? result.Data.Last().CloseTime : startTime.AddDays(200);
            klines.AddRange(result.Data.Select(x => (ApiKline)x));
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<List<ApiKline>>> GetPremiumIndexKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<ApiKline> klines = [];
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
            klines.AddRange(result.Data.Select(x => (ApiKline)x));
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
