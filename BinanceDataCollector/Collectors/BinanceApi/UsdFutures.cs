using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using Binance.Net.Enums;
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

    public override async Task<Result<List<BinanceMarkIndexKline>>> GetIndexPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<BinanceMarkIndexKline> klines = [];
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
            klines.AddRange(result.Data.Select(kline => new BinanceMarkIndexKline
            {
                OpenPrice = kline.OpenPrice,
                ClosePrice = kline.ClosePrice,
                HighPrice = kline.HighPrice,
                LowPrice = kline.LowPrice,
                OpenTime = kline.OpenTime,
                CloseTime = kline.CloseTime
            }));
        }
        return Result.Ok(klines);
    }

    public override async Task<Result<List<BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<BinanceMarkIndexKline> klines = [];
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
            startTime = result.Data.Length != 0 ? result.Data.Last().FundingTime : startTime.AddDays(200);
            fundingRates.AddRange(result.Data);
        }
        return Result.Ok(fundingRates);
    }

    public async Task<Result<List<BinanceFuturesOpenInterestHistory>>> GetOpenInterestHistoryAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesOpenInterestHistory> openInterestHistories = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesOpenInterestHistory[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetOpenInterestHistoryAsync(symbol, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesOpenInterestHistory[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue)];
            startTime = validData.Length != 0 ? validData.Last().Timestamp!.Value : startTime.AddDays(200);
            openInterestHistories.AddRange(validData);
        }

        return Result.Ok(openInterestHistories);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortPositionRatioAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetTopLongShortPositionRatioAsync(symbol, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue)];
            startTime = validData.Length != 0 ? validData.Last().Timestamp!.Value : startTime.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortAccountRatioAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetTopLongShortAccountRatioAsync(symbol, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue)];
            startTime = validData.Length != 0 ? validData.Last().Timestamp!.Value : startTime.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetGlobalLongShortAccountRatioAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.UsdFuturesApi.ExchangeData.GetGlobalLongShortAccountRatioAsync(symbol, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue)];
            startTime = validData.Length != 0 ? validData.Last().Timestamp!.Value : startTime.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }
}
