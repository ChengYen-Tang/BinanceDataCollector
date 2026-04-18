using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using ApiKline = Binance.Net.Interfaces.IBinanceKline;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class CoinFutures(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceFuturesCoinSymbol>(client)
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
                result = await base.client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
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
            WebCallResult<BinanceMarkIndexKline[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetIndexPriceKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
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

    public override async Task<Result<List<BinanceMarkIndexKline>>> GetMarkPriceKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Today;
        List<BinanceMarkIndexKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceMarkIndexKline[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetMarkPriceKlinesAsync(symbol, interval, 1500, startTime, endTime, ct);
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
                result = await base.client.CoinFuturesApi.ExchangeData.GetPremiumIndexKlinesAsync(symbol, interval, startTime, endTime, 1500, ct);
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

    public async Task<Result<List<BinanceFuturesFundingRateHistory>>> GetFundingRatesAsync(string symbol, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesFundingRateHistory> fundingRates = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesFundingRateHistory[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetFundingRatesAsync(symbol, startTime, endTime, 499, ct);
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

    public async Task<Result<List<BinanceFuturesCoinOpenInterestHistory>>> GetOpenInterestHistoryAsync(string pair, ContractType contractType, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesCoinOpenInterestHistory> openInterestHistories = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesCoinOpenInterestHistory[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetOpenInterestHistoryAsync(pair, contractType, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesCoinOpenInterestHistory[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue)];
            startTime = validData.Length != 0 ? validData.Last().Timestamp!.Value : startTime.AddDays(200);
            openInterestHistories.AddRange(validData);
        }

        return Result.Ok(openInterestHistories);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortPositionRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetTopLongShortPositionRatioAsync(pair, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
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

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortAccountRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetTopLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
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

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetGlobalLongShortAccountRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.UtcNow;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetGlobalLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 500, startTime, endTime, ct);
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
