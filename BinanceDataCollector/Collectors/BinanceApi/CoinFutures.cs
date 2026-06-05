using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using ApiKline = Binance.Net.Interfaces.IBinanceKline;

namespace BinanceDataCollector.Collectors.BinanceApi;

internal class CoinFutures(IBinanceRestClient client, string[] ignoneCoins) : BaseTrade<BinanceFuturesCoinSymbol>(client)
{
    public override async Task<Result<List<ApiKline>>> GetKlinesAsync(string symbol, KlineInterval interval, DateTime startTime, CancellationToken ct = default)
    {
        DateTime endTime = DateTime.Now;
        List<ApiKline> klines = [];
        while (startTime < endTime)
        {
            WebCallResult<ApiKline[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, interval, startTime, endTime.Add(GetKlineIntervalSpan(interval)), 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? GetNextKlineStartTime(result.Data.Last().CloseTime, interval) : startTime.AddDays(200);
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
                result = await base.client.CoinFuturesApi.ExchangeData.GetIndexPriceKlinesAsync(symbol, interval, startTime, endTime.Add(GetKlineIntervalSpan(interval)), 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? GetNextKlineStartTime(result.Data.Last().CloseTime, interval) : startTime.AddDays(200);
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
                result = await base.client.CoinFuturesApi.ExchangeData.GetMarkPriceKlinesAsync(symbol, interval, 1500, startTime, endTime.Add(GetKlineIntervalSpan(interval)), ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? GetNextKlineStartTime(result.Data.Last().CloseTime, interval) : startTime.AddDays(200);
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
                result = await base.client.CoinFuturesApi.ExchangeData.GetPremiumIndexKlinesAsync(symbol, interval, startTime, endTime.Add(GetKlineIntervalSpan(interval)), 1500, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? GetNextKlineStartTime(result.Data.Last().CloseTime, interval) : startTime.AddDays(200);
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
        DateTime endTime = DateTime.Today;
        List<BinanceFuturesFundingRateHistory> fundingRates = [];
        while (startTime < endTime)
        {
            WebCallResult<BinanceFuturesFundingRateHistory[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetFundingRatesAsync(symbol, startTime, endTime.AddHours(8), 499, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            startTime = result.Data.Length != 0 ? GetNextFundingRateStartTime(result.Data.Last().FundingTime) : startTime.AddDays(200);
            fundingRates.AddRange(result.Data);
        }
        return Result.Ok(fundingRates);
    }

    public async Task<Result<List<BinanceFuturesCoinOpenInterestHistory>>> GetOpenInterestHistoryAsync(string pair, ContractType contractType, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesCoinOpenInterestHistory> openInterestHistories = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesCoinOpenInterestHistory[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetOpenInterestHistoryAsync(pair, contractType, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesCoinOpenInterestHistory[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue && item.Timestamp.Value >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp!.Value) : cursor.AddDays(200);
            openInterestHistories.AddRange(validData);
        }

        return Result.Ok(openInterestHistories);
    }

    public async Task<Result<List<BinanceFuturesBasis>>> GetBasisAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesBasis> basis = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesBasis[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetBasisAsync(pair, ContractType.Perpetual, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesBasis[] validData = [.. result.Data.Where(item => item.Timestamp != default && item.Timestamp >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp) : cursor.AddDays(200);
            basis.AddRange(validData);
        }

        return Result.Ok(basis);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortPositionRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetTopLongShortPositionRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue && item.Timestamp.Value >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp!.Value) : cursor.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetTopLongShortAccountRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetTopLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue && item.Timestamp.Value >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp!.Value) : cursor.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

    public async Task<Result<List<BinanceFuturesLongShortRatio>>> GetGlobalLongShortAccountRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesLongShortRatio> ratios = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesLongShortRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetGlobalLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesLongShortRatio[] validData = [.. result.Data.Where(item => item.Timestamp.HasValue && item.Timestamp.Value >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp!.Value) : cursor.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

    public async Task<Result<List<BinanceFuturesCoinBuySellVolumeRatio>>> GetTakerLongShortRatioAsync(string pair, DateTime startTime, CancellationToken ct = default)
    {
        DateTime cursor = ClampRestrictedStartTime(startTime);
        DateTime overallEndTime = DateTime.Today;
        List<BinanceFuturesCoinBuySellVolumeRatio> ratios = [];
        while (cursor < overallEndTime)
        {
            DateTime requestStartTime = GetRestrictedRequestStartTime(cursor);
            DateTime requestEndTime = GetRestrictedEndTime(cursor, overallEndTime);
            WebCallResult<BinanceFuturesCoinBuySellVolumeRatio[]> result;
            try
            {
                result = await base.client.CoinFuturesApi.ExchangeData.GetTakerBuySellVolumeRatioAsync(pair, ContractType.Perpetual, PeriodInterval.FiveMinutes, 499, requestStartTime, requestEndTime, ct);
            }
            catch (Exception ex)
            {
                return Result.Fail(ex.Message);
            }
            if (!result.Success)
                return Result.Fail(result.Error!.Message);
            BinanceFuturesCoinBuySellVolumeRatio[] validData = [.. result.Data.Where(item => item.Timestamp != default && item.Timestamp >= cursor)];
            cursor = validData.Length != 0 ? GetNextRestrictedStartTime(cursor, validData.Last().Timestamp) : cursor.AddDays(200);
            ratios.AddRange(validData);
        }
        return Result.Ok(ratios);
    }

}
