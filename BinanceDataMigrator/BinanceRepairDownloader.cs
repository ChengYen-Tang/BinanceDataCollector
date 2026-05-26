using Binance.Net.Clients;
using Binance.Net.Enums;
using Binance.Net.Interfaces;
using Binance.Net.Interfaces.Clients;
using Binance.Net.Objects.Models.Futures;
using Binance.Net.Objects.Models.Spot;
using CollectorModels.Models.Storage;
using CryptoExchange.Net.Objects;

internal sealed class BinanceRepairDownloader(IBinanceRestClient client)
{
    private static readonly TimeSpan MaxRestrictedApiLookback = TimeSpan.FromDays(29);
    private static readonly TimeSpan RestrictedApiPageWindow = TimeSpan.FromMinutes(499 * 5);
    private static readonly TimeSpan RestrictedApiEndTimePadding = TimeSpan.FromMinutes(5);

    public static IBinanceRestClient CreateClient()
        => new BinanceRestClient();

    public async Task<List<Kline>> GetSpotKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<IBinanceKline> rows = await GetPagedKlinesAsync(
            (requestStart, requestEnd, token) => client.SpotApi.ExchangeData.GetKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            item => item.CloseTime,
            ct);

        return [.. rows.Select(item => new Kline
        {
            OpenPrice = decimal.ToDouble(item.OpenPrice),
            ClosePrice = decimal.ToDouble(item.ClosePrice),
            HighPrice = decimal.ToDouble(item.HighPrice),
            LowPrice = decimal.ToDouble(item.LowPrice),
            Volume = decimal.ToDouble(item.Volume),
            QuoteVolume = decimal.ToDouble(item.QuoteVolume),
            TakerBuyBaseVolume = decimal.ToDouble(item.TakerBuyBaseVolume),
            TakerBuyQuoteVolume = decimal.ToDouble(item.TakerBuyQuoteVolume),
            TradeCount = item.TradeCount,
            OpenTime = ToUnixMilliseconds(item.OpenTime),
            CloseTime = ToUnixMilliseconds(item.CloseTime)
        })];
    }

    public async Task<List<Kline>> GetUsdFuturesKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<IBinanceKline> rows = await GetPagedKlinesAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            item => item.CloseTime,
            ct);

        return [.. rows.Select(item => new Kline
        {
            OpenPrice = decimal.ToDouble(item.OpenPrice),
            ClosePrice = decimal.ToDouble(item.ClosePrice),
            HighPrice = decimal.ToDouble(item.HighPrice),
            LowPrice = decimal.ToDouble(item.LowPrice),
            Volume = decimal.ToDouble(item.Volume),
            QuoteVolume = decimal.ToDouble(item.QuoteVolume),
            TakerBuyBaseVolume = decimal.ToDouble(item.TakerBuyBaseVolume),
            TakerBuyQuoteVolume = decimal.ToDouble(item.TakerBuyQuoteVolume),
            TradeCount = item.TradeCount,
            OpenTime = ToUnixMilliseconds(item.OpenTime),
            CloseTime = ToUnixMilliseconds(item.CloseTime)
        })];
    }

    public async Task<List<Kline>> GetCoinFuturesKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<IBinanceKline> rows = await GetPagedKlinesAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            item => item.CloseTime,
            ct);

        return [.. rows.Select(item => new Kline
        {
            OpenPrice = decimal.ToDouble(item.OpenPrice),
            ClosePrice = decimal.ToDouble(item.ClosePrice),
            HighPrice = decimal.ToDouble(item.HighPrice),
            LowPrice = decimal.ToDouble(item.LowPrice),
            Volume = decimal.ToDouble(item.Volume),
            QuoteVolume = decimal.ToDouble(item.QuoteVolume),
            TakerBuyBaseVolume = decimal.ToDouble(item.TakerBuyBaseVolume),
            TakerBuyQuoteVolume = decimal.ToDouble(item.TakerBuyQuoteVolume),
            TradeCount = item.TradeCount,
            OpenTime = ToUnixMilliseconds(item.OpenTime),
            CloseTime = ToUnixMilliseconds(item.CloseTime)
        })];
    }

    public Task<List<PremiumIndexKline>> GetUsdFuturesPremiumIndexKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetMarkIndexKlinesAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetPremiumIndexKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            ct);

    public Task<List<PremiumIndexKline>> GetUsdFuturesIndexPriceKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetIndexPriceKlinesAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetIndexPriceKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            ct);

    public Task<List<PremiumIndexKline>> GetUsdFuturesMarkPriceKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetMarkIndexKlinesAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetMarkPriceKlinesAsync(symbol, KlineInterval.OneMinute, 1500, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<PremiumIndexKline>> GetCoinFuturesPremiumIndexKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetMarkIndexKlinesAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetPremiumIndexKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            ct);

    public Task<List<PremiumIndexKline>> GetCoinFuturesIndexPriceKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetMarkIndexKlinesAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetIndexPriceKlinesAsync(symbol, KlineInterval.OneMinute, requestStart, requestEnd, 1500, token),
            startTime,
            endTime,
            ct);

    public Task<List<PremiumIndexKline>> GetCoinFuturesMarkPriceKlinesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetMarkIndexKlinesAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetMarkPriceKlinesAsync(symbol, KlineInterval.OneMinute, 1500, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public async Task<List<FundingRate>> GetUsdFuturesFundingRatesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesFundingRateHistory> rows = await GetPagedRowsAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetFundingRatesAsync(symbol, requestStart, requestEnd, 499, token),
            startTime,
            endTime,
            item => item.FundingTime,
            _ => false,
            ct);

        return [.. rows.Select(item => new FundingRate
        {
            FundingTime = ToUnixMilliseconds(item.FundingTime),
            Rate = decimal.ToDouble(item.FundingRate),
            MarkPrice = item.MarkPrice.HasValue ? decimal.ToDouble(item.MarkPrice.Value) : null
        })];
    }

    public async Task<List<FundingRate>> GetCoinFuturesFundingRatesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesFundingRateHistory> rows = await GetPagedRowsAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetFundingRatesAsync(symbol, requestStart, requestEnd, 499, token),
            startTime,
            endTime,
            item => item.FundingTime,
            _ => false,
            ct);

        return [.. rows.Select(item => new FundingRate
        {
            FundingTime = ToUnixMilliseconds(item.FundingTime),
            Rate = decimal.ToDouble(item.FundingRate),
            MarkPrice = item.MarkPrice.HasValue ? decimal.ToDouble(item.MarkPrice.Value) : null
        })];
    }

    public async Task<List<OpenInterestHistory>> GetUsdFuturesOpenInterestHistoriesAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesOpenInterestHistory> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetOpenInterestHistoryAsync(symbol, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => !item.Timestamp.HasValue,
            ct);

        return [.. rows.Where(item => item.Timestamp.HasValue).Select(item => new OpenInterestHistory
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
            SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue)
        })];
    }

    public async Task<List<OpenInterestHistory>> GetCoinFuturesOpenInterestHistoriesAsync(string pair, ContractType contractType, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesCoinOpenInterestHistory> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetOpenInterestHistoryAsync(pair, contractType, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => !item.Timestamp.HasValue,
            ct);

        return [.. rows.Where(item => item.Timestamp.HasValue).Select(item => new OpenInterestHistory
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            SumOpenInterest = decimal.ToDouble(item.SumOpenInterest),
            SumOpenInterestValue = decimal.ToDouble(item.SumOpenInterestValue)
        })];
    }

    public async Task<List<FuturesBasisCsv>> GetUsdFuturesBasisAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesBasis> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetBasisAsync(symbol, ContractType.Perpetual, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => item.Timestamp == default,
            ct);

        return [.. rows.Select(item => new FuturesBasisCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp),
            FuturesPrice = decimal.ToDouble(item.FuturesPrice),
            IndexPrice = decimal.ToDouble(item.IndexPrice),
            BasisValue = decimal.ToDouble(item.Basis),
            BasisRate = decimal.ToDouble(item.BasisRate),
            AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null
        })];
    }

    public async Task<List<FuturesBasisCsv>> GetCoinFuturesBasisAsync(string pair, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesBasis> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetBasisAsync(pair, ContractType.Perpetual, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => item.Timestamp == default,
            ct);

        return [.. rows.Select(item => new FuturesBasisCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp),
            FuturesPrice = decimal.ToDouble(item.FuturesPrice),
            IndexPrice = decimal.ToDouble(item.IndexPrice),
            BasisValue = decimal.ToDouble(item.Basis),
            BasisRate = decimal.ToDouble(item.BasisRate),
            AnnualizedBasisRate = item.AnnualizedBasisRate.HasValue ? decimal.ToDouble(item.AnnualizedBasisRate.Value) : null
        })];
    }

    public Task<List<LongShortRatioCsv>> GetUsdFuturesTopLongShortPositionRatiosAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetTopLongShortPositionRatioAsync(symbol, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<LongShortRatioCsv>> GetCoinFuturesTopLongShortPositionRatiosAsync(string pair, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetTopLongShortPositionRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<LongShortRatioCsv>> GetUsdFuturesTopLongShortAccountRatiosAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetTopLongShortAccountRatioAsync(symbol, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<LongShortRatioCsv>> GetCoinFuturesTopLongShortAccountRatiosAsync(string pair, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetTopLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<LongShortRatioCsv>> GetUsdFuturesGlobalLongShortAccountRatiosAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetGlobalLongShortAccountRatioAsync(symbol, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public Task<List<LongShortRatioCsv>> GetCoinFuturesGlobalLongShortAccountRatiosAsync(string pair, DateTime startTime, DateTime endTime, CancellationToken ct = default)
        => GetLongShortRatiosAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetGlobalLongShortAccountRatioAsync(pair, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            ct);

    public async Task<List<TakerLongShortRatioCsv>> GetUsdFuturesTakerLongShortRatiosAsync(string symbol, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesBuySellVolumeRatio> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.UsdFuturesApi.ExchangeData.GetTakerBuySellVolumeRatioAsync(symbol, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => !item.Timestamp.HasValue,
            ct);

        return [.. rows.Where(item => item.Timestamp.HasValue).Select(item => new TakerLongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            BuySellRatio = decimal.ToDouble(item.BuySellRatio),
            BuyVolume = decimal.ToDouble(item.BuyVolume),
            SellVolume = decimal.ToDouble(item.SellVolume),
            BuyVolumeValue = null,
            SellVolumeValue = null
        })];
    }

    public async Task<List<TakerLongShortRatioCsv>> GetCoinFuturesTakerLongShortRatiosAsync(string pair, DateTime startTime, DateTime endTime, CancellationToken ct = default)
    {
        List<BinanceFuturesCoinBuySellVolumeRatio> rows = await GetRestrictedRowsAsync(
            (requestStart, requestEnd, token) => client.CoinFuturesApi.ExchangeData.GetTakerBuySellVolumeRatioAsync(pair, ContractType.Perpetual, PeriodInterval.FiveMinutes, 499, requestStart, requestEnd, token),
            startTime,
            endTime,
            item => item.Timestamp,
            item => item.Timestamp == default,
            ct);

        return [.. rows.Where(item => item.Timestamp != default).Select(item => new TakerLongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp),
            BuySellRatio = item.TakerSellVolume == 0 ? null : decimal.ToDouble(item.TakerBuyVolume / item.TakerSellVolume),
            BuyVolume = decimal.ToDouble(item.TakerBuyVolume),
            SellVolume = decimal.ToDouble(item.TakerSellVolume),
            BuyVolumeValue = decimal.ToDouble(item.TakerBuyVolumeValue),
            SellVolumeValue = decimal.ToDouble(item.TakerSellVolumeValue)
        })];
    }

    private async Task<List<LongShortRatioCsv>> GetLongShortRatiosAsync(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<BinanceFuturesLongShortRatio[]>>> request,
        DateTime startTime,
        DateTime endTime,
        CancellationToken ct)
    {
        List<BinanceFuturesLongShortRatio> rows = await GetRestrictedRowsAsync(
            request,
            startTime,
            endTime,
            item => item.Timestamp,
            item => !item.Timestamp.HasValue,
            ct);

        return [.. rows.Where(item => item.Timestamp.HasValue).Select(item => new LongShortRatioCsv
        {
            Timestamp = ToUnixMilliseconds(item.Timestamp!.Value),
            LongShortRatio = decimal.ToDouble(item.LongShortRatio),
            LongAccount = decimal.ToDouble(item.LongAccount),
            ShortAccount = decimal.ToDouble(item.ShortAccount)
        })];
    }

    private async Task<List<PremiumIndexKline>> GetMarkIndexKlinesAsync(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<BinanceMarkIndexKline[]>>> request,
        DateTime startTime,
        DateTime endTime,
        CancellationToken ct)
    {
        List<BinanceMarkIndexKline> rows = await GetPagedRowsAsync(
            request,
            startTime,
            endTime,
            item => item.CloseTime,
            _ => false,
            ct);

        return [.. rows.Select(item => new PremiumIndexKline
        {
            OpenPrice = decimal.ToDouble(item.OpenPrice),
            ClosePrice = decimal.ToDouble(item.ClosePrice),
            HighPrice = decimal.ToDouble(item.HighPrice),
            LowPrice = decimal.ToDouble(item.LowPrice),
            OpenTime = ToUnixMilliseconds(item.OpenTime),
            CloseTime = ToUnixMilliseconds(item.CloseTime)
        })];
    }

    private async Task<List<PremiumIndexKline>> GetIndexPriceKlinesAsync(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<IBinanceKline[]>>> request,
        DateTime startTime,
        DateTime endTime,
        CancellationToken ct)
    {
        List<IBinanceKline> rows = await GetPagedKlinesAsync(
            request,
            startTime,
            endTime,
            item => item.CloseTime,
            ct);

        return [.. rows.Select(item => new PremiumIndexKline
        {
            OpenPrice = decimal.ToDouble(item.OpenPrice),
            ClosePrice = decimal.ToDouble(item.ClosePrice),
            HighPrice = decimal.ToDouble(item.HighPrice),
            LowPrice = decimal.ToDouble(item.LowPrice),
            OpenTime = ToUnixMilliseconds(item.OpenTime),
            CloseTime = ToUnixMilliseconds(item.CloseTime)
        })];
    }

    private static async Task<List<IBinanceKline>> GetPagedKlinesAsync(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<IBinanceKline[]>>> request,
        DateTime startTime,
        DateTime endTime,
        Func<IBinanceKline, DateTime> getCursor,
        CancellationToken ct)
    {
        List<IBinanceKline> rows = [];
        DateTime currentStart = startTime;
        while (currentStart <= endTime)
        {
            WebCallResult<IBinanceKline[]> result = await request(currentStart, endTime, ct);
            EnsureSuccess(result);

            IBinanceKline[] page = result.Data ?? [];
            if (page.Length == 0)
                break;

            rows.AddRange(page.Where(item => getCursor(item) <= endTime));
            DateTime nextStart = getCursor(page[^1]).AddSeconds(1);
            if (nextStart <= currentStart)
                break;

            currentStart = nextStart;
        }

        return rows;
    }

    private static async Task<List<T>> GetPagedRowsAsync<T>(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<T[]>>> request,
        DateTime startTime,
        DateTime endTime,
        Func<T, DateTime> getCursor,
        Func<T, bool> shouldSkip,
        CancellationToken ct)
    {
        List<T> rows = [];
        DateTime currentStart = startTime;
        while (currentStart <= endTime)
        {
            WebCallResult<T[]> result = await request(currentStart, endTime, ct);
            EnsureSuccess(result);

            T[] page = result.Data ?? [];
            T[] validPage = [.. page.Where(item => !shouldSkip(item))];
            if (validPage.Length == 0)
                break;

            rows.AddRange(validPage.Where(item => getCursor(item) <= endTime));
            DateTime nextStart = getCursor(validPage[^1]).AddSeconds(1);
            if (nextStart <= currentStart)
                break;

            currentStart = nextStart;
        }

        return rows;
    }

    private static async Task<List<T>> GetRestrictedRowsAsync<T>(
        Func<DateTime, DateTime, CancellationToken, Task<WebCallResult<T[]>>> request,
        DateTime startTime,
        DateTime endTime,
        Func<T, DateTime?> getCursor,
        Func<T, bool> shouldSkip,
        CancellationToken ct)
    {
        DateTime currentStart = ClampRestrictedStartTime(startTime);
        if (currentStart > endTime)
            return [];

        List<T> rows = [];
        while (currentStart <= endTime)
        {
            DateTime requestEndTime = GetRestrictedEndTime(currentStart, endTime);
            WebCallResult<T[]> result = await request(currentStart, requestEndTime, ct);
            EnsureSuccess(result);

            T[] validPage = [.. (result.Data ?? []).Where(item => !shouldSkip(item) && getCursor(item).HasValue)];
            if (validPage.Length == 0)
            {
                currentStart = requestEndTime.AddSeconds(1);
                continue;
            }

            rows.AddRange(validPage.Where(item => getCursor(item)!.Value <= endTime));
            DateTime nextStart = getCursor(validPage[^1])!.Value.AddSeconds(1);
            if (nextStart <= currentStart)
                break;

            currentStart = nextStart;
        }

        return rows;
    }

    private static void EnsureSuccess<T>(WebCallResult<T> result)
    {
        if (result.Success)
            return;

        throw new InvalidOperationException(result.Error?.Message ?? "Binance API request failed.");
    }

    private static DateTime ClampRestrictedStartTime(DateTime startTime)
    {
        DateTime minStartTime = DateTime.Today.Subtract(MaxRestrictedApiLookback);
        return startTime < minStartTime ? minStartTime : startTime;
    }

    private static DateTime GetRestrictedEndTime(DateTime startTime, DateTime overallEndTime)
    {
        DateTime requestEndTime = startTime.Add(RestrictedApiPageWindow);
        DateTime effectiveOverallEndTime = overallEndTime.Add(RestrictedApiEndTimePadding);
        return requestEndTime < effectiveOverallEndTime ? requestEndTime : effectiveOverallEndTime;
    }

    private static long ToUnixMilliseconds(DateTime value)
        => new DateTimeOffset(value).ToUnixTimeMilliseconds();

}
