using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Binance.Net;
using Binance.Net.Enums;
using Binance.Net.Interfaces.SubClients;
using CollectorModels;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using MoreLinq;

namespace BinanceDataCollector
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ICollection<(Guid Id, string Symbol, string Market)> symbols = await GetSymbols();
            int i = 0;
            foreach(var (Id, Symbol, Market) in symbols)
            {
                IList<CoinDataModel> klines = await GetKlinesAsync(Id, Symbol, Market);
                using BinanceDbContext db = new();
                await db.BulkInsertOrUpdateAsync(klines);
                Console.WriteLine($"{Market}-{Symbol} is done. [{i}]");
                i++;
            }
        }

        private static async Task<IList<CoinDataModel>> GetKlinesAsync(Guid id, string symbol, string marketString)
        {
            using BinanceClient client = new();
            List<CoinDataModel> kLines = new();
            IBinanceClientMarket market = GetMarket(client, marketString);
            //DateTime startTime = opts.StartTime.Value;
            DateTime startTime = new(2021, 1, 1);
            int totalDay = (GetEndTime() - startTime).Days;

            while (startTime < GetEndTime())
            {
                DateTime endTime = startTime.AddHours(12);

                try
                {
                    var klines = await market.GetKlinesAsync(symbol, KlineInterval.OneMinute, startTime, endTime, 1000);
                    if (klines != null && klines.Success)
                    {
                        kLines.AddRange(klines.Data.OrderBy(item => item.CloseTime).Select(item =>
                            new CoinDataModel
                            {
                                Volume = item.BaseVolume,
                                Close = item.Close,
                                High = item.High,
                                Low = item.Low,
                                Open = item.Open,
                                Date = item.OpenTime,
                                Money = item.QuoteVolume,
                                TakerBuyBaseVolume = item.TakerBuyBaseVolume,
                                TakerBuyQuoteVolume = item.TakerBuyQuoteVolume,
                                TradeCount = item.TradeCount,
                                CoinId = id,
                                Key = $"{id}-{item.OpenTime}"
                            }));
                    }
                }
                catch
                {
                    Console.WriteLine($"{marketString}-{symbol} at {startTime} has error.");
                }

                await Task.Delay(500);

                int dayDiff = (GetEndTime() - startTime).Days;
                float dayDiffPercentage = (float)dayDiff / (float)totalDay;
                startTime = endTime;
            }

            kLines = kLines.DistinctBy(item => item.Date).OrderBy(item => item.Date).ToList();
            kLines.RemoveAt(kLines.Count - 1);
            return kLines;
        }

        static DateTime GetEndTime()
            //=> opts.EndTime == null ? DateTime.Now : opts.EndTime.Value;
            => DateTime.Now;

        static IBinanceClientMarket GetMarket(BinanceClient client, string market)
            => (market.ToLower()) switch
            {
                "futurescoin" => client.FuturesCoin.Market,
                "futuresusdt" => client.FuturesUsdt.Market,
                _ => client.Spot.Market
            };

        private static async Task<ICollection<(Guid Id, string Symbol, string Market)>> GetSymbols()
        {
            using BinanceDbContext db = new();
            return (await db.Coins.ToArrayAsync()).Select(item => {
                string[] temp = item.Name.Split('-');
                return (item.Id, temp[0], temp[1]);
            }).ToArray();
        }
    }
}
