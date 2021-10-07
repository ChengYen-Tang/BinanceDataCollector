using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CollectorModels;
using Magicodes.ExporterAndImporter.Core;
using Magicodes.ExporterAndImporter.Csv;
using Microsoft.EntityFrameworkCore;

namespace DownloadFromDataBase
{
    static class Program
    {
        static async Task Main(string[] args)
        {
            CoinModel[] coins = await GetCoinsId();
            int i = 0;
            foreach(CoinModel coin in coins)
            {
                using BinanceDbContext db = new();
                CoinDataModel[] data = await db.CoinData.Where(item => item.CoinId == coin.Id).OrderBy(item => item.Date).ToArrayAsync();
                ICollection<QlibKline> kline = CoinModelToQlibKlines(data, coin.Name);
                await ExportAsync("/Users/kenneth/OneDrive - 臺北科技大學 軟體工程實驗室/量化交易/General/原始資料集-測試", $"{coin.Name}.csv", kline);
                i++;
                Console.WriteLine($"{coin.Name}.csv [{i}]");
            }
        }

        static ICollection<QlibKline> CoinModelToQlibKlines(CoinDataModel[] coinData, string stockCode)
        {
            QlibKline[] qlibKlines = new QlibKline[coinData.Length];
            Parallel.ForEach(coinData, (kline, state, index) =>
            {
                qlibKlines[index] = index == 0 ? kline.ToQlibKline(stockCode, isFirst: true) : kline.ToQlibKline(stockCode, coinData[Convert.ToInt32(index) - 1].Close);
            });

            return qlibKlines;
        }

        static QlibKline ToQlibKline(this CoinDataModel data, string stockCode, decimal LastClose = default, bool isFirst = false)
            => new()
            {
                StockCode = stockCode,
                Date = data.Date,
                Open = data.Open,
                Close = data.Close,
                High = data.High,
                Low = data.Low,
                Volume = data.Volume,
                Money = data.Money,
                Factor = 1,
                TradeCount = data.TradeCount,
                TakerBuyBaseVolume = data.TakerBuyBaseVolume,
                TakerBuyQuoteVolume = data.TakerBuyQuoteVolume,
                //Change = isFirst ? 0 : (data.Close - LastClose) / LastClose
            };

        static async Task ExportAsync(string path, string fileName, ICollection<QlibKline> data)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            string file = Path.Combine(path, fileName);
            IExporter exporter = new CsvExporter();
            await exporter.Export(file, data);
        }

        static async Task<CoinModel[]> GetCoinsId()
        {
            using BinanceDbContext db = new();
            return await db.Coins.ToArrayAsync();
        }
    }
}
