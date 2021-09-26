using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CollectorModels;
using EFCore.BulkExtensions;
using Magicodes.ExporterAndImporter.Core;
using Magicodes.ExporterAndImporter.Csv;
using MoreLinq.Extensions;

namespace UploadToDataBase
{
    class Program
    {
        const string source = @"/Users/kenneth/OneDrive - 臺北科技大學 軟體工程實驗室/量化交易/General/原始資料集";

        static void Main(string[] args)
        {
            object lockObject = new();

            Parallel.ForEach(Directory.GetFiles(source, "*.csv"), new ParallelOptions { MaxDegreeOfParallelism = 6 }, (path) =>
            {
                QlibKline[] qlibKlines = LoadCSVAsync(path).Result;
                CoinModel coin = new() { Name = qlibKlines[0].StockCode };
                CoinDataModel[] coinDatas = qlibKlines.Select(item => new CoinDataModel()
                {
                    CoinId = coin.Id,
                    Key = $"{coin.Id}-{item.Date}",
                    Date = item.Date,
                    Close = item.Close,
                    High = item.High,
                    Low = item.Low,
                    Money = item.Money,
                    Open = item.Open,
                    TakerBuyBaseVolume = item.TakerBuyBaseVolume,
                    TakerBuyQuoteVolume = item.TakerBuyQuoteVolume,
                    TradeCount = item.TradeCount,
                    Volume = item.Volume
                }).ToArray();
                Console.WriteLine($"{qlibKlines[0].StockCode}-Load data");

                using BinanceDbContext db = new();
                db.ChangeTracker.AutoDetectChangesEnabled = false;
                db.ChangeTracker.LazyLoadingEnabled = false;
                db.Coins.Add(coin);
                db.SaveChanges();
                Console.WriteLine($"{qlibKlines[0].StockCode}-Add coin");
                db.BulkInsert(coinDatas);
                Console.WriteLine($"{qlibKlines[0].StockCode}-Add coin data");
            });

            Console.ReadKey();
        }

        static async Task<QlibKline[]> LoadCSVAsync(string path)
        {
            IImporter importer = new CsvImporter();
            var result = await importer.Import<QlibKline>(path);
            if (result.HasError)
            {
                Console.WriteLine($"{path} has error");
                throw result.Exception;
            }

            return result.Data.DistinctBy(item => item.Date).OrderBy(item => item.Date).ToArray();
        }
    }
}
