using EFCore.BulkExtensions;
using Magicodes.ExporterAndImporter.Core;
using Magicodes.ExporterAndImporter.Csv;
using UploadToDataBase.Models;

namespace UploadToDataBase
{
    class Program
    {
        const string source = @"C:\Users\kenneth\臺北科技大學 軟體工程實驗室\量化交易 - General\原始資料集";

        static async Task Main(string[] args)
        {
            foreach(string path in Directory.GetFiles(source))
            {
                QlibKline[] qlibKlines = await LoadCSVAsync(path);
                Console.WriteLine($"{qlibKlines[0].StockCode}-Load data");
                using BinanceDbContext db = new();
                db.ChangeTracker.AutoDetectChangesEnabled = false;
                db.ChangeTracker.LazyLoadingEnabled = false;
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

                db.Coins.Add(coin);
                await db.SaveChangesAsync();
                Console.WriteLine($"{qlibKlines[0].StockCode}-Add coin");
                await db.BulkInsertOrUpdateAsync(coinDatas, new BulkConfig() { BatchSize = 1000 });
                Console.WriteLine($"{qlibKlines[0].StockCode}-Add coin data");
            }
        }

        static async Task<QlibKline[]> LoadCSVAsync(string path)
        {
            IImporter importer = new CsvImporter();
            var result = await importer.Import<QlibKline>(path);
            if (result.HasError)
                throw result.Exception;

            return result.Data.DistinctBy(item => item.Date).OrderBy(item => item.Date).ToArray();
        }
    }
}
