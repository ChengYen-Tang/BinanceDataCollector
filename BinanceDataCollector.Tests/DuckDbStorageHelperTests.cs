using CollectorModels.Models.Csv;
using DuckDB.NET.Data;
using System.Text.Json;

namespace BinanceDataCollector.Tests;

[TestClass]
public sealed class DuckDbStorageHelperTests
{
    private string tempDirectory = null!;

    [TestInitialize]
    public void Initialize()
    {
        tempDirectory = Path.Combine(Path.GetTempPath(), "BinanceDataCollector.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDirectory);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(tempDirectory))
            Directory.Delete(tempDirectory, true);
    }

    [TestMethod]
    public async Task ReplaceTableAsync_ReplacesRowsAndPersistsSupportedTypes()
    {
        string dbPath = CreateDbPath();
        SampleRecord[] original =
        [
            new()
            {
                Id = "1",
                Name = "BTCUSDT",
                Count = 2,
                Ratio = 1.5,
                Amount = 12.34m,
                IsActive = true,
                OccurredAt = new DateTime(2026, 05, 26, 8, 30, 0, DateTimeKind.Utc),
                Status = SampleStatus.Live,
                Tags = ["spot", "core"]
            }
        ];
        SampleRecord[] replacement =
        [
            new()
            {
                Id = "2",
                Name = "ETHUSDT",
                Count = 3,
                Ratio = 2.5,
                Amount = 45.67m,
                IsActive = false,
                OccurredAt = new DateTime(2026, 05, 27, 9, 0, 0, DateTimeKind.Utc),
                Status = SampleStatus.Archived,
                Tags = ["futures"]
            }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", original);
        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", replacement);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name, Count, Ratio, Amount, IsActive, OccurredAt, Status, Tags FROM Symbols;";
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("2", reader.GetString(0));
        Assert.AreEqual("ETHUSDT", reader.GetString(1));
        Assert.AreEqual(3, reader.GetInt32(2));
        Assert.AreEqual(2.5d, reader.GetDouble(3));
        Assert.AreEqual(45.67m, reader.GetDecimal(4));
        Assert.IsFalse(reader.GetBoolean(5));
        Assert.AreEqual(replacement[0].OccurredAt, reader.GetDateTime(6));
        Assert.AreEqual("Archived", reader.GetString(7));
        CollectionAssert.AreEqual(replacement[0].Tags, JsonSerializer.Deserialize<string[]>(reader.GetString(8))!);
        Assert.IsFalse(await reader.ReadAsync());
    }

    [TestMethod]
    public async Task UpsertRowsAsync_ReplacesRowsWithMatchingKeyOnly()
    {
        string dbPath = CreateDbPath();
        UpsertRecord[] initial =
        [
            new() { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "2", Name = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) }
        ];
        UpsertRecord[] update =
        [
            new() { Id = "2", Name = "ETHUSDT-UPDATED", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "3", Name = "BNBUSDT", OccurredAt = new DateTime(2026, 05, 04, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", initial);
        await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Symbols", update, nameof(UpsertRecord.Id));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM Symbols ORDER BY Id;";
        await using var reader = await command.ExecuteReaderAsync();

        List<string> rows = [];
        while (await reader.ReadAsync())
            rows.Add($"{reader.GetString(0)}:{reader.GetString(1)}");

        CollectionAssert.AreEqual(
            new[] { "1:BTCUSDT", "2:ETHUSDT-UPDATED", "3:BNBUSDT" },
            rows);
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenIncomingRowsOverlapExistingRows_UpdatesAllMatchingKeysAndPreservesNonOverlappingRows()
    {
        string dbPath = CreateDbPath();
        UpsertRecord[] initial =
        [
            new() { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "2", Name = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "3", Name = "BNBUSDT", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "4", Name = "SOLUSDT", OccurredAt = new DateTime(2026, 05, 04, 0, 0, 0, DateTimeKind.Utc) }
        ];
        UpsertRecord[] overlap =
        [
            new() { Id = "2", Name = "ETHUSDT-V2", OccurredAt = new DateTime(2026, 05, 12, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "3", Name = "BNBUSDT-V2", OccurredAt = new DateTime(2026, 05, 13, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "5", Name = "XRPUSDT", OccurredAt = new DateTime(2026, 05, 14, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", initial);
        await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Symbols", overlap, nameof(UpsertRecord.Id));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM Symbols ORDER BY Id;";
        await using var reader = await command.ExecuteReaderAsync();

        List<string> rows = [];
        while (await reader.ReadAsync())
            rows.Add($"{reader.GetString(0)}:{reader.GetString(1)}");

        CollectionAssert.AreEqual(
            new[]
            {
                "1:BTCUSDT",
                "2:ETHUSDT-V2",
                "3:BNBUSDT-V2",
                "4:SOLUSDT",
                "5:XRPUSDT"
            },
            rows);
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenIncomingBatchContainsDuplicateKeysOnUpsertCreatedTable_ThrowsConstraintException()
    {
        string dbPath = CreateDbPath();
        UpsertRecord[] initialUpsert =
        [
            new() { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }
        ];
        UpsertRecord[] duplicatedIncoming =
        [
            new() { Id = "1", Name = "BTCUSDT-V2", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "1", Name = "BTCUSDT-V3", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Symbols", initialUpsert, nameof(UpsertRecord.Id));

        await Assert.ThrowsExactlyAsync<DuckDBException>(async () =>
            await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Symbols", duplicatedIncoming, nameof(UpsertRecord.Id)));
    }

    [TestMethod]
    public async Task GetStringValuesAndGetMaxDateTimeAsync_ReturnExpectedResults()
    {
        string dbPath = CreateDbPath();
        QueryRecord[] records =
        [
            new() { Id = "1", SymbolInfoId = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc), Category = RecordCategory.Monthly },
            new() { Id = "2", SymbolInfoId = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc), Category = RecordCategory.Monthly },
            new() { Id = "3", SymbolInfoId = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 05, 0, 0, 0, DateTimeKind.Utc), Category = RecordCategory.Daily }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", records);

        List<string> values = await DuckDbStorageHelper.GetStringValuesAsync(dbPath, "Rows", nameof(QueryRecord.SymbolInfoId));
        DateTime? maxForBtcDaily = await DuckDbStorageHelper.GetMaxDateTimeAsync(
            dbPath,
            "Rows",
            nameof(QueryRecord.OccurredAt),
            new Dictionary<string, object?>
            {
                [nameof(QueryRecord.SymbolInfoId)] = "BTCUSDT",
                [nameof(QueryRecord.Category)] = RecordCategory.Daily
            });

        CollectionAssert.AreEqual(new[] { "BTCUSDT", "BTCUSDT", "ETHUSDT" }, values);
        Assert.AreEqual(new DateTime(2026, 05, 05, 0, 0, 0, DateTimeKind.Utc), maxForBtcDaily);
    }

    [TestMethod]
    public async Task DeleteRowsBeforeAsync_RemovesOnlyOlderRows()
    {
        string dbPath = CreateDbPath();
        QueryRecord[] records =
        [
            new() { Id = "1", SymbolInfoId = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc), Category = RecordCategory.Monthly },
            new() { Id = "2", SymbolInfoId = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 10, 0, 0, 0, DateTimeKind.Utc), Category = RecordCategory.Monthly }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", records);
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "Rows", nameof(QueryRecord.OccurredAt), new DateTime(2026, 05, 05, 0, 0, 0, DateTimeKind.Utc));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM Rows;";

        object? count = await command.ExecuteScalarAsync();
        Assert.AreEqual(1, Convert.ToInt32(count));
    }

    [TestMethod]
    public async Task DropTableIfExistsAsync_RemovesExistingTableAndIgnoresMissingTable()
    {
        string dbPath = CreateDbPath();
        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            new[] { new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) } });

        await DuckDbStorageHelper.DropTableIfExistsAsync(dbPath, "Rows");
        await DuckDbStorageHelper.DropTableIfExistsAsync(dbPath, "Rows");

        DateTime? max = await DuckDbStorageHelper.GetMaxDateTimeAsync(dbPath, "Rows", nameof(UpsertRecord.OccurredAt));
        Assert.IsNull(max);
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenSymbolInfoCsvNameOverlaps_ReplacesMatchingNameOnly()
    {
        string dbPath = CreateDbPath();
        SymbolInfoCsv[] initial =
        [
            new() { Name = "BTCUSDT", Status = "Trading", BaseAsset = "BTC", QuoteAsset = "USDT", BaseAssetPrecision = 8 },
            new() { Name = "ETHUSDT", Status = "Trading", BaseAsset = "ETH", QuoteAsset = "USDT", BaseAssetPrecision = 8 }
        ];
        SymbolInfoCsv[] update =
        [
            new() { Name = "ETHUSDT", Status = "Break", BaseAsset = "ETH", QuoteAsset = "USDT", BaseAssetPrecision = 18 },
            new() { Name = "BNBUSDT", Status = "Trading", BaseAsset = "BNB", QuoteAsset = "USDT", BaseAssetPrecision = 8 }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Spot", initial);
        await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Spot", update, nameof(SymbolInfoCsv.Name));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Name, Status, BaseAssetPrecision FROM Spot ORDER BY Name;";
        await using var reader = await command.ExecuteReaderAsync();

        List<string> rows = [];
        while (await reader.ReadAsync())
            rows.Add($"{reader.GetString(0)}:{reader.GetString(1)}:{reader.GetInt32(2)}");

        CollectionAssert.AreEqual(
            new[]
            {
                "BNBUSDT:Trading:8",
                "BTCUSDT:Trading:8",
                "ETHUSDT:Break:18"
            },
            rows);
    }

    [TestMethod]
    public async Task ReplaceTableAsync_WhenKeyColumnProvided_RecreatesLegacyTableWithPrimaryKey()
    {
        string dbPath = CreateDbPath();
        SymbolInfoCsv[] legacyRows =
        [
            new() { Name = "BTCUSDT", Status = "Trading", BaseAsset = "BTC", QuoteAsset = "USDT", BaseAssetPrecision = 8 }
        ];
        SymbolInfoCsv[] replacement =
        [
            new() { Name = "ETHUSDT", Status = "Trading", BaseAsset = "ETH", QuoteAsset = "USDT", BaseAssetPrecision = 18 }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Spot", legacyRows);
        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Spot", replacement, nameof(SymbolInfoCsv.Name), true);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();

        using (DuckDBCommand primaryKeyCommand = connection.CreateCommand())
        {
            primaryKeyCommand.CommandText = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = 'Spot' AND constraint_type = 'PRIMARY KEY';";
            object? constraint = await primaryKeyCommand.ExecuteScalarAsync();
            Assert.AreEqual("PRIMARY KEY", Convert.ToString(constraint));
        }

        await Assert.ThrowsExactlyAsync<DuckDBException>(async () =>
            await DuckDbStorageHelper.UpsertRowsAsync(
                dbPath,
                "Spot",
                [
                    new SymbolInfoCsv { Name = "ETHUSDT", Status = "Break" },
                    new SymbolInfoCsv { Name = "ETHUSDT", Status = "Trading" }
                ],
                nameof(SymbolInfoCsv.Name)));
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenKlineCloseTimeOverlaps_ReplacesOnlyMatchingTimeKeys()
    {
        string dbPath = CreateDbPath();
        Kline[] initial =
        [
            CreateKline(1000, 1999, 100),
            CreateKline(2000, 2999, 200),
            CreateKline(3000, 3999, 300)
        ];
        Kline[] overlap =
        [
            CreateKline(2000, 2999, 222),
            CreateKline(3000, 3999, 333),
            CreateKline(4000, 4999, 444)
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "BTCUSDT", initial);
        await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "BTCUSDT", overlap, nameof(Kline.CloseTime));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT OpenTime, CloseTime, ClosePrice FROM BTCUSDT ORDER BY CloseTime;";
        await using var reader = await command.ExecuteReaderAsync();

        List<string> rows = [];
        while (await reader.ReadAsync())
            rows.Add($"{reader.GetInt64(0)}:{reader.GetInt64(1)}:{reader.GetDouble(2)}");

        CollectionAssert.AreEqual(
            new[]
            {
                "1000:1999:100",
                "2000:2999:222",
                "3000:3999:333",
                "4000:4999:444"
            },
            rows);
    }

    [TestMethod]
    public async Task GetMaxInt64AsyncAndDeleteRowsBeforeAsync_WorkWithCsvTimeKeys()
    {
        string dbPath = CreateDbPath();
        FundingRate[] rows =
        [
            new() { FundingTime = 1_000, Rate = 0.001, MarkPrice = 100_000 },
            new() { FundingTime = 2_000, Rate = 0.002, MarkPrice = 101_000 },
            new() { FundingTime = 3_000, Rate = 0.003, MarkPrice = 102_000 }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "BTCUSDT", rows);

        long? latest = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "BTCUSDT", nameof(FundingRate.FundingTime));
        Assert.AreEqual(3_000L, latest);

        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "BTCUSDT", nameof(FundingRate.FundingTime), 2_500L);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT FundingTime FROM BTCUSDT ORDER BY FundingTime;";
        await using var reader = await command.ExecuteReaderAsync();

        List<long> remaining = [];
        while (await reader.ReadAsync())
            remaining.Add(reader.GetInt64(0));

        CollectionAssert.AreEqual(new long[] { 3_000 }, remaining);
    }

    private string CreateDbPath()
        => Path.Combine(tempDirectory, "storage.duckdb");

    private static Kline CreateKline(long openTime, long closeTime, double closePrice)
        => new()
        {
            OpenTime = openTime,
            OpenPrice = closePrice - 1,
            HighPrice = closePrice + 1,
            LowPrice = closePrice - 2,
            ClosePrice = closePrice,
            Volume = closePrice * 10,
            CloseTime = closeTime,
            QuoteVolume = closePrice * 20,
            TradeCount = (int)closePrice,
            TakerBuyBaseVolume = closePrice * 5,
            TakerBuyQuoteVolume = closePrice * 6
        };

    private sealed class SampleRecord
    {
        public required string Id { get; set; }
        public required string Name { get; set; }
        public int Count { get; set; }
        public double Ratio { get; set; }
        public decimal Amount { get; set; }
        public bool IsActive { get; set; }
        public DateTime OccurredAt { get; set; }
        public SampleStatus Status { get; set; }
        public string[] Tags { get; set; } = [];
    }

    private sealed class UpsertRecord
    {
        public required string Id { get; set; }
        public required string Name { get; set; }
        public DateTime OccurredAt { get; set; }
    }

    private sealed class QueryRecord
    {
        public required string Id { get; set; }
        public required string SymbolInfoId { get; set; }
        public DateTime OccurredAt { get; set; }
        public RecordCategory Category { get; set; }
    }

    private enum SampleStatus
    {
        Live,
        Archived
    }

    private enum RecordCategory
    {
        Daily,
        Monthly
    }
}
