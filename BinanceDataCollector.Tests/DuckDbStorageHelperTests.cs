using CollectorModels.Models.Storage;
using DuckDB.NET.Data;
using System.Collections.Concurrent;
using System.Text.Json;

namespace BinanceDataCollector.Tests;

[TestClass]
[DoNotParallelize]
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
        DuckDbStorageHelper.CloseAllConnections();

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
    public async Task ReplaceTableAsync_WhenRowsContainNullValues_PersistsDbNulls()
    {
        string dbPath = CreateDbPath();
        NullableRecord[] rows =
        [
            new()
            {
                Id = "1",
                OptionalName = null,
                OptionalCount = null,
                OptionalOccurredAt = null,
                OptionalStatus = null
            }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", rows, nameof(NullableRecord.Id), recreateTable: true);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT OptionalName, OptionalCount, OptionalOccurredAt, OptionalStatus FROM Rows;";
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.IsTrue(reader.IsDBNull(0));
        Assert.IsTrue(reader.IsDBNull(1));
        Assert.IsTrue(reader.IsDBNull(2));
        Assert.IsTrue(reader.IsDBNull(3));
    }

    [TestMethod]
    public async Task ReplaceTableAsync_WhenRecordContainsEnumCollection_PersistsAsJsonStringArray()
    {
        string dbPath = CreateDbPath();
        EnumCollectionRecord[] rows =
        [
            new()
            {
                Id = "1",
                States = [SampleStatus.Live, SampleStatus.Archived]
            }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", rows);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT States FROM Rows;";
        string payload = Convert.ToString(await command.ExecuteScalarAsync())!;

        CollectionAssert.AreEqual(new[] { "Live", "Archived" }, JsonSerializer.Deserialize<string[]>(payload)!);
    }

    [TestMethod]
    public async Task ReplaceTableAsync_WhenCalledWithEmptyReplacement_ClearsExistingRowsAndKeepsTableQueryable()
    {
        string dbPath = CreateDbPath();

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            new[]
            {
                new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }
            });

        await DuckDbStorageHelper.ReplaceTableAsync<UpsertRecord>(dbPath, "Rows", []);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM Rows;";

        object? count = await command.ExecuteScalarAsync();
        Assert.AreEqual(0, Convert.ToInt32(count));
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
    public async Task ReplaceTableAsync_WhenAppenderInsertFails_RollsBackPriorDelete()
    {
        string dbPath = CreateDbPath();
        UpsertRecord[] initial =
        [
            new() { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }
        ];
        UpsertRecord[] duplicatedReplacement =
        [
            new() { Id = "2", Name = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "2", Name = "ETHUSDT-DUP", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", initial, nameof(UpsertRecord.Id), recreateTable: true);

        await Assert.ThrowsExactlyAsync<DuckDBException>(async () =>
            await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", duplicatedReplacement, nameof(UpsertRecord.Id), recreateTable: false));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM Symbols ORDER BY Id;";
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("1", reader.GetString(0));
        Assert.AreEqual("BTCUSDT", reader.GetString(1));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenAppenderInsertFails_RollsBackDeleteAndPreservesExistingRows()
    {
        string dbPath = CreateDbPath();
        UpsertRecord[] initial =
        [
            new() { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }
        ];
        UpsertRecord[] duplicatedIncoming =
        [
            new() { Id = "1", Name = "BTCUSDT-V2", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "1", Name = "BTCUSDT-V3", OccurredAt = new DateTime(2026, 05, 03, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Symbols", initial, nameof(UpsertRecord.Id), recreateTable: true);

        await Assert.ThrowsExactlyAsync<DuckDBException>(async () =>
            await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Symbols", duplicatedIncoming, nameof(UpsertRecord.Id)));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM Symbols ORDER BY Id;";
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("1", reader.GetString(0));
        Assert.AreEqual("BTCUSDT", reader.GetString(1));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenKeyColumnDoesNotExist_ThrowsInvalidOperationException()
    {
        string dbPath = CreateDbPath();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await DuckDbStorageHelper.UpsertRowsAsync(
                dbPath,
                "Symbols",
                [new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }],
                "MissingKey"));
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenRecordsAreEmpty_DoesNotCreateTable()
    {
        string dbPath = CreateDbPath();

        await DuckDbStorageHelper.UpsertRowsAsync<UpsertRecord>(dbPath, "Symbols", [], nameof(UpsertRecord.Id));

        long? max = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Symbols", nameof(QueryLongRecord.Sequence));
        Assert.IsNull(max);
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
    public async Task GetStringValuesAsync_WhenDatabaseOrTableDoesNotExist_ReturnsEmptyList()
    {
        string dbPath = CreateDbPath();

        List<string> valuesFromMissingDb = await DuckDbStorageHelper.GetStringValuesAsync(dbPath, "Rows", "Name");
        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "OtherRows",
            [new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }]);
        List<string> valuesFromMissingTable = await DuckDbStorageHelper.GetStringValuesAsync(dbPath, "Rows", "Name");

        Assert.IsEmpty(valuesFromMissingDb);
        Assert.IsEmpty(valuesFromMissingTable);
    }

    [TestMethod]
    public async Task GetMaxDateTimeAsyncAndGetMaxInt64Async_WhenDatabaseOrTableDoesNotExist_ReturnNull()
    {
        string dbPath = CreateDbPath();

        DateTime? missingDate = await DuckDbStorageHelper.GetMaxDateTimeAsync(dbPath, "Rows", nameof(QueryRecord.OccurredAt));
        long? missingLong = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", nameof(QueryLongRecord.Sequence));

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "OtherRows",
            [new QueryLongRecord { Id = "1", Sequence = 123 }]);

        DateTime? missingTableDate = await DuckDbStorageHelper.GetMaxDateTimeAsync(dbPath, "Rows", nameof(QueryRecord.OccurredAt));
        long? missingTableLong = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", nameof(QueryLongRecord.Sequence));

        Assert.IsNull(missingDate);
        Assert.IsNull(missingLong);
        Assert.IsNull(missingTableDate);
        Assert.IsNull(missingTableLong);
    }

    [TestMethod]
    public async Task DeleteRowsBeforeAsyncAndDropTableIfExistsAsync_WhenDatabaseOrTableDoesNotExist_DoNotThrow()
    {
        string dbPath = CreateDbPath();

        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "Rows", nameof(QueryRecord.OccurredAt), new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc));
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "Rows", nameof(QueryLongRecord.Sequence), 100L);
        await DuckDbStorageHelper.DropTableIfExistsAsync(dbPath, "Rows");

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "OtherRows",
            [new QueryLongRecord { Id = "1", Sequence = 123 }]);

        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "Rows", nameof(QueryRecord.OccurredAt), new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc));
        await DuckDbStorageHelper.DeleteRowsBeforeAsync(dbPath, "Rows", nameof(QueryLongRecord.Sequence), 100L);
        await DuckDbStorageHelper.DropTableIfExistsAsync(dbPath, "Rows");
    }

    [TestMethod]
    public async Task EnsureDatabaseCreatedAsync_WhenDatabaseDoesNotExist_CreatesFile()
    {
        string dbPath = Path.Combine(tempDirectory, "bootstrap", "storage.duckdb");

        await DuckDbStorageHelper.EnsureDatabaseCreatedAsync(dbPath);

        Assert.IsTrue(File.Exists(dbPath));
    }

    [TestMethod]
    public async Task EnsureDatabaseCreatedAsync_AllowsSubsequentWrites()
    {
        string dbPath = Path.Combine(tempDirectory, "bootstrap-write", "storage.duckdb");

        await DuckDbStorageHelper.EnsureDatabaseCreatedAsync(dbPath);

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            [new QueryLongRecord { Id = "1", Sequence = 123L }]);

        long? max = await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", nameof(QueryLongRecord.Sequence));
        Assert.AreEqual(123L, max);
    }

    [TestMethod]
    public async Task GetMaxDateTimeAsync_WhenFilteredByNullValue_ReturnsMatchingRow()
    {
        string dbPath = CreateDbPath();
        NullableRecord[] rows =
        [
            new() { Id = "1", OptionalName = null, OptionalOccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = "2", OptionalName = "BTCUSDT", OptionalOccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) }
        ];

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", rows);

        DateTime? max = await DuckDbStorageHelper.GetMaxDateTimeAsync(
            dbPath,
            "Rows",
            nameof(NullableRecord.OptionalOccurredAt),
            new Dictionary<string, object?> { [nameof(NullableRecord.OptionalName)] = null });

        Assert.AreEqual(new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc), max);
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

    [TestMethod]
    public async Task CloseAllConnections_WhenDatabaseWasWritten_ReleasesFileHandle()
    {
        string dbPath = CreateDbPath();

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            new[]
            {
                new UpsertRecord
                {
                    Id = "1",
                    Name = "BTCUSDT",
                    OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc)
                }
            });

        DuckDbStorageHelper.CloseAllConnections();

        File.Delete(dbPath);

        Assert.IsFalse(File.Exists(dbPath));
    }

    [TestMethod]
    public void CloseAllConnections_WhenCalledMultipleTimes_IsIdempotent()
    {
        DuckDbStorageHelper.CloseAllConnections();
        DuckDbStorageHelper.CloseAllConnections();
    }

    [TestMethod]
    public async Task ReplaceTableAsync_AfterCloseAllConnections_ReopensDatabaseAndContinuesWriting()
    {
        string dbPath = CreateDbPath();

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            new[]
            {
                new UpsertRecord
                {
                    Id = "1",
                    Name = "BTCUSDT",
                    OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc)
                }
            });

        DuckDbStorageHelper.CloseAllConnections();

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            new[]
            {
                new UpsertRecord
                {
                    Id = "2",
                    Name = "ETHUSDT",
                    OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc)
                }
            });

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM Rows;";
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("2", reader.GetString(0));
        Assert.AreEqual("ETHUSDT", reader.GetString(1));
        Assert.IsFalse(await reader.ReadAsync());
    }

    [TestMethod]
    public async Task ReplaceTableAsync_WhenInitialOpenFails_DoesNotPoisonFutureConnections()
    {
        string dbPath = CreateDbPath();
        Directory.CreateDirectory(dbPath);

        await Assert.ThrowsExactlyAsync<DuckDBException>(async () =>
            await DuckDbStorageHelper.ReplaceTableAsync(
                dbPath,
                "Rows",
                [new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }]));

        Directory.Delete(dbPath, true);

        await DuckDbStorageHelper.ReplaceTableAsync(
            dbPath,
            "Rows",
            [new UpsertRecord { Id = "2", Name = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) }]);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id FROM Rows;";

        Assert.AreEqual("2", Convert.ToString(await command.ExecuteScalarAsync()));
    }

    [TestMethod]
    public async Task ReplaceTableAsync_WhenUsingDifferentPathRepresentations_ReusesSameDatabaseFile()
    {
        string dbPath = CreateDbPath();
        string relativePath = Path.GetRelativePath(Directory.GetCurrentDirectory(), dbPath);

        await DuckDbStorageHelper.ReplaceTableAsync(
            relativePath,
            "Rows",
            [new UpsertRecord { Id = "1", Name = "BTCUSDT", OccurredAt = new DateTime(2026, 05, 01, 0, 0, 0, DateTimeKind.Utc) }]);

        await DuckDbStorageHelper.UpsertRowsAsync(
            dbPath,
            "Rows",
            [new UpsertRecord { Id = "2", Name = "ETHUSDT", OccurredAt = new DateTime(2026, 05, 02, 0, 0, 0, DateTimeKind.Utc) }],
            nameof(UpsertRecord.Id));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM Rows;";

        object? count = await command.ExecuteScalarAsync();
        Assert.AreEqual(2, Convert.ToInt32(count));
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenSameDatabaseFileIsAccessedConcurrently_ProducesConsistentRows()
    {
        string dbPath = CreateDbPath();

        Task[] tasks =
        [
            DuckDbStorageHelper.UpsertRowsAsync(
                dbPath,
                "BTCUSDT",
                [CreateKline(1000, 1999, 100), CreateKline(2000, 2999, 200)],
                nameof(Kline.CloseTime)),
            DuckDbStorageHelper.UpsertRowsAsync(
                dbPath,
                "ETHUSDT",
                [CreateKline(3000, 3999, 300), CreateKline(4000, 4999, 400)],
                nameof(Kline.CloseTime))
        ];

        await Task.WhenAll(tasks);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();

        using DuckDBCommand btcCommand = connection.CreateCommand();
        btcCommand.CommandText = "SELECT COUNT(*) FROM BTCUSDT;";
        using DuckDBCommand ethCommand = connection.CreateCommand();
        ethCommand.CommandText = "SELECT COUNT(*) FROM ETHUSDT;";

        Assert.AreEqual(2, Convert.ToInt32(await btcCommand.ExecuteScalarAsync()));
        Assert.AreEqual(2, Convert.ToInt32(await ethCommand.ExecuteScalarAsync()));
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenSameTableIsUpsertedConcurrentlyWithDisjointKeys_PreservesAllRows()
    {
        string dbPath = CreateDbPath();

        Task firstTask = Task.Run(() => DuckDbStorageHelper.UpsertRowsAsync(
            dbPath,
            "BTCUSDT",
            CreateKlines(0, 20, 100),
            nameof(Kline.CloseTime)));
        Task secondTask = Task.Run(() => DuckDbStorageHelper.UpsertRowsAsync(
            dbPath,
            "BTCUSDT",
            CreateKlines(20, 20, 200),
            nameof(Kline.CloseTime)));

        await Task.WhenAll(firstTask, secondTask);

        List<long> closeTimes = await ReadKlineCloseTimesAsync(dbPath, "BTCUSDT");

        Assert.HasCount(40, closeTimes);
        CollectionAssert.AreEqual(Enumerable.Range(0, 40).Select(index => 1_999L + (index * 1_000L)).ToList(), closeTimes);
    }

    [TestMethod]
    public async Task UpsertRowsAsync_WhenSameDatabaseIsWrittenConcurrentlyAcrossMultipleRounds_RemainsConsistent()
    {
        string dbPath = CreateDbPath();

        for (int round = 0; round < 8; round++)
        {
            Task[] tasks =
            [
                Task.Run(() => DuckDbStorageHelper.UpsertRowsAsync(
                    dbPath,
                    "BTCUSDT",
                    CreateKlines(round * 100, 12, 100 + round),
                    nameof(Kline.CloseTime))),
                Task.Run(() => DuckDbStorageHelper.UpsertRowsAsync(
                    dbPath,
                    "ETHUSDT",
                    CreateKlines((round * 100) + 1_000, 12, 200 + round),
                    nameof(Kline.CloseTime))),
                Task.Run(() => DuckDbStorageHelper.UpsertRowsAsync(
                    dbPath,
                    "SOLUSDT",
                    CreateKlines((round * 100) + 2_000, 12, 300 + round),
                    nameof(Kline.CloseTime)))
            ];

            await Task.WhenAll(tasks);
        }

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();

        foreach (string tableName in new[] { "BTCUSDT", "ETHUSDT", "SOLUSDT" })
        {
            using DuckDBCommand countCommand = connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM {tableName};";
            Assert.AreEqual(96, Convert.ToInt32(await countCommand.ExecuteScalarAsync()));
        }
    }

    [TestMethod]
    public async Task ExecuteWithConnectionAsync_WhenSamePathIsUsedConcurrently_UsesIndependentConnections()
    {
        string dbPath = CreateDbPath();
        ConcurrentDictionary<int, byte> connectionIds = [];

        Task[] tasks = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => DuckDbStorageHelper.ExecuteWithConnectionAsync(
                dbPath,
                async connection =>
                {
                    connectionIds.TryAdd(connection.GetHashCode(), 0);
                    await Task.Delay(10);
                })))
            .ToArray();

        await Task.WhenAll(tasks);

        Assert.IsGreaterThan(1, connectionIds.Count);
    }

    [TestMethod]
    public async Task ExecuteWithConnectionAsync_WhenSamePathIsUsedConcurrently_AllowsConcurrentCallbacks()
    {
        string dbPath = CreateDbPath();
        int concurrentCallbacks = 0;
        int observedMaxConcurrency = 0;
        using ManualResetEventSlim releaseCallbacks = new(false);

        Task[] tasks = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => DuckDbStorageHelper.ExecuteWithConnectionAsync(
                dbPath,
                async _ =>
                {
                    int current = Interlocked.Increment(ref concurrentCallbacks);
                    int snapshotMax;
                    do
                    {
                        snapshotMax = observedMaxConcurrency;
                        if (snapshotMax >= current)
                            break;
                    }
                    while (Interlocked.CompareExchange(ref observedMaxConcurrency, current, snapshotMax) != snapshotMax);

                    releaseCallbacks.Wait(TimeSpan.FromSeconds(5));
                    Interlocked.Decrement(ref concurrentCallbacks);
                })))
            .ToArray();

        SpinWait.SpinUntil(() => Volatile.Read(ref concurrentCallbacks) >= 2, TimeSpan.FromSeconds(5));
        releaseCallbacks.Set();
        await Task.WhenAll(tasks);

        Assert.IsGreaterThan(1, observedMaxConcurrency);
        Assert.AreEqual(0, concurrentCallbacks);
    }

    [TestMethod]
    public async Task GetMaxInt64Async_WhenReadsAndWritesRunConcurrently_DoesNotReturnOutOfRangeValue()
    {
        string dbPath = CreateDbPath();
        Task writerTask = Task.Run(async () =>
        {
            for (int batch = 0; batch < 10; batch++)
            {
                await DuckDbStorageHelper.UpsertRowsAsync(
                    dbPath,
                    "BTCUSDT",
                    CreateFundingRates(batch * 10, 10),
                    nameof(FundingRate.FundingTime));
            }
        });

        List<long?> observedValues = [];
        Task readerTask = Task.Run(async () =>
        {
            while (!writerTask.IsCompleted)
            {
                observedValues.Add(await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "BTCUSDT", nameof(FundingRate.FundingTime)));
                await Task.Delay(5);
            }

            observedValues.Add(await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "BTCUSDT", nameof(FundingRate.FundingTime)));
        });

        await Task.WhenAll(writerTask, readerTask);

        Assert.IsTrue(observedValues.All(value => value is null or >= 0 and <= 99_000));
        Assert.AreEqual(99_000L, observedValues[^1]);
    }

    [TestMethod]
    public async Task GetMaxInt64Async_WhenCanceledBeforeOperation_ThrowsOperationCanceledException()
    {
        string dbPath = CreateDbPath();

        using CancellationTokenSource cts = new();
        cts.Cancel();

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () =>
            await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", nameof(QueryLongRecord.Sequence), ct: cts.Token));
    }

    [TestMethod]
    public async Task AppendAggTradesFromCsvAsync_WhenFinalBatchIsPartial_DoesNotPersistStaleRows()
    {
        string dbPath = CreateDbPath();
        string csvPath = Path.Combine(tempDirectory, "aggTrades.csv");
        const int totalRows = 100_001;

        WriteAggTradesCsv(csvPath, totalRows, AggTradesCsvSchema.Futures);

        await DuckDbStorageHelper.AppendAggTradesFromCsvAsync(
            dbPath,
            "BTCUSDT",
            csvPath,
            AggTradesCsvSchema.Futures,
            sourceIsMicroseconds: false);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(parquetagg_trade_id), MAX(parquetagg_trade_id), MAX(transact_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(totalRows, reader.GetInt64(0));
        Assert.AreEqual(1L, reader.GetInt64(1));
        Assert.AreEqual(totalRows, reader.GetInt64(2));
        Assert.AreEqual(totalRows * 1_000_000L, reader.GetInt64(3));
    }

    [TestMethod]
    public async Task AppendAggTradesFromCsvAsync_WhenCsvHasHeader_ImportsAllRows()
    {
        string dbPath = CreateDbPath();
        string csvPath = Path.Combine(tempDirectory, "aggTrades-with-header.csv");

        WriteAggTradesCsv(csvPath, totalRows: 3, AggTradesCsvSchema.Futures, includeHeader: true);

        await DuckDbStorageHelper.AppendAggTradesFromCsvAsync(
            dbPath,
            "BTCUSDT",
            csvPath,
            AggTradesCsvSchema.Futures,
            sourceIsMicroseconds: false);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(parquetagg_trade_id), MAX(parquetagg_trade_id)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3L, reader.GetInt64(0));
        Assert.AreEqual(1L, reader.GetInt64(1));
        Assert.AreEqual(3L, reader.GetInt64(2));
    }

    [TestMethod]
    public async Task AppendAggTradesFromCsvAsync_WhenSpotCsvHasEightColumns_ImportsAllRows()
    {
        string dbPath = CreateDbPath();
        string csvPath = Path.Combine(tempDirectory, "spot-aggTrades-no-header.csv");

        WriteAggTradesCsv(csvPath, totalRows: 3, AggTradesCsvSchema.Spot, includeHeader: false);

        await DuckDbStorageHelper.AppendAggTradesFromCsvAsync(
            dbPath,
            "BTCUSDT",
            csvPath,
            AggTradesCsvSchema.Spot,
            sourceIsMicroseconds: true);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(parquetagg_trade_id), MAX(parquetagg_trade_id), MAX(transact_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3L, reader.GetInt64(0));
        Assert.AreEqual(1L, reader.GetInt64(1));
        Assert.AreEqual(3L, reader.GetInt64(2));
        Assert.AreEqual(3_000L, reader.GetInt64(3));
    }

    [TestMethod]
    public async Task AppendAggTradesFromCsvAsync_WhenDatabaseDoesNotExist_CreatesFileAndImportsRows()
    {
        string dbPath = CreateMarketDataDbPath("aggTrades");
        string csvPath = Path.Combine(tempDirectory, "aggTrades-first-write.csv");

        WriteAggTradesCsv(csvPath, totalRows: 3, AggTradesCsvSchema.Futures, includeHeader: true);

        await DuckDbStorageHelper.AppendAggTradesFromCsvAsync(
            dbPath,
            "BTCUSDT",
            csvPath,
            AggTradesCsvSchema.Futures,
            sourceIsMicroseconds: false);

        Assert.IsTrue(File.Exists(dbPath));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(parquetagg_trade_id), MAX(parquetagg_trade_id), MAX(transact_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3L, reader.GetInt64(0));
        Assert.AreEqual(1L, reader.GetInt64(1));
        Assert.AreEqual(3L, reader.GetInt64(2));
        Assert.AreEqual(3_000_000L, reader.GetInt64(3));
    }

    [TestMethod]
    public async Task ReplaceBookDepthTailFromCsvAsync_WhenFinalBatchIsPartial_DoesNotPersistStaleRows()
    {
        string dbPath = CreateDbPath();
        string csvPath = Path.Combine(tempDirectory, "bookDepth.csv");
        const int totalRows = 100_001;

        WriteBookDepthCsv(csvPath, totalRows);

        await DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(dbPath, "BTCUSDT", csvPath);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(snapshot_time), MAX(snapshot_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(totalRows, reader.GetInt64(0));
        Assert.AreEqual(1_735_689_600_000L, reader.GetInt64(1));
        Assert.AreEqual(1_735_689_600_000L + ((totalRows - 1) * 60_000L), reader.GetInt64(2));
    }

    [TestMethod]
    public async Task ReplaceBookDepthTailFromCsvAsync_WhenCsvHasNoHeader_ImportsFirstRow()
    {
        string dbPath = CreateDbPath();
        string csvPath = Path.Combine(tempDirectory, "bookDepth-no-header.csv");

        WriteBookDepthCsv(csvPath, totalRows: 3, includeHeader: false);

        await DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(dbPath, "BTCUSDT", csvPath);

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(snapshot_time), MAX(snapshot_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3L, reader.GetInt64(0));
        Assert.AreEqual(1_735_689_600_000L, reader.GetInt64(1));
        Assert.AreEqual(1_735_689_720_000L, reader.GetInt64(2));
    }

    [TestMethod]
    public async Task ReplaceBookDepthTailFromCsvAsync_WhenDatabaseDoesNotExist_CreatesFileAndImportsRows()
    {
        string dbPath = CreateMarketDataDbPath("bookDepth");
        string csvPath = Path.Combine(tempDirectory, "bookDepth-first-write.csv");

        WriteBookDepthCsv(csvPath, totalRows: 3, includeHeader: true);

        await DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(
            dbPath,
            "BTCUSDT",
            csvPath);

        Assert.IsTrue(File.Exists(dbPath));

        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*), MIN(snapshot_time), MAX(snapshot_time)
            FROM BTCUSDT;
            """;
        await using var reader = await command.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(3L, reader.GetInt64(0));
        Assert.AreEqual(1_735_689_600_000L, reader.GetInt64(1));
        Assert.AreEqual(1_735_689_720_000L, reader.GetInt64(2));
    }

    [TestMethod]
    [DataRow("aggTrades", 524288)]
    [DataRow("bookDepth", 262144)]
    [DataRow("Kline", 122880)]
    [DataRow("IndexPriceKline", 122880)]
    [DataRow("MarkPriceKline", 122880)]
    [DataRow("PremiumIndexKline", 122880)]
    [DataRow("FundingRate", 122880)]
    [DataRow("OpenInterestHistory", 122880)]
    [DataRow("Basis", 122880)]
    [DataRow("GlobalLongShortAccountRatio", 122880)]
    [DataRow("TopLongShortAccountRatio", 122880)]
    [DataRow("TopLongShortPositionRatio", 122880)]
    [DataRow("TakerLongShortRatio", 122880)]
    [DataRow("SymbolInfo", 122880)]
    public async Task StorageSettings_FirstWriteAndReopen_UseLatestAndExpectedRowGroups(string series, int expectedGroup)
    {
        string dbPath = CreateMarketDataDbPath(series);
        Assert.IsNull(await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", "Sequence"));
        Assert.IsFalse(Directory.Exists(Path.GetDirectoryName(dbPath)), "A missing-file read must not create storage folders.");

        await DuckDbStorageHelper.ReplaceTableAsync(dbPath, "Rows", [new QueryLongRecord { Id = "1", Sequence = 123 }]);
        string expectedVersion;
        using (DuckDBConnection reference = new("Data Source=:memory:"))
        {
            reference.Open();
            string referencePath = Path.Combine(tempDirectory, "native-latest.duckdb").Replace("'", "''", StringComparison.Ordinal);
            using DuckDBCommand command = reference.CreateCommand();
            command.CommandText = $"ATTACH '{referencePath}' AS expected (STORAGE_VERSION 'latest'); USE expected; CREATE TABLE reference_row(id INTEGER); CHECKPOINT;";
            command.ExecuteNonQuery();
            command.CommandText = "SELECT tags['storage_version'] FROM duckdb_databases() WHERE database_name=current_database();";
            expectedVersion = (string)command.ExecuteScalar()!;
        }

        foreach (string table in new[] { "first_open", "reopened" })
        {
            await DuckDbStorageHelper.ExecuteWithConnectionAsync(dbPath, connection =>
            {
                using DuckDBCommand command = connection.CreateCommand();
                command.CommandText = $"SET threads=1; CREATE TABLE {table} AS SELECT range AS id FROM range(600000); CHECKPOINT;";
                command.ExecuteNonQuery();
                return Task.CompletedTask;
            });
            await AssertStorageLayoutAsync(dbPath, table, expectedVersion, expectedGroup);
        }

        await DuckDbStorageHelper.CheckpointAsync(dbPath);
        await DuckDbStorageHelper.CheckpointDirtyDatabasesAsync();
        await DuckDbStorageHelper.CheckpointStorageAsync(Path.Combine(tempDirectory, series));
        await AssertStorageLayoutAsync(dbPath, "reopened", expectedVersion, expectedGroup);
        Assert.AreEqual(123L, await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows", "Sequence"));
    }

    [TestMethod]
    [DataRow("aggTrades")]
    [DataRow("bookDepth")]
    public async Task StorageSettings_TunedFiles_AllowConcurrentOperationsAndCheckpoint(string series)
    {
        string dbPath = CreateMarketDataDbPath(series);
        Task[] tasks = Enumerable.Range(0, 8).Select(index => Task.Run(async () =>
        {
            await DuckDbStorageHelper.UpsertRowsAsync(dbPath, "Rows" + index,
                [new QueryLongRecord { Id = "1", Sequence = index }], nameof(QueryLongRecord.Id));
            await DuckDbStorageHelper.ExecuteWithConnectionAsync(dbPath, async connection =>
            {
                using DuckDBCommand command = connection.CreateCommand();
                command.CommandText = $"SELECT Sequence FROM Rows{index};";
                Assert.AreEqual((long)index, Convert.ToInt64(await command.ExecuteScalarAsync()));
            });
            await DuckDbStorageHelper.CheckpointAsync(dbPath);
        })).ToArray();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30));
        CollectionAssert.AreEquivalent(Enumerable.Range(0, 8).Select(i => "Rows" + i).ToArray(),
            await DuckDbStorageHelper.GetTableNamesAsync(dbPath));
        for (int index = 0; index < 8; index++)
            Assert.AreEqual((long)index, await DuckDbStorageHelper.GetMaxInt64Async(dbPath, "Rows" + index, "Sequence"));
    }

    [TestMethod]
    [DataRow("aggTrades", "UsdFutures")]
    [DataRow("bookDepth", "CoinFutures")]
    public async Task StorageSettings_DifferentSymbols_RunConcurrently(string series, string market)
    {
        TaskCompletionSource secondEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task first = DuckDbStorageHelper.ExecuteWithConnectionAsync(Path.Combine(tempDirectory, series, market, "AAA.duckdb"),
            async connection =>
            {
                await secondEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                using DuckDBCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1;";
                Assert.AreEqual(1, Convert.ToInt32(command.ExecuteScalar()));
            });
        Task second = DuckDbStorageHelper.ExecuteWithConnectionAsync(Path.Combine(tempDirectory, series, market, "BBB.duckdb"),
            connection =>
            {
                using DuckDBCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 2;";
                Assert.AreEqual(2, Convert.ToInt32(command.ExecuteScalar()));
                secondEntered.SetResult();
                return Task.CompletedTask;
            });
        await Task.WhenAll(first, second);
    }

    [TestMethod]
    [DataRow("aggTrades")]
    [DataRow("bookDepth")]
    public async Task StorageSettings_CanceledCsvImport_DoesNotCreateStorage(string series)
    {
        string dbPath = CreateMarketDataDbPath(series);
        string csvPath = Path.Combine(tempDirectory, "canceled.csv");
        using CancellationTokenSource cts = new();
        cts.Cancel();
        if (series == "aggTrades")
        {
            WriteAggTradesCsv(csvPath, 3, AggTradesCsvSchema.Futures);
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
                DuckDbStorageHelper.AppendAggTradesFromCsvAsync(dbPath, "data", csvPath, AggTradesCsvSchema.Futures, false, cts.Token));
        }
        else
        {
            WriteBookDepthCsv(csvPath, 3);
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
                DuckDbStorageHelper.ReplaceBookDepthTailFromCsvAsync(dbPath, "data", csvPath, cts.Token));
        }
        Assert.IsFalse(File.Exists(dbPath));
        Assert.IsFalse(Directory.Exists(Path.GetDirectoryName(dbPath)));
    }

    [TestMethod]
    [DataRow("Kline", "CloseTime", "CloseTimeMinute", 60000L, 59999L, "ClosePrice")]
    [DataRow("IndexPriceKline", "CloseTime", "CloseTimeMinute", 60000L, 59999L, "ClosePrice")]
    [DataRow("MarkPriceKline", "CloseTime", "CloseTimeMinute", 60000L, 59999L, "ClosePrice")]
    [DataRow("PremiumIndexKline", "CloseTime", "CloseTimeMinute", 60000L, 59999L, "ClosePrice")]
    [DataRow("OpenInterestHistory", "Timestamp", "Timestamp5Minute", 300000L, 0L, "SumOpenInterest")]
    [DataRow("Basis", "Timestamp", "Timestamp5Minute", 300000L, 0L, "BasisValue")]
    [DataRow("GlobalLongShortAccountRatio", "Timestamp", "Timestamp5Minute", 300000L, 0L, "LongShortRatio")]
    [DataRow("TopLongShortAccountRatio", "Timestamp", "Timestamp5Minute", 300000L, 0L, "LongShortRatio")]
    [DataRow("TopLongShortPositionRatio", "Timestamp", "Timestamp5Minute", 300000L, 0L, "LongShortRatio")]
    [DataRow("TakerLongShortRatio", "Timestamp", "Timestamp5Minute", 300000L, 0L, "BuySellRatio")]
    public async Task TimeKeys_AllSeries_PreserveAtomicUpsertsQueriesAndPrimaryKey(
        string series, string logicalColumn, string physicalColumn, long unit, long offset, string valueColumn)
    {
        const long firstKey = 28_000_000;
        double original = BitConverter.Int64BitsToDouble(0x3FF0000000000001);
        foreach (string market in new[] { "Spot", "UsdFutures", "CoinFutures" })
        {
            string path = Path.Combine(tempDirectory, series, market, "TEST.duckdb");
            long first = firstKey * unit + offset;
            await UpsertTimeRowsAsync(series, path, [(first, original), (first + unit, 2.5)]);
            await DuckDbStorageHelper.CheckpointAsync(path);
            await UpsertTimeRowsAsync(series, path, [(first + unit, 3.5), (first + unit * 2, 4.5)]);
            await DuckDbStorageHelper.CheckpointAsync(path);

            Assert.AreEqual(first + unit * 2, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn));
            Assert.AreEqual(first, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn,
                new Dictionary<string, object?> { [logicalColumn] = first }));
            Assert.IsNull(await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn,
                new Dictionary<string, object?> { [logicalColumn] = first + 1 }));
            Assert.IsNull(await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn,
                new Dictionary<string, object?> { [logicalColumn] = null }));

            using (DuckDBConnection connection = new(new DuckDBConnectionStringBuilder { DataSource = path }.ConnectionString))
            {
                connection.Open();
                using DuckDBCommand command = connection.CreateCommand();
                command.CommandText = $"SELECT {physicalColumn}, {valueColumn} FROM data ORDER BY {physicalColumn};";
                using (var reader = command.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(firstKey, reader.GetInt64(0));
                    Assert.AreEqual(BitConverter.DoubleToInt64Bits(original), BitConverter.DoubleToInt64Bits(reader.GetDouble(1)));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(firstKey + 1, reader.GetInt64(0));
                    Assert.AreEqual(3.5, reader.GetDouble(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(firstKey + 2, reader.GetInt64(0));
                    Assert.AreEqual(4.5, reader.GetDouble(1));
                    Assert.IsFalse(reader.Read());
                }
                command.CommandText = $"SELECT COUNT(*) FROM duckdb_constraints() WHERE database_name=current_database() AND table_name='data' AND constraint_type='PRIMARY KEY' AND constraint_column_names=['{physicalColumn}'];";
                Assert.AreEqual(1L, Convert.ToInt64(command.ExecuteScalar()));
                command.CommandText = "INSERT INTO data SELECT * FROM data LIMIT 1;";
                Assert.Throws<DuckDBException>(() => command.ExecuteNonQuery());
                command.CommandText = $"INSERT INTO data ({physicalColumn}) VALUES (NULL);";
                Assert.Throws<DuckDBException>(() => command.ExecuteNonQuery());
            }

            // An off-grid retention threshold must delete the preceding aligned key.
            await DuckDbStorageHelper.DeleteRowsBeforeAsync(path, "data", logicalColumn, first + unit + 1);
            Assert.AreEqual(first + unit * 2, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn));
            await DuckDbStorageHelper.DeleteRowsBeforeAsync(path, "data", logicalColumn, first + unit * 2);
            Assert.AreEqual(first + unit * 2, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn));
            await DuckDbStorageHelper.DeleteRowsBeforeAsync(path, "data", logicalColumn, first + unit * 2 + 1);
            Assert.IsNull(await DuckDbStorageHelper.GetMaxInt64Async(path, "data", logicalColumn));
        }
    }

    [TestMethod]
    [DataRow("Kline", "CloseTime", 60000L, 59999L)]
    [DataRow("OpenInterestHistory", "Timestamp", 300000L, 0L)]
    public async Task TimeKeys_InvalidInputAndCanceledStart_DoNotCreateOrMutateStorage(string series, string column, long unit, long offset)
    {
        string path = CreateMarketDataDbPath(series);
        long first = 28_000_000 * unit + offset;
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => UpsertTimeRowsAsync(series, path, [(first + 1, 1)]));
        Assert.IsFalse(Directory.Exists(Path.GetDirectoryName(path)));
        using CancellationTokenSource canceled = new();
        canceled.Cancel();
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => UpsertTimeRowsAsync(series, path, [(first, 1)], canceled.Token));
        Assert.IsFalse(Directory.Exists(Path.GetDirectoryName(path)));

        await UpsertTimeRowsAsync(series, path, [(first, 1)]);
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => UpsertTimeRowsAsync(series, path, [(first, 9), (first + unit + 1, 2)]));
        Assert.AreEqual(first, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", column));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT {(series == "Kline" ? "ClosePrice" : "SumOpenInterest")} FROM data;";
            Assert.AreEqual(1d, Convert.ToDouble(command.ExecuteScalar()));
            return Task.CompletedTask;
        });

        // Duplicate input fails after the DELETE; the surrounding transaction restores the original row.
        await Assert.ThrowsAsync<DuckDBException>(() => UpsertTimeRowsAsync(series, path, [(first, 8), (first, 9)]));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT {(series == "Kline" ? "ClosePrice" : "SumOpenInterest")} FROM data;";
            Assert.AreEqual(1d, Convert.ToDouble(command.ExecuteScalar()));
            return Task.CompletedTask;
        });
    }

    [TestMethod]
    [DataRow("Kline", "CloseTime", 119999L)]
    [DataRow("OpenInterestHistory", "Timestamp", 300000L)]
    public async Task TimeKeys_OldSchema_RequiresRebuildWithoutChangingOriginalRows(string series, string column, long timestamp)
    {
        string path = CreateMarketDataDbPath(series);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using (DuckDBConnection connection = new(new DuckDBConnectionStringBuilder { DataSource = path }.ConnectionString))
        {
            connection.Open();
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"CREATE TABLE data ({column} BIGINT PRIMARY KEY); INSERT INTO data VALUES ({timestamp});";
            command.ExecuteNonQuery();
        }
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => UpsertTimeRowsAsync(series, path, [(timestamp, 1)]));
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => DuckDbStorageHelper.GetMaxInt64Async(path, "data", column));
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => DuckDbStorageHelper.DeleteRowsBeforeAsync(path, "data", column, timestamp + 1));
        string target = Path.Combine(tempDirectory, series, "Spot", "copy.duckdb");
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => DuckDbStorageHelper.CopyTableAsync(path, "data", target, "data"));
        Assert.IsFalse(File.Exists(target));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT {column} FROM data;";
            Assert.AreEqual(timestamp, Convert.ToInt64(command.ExecuteScalar()));
            return Task.CompletedTask;
        });
    }

    [TestMethod]
    [DataRow("Kline", "CloseTime", 60000L, 59999L)]
    [DataRow("OpenInterestHistory", "Timestamp", 300000L, 0L)]
    public void TimeKeys_IntegerBounds_AreExactAtNegativeTimesAndInt64Limits(string series, string column, long unit, long offset)
    {
        DuckDbTimeKey encoding = DuckDbTimeKey.ForColumn(CreateMarketDataDbPath(series), column)!;
        foreach (long threshold in new[] { long.MinValue, -unit - 1, -unit, -1L, 0L, offset, offset + 1, unit, long.MaxValue })
        {
            long boundary = encoding.ExclusiveUpperBound(threshold);
            Int128 firstRetained = (Int128)boundary * unit + offset;
            Assert.IsTrue(firstRetained >= threshold);
            Assert.IsTrue(firstRetained - unit < threshold);
            if (firstRetained <= long.MaxValue)
                Assert.AreEqual((long)firstRetained, encoding.Decode(encoding.Encode((long)firstRetained)));
        }
        Assert.ThrowsExactly<InvalidDataException>(() => encoding.Encode(offset + 1));
        Assert.ThrowsExactly<OverflowException>(() => encoding.Decode(long.MaxValue));
    }

    [TestMethod]
    public async Task TimeKeys_PersistedMultiRowGroupIndex_AllowsOverlappingDailyBatch()
    {
        string path = CreateMarketDataDbPath("Kline");
        const int prefixCount = 130_000;
        const long start = 1_735_689_659_999;
        await DuckDbStorageHelper.ReplaceTableAsync(path, "data",
            Enumerable.Range(0, prefixCount).Select(index => CreateKline(start + index * 60000L - 59999, start + index * 60000L, index + 1)),
            "CloseTime", recreateTable: true);
        await DuckDbStorageHelper.CheckpointAsync(path);
        long tail = start + (prefixCount - 1) * 60000L;
        await UpsertTimeRowsAsync("Kline", path, Enumerable.Range(0, 1441).Select(index => (tail + index * 60000L, 900d + index)).ToArray());
        await DuckDbStorageHelper.CheckpointAsync(path);
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*), COUNT(DISTINCT CloseTimeMinute), MIN(CloseTimeMinute), MAX(CloseTimeMinute) FROM data;";
            using (var reader = command.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(prefixCount + 1440L, reader.GetInt64(0));
                Assert.AreEqual(prefixCount + 1440L, reader.GetInt64(1));
                Assert.AreEqual((start - 59999) / 60000, reader.GetInt64(2));
                Assert.AreEqual((tail + 1440L * 60000 - 59999) / 60000, reader.GetInt64(3));
            }
            command.CommandText = $"SELECT ClosePrice FROM data WHERE CloseTimeMinute={(tail - 59999) / 60000};";
            Assert.AreEqual(900d, Convert.ToDouble(command.ExecuteScalar()));
            command.CommandText = $"SELECT COUNT(*) FROM data WHERE CloseTimeMinute < {(tail - 59999) / 60000} AND ClosePrice <> CloseTimeMinute - {(start - 59999) / 60000} + 1;";
            Assert.AreEqual(0L, Convert.ToInt64(command.ExecuteScalar()));
            return Task.CompletedTask;
        });
        Assert.AreEqual(tail + 1440L * 60000, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", "CloseTime"));
    }

    [TestMethod]
    public async Task TimeKeys_ReplaceTableAndEmptyReplacement_PreserveEncodingAndConstraints()
    {
        string path = CreateMarketDataDbPath("OpenInterestHistory");
        await DuckDbStorageHelper.ReplaceTableAsync(path, "data", new[] { new OpenInterestHistory { Timestamp = 300000, SumOpenInterest = 1 } }, "Timestamp", true);
        await DuckDbStorageHelper.ReplaceTableAsync(path, "data", new[] { new OpenInterestHistory { Timestamp = 600000, SumOpenInterest = 2 } }, "Timestamp", false);
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => DuckDbStorageHelper.ReplaceTableAsync(path, "data",
            new[] { new OpenInterestHistory { Timestamp = 600001, SumOpenInterest = 3 } }, "Timestamp", true));
        Assert.AreEqual(600000L, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", "Timestamp"));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = "SELECT Timestamp5Minute, SumOpenInterest FROM data;";
            using var reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2L, reader.GetInt64(0));
            Assert.AreEqual(2d, reader.GetDouble(1));
            Assert.IsFalse(reader.Read());
            return Task.CompletedTask;
        });
        await DuckDbStorageHelper.ReplaceTableAsync<OpenInterestHistory>(path, "data", [], "Timestamp", false);
        Assert.IsNull(await DuckDbStorageHelper.GetMaxInt64Async(path, "data", "Timestamp"));
    }

    [TestMethod]
    public async Task TimeKeys_CancellationAfterBatchAccepted_FinishesBatchAndCheckpoint()
    {
        string path = CreateMarketDataDbPath("Kline");
        using CancellationTokenSource canceled = new();
        IEnumerable<Kline> Rows()
        {
            yield return CreateKline(0, 59999, 1);
            canceled.Cancel();
            yield return CreateKline(60000, 119999, 2);
        }
        await DuckDbStorageHelper.UpsertRowsAsync(path, "data", Rows(), "CloseTime", canceled.Token);
        Assert.IsTrue(canceled.IsCancellationRequested);
        await DuckDbStorageHelper.CheckpointAsync(path, CancellationToken.None);
        Assert.AreEqual(119999L, await DuckDbStorageHelper.GetMaxInt64Async(path, "data", "CloseTime"));
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => UpsertTimeRowsAsync("Kline", path, [(179999, 3)], canceled.Token));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*), SUM(ClosePrice) FROM data;";
            using var reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2L, reader.GetInt64(0));
            Assert.AreEqual(3d, reader.GetDouble(1));
            return Task.CompletedTask;
        });
    }

    [TestMethod]
    public async Task TimeKeys_EncodedColumnWithoutPrimaryKey_IsRejected()
    {
        string path = CreateMarketDataDbPath("OpenInterestHistory");
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE data(Timestamp5Minute BIGINT, SumOpenInterest DOUBLE, SumOpenInterestValue DOUBLE); INSERT INTO data VALUES (1, 5, 6);";
            command.ExecuteNonQuery();
            return Task.CompletedTask;
        });
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => UpsertTimeRowsAsync("OpenInterestHistory", path, [(300000, 9)]));
        await DuckDbStorageHelper.ExecuteWithConnectionAsync(path, connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = "SELECT SumOpenInterest FROM data;";
            Assert.AreEqual(5d, Convert.ToDouble(command.ExecuteScalar()));
            return Task.CompletedTask;
        });
    }

    private static Task UpsertTimeRowsAsync(string series, string path, (long Time, double Value)[] rows, CancellationToken ct = default)
        => series switch
        {
            "Kline" => DuckDbStorageHelper.UpsertRowsAsync(path, "data", rows.Select(row => CreateKline(row.Time - 59999, row.Time, row.Value)), "CloseTime", ct),
            "IndexPriceKline" or "MarkPriceKline" or "PremiumIndexKline" => DuckDbStorageHelper.UpsertRowsAsync(path, "data",
                rows.Select(row => new PremiumIndexKline { CloseTime = row.Time, ClosePrice = row.Value }), "CloseTime", ct),
            "OpenInterestHistory" => DuckDbStorageHelper.UpsertRowsAsync(path, "data",
                rows.Select(row => new OpenInterestHistory { Timestamp = row.Time, SumOpenInterest = row.Value }), "Timestamp", ct),
            "Basis" => DuckDbStorageHelper.UpsertRowsAsync(path, "data",
                rows.Select(row => new FuturesBasisCsv { Timestamp = row.Time, BasisValue = row.Value }), "Timestamp", ct),
            "TakerLongShortRatio" => DuckDbStorageHelper.UpsertRowsAsync(path, "data",
                rows.Select(row => new TakerLongShortRatioCsv { Timestamp = row.Time, BuySellRatio = row.Value }), "Timestamp", ct),
            _ => DuckDbStorageHelper.UpsertRowsAsync(path, "data",
                rows.Select(row => new LongShortRatioCsv { Timestamp = row.Time, LongShortRatio = row.Value }), "Timestamp", ct)
        };

    private static async Task AssertStorageLayoutAsync(string dbPath, string table, string expectedVersion, int expectedGroup)
    {
        await using DuckDBConnection connection = new(new DuckDBConnectionStringBuilder { DataSource = dbPath }.ConnectionString);
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT tags['storage_version'] FROM duckdb_databases() WHERE database_name=current_database();";
        Assert.AreEqual(expectedVersion, (string)(await command.ExecuteScalarAsync())!);
        command.CommandText = $"SELECT MAX(rows) FROM (SELECT row_group_id,SUM(count)::BIGINT AS rows FROM pragma_storage_info('{table}') WHERE column_name='id' AND segment_type<>'VALIDITY' GROUP BY row_group_id);";
        Assert.AreEqual((long)expectedGroup, Convert.ToInt64(await command.ExecuteScalarAsync()), dbPath);
        command.CommandText = $"SELECT COUNT(*) FROM {table} WHERE id BETWEEN 0 AND 599999;";
        Assert.AreEqual(600000L, Convert.ToInt64(await command.ExecuteScalarAsync()));
    }

    private string CreateDbPath()
        => Path.Combine(tempDirectory, "storage.duckdb");

    private string CreateMarketDataDbPath(string dataType)
        => Path.Combine(tempDirectory, dataType, "Spot", "storage.duckdb");

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

    private static Kline[] CreateKlines(int startIndex, int count, double priceBase)
        => Enumerable.Range(startIndex, count)
            .Select(index =>
            {
                long openTime = 1_000L + (index * 1_000L);
                long closeTime = openTime + 999L;
                return CreateKline(openTime, closeTime, priceBase + index);
            })
            .ToArray();

    private static FundingRate[] CreateFundingRates(int startIndex, int count)
        => Enumerable.Range(startIndex, count)
            .Select(index => new FundingRate
            {
                FundingTime = index * 1_000L,
                Rate = index / 1000d,
                MarkPrice = 100_000 + index
            })
            .ToArray();

    private static void WriteAggTradesCsv(
        string csvPath,
        int totalRows,
        AggTradesCsvSchema schema,
        bool includeHeader = false)
    {
        using StreamWriter writer = new(csvPath);
        if (includeHeader)
        {
            writer.Write("agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker");
            if (schema == AggTradesCsvSchema.Spot)
                writer.Write(",is_best_match");
            writer.WriteLine();
        }

        for (int index = 1; index <= totalRows; index++)
        {
            writer.Write(index.ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((100 + index).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((1.5 + index).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((index * 10L).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write(((index * 10L) + 1).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((index * 1_000L).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write(index % 2 == 0 ? "true" : "false");
            if (schema == AggTradesCsvSchema.Spot)
            {
                writer.Write(',');
                writer.Write(index % 2 != 0 ? "true" : "false");
            }
            writer.WriteLine();
        }
    }

    private static void WriteBookDepthCsv(string csvPath, int totalRows, bool includeHeader = true)
    {
        DateTime startTime = new(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);
        using StreamWriter writer = new(csvPath);
        if (includeHeader)
            writer.WriteLine("timestamp,percentage,depth,notional");

        for (int index = 0; index < totalRows; index++)
        {
            writer.Write(startTime.AddMinutes(index).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write(((index % 100) / 10m).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((1000 + index).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.Write(',');
            writer.Write((5000 + index).ToString(System.Globalization.CultureInfo.InvariantCulture));
            writer.WriteLine();
        }
    }

    private static async Task<List<long>> ReadKlineCloseTimesAsync(string dbPath, string tableName)
    {
        await using DuckDBConnection connection = new($"Data Source={dbPath}");
        await connection.OpenAsync();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT CloseTime FROM {tableName} ORDER BY CloseTime;";
        await using var reader = await command.ExecuteReaderAsync();

        List<long> closeTimes = [];
        while (await reader.ReadAsync())
            closeTimes.Add(reader.GetInt64(0));

        return closeTimes;
    }

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

    private sealed class QueryLongRecord
    {
        public required string Id { get; set; }
        public long Sequence { get; set; }
    }

    private sealed class NullableRecord
    {
        public required string Id { get; set; }
        public string? OptionalName { get; set; }
        public int? OptionalCount { get; set; }
        public DateTime? OptionalOccurredAt { get; set; }
        public SampleStatus? OptionalStatus { get; set; }
    }

    private sealed class EnumCollectionRecord
    {
        public required string Id { get; set; }
        public SampleStatus[] States { get; set; } = [];
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
