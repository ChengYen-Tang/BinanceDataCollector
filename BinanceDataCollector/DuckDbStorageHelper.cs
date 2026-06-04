using CollectorModels.Models.Storage;
using DuckDB.NET.Data;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using System.Threading;

namespace BinanceDataCollector;

internal static class DuckDbStorageHelper
{
    private const int MarketDataImportBatchSize = 10_0000;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General);
    private static readonly CultureInfo InvariantCulture = CultureInfo.InvariantCulture;
    private static readonly StringComparer PathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly ConcurrentDictionary<DbTableLockKey, SemaphoreSlim> TableLocks = new(new DbTableLockKeyComparer());
    private static readonly Lock LifecycleSync = new();
    private static readonly ManualResetEventSlim NoActiveOperations = new(initialState: true);
    private static int activeOperations;
    private static bool isClosing;
    private static long tempTableId;

    public static Task ReplaceTableAsync<T>(string dbPath, string tableName, IEnumerable<T> records, CancellationToken ct = default)
        => ReplaceTableAsync(dbPath, tableName, records, null, false, ct);

    public static async Task ReplaceTableAsync<T>(
        string dbPath,
        string tableName,
        IEnumerable<T> records,
        string? keyColumn,
        bool recreateTable,
        CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the table gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();

            List<T> recordList = [.. records];
            PropertyInfo[] properties = GetPersistedProperties(typeof(T));
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            BatchTempTables tempTables = CreateGenericBatchTables(connection, properties);
            try
            {
                // After the DuckDB mutation starts, cancellation is intentionally
                // ignored so a batch cannot be half-written into the database.
                AppendObjectRows(connection, tempTables.RowsTableName, properties, recordList, CancellationToken.None);

                using DuckDBTransaction transaction = connection.BeginTransaction();

                if (recreateTable)
                {
                    using DuckDBCommand dropCommand = connection.CreateCommand();
                    dropCommand.Transaction = transaction;
                    dropCommand.CommandText = $"DROP TABLE IF EXISTS {QuoteIdentifier(tableName)};";
                    await dropCommand.ExecuteNonQueryAsync(CancellationToken.None);
                }

                await EnsureTableAsync(connection, transaction, tableName, properties, keyColumn, CancellationToken.None);
                DeleteAllRows(connection, transaction, tableName);
                InsertFromStagingTable(connection, transaction, tableName, tempTables.RowsTableName, properties);
                transaction.Commit();
            }
            finally
            {
                DropBatchTempTables(connection, tempTables);
            }
        });
    }

    public static async Task UpsertRowsAsync<T>(string dbPath, string tableName, IEnumerable<T> records, string keyColumn, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the table gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();

            IReadOnlyList<T> recordList = records as IReadOnlyList<T> ?? [.. records];
            if (recordList.Count == 0)
                return;

            PropertyInfo[] properties = GetPersistedProperties(typeof(T));
            PropertyInfo keyProperty = properties.FirstOrDefault(property => property.Name == keyColumn)
                ?? throw new InvalidOperationException($"Key column '{keyColumn}' was not found on type '{typeof(T).FullName}'.");

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            BatchTempTables tempTables = CreateGenericBatchTables(connection, properties);
            try
            {
                // After the DuckDB mutation starts, cancellation is intentionally
                // ignored so a batch cannot be half-written into the database.
                AppendObjectRows(connection, tempTables.RowsTableName, properties, recordList, CancellationToken.None);

                using DuckDBTransaction transaction = connection.BeginTransaction();
                await EnsureTableAsync(connection, transaction, tableName, properties, keyColumn, CancellationToken.None);
                DeleteRowsByJoinKey(connection, transaction, tableName, tempTables.RowsTableName, keyProperty.Name);
                InsertFromStagingTable(connection, transaction, tableName, tempTables.RowsTableName, properties);
                transaction.Commit();
            }
            finally
            {
                DropBatchTempTables(connection, tempTables);
            }
        });
    }

    public static async Task<List<string>> GetStringValuesAsync(string dbPath, string tableName, string columnName, CancellationToken ct = default)
    {
        return await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return [];

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return [];

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT {QuoteIdentifier(columnName)} FROM {QuoteIdentifier(tableName)} ORDER BY {QuoteIdentifier(columnName)};";

            List<string> values = [];
            await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (!reader.IsDBNull(0))
                    values.Add(reader.GetString(0));
            }

            return values;
        }, ct);
    }

    public static async Task<List<string>> GetTableNamesAsync(string dbPath, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        if (!File.Exists(normalizedPath))
            return [];

        return await WithConnectionAsync(dbPath, async connection =>
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """;

            List<string> tableNames = [];
            await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (!reader.IsDBNull(0))
                    tableNames.Add(reader.GetString(0));
            }

            return tableNames;
        }, ct);
    }

    public static async Task<DateTime?> GetMaxDateTimeAsync(
        string dbPath,
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?>? filters = null,
        CancellationToken ct = default)
    {
        return await WithTableLockAsync<DateTime?>(dbPath, tableName, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return null;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return null;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = BuildMaxDateTimeSql(tableName, columnName, filters);
            AddFilterParameters(command, filters);

            object? value = await command.ExecuteScalarAsync(ct);
            if (value is null || value is DBNull)
                return null;

            return value switch
            {
                DateTime dateTime => dateTime,
                DateTimeOffset offset => offset.UtcDateTime,
                _ => Convert.ToDateTime(value)
            };
        }, ct);
    }

    public static async Task<long?> GetMaxInt64Async(
        string dbPath,
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?>? filters = null,
        CancellationToken ct = default)
    {
        return await WithTableLockAsync<long?>(dbPath, tableName, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return null;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return null;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = BuildMaxDateTimeSql(tableName, columnName, filters);
            AddFilterParameters(command, filters);

            object? value = await command.ExecuteScalarAsync(ct);
            if (value is null || value is DBNull)
                return null;

            return Convert.ToInt64(value);
        }, ct);
    }

    public static async Task DeleteRowsBeforeAsync(string dbPath, string tableName, string columnName, DateTime threshold, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the table gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            if (!File.Exists(normalizedPath))
                return;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE {QuoteIdentifier(columnName)} < ?;";
            IDbDataParameter parameter = command.CreateParameter();
            parameter.DbType = DbType.DateTime;
            parameter.Value = threshold;
            command.Parameters.Add(parameter);
            await command.ExecuteNonQueryAsync(CancellationToken.None);
        });
    }

    public static async Task DeleteRowsBeforeAsync(string dbPath, string tableName, string columnName, long threshold, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the table gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            if (!File.Exists(normalizedPath))
                return;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE {QuoteIdentifier(columnName)} < ?;";
            IDbDataParameter parameter = command.CreateParameter();
            parameter.DbType = DbType.Int64;
            parameter.Value = threshold;
            command.Parameters.Add(parameter);
            await command.ExecuteNonQueryAsync(CancellationToken.None);
        });
    }

    public static async Task DropTableIfExistsAsync(string dbPath, string tableName, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the table gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            if (!File.Exists(normalizedPath))
                return;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {QuoteIdentifier(tableName)};";
            await command.ExecuteNonQueryAsync(CancellationToken.None);
        });
    }

    public static Task ExecuteWithConnectionAsync(
        string dbPath,
        Func<DuckDBConnection, Task> action)
    {
        // Intentionally do not pass the caller token into the operation gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        return WithConnectionAsync(dbPath, action);
    }

    public static Task<T> ExecuteWithConnectionAsync<T>(
        string dbPath,
        Func<DuckDBConnection, Task<T>> action)
    {
        // Intentionally do not pass the caller token into the operation gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        return WithConnectionAsync(dbPath, action);
    }

    public static async Task ReplaceAggTradesTailFromCsvAsync(
        string dbPath,
        string tableName,
        string csvPath,
        bool sourceIsMicroseconds,
        CancellationToken ct = default)
    {
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            EnsureAggTradesTable(connection, tableName);
            ImportAggTradesCsv(connection, tableName, csvPath, sourceIsMicroseconds, replaceTail: true);
            await Task.CompletedTask;
        });
    }

    public static async Task AppendAggTradesFromCsvAsync(
        string dbPath,
        string tableName,
        string csvPath,
        bool sourceIsMicroseconds,
        CancellationToken ct = default)
    {
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            EnsureAggTradesTable(connection, tableName);
            ImportAggTradesCsv(connection, tableName, csvPath, sourceIsMicroseconds, replaceTail: false);
            await Task.CompletedTask;
        });
    }

    public static async Task ReplaceBookDepthTailFromCsvAsync(
        string dbPath,
        string tableName,
        string csvPath,
        CancellationToken ct = default)
    {
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            EnsureBookDepthTable(connection, tableName);
            ImportBookDepthCsv(connection, tableName, csvPath);
            await Task.CompletedTask;
        });
    }

    public static async Task CheckpointAsync(string dbPath, CancellationToken ct = default)
    {
        await WithConnectionAsync(dbPath, async connection =>
        {
            ct.ThrowIfCancellationRequested();
            // Once checkpoint starts, do not let cancellation interrupt it midway.
            ExecuteDuckDbNonQuery(connection, "FORCE CHECKPOINT;");
            await Task.CompletedTask;
        }, ct);
    }

    public static async Task NormalizeAggTradesStoredTimeAsync(
        string dbPath,
        string tableName,
        long microsecondsBoundary,
        CancellationToken ct = default)
    {
        await WithTableLockAsync(dbPath, tableName, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            if (!File.Exists(normalizedPath))
                return;

            using DuckDBConnection connection = OpenConnection(normalizedPath);
            if (!TableExists(connection, tableName))
                return;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"""
                UPDATE {QuoteIdentifier(tableName)}
                SET transact_time = transact_time * 1000
                WHERE transact_time < $microseconds_boundary;
                """;
            command.Parameters.Add(new DuckDBParameter("microseconds_boundary", microsecondsBoundary));
            command.ExecuteNonQuery();
            await Task.CompletedTask;
        });
    }

    public static void CloseAllConnections()
        => ExecuteExclusiveAsync(static () => Task.CompletedTask).GetAwaiter().GetResult();

    public static async Task ExecuteExclusiveAsync(Func<Task> action, CancellationToken ct = default)
    {
        BeginExclusiveMode();
        try
        {
            NoActiveOperations.Wait(ct);
            await action();
        }
        finally
        {
            EndExclusiveMode();
        }
    }

    public static Task CheckpointStorageAsync(string storageRootPath, CancellationToken ct = default)
        => ExecuteExclusiveAsync(() =>
        {
            if (!Directory.Exists(storageRootPath))
                return Task.CompletedTask;

            foreach (string dbPath in Directory.EnumerateFiles(storageRootPath, "*.duckdb", SearchOption.AllDirectories))
            {
                ct.ThrowIfCancellationRequested();

                string normalizedPath = Path.GetFullPath(dbPath);
                if (!File.Exists(normalizedPath))
                    continue;

                using DuckDBConnection connection = OpenConnection(normalizedPath);
                ExecuteDuckDbNonQuery(connection, "FORCE CHECKPOINT;");
            }

            return Task.CompletedTask;
        }, ct);

    private static DuckDBConnection OpenConnection(string normalizedPath)
    {
        string? directoryPath = Path.GetDirectoryName(normalizedPath);
        if (!string.IsNullOrWhiteSpace(directoryPath))
            Directory.CreateDirectory(directoryPath);

        DuckDBConnection connection = new($"Data Source={normalizedPath}");
        connection.Open();
        return connection;
    }

    private static void BeginExclusiveMode()
    {
        lock (LifecycleSync)
        {
            isClosing = true;
            if (activeOperations == 0)
                NoActiveOperations.Set();
        }
    }

    private static void EndExclusiveMode()
    {
        lock (LifecycleSync)
        {
            isClosing = false;
            NoActiveOperations.Set();
        }
    }

    private static async Task WithTableLockAsync(string dbPath, string tableName, Func<string, Task> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            SemaphoreSlim gate = TableLocks.GetOrAdd(new DbTableLockKey(normalizedPath, tableName), static _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(ct);
            try
            {
                await action(normalizedPath);
            }
            finally
            {
                gate.Release();
            }
        }
        finally
        {
            ExitOperation();
        }
    }

    private static async Task<T> WithTableLockAsync<T>(string dbPath, string tableName, Func<string, Task<T>> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            SemaphoreSlim gate = TableLocks.GetOrAdd(new DbTableLockKey(normalizedPath, tableName), static _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(ct);
            try
            {
                return await action(normalizedPath);
            }
            finally
            {
                gate.Release();
            }
        }
        finally
        {
            ExitOperation();
        }
    }

    private static async Task WithConnectionAsync(string dbPath, Func<DuckDBConnection, Task> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            await action(connection);
        }
        finally
        {
            ExitOperation();
        }
    }

    private static async Task<T> WithConnectionAsync<T>(string dbPath, Func<DuckDBConnection, Task<T>> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            using DuckDBConnection connection = OpenConnection(normalizedPath);
            return await action(connection);
        }
        finally
        {
            ExitOperation();
        }
    }

    private static async Task EnterOperationAsync(CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            lock (LifecycleSync)
            {
                if (!isClosing)
                {
                    activeOperations++;
                    NoActiveOperations.Reset();
                    return;
                }
            }

            await Task.Delay(10, ct);
        }
    }

    private static void ExitOperation()
    {
        lock (LifecycleSync)
        {
            activeOperations--;
            if (activeOperations == 0)
                NoActiveOperations.Set();
        }
    }

    private static async Task EnsureTableAsync(
        DuckDBConnection connection,
        DuckDBTransaction transaction,
        string tableName,
        IReadOnlyList<PropertyInfo> properties,
        string? keyColumn,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCreateTableSql(tableName, properties, keyColumn);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static void DeleteAllRows(DuckDBConnection connection, DuckDBTransaction transaction, string tableName)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)};";
        command.ExecuteNonQuery();
    }

    private static void DeleteRowsByJoinKey(DuckDBConnection connection, DuckDBTransaction transaction, string tableName, string stagingTableName, string keyColumn)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            DELETE FROM {QuoteIdentifier(tableName)}
            USING {QuoteIdentifier(stagingTableName)}
            WHERE {QuoteIdentifier(tableName)}.{QuoteIdentifier(keyColumn)} = {QuoteIdentifier(stagingTableName)}.{QuoteIdentifier(keyColumn)};
            """;
        command.ExecuteNonQuery();
    }

    private static void InsertFromStagingTable(
        DuckDBConnection connection,
        DuckDBTransaction transaction,
        string tableName,
        string stagingTableName,
        IReadOnlyList<PropertyInfo> properties)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            INSERT INTO {QuoteIdentifier(tableName)} ({BuildColumnListSql(properties)})
            SELECT {BuildColumnListSql(properties)}
            FROM {QuoteIdentifier(stagingTableName)};
            """;
        command.ExecuteNonQuery();
    }

    private static void AppendObjectRows<T>(
        DuckDBConnection connection,
        string tableName,
        IReadOnlyList<PropertyInfo> properties,
        IReadOnlyList<T> records,
        CancellationToken ct)
    {
        AppendRows(connection, tableName, records, static (row, record, state) =>
        {
            foreach (PropertyInfo property in state)
                AppendDbValue(row, ConvertToDbValue(property.GetValue(record), property.PropertyType));
        }, properties, ct);
    }

    private static void EnsureAggTradesTable(DuckDBConnection connection, string tableName)
        => ExecuteDuckDbNonQuery(connection, $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (
                parquetagg_trade_id BIGINT,
                price DOUBLE,
                quantity DOUBLE,
                first_trade_id BIGINT,
                last_trade_id BIGINT,
                transact_time BIGINT,
                is_buyer_maker BOOLEAN
            );
            """);

    private static void EnsureBookDepthTable(DuckDBConnection connection, string tableName)
        => ExecuteDuckDbNonQuery(connection, $"""
            CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (
                snapshot_time BIGINT,
                percentage DECIMAL(10,2),
                depth DOUBLE,
                notional DOUBLE
            );
            """);

    private static void ImportAggTradesCsv(
        DuckDBConnection connection,
        string tableName,
        string csvPath,
        bool sourceIsMicroseconds,
        bool replaceTail)
    {
        List<AggTradeImportRow> batchRows = new(MarketDataImportBatchSize);
        Dictionary<long, AggTradeImportRow> batchDedup = new(MarketDataImportBatchSize);
        bool tailDeleted = !replaceTail;
        long replaceFrom = replaceTail
            ? GetAggTradesCsvMinTimestamp(csvPath, sourceIsMicroseconds)
            : 0;

        using StreamReader reader = new(csvPath);
        bool isFirstLine = true;
        while (reader.ReadLine() is { } line)
        {
            if (isFirstLine)
            {
                isFirstLine = false;
                if (IsAggTradesHeader(line.AsSpan()))
                    continue;
            }

            if (string.IsNullOrWhiteSpace(line))
                continue;

            AggTradeImportRow row = ParseAggTrade(line.AsSpan(), sourceIsMicroseconds);
            batchDedup[row.ParquetAggTradeId] = row;
            if (batchDedup.Count >= MarketDataImportBatchSize)
                FlushAggTradeBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
        }

        FlushAggTradeBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
    }

    private static void ImportBookDepthCsv(DuckDBConnection connection, string tableName, string csvPath)
    {
        List<BookDepthImportRow> batchRows = new(MarketDataImportBatchSize);
        Dictionary<BookDepthRowKey, BookDepthImportRow> batchDedup = new(MarketDataImportBatchSize);
        bool tailDeleted = false;
        long replaceFrom = GetBookDepthCsvMinTimestamp(csvPath);

        using StreamReader reader = new(csvPath);
        bool isFirstLine = true;
        while (reader.ReadLine() is { } line)
        {
            if (isFirstLine)
            {
                isFirstLine = false;
                if (IsBookDepthHeader(line.AsSpan()))
                    continue;
            }

            if (string.IsNullOrWhiteSpace(line))
                continue;

            BookDepthImportRow row = ParseBookDepth(line.AsSpan());
            batchDedup[new BookDepthRowKey(row.SnapshotTime, row.Percentage)] = row;
            if (batchDedup.Count >= MarketDataImportBatchSize)
                FlushBookDepthBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
        }

        FlushBookDepthBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
    }

    private static void FlushAggTradeBatch(
        DuckDBConnection connection,
        string tableName,
        List<AggTradeImportRow> batchRows,
        Dictionary<long, AggTradeImportRow> batchDedup,
        ref bool tailDeleted,
        long replaceFrom)
    {
        if (batchDedup.Count == 0)
            return;

        batchRows.Clear();
        foreach (AggTradeImportRow row in batchDedup.Values)
            batchRows.Add(row);
        batchRows.Sort(static (x, y) =>
        {
            int timeCompare = x.TransactTime.CompareTo(y.TransactTime);
            return timeCompare != 0 ? timeCompare : x.ParquetAggTradeId.CompareTo(y.ParquetAggTradeId);
        });

        BatchTempTables tempTables = CreateAggTradesBatchTables(connection);
        try
        {
            AppendAggTradeRows(connection, tempTables.RowsTableName, batchRows);

            using DuckDBTransaction transaction = connection.BeginTransaction();
            if (!tailDeleted)
            {
                using DuckDBCommand deleteTailCommand = connection.CreateCommand();
                deleteTailCommand.Transaction = transaction;
                deleteTailCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE transact_time >= $replace_from;";
                deleteTailCommand.Parameters.Add(new DuckDBParameter("replace_from", replaceFrom));
                deleteTailCommand.ExecuteNonQuery();
                tailDeleted = true;
            }

            using (DuckDBCommand deleteByKeyCommand = connection.CreateCommand())
            {
                deleteByKeyCommand.Transaction = transaction;
                deleteByKeyCommand.CommandText = $"""
                    DELETE FROM {QuoteIdentifier(tableName)}
                    USING {QuoteIdentifier(tempTables.RowsTableName)}
                    WHERE {QuoteIdentifier(tableName)}.parquetagg_trade_id = {QuoteIdentifier(tempTables.RowsTableName)}.parquetagg_trade_id;
                    """;
                deleteByKeyCommand.ExecuteNonQuery();
            }

            using (DuckDBCommand insertCommand = connection.CreateCommand())
            {
                insertCommand.Transaction = transaction;
                insertCommand.CommandText = $"""
                    INSERT INTO {QuoteIdentifier(tableName)}
                    SELECT parquetagg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
                    FROM {QuoteIdentifier(tempTables.RowsTableName)}
                    ORDER BY transact_time, parquetagg_trade_id;
                    """;
                insertCommand.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        finally
        {
            DropBatchTempTables(connection, tempTables);
        }

        batchDedup.Clear();
        batchRows.Clear();
    }

    private static void FlushBookDepthBatch(
        DuckDBConnection connection,
        string tableName,
        List<BookDepthImportRow> batchRows,
        Dictionary<BookDepthRowKey, BookDepthImportRow> batchDedup,
        ref bool tailDeleted,
        long replaceFrom)
    {
        if (batchDedup.Count == 0)
            return;

        batchRows.Clear();
        foreach (BookDepthImportRow row in batchDedup.Values)
            batchRows.Add(row);
        batchRows.Sort(static (x, y) =>
        {
            int timeCompare = x.SnapshotTime.CompareTo(y.SnapshotTime);
            return timeCompare != 0 ? timeCompare : x.Percentage.CompareTo(y.Percentage);
        });

        BatchTempTables tempTables = CreateBookDepthBatchTables(connection);
        try
        {
            AppendBookDepthRows(connection, tempTables.RowsTableName, batchRows);

            using DuckDBTransaction transaction = connection.BeginTransaction();
            if (!tailDeleted)
            {
                using DuckDBCommand deleteTailCommand = connection.CreateCommand();
                deleteTailCommand.Transaction = transaction;
                deleteTailCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE snapshot_time >= $replace_from;";
                deleteTailCommand.Parameters.Add(new DuckDBParameter("replace_from", replaceFrom));
                deleteTailCommand.ExecuteNonQuery();
                tailDeleted = true;
            }

            using (DuckDBCommand deleteByKeyCommand = connection.CreateCommand())
            {
                deleteByKeyCommand.Transaction = transaction;
                deleteByKeyCommand.CommandText = $"""
                    DELETE FROM {QuoteIdentifier(tableName)}
                    USING {QuoteIdentifier(tempTables.RowsTableName)}
                    WHERE {QuoteIdentifier(tableName)}.snapshot_time = {QuoteIdentifier(tempTables.RowsTableName)}.snapshot_time
                      AND {QuoteIdentifier(tableName)}.percentage = {QuoteIdentifier(tempTables.RowsTableName)}.percentage;
                    """;
                deleteByKeyCommand.ExecuteNonQuery();
            }

            using (DuckDBCommand insertCommand = connection.CreateCommand())
            {
                insertCommand.Transaction = transaction;
                insertCommand.CommandText = $"""
                    INSERT INTO {QuoteIdentifier(tableName)}
                    SELECT snapshot_time, percentage, depth, notional
                    FROM {QuoteIdentifier(tempTables.RowsTableName)}
                    ORDER BY snapshot_time, percentage;
                    """;
                insertCommand.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        finally
        {
            DropBatchTempTables(connection, tempTables);
        }

        batchDedup.Clear();
        batchRows.Clear();
    }

    private static BatchTempTables CreateAggTradesBatchTables(DuckDBConnection connection)
    {
        BatchTempTables tempTables = CreateBatchTempTables("temp_agg_trades_batch");
        CreateTemporaryTable(connection, tempTables.RowsTableName, """
            parquetagg_trade_id BIGINT,
            price DOUBLE,
            quantity DOUBLE,
            first_trade_id BIGINT,
            last_trade_id BIGINT,
            transact_time BIGINT,
            is_buyer_maker BOOLEAN
            """);

        return tempTables;
    }

    private static BatchTempTables CreateBookDepthBatchTables(DuckDBConnection connection)
    {
        BatchTempTables tempTables = CreateBatchTempTables("temp_book_depth_batch");
        CreateTemporaryTable(connection, tempTables.RowsTableName, """
            snapshot_time BIGINT,
            percentage DECIMAL(10,2),
            depth DOUBLE,
            notional DOUBLE
            """);

        return tempTables;
    }

    private static BatchTempTables CreateGenericBatchTables(DuckDBConnection connection, IReadOnlyList<PropertyInfo> properties)
    {
        BatchTempTables tempTables = CreateBatchTempTables("temp_generic_batch");
        CreateTemporaryTable(connection, tempTables.RowsTableName, BuildColumnDefinitionsSql(properties));

        return tempTables;
    }

    private static BatchTempTables CreateBatchTempTables(string prefix)
    {
        long id = Interlocked.Increment(ref tempTableId);
        return new($"{prefix}_{id}_rows");
    }

    private static void DropBatchTempTables(DuckDBConnection connection, BatchTempTables tempTables)
        => ExecuteDuckDbNonQuery(connection, $"""
            DROP TABLE IF EXISTS {QuoteIdentifier(tempTables.RowsTableName)};
            """);

    private static void AppendAggTradeRows(DuckDBConnection connection, string rowsTableName, IReadOnlyList<AggTradeImportRow> rows)
        => AppendRows<AggTradeImportRow, object?>(connection, rowsTableName, rows, static (appenderRow, row, _) =>
            appenderRow
                .AppendValue(row.ParquetAggTradeId)
                .AppendValue(row.Price)
                .AppendValue(row.Quantity)
                .AppendValue(row.FirstTradeId)
                .AppendValue(row.LastTradeId)
                .AppendValue(row.TransactTime)
                .AppendValue(row.IsBuyerMaker));

    private static void AppendBookDepthRows(DuckDBConnection connection, string rowsTableName, IReadOnlyList<BookDepthImportRow> rows)
    {
        AppendRows<BookDepthImportRow, object?>(connection, rowsTableName, rows, static (appenderRow, row, _) =>
            appenderRow
                .AppendValue(row.SnapshotTime)
                .AppendValue(row.Percentage)
                .AppendValue(row.Depth)
                .AppendValue(row.Notional));
    }

    private static void AppendRows<TRow, TState>(
        DuckDBConnection connection,
        string tableName,
        IReadOnlyList<TRow> rows,
        Action<IDuckDBAppenderRow, TRow, TState> appendRow,
        TState state = default!,
        CancellationToken ct = default)
    {
        // DuckDB appenders use the transaction context of the owning connection.
        using DuckDBAppender appender = connection.CreateAppender(tableName);
        foreach (TRow row in rows)
        {
            ct.ThrowIfCancellationRequested();
            IDuckDBAppenderRow appenderRow = appender.CreateRow();
            appendRow(appenderRow, row, state);
            appenderRow.EndRow();
        }

        appender.Close();
    }

    private static long GetAggTradesCsvMinTimestamp(string csvPath, bool sourceIsMicroseconds)
    {
        long? minTimestamp = null;
        using StreamReader reader = new(csvPath);
        bool isFirstLine = true;
        while (reader.ReadLine() is { } line)
        {
            if (isFirstLine)
            {
                isFirstLine = false;
                if (IsAggTradesHeader(line.AsSpan()))
                    continue;
            }

            if (string.IsNullOrWhiteSpace(line))
                continue;

            long timestamp = long.Parse(ReadCsvField(line.AsSpan(), 5), NumberStyles.Integer, InvariantCulture);
            if (!sourceIsMicroseconds)
                timestamp *= 1000;

            minTimestamp = !minTimestamp.HasValue || timestamp < minTimestamp.Value
                ? timestamp
                : minTimestamp;
        }

        return minTimestamp ?? throw new InvalidDataException($"CSV does not contain aggTrades rows: {csvPath}");
    }

    private static long GetBookDepthCsvMinTimestamp(string csvPath)
    {
        long? minTimestamp = null;
        using StreamReader reader = new(csvPath);
        bool isFirstLine = true;
        while (reader.ReadLine() is { } line)
        {
            if (isFirstLine)
            {
                isFirstLine = false;
                if (IsBookDepthHeader(line.AsSpan()))
                    continue;
            }

            if (string.IsNullOrWhiteSpace(line))
                continue;

            long timestamp = ParseBookDepth(line.AsSpan()).SnapshotTime;
            minTimestamp = !minTimestamp.HasValue || timestamp < minTimestamp.Value
                ? timestamp
                : minTimestamp;
        }

        return minTimestamp ?? throw new InvalidDataException($"CSV does not contain bookDepth rows: {csvPath}");
    }

    private static AggTradeImportRow ParseAggTrade(ReadOnlySpan<char> line, bool sourceIsMicroseconds)
    {
        long transactTime = long.Parse(ReadCsvField(line, 5), NumberStyles.Integer, InvariantCulture);
        if (!sourceIsMicroseconds)
            transactTime *= 1000;

        return new AggTradeImportRow(
            long.Parse(ReadCsvField(line, 0), NumberStyles.Integer, InvariantCulture),
            double.Parse(ReadCsvField(line, 1), NumberStyles.Float | NumberStyles.AllowThousands, InvariantCulture),
            double.Parse(ReadCsvField(line, 2), NumberStyles.Float | NumberStyles.AllowThousands, InvariantCulture),
            long.Parse(ReadCsvField(line, 3), NumberStyles.Integer, InvariantCulture),
            long.Parse(ReadCsvField(line, 4), NumberStyles.Integer, InvariantCulture),
            transactTime,
            bool.Parse(ReadCsvField(line, 6)));
    }

    private static BookDepthImportRow ParseBookDepth(ReadOnlySpan<char> line)
    {
        DateTime snapshotTime = DateTime.ParseExact(
            ReadCsvField(line, 0),
            "yyyy-MM-dd HH:mm:ss",
            InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

        return new BookDepthImportRow(
            new DateTimeOffset(snapshotTime).ToUnixTimeMilliseconds(),
            decimal.Parse(ReadCsvField(line, 1), NumberStyles.Number, InvariantCulture),
            double.Parse(ReadCsvField(line, 2), NumberStyles.Float | NumberStyles.AllowThousands, InvariantCulture),
            double.Parse(ReadCsvField(line, 3), NumberStyles.Float | NumberStyles.AllowThousands, InvariantCulture));
    }

    private static ReadOnlySpan<char> ReadCsvField(ReadOnlySpan<char> line, int fieldIndex)
    {
        int currentField = 0;
        int start = 0;
        for (int index = 0; index <= line.Length; index++)
        {
            if (index != line.Length && line[index] != ',')
                continue;

            if (currentField == fieldIndex)
                return line[start..index];

            currentField++;
            start = index + 1;
        }

        throw new FormatException($"CSV line does not contain field index {fieldIndex}.");
    }

    private static bool IsAggTradesHeader(ReadOnlySpan<char> line)
        => ReadCsvField(line, 0).Equals("agg_trade_id".AsSpan(), StringComparison.OrdinalIgnoreCase);

    private static bool IsBookDepthHeader(ReadOnlySpan<char> line)
        => ReadCsvField(line, 0).Equals("timestamp".AsSpan(), StringComparison.OrdinalIgnoreCase);

    private static void ExecuteDuckDbNonQuery(DuckDBConnection connection, string commandText)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = commandText;
        command.ExecuteNonQuery();
    }

    private static bool IsSupportedPropertyType(Type type)
    {
        Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType.IsEnum)
            return true;

        if (underlyingType == typeof(string)
            || underlyingType == typeof(bool)
            || underlyingType == typeof(int)
            || underlyingType == typeof(long)
            || underlyingType == typeof(double)
            || underlyingType == typeof(float)
            || underlyingType == typeof(decimal)
            || underlyingType == typeof(DateTime))
            return true;

        if (underlyingType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(underlyingType))
        {
            Type? elementType = underlyingType.IsArray
                ? underlyingType.GetElementType()
                : underlyingType.GetGenericArguments().FirstOrDefault();

            if (elementType is not null)
            {
                Type collectionType = Nullable.GetUnderlyingType(elementType) ?? elementType;
                return collectionType == typeof(string) || collectionType.IsEnum;
            }
        }

        return false;
    }

    private static PropertyInfo[] GetPersistedProperties(Type type)
        => type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.CanRead && property.CanWrite && IsSupportedPropertyType(property.PropertyType))
            .ToArray();

    private static string BuildCreateTableSql(string tableName, IReadOnlyList<PropertyInfo> properties, string? keyColumn)
    {
        List<string> columns = [BuildColumnDefinitionsSql(properties)];

        if (!string.IsNullOrWhiteSpace(keyColumn))
            columns.Add($"PRIMARY KEY ({QuoteIdentifier(keyColumn)})");

        return $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} ({string.Join(", ", columns)});";
    }

    private static string BuildColumnDefinitionsSql(IReadOnlyList<PropertyInfo> properties)
        => string.Join(", ", properties.Select(property => $"{QuoteIdentifier(property.Name)} {MapDuckDbType(property.PropertyType)}"));

    private static string BuildColumnListSql(IReadOnlyList<PropertyInfo> properties)
        => string.Join(", ", properties.Select(property => QuoteIdentifier(property.Name)));

    private static void CreateTemporaryTable(DuckDBConnection connection, string tableName, string columnDefinitions)
        => ExecuteDuckDbNonQuery(connection, $"""
            CREATE TEMP TABLE {QuoteIdentifier(tableName)} (
                {columnDefinitions}
            );
            """);

    private static string BuildMaxDateTimeSql(string tableName, string columnName, IReadOnlyDictionary<string, object?>? filters)
    {
        string sql = $"SELECT MAX({QuoteIdentifier(columnName)}) FROM {QuoteIdentifier(tableName)}";
        if (filters is null || filters.Count == 0)
            return sql + ";";

        string[] predicates = filters
            .Select(filter => filter.Value is null
                ? $"{QuoteIdentifier(filter.Key)} IS NULL"
                : $"{QuoteIdentifier(filter.Key)} = ?")
            .ToArray();
        return $"{sql} WHERE {string.Join(" AND ", predicates)};";
    }

    private static void AddFilterParameters(DuckDBCommand command, IReadOnlyDictionary<string, object?>? filters)
    {
        if (filters is null || filters.Count == 0)
            return;

        foreach ((string key, object? value) in filters)
        {
            if (value is null)
                continue;

            IDbDataParameter parameter = command.CreateParameter();
            parameter.Value = ConvertToDbValue(value, value?.GetType() ?? typeof(string));
            command.Parameters.Add(parameter);
        }
    }

    private static async Task<bool> TableExistsAsync(DuckDBConnection connection, string tableName, CancellationToken ct)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?;";
        IDbDataParameter parameter = command.CreateParameter();
        parameter.DbType = DbType.String;
        parameter.Value = tableName;
        command.Parameters.Add(parameter);

        object? result = await command.ExecuteScalarAsync(ct);
        return result is not null && result is not DBNull && Convert.ToInt32(result) > 0;
    }

    private static bool TableExists(DuckDBConnection connection, string tableName)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?;";
        IDbDataParameter parameter = command.CreateParameter();
        parameter.DbType = DbType.String;
        parameter.Value = tableName;
        command.Parameters.Add(parameter);

        object? result = command.ExecuteScalar();
        return result is not null && result is not DBNull && Convert.ToInt32(result) > 0;
    }

    private static object ConvertToDbValue(object? value, Type declaredType)
    {
        if (value is null)
            return DBNull.Value;

        Type underlyingType = Nullable.GetUnderlyingType(declaredType) ?? declaredType;
        if (underlyingType.IsEnum)
            return value.ToString() ?? string.Empty;

        if (underlyingType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(underlyingType))
        {
            IEnumerable<object?> items = ((System.Collections.IEnumerable)value).Cast<object?>();
            string[] serializedItems = items
                .Select(item => item?.ToString() ?? string.Empty)
                .ToArray();
            return JsonSerializer.Serialize(serializedItems, JsonOptions);
        }

        return value;
    }

    private static void AppendDbValue(IDuckDBAppenderRow row, object value)
    {
        switch (value)
        {
            case DBNull:
                row.AppendNullValue();
                break;
            case string text:
                row.AppendValue(text);
                break;
            case bool boolean:
                row.AppendValue(boolean);
                break;
            case int int32:
                row.AppendValue(int32);
                break;
            case long int64:
                row.AppendValue(int64);
                break;
            case double doubleValue:
                row.AppendValue(doubleValue);
                break;
            case float singleValue:
                row.AppendValue(singleValue);
                break;
            case DateTime dateTime:
                row.AppendValue(dateTime);
                break;
            case decimal decimalValue:
                row.AppendValue(decimalValue);
                break;
            default:
                throw new NotSupportedException($"Unsupported appender value type: {value.GetType().FullName}");
        }
    }

    private static string MapDuckDbType(Type type)
    {
        Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType.IsEnum)
            return "VARCHAR";
        if (underlyingType == typeof(string))
            return "VARCHAR";
        if (underlyingType == typeof(bool))
            return "BOOLEAN";
        if (underlyingType == typeof(int))
            return "INTEGER";
        if (underlyingType == typeof(long))
            return "BIGINT";
        if (underlyingType == typeof(double))
            return "DOUBLE";
        if (underlyingType == typeof(float))
            return "REAL";
        if (underlyingType == typeof(DateTime))
            return "TIMESTAMP";
        if (underlyingType == typeof(decimal))
            return "DECIMAL(38, 18)";
        if (underlyingType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(underlyingType))
            return "VARCHAR";

        throw new NotSupportedException($"Unsupported DuckDB type: {type.FullName}");
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private readonly record struct DbTableLockKey(string NormalizedPath, string TableName);

    private readonly record struct BatchTempTables(string RowsTableName);

    private sealed class DbTableLockKeyComparer : IEqualityComparer<DbTableLockKey>
    {
        public bool Equals(DbTableLockKey x, DbTableLockKey y)
            => PathComparer.Equals(x.NormalizedPath, y.NormalizedPath)
                && StringComparer.Ordinal.Equals(x.TableName, y.TableName);

        public int GetHashCode(DbTableLockKey obj)
            => HashCode.Combine(PathComparer.GetHashCode(obj.NormalizedPath), StringComparer.Ordinal.GetHashCode(obj.TableName));
    }

}
