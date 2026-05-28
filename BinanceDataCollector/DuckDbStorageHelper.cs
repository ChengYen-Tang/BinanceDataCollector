using CollectorModels.Models.Storage;
using DuckDB.NET.Data;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.ObjectPool;

namespace BinanceDataCollector;

internal static class DuckDbStorageHelper
{
    private const int MarketDataImportBatchSize = 10_000;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General);
    private static readonly CultureInfo InvariantCulture = CultureInfo.InvariantCulture;
    private static readonly StringComparer PathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly ConcurrentDictionary<string, DuckDBConnection> ConnectionCache = new(PathComparer);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> ConnectionLocks = new(PathComparer);
    private static readonly object LifecycleSync = new();
    private static readonly ManualResetEventSlim NoActiveOperations = new(initialState: true);
    private static readonly DefaultObjectPoolProvider PoolProvider = new();
    private static readonly ObjectPool<List<AggTradeImportRow>> AggTradeBatchPool = PoolProvider.Create(new PooledListPolicy<AggTradeImportRow>(MarketDataImportBatchSize));
    private static readonly ObjectPool<Dictionary<long, AggTradeImportRow>> AggTradeDedupPool = PoolProvider.Create(new PooledDictionaryPolicy<long, AggTradeImportRow>(MarketDataImportBatchSize));
    private static readonly ObjectPool<List<BookDepthImportRow>> BookDepthBatchPool = PoolProvider.Create(new PooledListPolicy<BookDepthImportRow>(MarketDataImportBatchSize));
    private static readonly ObjectPool<Dictionary<BookDepthRowKey, BookDepthImportRow>> BookDepthDedupPool = PoolProvider.Create(new PooledDictionaryPolicy<BookDepthRowKey, BookDepthImportRow>(MarketDataImportBatchSize));
    private static int activeOperations;
    private static bool isClosing;

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
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();

            List<T> recordList = [.. records];
            PropertyInfo[] properties = GetPersistedProperties(typeof(T));
            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            using DuckDBTransaction transaction = connection.BeginTransaction();

            if (recreateTable)
            {
                using DuckDBCommand dropCommand = connection.CreateCommand();
                dropCommand.Transaction = transaction;
                dropCommand.CommandText = $"DROP TABLE IF EXISTS {QuoteIdentifier(tableName)};";
                await dropCommand.ExecuteNonQueryAsync(ct);
            }

            await EnsureTableAsync(connection, transaction, tableName, properties, keyColumn, ct);

            using DuckDBCommand deleteCommand = connection.CreateCommand();
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)};";
            await deleteCommand.ExecuteNonQueryAsync(ct);

            await InsertRowsAsync(connection, transaction, tableName, properties, recordList, ct);
            transaction.Commit();
        });
    }

    public static async Task UpsertRowsAsync<T>(string dbPath, string tableName, IEnumerable<T> records, string keyColumn, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();

            List<T> recordList = [.. records];
            if (recordList.Count == 0)
                return;

            PropertyInfo[] properties = GetPersistedProperties(typeof(T));
            PropertyInfo keyProperty = properties.FirstOrDefault(property => property.Name == keyColumn)
                ?? throw new InvalidOperationException($"Key column '{keyColumn}' was not found on type '{typeof(T).FullName}'.");

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            using DuckDBTransaction transaction = connection.BeginTransaction();

            await EnsureTableAsync(connection, transaction, tableName, properties, keyColumn, ct);

            using DuckDBCommand deleteByKeyCommand = connection.CreateCommand();
            deleteByKeyCommand.Transaction = transaction;
            deleteByKeyCommand.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE {QuoteIdentifier(keyColumn)} = ?;";
            IDbDataParameter deleteKeyParameter = deleteByKeyCommand.CreateParameter();
            deleteKeyParameter.DbType = MapDbType(keyProperty.PropertyType);
            deleteByKeyCommand.Parameters.Add(deleteKeyParameter);

            foreach (T record in recordList)
            {
                ct.ThrowIfCancellationRequested();
                deleteKeyParameter.Value = ConvertToDbValue(keyProperty.GetValue(record), keyProperty.PropertyType);
                await deleteByKeyCommand.ExecuteNonQueryAsync(ct);
            }

            await InsertRowsAsync(connection, transaction, tableName, properties, recordList, ct);
            transaction.Commit();
        });
    }

    public static async Task<List<string>> GetStringValuesAsync(string dbPath, string tableName, string columnName, CancellationToken ct = default)
    {
        return await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return [];

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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

    public static async Task<DateTime?> GetMaxDateTimeAsync(
        string dbPath,
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?>? filters = null,
        CancellationToken ct = default)
    {
        return await WithConnectionLockAsync<DateTime?>(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return null;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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
        return await WithConnectionLockAsync<long?>(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return null;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE {QuoteIdentifier(columnName)} < ?;";
            IDbDataParameter parameter = command.CreateParameter();
            parameter.DbType = DbType.DateTime;
            parameter.Value = threshold;
            command.Parameters.Add(parameter);
            await command.ExecuteNonQueryAsync(ct);
        });
    }

    public static async Task DeleteRowsBeforeAsync(string dbPath, string tableName, string columnName, long threshold, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            if (!await TableExistsAsync(connection, tableName, ct))
                return;

            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {QuoteIdentifier(tableName)} WHERE {QuoteIdentifier(columnName)} < ?;";
            IDbDataParameter parameter = command.CreateParameter();
            parameter.DbType = DbType.Int64;
            parameter.Value = threshold;
            command.Parameters.Add(parameter);
            await command.ExecuteNonQueryAsync(ct);
        });
    }

    public static async Task DropTableIfExistsAsync(string dbPath, string tableName, CancellationToken ct = default)
    {
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            if (!File.Exists(normalizedPath))
                return;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {QuoteIdentifier(tableName)};";
            await command.ExecuteNonQueryAsync(ct);
        });
    }

    public static Task ExecuteWithConnectionAsync(
        string dbPath,
        Func<DuckDBConnection, Task> action)
    {
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        return WithConnectionLockAsync(dbPath, normalizedPath => action(GetOpenConnection(normalizedPath)));
    }

    public static Task<T> ExecuteWithConnectionAsync<T>(
        string dbPath,
        Func<DuckDBConnection, Task<T>> action)
    {
        // Intentionally do not pass the caller token into the connection gate.
        // Once a write/delete operation is enqueued, it must wait its turn and
        // finish consistently rather than abandoning the mutation mid-pipeline.
        return WithConnectionLockAsync(dbPath, normalizedPath => action(GetOpenConnection(normalizedPath)));
    }

    public static async Task ReplaceAggTradesTailFromCsvAsync(
        string dbPath,
        string tableName,
        string csvPath,
        bool sourceIsMicroseconds,
        CancellationToken ct = default)
    {
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            EnsureBookDepthTable(connection, tableName);
            ImportBookDepthCsv(connection, tableName, csvPath);
            await Task.CompletedTask;
        });
    }

    public static async Task CheckpointAsync(string dbPath, CancellationToken ct = default)
    {
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            DuckDBConnection connection = GetOpenConnection(normalizedPath);
            ExecuteDuckDbNonQuery(connection, "CHECKPOINT;");
            await Task.CompletedTask;
        });
    }

    public static async Task NormalizeAggTradesStoredTimeAsync(
        string dbPath,
        string tableName,
        long microsecondsBoundary,
        CancellationToken ct = default)
    {
        await WithConnectionLockAsync(dbPath, async normalizedPath =>
        {
            ct.ThrowIfCancellationRequested();
            if (!File.Exists(normalizedPath))
                return;

            DuckDBConnection connection = GetOpenConnection(normalizedPath);
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
    {
        lock (LifecycleSync)
        {
            isClosing = true;
            if (activeOperations == 0)
                NoActiveOperations.Set();
        }

        NoActiveOperations.Wait();

        foreach ((string _, DuckDBConnection connection) in ConnectionCache)
        {
            try
            {
                connection.Dispose();
            }
            catch
            {
            }
        }

        ConnectionCache.Clear();

        lock (LifecycleSync)
        {
            isClosing = false;
            NoActiveOperations.Set();
        }
    }

    private static DuckDBConnection GetOpenConnection(string normalizedPath)
        => ConnectionCache.GetOrAdd(normalizedPath, static path =>
        {
            string? directoryPath = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(directoryPath))
                Directory.CreateDirectory(directoryPath);

            DuckDBConnection connection = new($"Data Source={path}");
            connection.Open();
            return connection;
        });

    private static async Task WithConnectionLockAsync(string dbPath, Func<string, Task> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            SemaphoreSlim gate = ConnectionLocks.GetOrAdd(normalizedPath, static _ => new SemaphoreSlim(1, 1));
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

    private static async Task<T> WithConnectionLockAsync<T>(string dbPath, Func<string, Task<T>> action, CancellationToken ct = default)
    {
        string normalizedPath = Path.GetFullPath(dbPath);
        await EnterOperationAsync(ct);
        try
        {
            SemaphoreSlim gate = ConnectionLocks.GetOrAdd(normalizedPath, static _ => new SemaphoreSlim(1, 1));
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
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCreateTableSql(tableName, properties, keyColumn);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertRowsAsync<T>(
        DuckDBConnection connection,
        DuckDBTransaction transaction,
        string tableName,
        PropertyInfo[] properties,
        IReadOnlyList<T> records,
        CancellationToken ct)
    {
        using DuckDBCommand insertCommand = connection.CreateCommand();
        insertCommand.Transaction = transaction;
        insertCommand.CommandText = BuildInsertSql(tableName, properties);

        for (int i = 0; i < properties.Length; i++)
        {
            IDbDataParameter parameter = insertCommand.CreateParameter();
            parameter.DbType = MapDbType(properties[i].PropertyType);
            insertCommand.Parameters.Add(parameter);
        }

        foreach (T record in records)
        {
            ct.ThrowIfCancellationRequested();
            for (int i = 0; i < properties.Length; i++)
            {
                PropertyInfo property = properties[i];
                insertCommand.Parameters[i].Value = ConvertToDbValue(property.GetValue(record), property.PropertyType);
            }

            await insertCommand.ExecuteNonQueryAsync(ct);
        }
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
        EnsureAggTradesBatchTables(connection);
        List<AggTradeImportRow> batchRows = AggTradeBatchPool.Get();
        Dictionary<long, AggTradeImportRow> batchDedup = AggTradeDedupPool.Get();
        bool tailDeleted = !replaceTail;
        long replaceFrom = replaceTail
            ? GetAggTradesCsvMinTimestamp(csvPath, sourceIsMicroseconds)
            : 0;

        try
        {
            using StreamReader reader = new(csvPath);
            while (reader.ReadLine() is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                AggTradeImportRow row = ParseAggTrade(line.AsSpan(), sourceIsMicroseconds);
                batchDedup[row.ParquetAggTradeId] = row;
                if (batchDedup.Count >= MarketDataImportBatchSize)
                    FlushAggTradeBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
            }

            FlushAggTradeBatch(connection, tableName, batchRows, batchDedup, ref tailDeleted, replaceFrom);
        }
        finally
        {
            batchRows.Clear();
            AggTradeBatchPool.Return(batchRows);
            batchDedup.Clear();
            AggTradeDedupPool.Return(batchDedup);
        }
    }

    private static void ImportBookDepthCsv(DuckDBConnection connection, string tableName, string csvPath)
    {
        EnsureBookDepthBatchTables(connection);
        List<BookDepthImportRow> batchRows = BookDepthBatchPool.Get();
        Dictionary<BookDepthRowKey, BookDepthImportRow> batchDedup = BookDepthDedupPool.Get();
        bool tailDeleted = false;
        long replaceFrom = GetBookDepthCsvMinTimestamp(csvPath);

        try
        {
            using StreamReader reader = new(csvPath);
            bool isFirstLine = true;
            while (reader.ReadLine() is { } line)
            {
                if (isFirstLine)
                {
                    isFirstLine = false;
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
        finally
        {
            batchRows.Clear();
            BookDepthBatchPool.Return(batchRows);
            batchDedup.Clear();
            BookDepthDedupPool.Return(batchDedup);
        }
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
        batchRows.AddRange(batchDedup.Values);
        batchRows.Sort(static (x, y) =>
        {
            int timeCompare = x.TransactTime.CompareTo(y.TransactTime);
            return timeCompare != 0 ? timeCompare : x.ParquetAggTradeId.CompareTo(y.ParquetAggTradeId);
        });

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

        ClearAggTradesBatchTables(connection, transaction);
        using (DuckDBCommand insertRowCommand = CreateAggTradesBatchRowInsertCommand(connection, transaction))
        using (DuckDBCommand insertKeyCommand = CreateAggTradesBatchKeyInsertCommand(connection, transaction))
        {
            foreach (AggTradeImportRow row in batchRows)
            {
                BindAggTradeRow(insertRowCommand, row);
                insertRowCommand.ExecuteNonQuery();
                insertKeyCommand.Parameters[0].Value = row.ParquetAggTradeId;
                insertKeyCommand.ExecuteNonQuery();
            }
        }

        using (DuckDBCommand deleteByKeyCommand = connection.CreateCommand())
        {
            deleteByKeyCommand.Transaction = transaction;
            deleteByKeyCommand.CommandText = $"""
                DELETE FROM {QuoteIdentifier(tableName)}
                USING temp_agg_trades_batch_keys
                WHERE {QuoteIdentifier(tableName)}.parquetagg_trade_id = temp_agg_trades_batch_keys.parquetagg_trade_id;
                """;
            deleteByKeyCommand.ExecuteNonQuery();
        }

        using (DuckDBCommand insertCommand = connection.CreateCommand())
        {
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"""
                INSERT INTO {QuoteIdentifier(tableName)}
                SELECT parquetagg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
                FROM temp_agg_trades_batch_rows
                ORDER BY transact_time, parquetagg_trade_id;
                """;
            insertCommand.ExecuteNonQuery();
        }

        transaction.Commit();
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
        batchRows.AddRange(batchDedup.Values);
        batchRows.Sort(static (x, y) =>
        {
            int timeCompare = x.SnapshotTime.CompareTo(y.SnapshotTime);
            return timeCompare != 0 ? timeCompare : x.Percentage.CompareTo(y.Percentage);
        });

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

        ClearBookDepthBatchTables(connection, transaction);
        using (DuckDBCommand insertRowCommand = CreateBookDepthBatchRowInsertCommand(connection, transaction))
        using (DuckDBCommand insertKeyCommand = CreateBookDepthBatchKeyInsertCommand(connection, transaction))
        {
            foreach (BookDepthImportRow row in batchRows)
            {
                BindBookDepthRow(insertRowCommand, row);
                insertRowCommand.ExecuteNonQuery();
                insertKeyCommand.Parameters[0].Value = row.SnapshotTime;
                insertKeyCommand.Parameters[1].Value = row.Percentage;
                insertKeyCommand.ExecuteNonQuery();
            }
        }

        using (DuckDBCommand deleteByKeyCommand = connection.CreateCommand())
        {
            deleteByKeyCommand.Transaction = transaction;
            deleteByKeyCommand.CommandText = $"""
                DELETE FROM {QuoteIdentifier(tableName)}
                USING temp_book_depth_batch_keys
                WHERE {QuoteIdentifier(tableName)}.snapshot_time = temp_book_depth_batch_keys.snapshot_time
                  AND {QuoteIdentifier(tableName)}.percentage = temp_book_depth_batch_keys.percentage;
                """;
            deleteByKeyCommand.ExecuteNonQuery();
        }

        using (DuckDBCommand insertCommand = connection.CreateCommand())
        {
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"""
                INSERT INTO {QuoteIdentifier(tableName)}
                SELECT snapshot_time, percentage, depth, notional
                FROM temp_book_depth_batch_rows
                ORDER BY snapshot_time, percentage;
                """;
            insertCommand.ExecuteNonQuery();
        }

        transaction.Commit();
        batchDedup.Clear();
        batchRows.Clear();
    }

    private static void EnsureAggTradesBatchTables(DuckDBConnection connection)
        => ExecuteDuckDbNonQuery(connection, """
            CREATE TEMP TABLE IF NOT EXISTS temp_agg_trades_batch_rows (
                parquetagg_trade_id BIGINT,
                price DOUBLE,
                quantity DOUBLE,
                first_trade_id BIGINT,
                last_trade_id BIGINT,
                transact_time BIGINT,
                is_buyer_maker BOOLEAN
            );
            CREATE TEMP TABLE IF NOT EXISTS temp_agg_trades_batch_keys (
                parquetagg_trade_id BIGINT
            );
            """);

    private static void EnsureBookDepthBatchTables(DuckDBConnection connection)
        => ExecuteDuckDbNonQuery(connection, """
            CREATE TEMP TABLE IF NOT EXISTS temp_book_depth_batch_rows (
                snapshot_time BIGINT,
                percentage DECIMAL(10,2),
                depth DOUBLE,
                notional DOUBLE
            );
            CREATE TEMP TABLE IF NOT EXISTS temp_book_depth_batch_keys (
                snapshot_time BIGINT,
                percentage DECIMAL(10,2)
            );
            """);

    private static void ClearAggTradesBatchTables(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM temp_agg_trades_batch_rows;
            DELETE FROM temp_agg_trades_batch_keys;
            """;
        command.ExecuteNonQuery();
    }

    private static void ClearBookDepthBatchTables(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM temp_book_depth_batch_rows;
            DELETE FROM temp_book_depth_batch_keys;
            """;
        command.ExecuteNonQuery();
    }

    private static DuckDBCommand CreateAggTradesBatchRowInsertCommand(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO temp_agg_trades_batch_rows
            VALUES ($parquetagg_trade_id, $price, $quantity, $first_trade_id, $last_trade_id, $transact_time, $is_buyer_maker);
            """;
        command.Parameters.Add(new DuckDBParameter { ParameterName = "parquetagg_trade_id", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "price", DbType = DbType.Double });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "quantity", DbType = DbType.Double });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "first_trade_id", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "last_trade_id", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "transact_time", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "is_buyer_maker", DbType = DbType.Boolean });
        return command;
    }

    private static DuckDBCommand CreateAggTradesBatchKeyInsertCommand(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO temp_agg_trades_batch_keys
            VALUES ($parquetagg_trade_id);
            """;
        command.Parameters.Add(new DuckDBParameter { ParameterName = "parquetagg_trade_id", DbType = DbType.Int64 });
        return command;
    }

    private static DuckDBCommand CreateBookDepthBatchRowInsertCommand(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO temp_book_depth_batch_rows
            VALUES ($snapshot_time, $percentage, $depth, $notional);
            """;
        command.Parameters.Add(new DuckDBParameter { ParameterName = "snapshot_time", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "percentage", DbType = DbType.Decimal });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "depth", DbType = DbType.Double });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "notional", DbType = DbType.Double });
        return command;
    }

    private static DuckDBCommand CreateBookDepthBatchKeyInsertCommand(DuckDBConnection connection, DuckDBTransaction transaction)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO temp_book_depth_batch_keys
            VALUES ($snapshot_time, $percentage);
            """;
        command.Parameters.Add(new DuckDBParameter { ParameterName = "snapshot_time", DbType = DbType.Int64 });
        command.Parameters.Add(new DuckDBParameter { ParameterName = "percentage", DbType = DbType.Decimal });
        return command;
    }

    private static void BindAggTradeRow(DuckDBCommand command, AggTradeImportRow row)
    {
        command.Parameters[0].Value = row.ParquetAggTradeId;
        command.Parameters[1].Value = row.Price;
        command.Parameters[2].Value = row.Quantity;
        command.Parameters[3].Value = row.FirstTradeId;
        command.Parameters[4].Value = row.LastTradeId;
        command.Parameters[5].Value = row.TransactTime;
        command.Parameters[6].Value = row.IsBuyerMaker;
    }

    private static void BindBookDepthRow(DuckDBCommand command, BookDepthImportRow row)
    {
        command.Parameters[0].Value = row.SnapshotTime;
        command.Parameters[1].Value = row.Percentage;
        command.Parameters[2].Value = row.Depth;
        command.Parameters[3].Value = row.Notional;
    }

    private static long GetAggTradesCsvMinTimestamp(string csvPath, bool sourceIsMicroseconds)
    {
        long? minTimestamp = null;
        using StreamReader reader = new(csvPath);
        while (reader.ReadLine() is { } line)
        {
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
        List<string> columns = [];
        foreach (PropertyInfo property in properties)
            columns.Add($"{QuoteIdentifier(property.Name)} {MapDuckDbType(property.PropertyType)}");

        if (!string.IsNullOrWhiteSpace(keyColumn))
            columns.Add($"PRIMARY KEY ({QuoteIdentifier(keyColumn)})");

        return $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} ({string.Join(", ", columns)});";
    }

    private static string BuildInsertSql(string tableName, IReadOnlyList<PropertyInfo> properties)
    {
        string[] columns = properties.Select(property => QuoteIdentifier(property.Name)).ToArray();
        string[] parameters = properties.Select(_ => "?").ToArray();
        return $"INSERT INTO {QuoteIdentifier(tableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)});";
    }

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

    private static DbType MapDbType(Type type)
    {
        Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType.IsEnum)
            return DbType.String;
        if (underlyingType == typeof(string))
            return DbType.String;
        if (underlyingType == typeof(bool))
            return DbType.Boolean;
        if (underlyingType == typeof(int))
            return DbType.Int32;
        if (underlyingType == typeof(long))
            return DbType.Int64;
        if (underlyingType == typeof(double))
            return DbType.Double;
        if (underlyingType == typeof(float))
            return DbType.Single;
        if (underlyingType == typeof(DateTime))
            return DbType.DateTime;
        if (underlyingType == typeof(decimal))
            return DbType.Decimal;
        if (underlyingType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(underlyingType))
            return DbType.String;

        throw new NotSupportedException($"Unsupported DB parameter type: {type.FullName}");
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private sealed class PooledListPolicy<T>(int initialCapacity) : PooledObjectPolicy<List<T>>
    {
        public override List<T> Create()
            => new(initialCapacity);

        public override bool Return(List<T> obj)
        {
            obj.Clear();
            return true;
        }
    }

    private sealed class PooledDictionaryPolicy<TKey, TValue>(int initialCapacity) : PooledObjectPolicy<Dictionary<TKey, TValue>>
        where TKey : notnull
    {
        public override Dictionary<TKey, TValue> Create()
            => new(initialCapacity);

        public override bool Return(Dictionary<TKey, TValue> obj)
        {
            obj.Clear();
            return true;
        }
    }
}
