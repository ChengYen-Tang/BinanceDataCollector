using DuckDB.NET.Data;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Collections.Concurrent;
using System.Text.Json;

namespace BinanceDataCollector;

internal static class DuckDbStorageHelper
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General);
    private static readonly StringComparer PathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly ConcurrentDictionary<string, DuckDBConnection> ConnectionCache = new(PathComparer);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> ConnectionLocks = new(PathComparer);
    private static readonly object LifecycleSync = new();
    private static readonly ManualResetEventSlim NoActiveOperations = new(initialState: true);
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
}
