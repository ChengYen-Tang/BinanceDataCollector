using DuckDB.NET.Data;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace BinanceDataIntegrityChecker;

public sealed class MarketDataIntegrityChecker(
    IOptions<IntegrityCheckOptions> options,
    ILogger<MarketDataIntegrityChecker> logger)
{
    private const string AggTradesDataType = "aggTrades";
    private const string BookDepthDataType = "bookDepth";

    private readonly IntegrityCheckOptions options = options.Value;

    public void Run(CancellationToken ct)
    {
        string rootFolder = Path.GetFullPath(options.RootFolder);
        if (!Directory.Exists(rootFolder))
        {
            logger.LogError("Integrity check root folder does not exist. RootFolder: {RootFolder}", rootFolder);
            return;
        }

        CheckSummary summary = new();

        if (options.DataTypes.AggTrades)
            CheckDataType(rootFolder, DataTypeProfile.AggTrades, summary, ct);

        if (options.DataTypes.BookDepth)
            CheckDataType(rootFolder, DataTypeProfile.BookDepth, summary, ct);

        logger.LogInformation(
            "DuckDB integrity check finished. Databases: {DatabaseCount}, Tables: {TableCount}, TablesWithIssues: {TablesWithIssues}, MissingDays: {MissingDays}, InvalidRows: {InvalidRows}, AllZeroColumns: {AllZeroColumns}, FailedTables: {FailedTables}",
            summary.DatabaseCount,
            summary.TableCount,
            summary.TablesWithIssues,
            summary.MissingDays,
            summary.InvalidRows,
            summary.AllZeroColumns,
            summary.FailedTables);
    }

    private void CheckDataType(string rootFolder, DataTypeProfile profile, CheckSummary summary, CancellationToken ct)
    {
        string dataTypeFolder = Path.Combine(rootFolder, profile.FolderName);
        if (!Directory.Exists(dataTypeFolder))
        {
            logger.LogWarning("Integrity check data type folder does not exist. DataType: {DataType}, Folder: {Folder}", profile.DataType, dataTypeFolder);
            return;
        }

        foreach (string databasePath in Directory.EnumerateFiles(dataTypeFolder, "*.duckdb", SearchOption.TopDirectoryOnly).Order(StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            summary.DatabaseCount++;
            CheckDatabase(databasePath, profile, summary, ct);
        }
    }

    private void CheckDatabase(string databasePath, DataTypeProfile profile, CheckSummary summary, CancellationToken ct)
    {
        try
        {
            using DuckDBConnection connection = new($"Data Source={databasePath}");
            connection.Open();

            foreach (string tableName in GetTableNames(connection))
            {
                ct.ThrowIfCancellationRequested();
                summary.TableCount++;
                CheckTable(connection, databasePath, tableName, profile, summary);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to check DuckDB file. DataType: {DataType}, DatabasePath: {DatabasePath}", profile.DataType, databasePath);
        }
    }

    private void CheckTable(
        DuckDBConnection connection,
        string databasePath,
        string tableName,
        DataTypeProfile profile,
        CheckSummary summary)
    {
        bool hasIssue = false;

        try
        {
            long rowCount = GetRowCount(connection, tableName);
            if (rowCount == 0)
            {
                logger.LogWarning(
                    "DuckDB table has no rows. DataType: {DataType}, DatabasePath: {DatabasePath}, Table: {Table}",
                    profile.DataType,
                    databasePath,
                    tableName);
                summary.TablesWithIssues++;
                return;
            }

            long invalidRows = GetInvalidRowCount(connection, tableName, profile);
            if (invalidRows > 0)
            {
                hasIssue = true;
                summary.InvalidRows += invalidRows;
                logger.LogWarning(
                    "DuckDB table contains invalid rows. DataType: {DataType}, DatabasePath: {DatabasePath}, Table: {Table}, InvalidRows: {InvalidRows}",
                    profile.DataType,
                    databasePath,
                    tableName,
                    invalidRows);
            }

            foreach (string columnName in GetAllZeroColumns(connection, tableName, profile))
            {
                hasIssue = true;
                summary.AllZeroColumns++;
                logger.LogWarning(
                    "DuckDB table numeric column contains only zero values. DataType: {DataType}, DatabasePath: {DatabasePath}, Table: {Table}, Column: {Column}, RowCount: {RowCount}",
                    profile.DataType,
                    databasePath,
                    tableName,
                    columnName,
                    rowCount);
            }

            IReadOnlyList<DateTime> missingDays = GetMissingDays(connection, tableName, profile);
            if (missingDays.Count > 0)
            {
                hasIssue = true;
                summary.MissingDays += missingDays.Count;
                logger.LogWarning(
                    "DuckDB table has missing UTC days. DataType: {DataType}, DatabasePath: {DatabasePath}, Table: {Table}, MissingDayCount: {MissingDayCount}, MissingDaysSample: {MissingDaysSample}",
                    profile.DataType,
                    databasePath,
                    tableName,
                    missingDays.Count,
                    FormatMissingDays(missingDays));
            }

            if (hasIssue)
                summary.TablesWithIssues++;
        }
        catch (Exception ex)
        {
            summary.FailedTables++;
            logger.LogError(
                ex,
                "Failed to check DuckDB table. DataType: {DataType}, DatabasePath: {DatabasePath}, Table: {Table}",
                profile.DataType,
                databasePath,
                tableName);
        }
    }

    private string FormatMissingDays(IReadOnlyList<DateTime> missingDays)
    {
        int takeCount = Math.Max(0, options.MaxMissingDaysToLog);
        IEnumerable<string> sample = missingDays
            .Take(takeCount)
            .Select(static day => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

        string value = string.Join(", ", sample);
        return missingDays.Count > takeCount
            ? $"{value}, ..."
            : value;
    }

    private static List<string> GetTableNames(DuckDBConnection connection)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
            """;

        using DuckDBDataReader reader = command.ExecuteReader();
        List<string> tableNames = [];
        while (reader.Read())
            tableNames.Add(reader.GetString(0));

        return tableNames;
    }

    private static long GetRowCount(DuckDBConnection connection, string tableName)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {QuoteIdentifier(tableName)};";
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static long GetInvalidRowCount(DuckDBConnection connection, string tableName, DataTypeProfile profile)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(tableName)}
            WHERE {profile.InvalidRowPredicate};
            """;

        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static IReadOnlyList<string> GetAllZeroColumns(DuckDBConnection connection, string tableName, DataTypeProfile profile)
    {
        List<string> allZeroColumns = [];
        foreach (string columnName in profile.AllZeroColumns)
        {
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(*) FILTER (WHERE {QuoteIdentifier(columnName)} = 0) AS zero_rows,
                    COUNT(*) FILTER (WHERE {QuoteIdentifier(columnName)} IS NULL OR NOT isfinite({QuoteIdentifier(columnName)})) AS invalid_value_rows
                FROM {QuoteIdentifier(tableName)}
                """;

            using DuckDBDataReader reader = command.ExecuteReader();
            if (!reader.Read())
                continue;

            long totalRows = reader.GetInt64(0);
            long zeroRows = reader.GetInt64(1);
            long invalidValueRows = reader.GetInt64(2);
            if (totalRows > 0 && invalidValueRows == 0 && zeroRows == totalRows)
                allZeroColumns.Add(columnName);
        }

        return allZeroColumns;
    }

    private static IReadOnlyList<DateTime> GetMissingDays(DuckDBConnection connection, string tableName, DataTypeProfile profile)
    {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT FLOOR({profile.TimeColumn} / {profile.TimestampUnitsPerDay})::BIGINT AS utc_day_number
            FROM {QuoteIdentifier(tableName)}
            WHERE {profile.TimeColumn} IS NOT NULL
              AND {profile.TimeColumn} > 0
            GROUP BY utc_day_number
            ORDER BY utc_day_number;
            """;

        using DuckDBDataReader reader = command.ExecuteReader();
        List<long> existingDayNumbers = [];
        while (reader.Read())
            existingDayNumbers.Add(reader.GetInt64(0));

        if (existingDayNumbers.Count <= 1)
            return [];

        HashSet<long> daySet = [.. existingDayNumbers];
        List<DateTime> missingDays = [];
        for (long dayNumber = existingDayNumbers[0]; dayNumber <= existingDayNumbers[^1]; dayNumber++)
        {
            if (!daySet.Contains(dayNumber))
                missingDays.Add(DateTime.UnixEpoch.AddDays(dayNumber));
        }

        return missingDays;
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private sealed record DataTypeProfile(
        string DataType,
        string FolderName,
        string TimeColumn,
        string TimestampUnitsPerDay,
        string InvalidRowPredicate,
        IReadOnlyList<string> AllZeroColumns)
    {
        public static DataTypeProfile AggTrades { get; } = new(
            AggTradesDataType,
            AggTradesDataType,
            "transact_time",
            "86400000000",
            """
            parquetagg_trade_id IS NULL
            OR parquetagg_trade_id < 0
            OR price IS NULL
            OR NOT isfinite(price)
            OR price < 0
            OR quantity IS NULL
            OR NOT isfinite(quantity)
            OR quantity < 0
            OR first_trade_id IS NULL
            OR first_trade_id < 0
            OR last_trade_id IS NULL
            OR last_trade_id < 0
            OR last_trade_id < first_trade_id
            OR transact_time IS NULL
            OR transact_time <= 0
            OR is_buyer_maker IS NULL
            """,
            ["price", "quantity"]);

        public static DataTypeProfile BookDepth { get; } = new(
            BookDepthDataType,
            BookDepthDataType,
            "snapshot_time",
            "86400000",
            """
            snapshot_time IS NULL
            OR snapshot_time <= 0
            OR percentage IS NULL
            OR percentage < 0
            OR depth IS NULL
            OR NOT isfinite(depth)
            OR depth < 0
            OR notional IS NULL
            OR NOT isfinite(notional)
            OR notional < 0
            """,
            ["depth", "notional"]);
    }

    private sealed class CheckSummary
    {
        public int DatabaseCount { get; set; }

        public int TableCount { get; set; }

        public int TablesWithIssues { get; set; }

        public long MissingDays { get; set; }

        public long InvalidRows { get; set; }

        public int AllZeroColumns { get; set; }

        public int FailedTables { get; set; }
    }
}
