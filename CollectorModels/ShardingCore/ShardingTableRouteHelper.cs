using Microsoft.Data.SqlClient;
using ShardingCore.TableCreator;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace CollectorModels.ShardingCore;

internal static class ShardingTableRouteHelper
{
    private static readonly ConcurrentDictionary<Type, string> logicalTableNameCache = new();
    private static readonly ConcurrentDictionary<string, Lazy<ConcurrentDictionary<string, byte>>> tailCache = new(StringComparer.OrdinalIgnoreCase);

    public static string GetLogicalTableName<TEntity>()
        => logicalTableNameCache.GetOrAdd(typeof(TEntity), static entityType =>
        {
            string? dbSetName = typeof(BinanceDbContext).GetProperties()
                .Where(property => property.PropertyType.IsGenericType)
                .FirstOrDefault(property =>
                    property.PropertyType.GetGenericTypeDefinition() == typeof(Microsoft.EntityFrameworkCore.DbSet<>)
                    && property.PropertyType.GetGenericArguments()[0] == entityType)
                ?.Name;

            return dbSetName ?? entityType.Name;
        });

    public static ConcurrentDictionary<string, byte> GetOrCreateTailCache(string connectionString, string logicalTableName)
        => tailCache.GetOrAdd(logicalTableName, _ => new Lazy<ConcurrentDictionary<string, byte>>(
            () => LoadExistingTailCache(connectionString, logicalTableName),
            LazyThreadSafetyMode.ExecutionAndPublication)).Value;

    public static void EnsureTableTail<TEntity>(IShardingTableCreator tableCreator, string connectionString, string dataSourceName, string logicalTableName, ConcurrentDictionary<string, byte> tails, string tail)
        where TEntity : class
    {
        if (tails.ContainsKey(tail))
            return;

        if (TableExists(connectionString, logicalTableName, tail))
        {
            tails.TryAdd(tail, 0);
            return;
        }

        try
        {
            tableCreator.CreateTable<TEntity>(dataSourceName, tail);
        }
        catch (SqlException ex) when (ex.Number == 2714)
        {
            if (!TableExists(connectionString, logicalTableName, tail))
                throw;
        }

        tails.TryAdd(tail, 0);
    }

    private static ConcurrentDictionary<string, byte> LoadExistingTailCache(string connectionString, string logicalTableName)
    {
        const string sql = """
            SELECT t.name
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = N'dbo'
              AND t.name LIKE @tablePrefix ESCAPE '\'
            """;

        ConcurrentDictionary<string, byte> tails = new(StringComparer.OrdinalIgnoreCase);
        using SqlConnection connection = new(connectionString);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@tablePrefix", $"{logicalTableName}\\_%");

        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            if (!tableName.StartsWith(logicalTableName + "_", StringComparison.OrdinalIgnoreCase))
                continue;

            string tail = tableName[(logicalTableName.Length + 1)..];
            tails.TryAdd(tail, 0);
        }

        return tails;
    }

    private static bool TableExists(string connectionString, string logicalTableName, string tail)
    {
        string tableName = $"{logicalTableName}_{tail}";
        using SqlConnection connection = new(connectionString);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandText = "SELECT CASE WHEN OBJECT_ID(@tableName, N'U') IS NULL THEN 0 ELSE 1 END";
        command.Parameters.AddWithValue("@tableName", $"[dbo].[{tableName}]");

        object? value = command.ExecuteScalar();
        return Convert.ToInt32(value) == 1;
    }
}
