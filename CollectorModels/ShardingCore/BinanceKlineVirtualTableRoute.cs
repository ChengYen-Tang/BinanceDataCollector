using CollectorModels.Models;
using Microsoft.Data.SqlClient;
using ShardingCore.Core.EntityMetadatas;
using ShardingCore.Core.VirtualDatabase.VirtualDataSources;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes.DataSourceRoutes.RouteRuleEngine;
using ShardingCore.Core.VirtualRoutes.TableRoutes.Abstractions;
using ShardingCore.TableCreator;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace CollectorModels.ShardingCore;

public class BinanceKlineVirtualTableRoute<T> : AbstractShardingOperatorVirtualTableRoute<T, string>
    where T : BinanceMarkIndexKline
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly HashSet<string> tails;
    private readonly ReaderWriterLockSlim tailsLock;
    private readonly string CurrentTableName;

    private const string Tables = "Tables";
    private const string TABLE_SCHEMA = "TABLE_SCHEMA";
    private const string TABLE_NAME = "TABLE_NAME";

    public BinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        CurrentTableName = typeof(T).Name;
        tails = [];
        tailsLock = new(LockRecursionPolicy.SupportsRecursion);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
        InitTails();
    }

    private void InitTails()
    {

        using SqlConnection connection = new(virtualDataSource.DefaultConnectionString);
        connection.Open();

        using var dataTable = connection.GetSchema(Tables);

        for (int i = 0; i < dataTable.Rows.Count; i++)
        {
            var schema = dataTable.Rows[i][TABLE_SCHEMA] as string;
            if ("dbo".Equals(schema, StringComparison.OrdinalIgnoreCase))
            {
                var tableName = dataTable.Rows[i][TABLE_NAME]?.ToString() ?? string.Empty;
                if (tableName.StartsWith(CurrentTableName, StringComparison.OrdinalIgnoreCase))
                {
                    tailsLock.EnterWriteLock();
                    try
                    {
                        tails.Add(tableName[(CurrentTableName.Length + 2)..]);
                    }
                    finally
                    {
                        if (tailsLock.IsWriteLockHeld)
                            tailsLock.ExitWriteLock();
                    }
                }
            }
        }
    }

    public override void Configure(EntityMetadataTableBuilder<T> builder)
        => builder.ShardingProperty(item => item.SymbolInfoId);

    public override Func<string, bool> GetRouteToFilter(string shardingKey, ShardingOperatorEnum shardingOperator)
    {
        switch (shardingOperator)
        {
            case ShardingOperatorEnum.Equal:
                return tail => tail == shardingKey;
            default:
                {
                    Debug.WriteLine($"shardingOperator is not equal scan all table tail.");
                    return tail => true;
                }
        }
    }

    public override List<string> GetTails()
    {
        tailsLock.EnterReadLock();
        try
        {
            return [.. tails];
        }
        finally
        {
            if (tailsLock.IsReadLockHeld)
                tailsLock.ExitReadLock();
        }
    }

    public override TableRouteUnit RouteWithValue(DataSourceRouteResult dataSourceRouteResult, object shardingKey)
    {
        string shardingKeyToTail = ShardingKeyToTail(shardingKey);
        tailsLock.EnterWriteLock();
        try
        {
            if (!tails.Contains(shardingKeyToTail))
            {
                tableCreator.CreateTable<T>(virtualDataSource.DefaultDataSourceName, shardingKeyToTail);
                tails.Add(shardingKeyToTail);
            }
        }
        finally
        {
            if (tailsLock.IsWriteLockHeld)
                tailsLock.ExitWriteLock();
        }

        return base.RouteWithValue(dataSourceRouteResult, shardingKey);
    }

    public override string ShardingKeyToTail(object shardingKey)
        => shardingKey.ToString();
}

public class SpotBinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<SpotBinanceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesUsdtBinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesUsdtBinanceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesUsdtBinancePremiumIndexKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesUsdtBinancePremiumIndexKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinanceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinancePremiumIndexKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinancePremiumIndexKline>(tableCreator, virtualDataSource)
{
}
