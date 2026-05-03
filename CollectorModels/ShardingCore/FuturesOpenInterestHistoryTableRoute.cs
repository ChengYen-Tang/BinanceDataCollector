using CollectorModels.Models;
using ShardingCore.Core.EntityMetadatas;
using ShardingCore.Core.VirtualDatabase.VirtualDataSources;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes.DataSourceRoutes.RouteRuleEngine;
using ShardingCore.Core.VirtualRoutes.TableRoutes.Abstractions;
using ShardingCore.TableCreator;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;

namespace CollectorModels.ShardingCore;

public abstract class FuturesOpenInterestHistoryTableRoute<TOpenInterest> : AbstractShardingOperatorVirtualTableRoute<TOpenInterest, string>
    where TOpenInterest : FuturesOpenInterestHistory
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly ConcurrentDictionary<string, byte> tails;
    private readonly string currentTableName;

    protected FuturesOpenInterestHistoryTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        currentTableName = ShardingTableRouteHelper.GetLogicalTableName<TOpenInterest>();
        tails = ShardingTableRouteHelper.GetOrCreateTailCache(virtualDataSource.DefaultConnectionString, currentTableName);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
    }

    public override void Configure(EntityMetadataTableBuilder<TOpenInterest> builder)
        => builder.ShardingProperty(item => item.SymbolInfoId);

    public override Func<string, bool> GetRouteToFilter(string shardingKey, ShardingOperatorEnum shardingOperator)
    {
        switch (shardingOperator)
        {
            case ShardingOperatorEnum.Equal:
                return tail => tail == shardingKey;
            default:
                Debug.WriteLine("shardingOperator is not equal scan all table tail.");
                return _ => true;
        }
    }

    public override List<string> GetTails()
        => [.. tails.Keys];

    public override TableRouteUnit RouteWithValue(DataSourceRouteResult dataSourceRouteResult, object shardingKey)
    {
        string shardingKeyToTail = ShardingKeyToTail(shardingKey);
        ShardingTableRouteHelper.EnsureTableTail<TOpenInterest>(tableCreator, virtualDataSource.DefaultConnectionString, virtualDataSource.DefaultDataSourceName, currentTableName, tails, shardingKeyToTail);
        return base.RouteWithValue(dataSourceRouteResult, shardingKey);
    }

    public override string ShardingKeyToTail(object shardingKey)
        => shardingKey.ToString();
}

public sealed class FuturesUsdtOpenInterestHistoryTableRoute : FuturesOpenInterestHistoryTableRoute<FuturesUsdtOpenInterestHistory>
{
    public FuturesUsdtOpenInterestHistoryTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinOpenInterestHistoryTableRoute : FuturesOpenInterestHistoryTableRoute<FuturesCoinOpenInterestHistory>
{
    public FuturesCoinOpenInterestHistoryTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}
