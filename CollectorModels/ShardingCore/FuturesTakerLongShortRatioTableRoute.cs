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

public abstract class FuturesTakerLongShortRatioTableRoute<TRatio> : AbstractShardingOperatorVirtualTableRoute<TRatio, string>
    where TRatio : FuturesTakerLongShortRatio
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly ConcurrentDictionary<string, byte> tails;
    private readonly string currentTableName;

    protected FuturesTakerLongShortRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        currentTableName = ShardingTableRouteHelper.GetLogicalTableName<TRatio>();
        tails = ShardingTableRouteHelper.GetOrCreateTailCache(virtualDataSource.DefaultConnectionString, currentTableName);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
    }

    public override void Configure(EntityMetadataTableBuilder<TRatio> builder)
        => builder.ShardingProperty(item => item.SymbolInfoId);

    public override Func<string, bool> GetRouteToFilter(string shardingKey, ShardingOperatorEnum shardingOperator)
        => shardingOperator == ShardingOperatorEnum.Equal ? tail => tail == shardingKey : _ => true;

    public override List<string> GetTails()
        => [.. tails.Keys];

    public override TableRouteUnit RouteWithValue(DataSourceRouteResult dataSourceRouteResult, object shardingKey)
    {
        string shardingKeyToTail = ShardingKeyToTail(shardingKey);
        ShardingTableRouteHelper.EnsureTableTail<TRatio>(tableCreator, virtualDataSource.DefaultConnectionString, virtualDataSource.DefaultDataSourceName, currentTableName, tails, shardingKeyToTail);
        return base.RouteWithValue(dataSourceRouteResult, shardingKey);
    }

    public override string ShardingKeyToTail(object shardingKey)
        => shardingKey.ToString();
}

public sealed class FuturesUsdtTakerLongShortRatioTableRoute : FuturesTakerLongShortRatioTableRoute<FuturesUsdtTakerLongShortRatio>
{
    public FuturesUsdtTakerLongShortRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinTakerLongShortRatioTableRoute : FuturesTakerLongShortRatioTableRoute<FuturesCoinTakerLongShortRatio>
{
    public FuturesCoinTakerLongShortRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}
