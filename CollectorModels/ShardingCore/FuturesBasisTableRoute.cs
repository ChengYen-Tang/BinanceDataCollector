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

namespace CollectorModels.ShardingCore;

public abstract class FuturesBasisTableRoute<TBasis> : AbstractShardingOperatorVirtualTableRoute<TBasis, string>
    where TBasis : FuturesBasis
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly ConcurrentDictionary<string, byte> tails;
    private readonly string currentTableName;

    protected FuturesBasisTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        currentTableName = ShardingTableRouteHelper.GetLogicalTableName<TBasis>();
        tails = ShardingTableRouteHelper.GetOrCreateTailCache(virtualDataSource.DefaultConnectionString, currentTableName);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
    }

    public override void Configure(EntityMetadataTableBuilder<TBasis> builder)
        => builder.ShardingProperty(item => item.SymbolInfoId);

    public override Func<string, bool> GetRouteToFilter(string shardingKey, ShardingOperatorEnum shardingOperator)
        => shardingOperator == ShardingOperatorEnum.Equal ? tail => tail == shardingKey : _ => true;

    public override List<string> GetTails()
        => [.. tails.Keys];

    public override TableRouteUnit RouteWithValue(DataSourceRouteResult dataSourceRouteResult, object shardingKey)
    {
        string shardingKeyToTail = ShardingKeyToTail(shardingKey);
        ShardingTableRouteHelper.EnsureTableTail<TBasis>(tableCreator, virtualDataSource.DefaultConnectionString, virtualDataSource.DefaultDataSourceName, currentTableName, tails, shardingKeyToTail);
        return base.RouteWithValue(dataSourceRouteResult, shardingKey);
    }

    public override string ShardingKeyToTail(object shardingKey)
        => shardingKey.ToString();
}

public sealed class FuturesUsdtBasisTableRoute : FuturesBasisTableRoute<FuturesUsdtBasis>
{
    public FuturesUsdtBasisTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinBasisTableRoute : FuturesBasisTableRoute<FuturesCoinBasis>
{
    public FuturesCoinBasisTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}
