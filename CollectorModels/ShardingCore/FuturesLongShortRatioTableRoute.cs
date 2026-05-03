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

public abstract class FuturesLongShortRatioTableRoute<TRatio> : AbstractShardingOperatorVirtualTableRoute<TRatio, string>
    where TRatio : FuturesLongShortRatio
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly ConcurrentDictionary<string, byte> tails;
    private readonly string currentTableName;

    protected FuturesLongShortRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        currentTableName = ShardingTableRouteHelper.GetLogicalTableName<TRatio>();
        tails = ShardingTableRouteHelper.GetOrCreateTailCache(virtualDataSource.DefaultConnectionString, currentTableName);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
    }

    public override void Configure(EntityMetadataTableBuilder<TRatio> builder)
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
        ShardingTableRouteHelper.EnsureTableTail<TRatio>(tableCreator, virtualDataSource.DefaultConnectionString, virtualDataSource.DefaultDataSourceName, currentTableName, tails, shardingKeyToTail);
        return base.RouteWithValue(dataSourceRouteResult, shardingKey);
    }

    public override string ShardingKeyToTail(object shardingKey)
        => shardingKey.ToString();
}

public sealed class FuturesUsdtTopLongShortPositionRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesUsdtTopLongShortPositionRatio>
{
    public FuturesUsdtTopLongShortPositionRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinTopLongShortPositionRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesCoinTopLongShortPositionRatio>
{
    public FuturesCoinTopLongShortPositionRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesUsdtTopLongShortAccountRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesUsdtTopLongShortAccountRatio>
{
    public FuturesUsdtTopLongShortAccountRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinTopLongShortAccountRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesCoinTopLongShortAccountRatio>
{
    public FuturesCoinTopLongShortAccountRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesUsdtGlobalLongShortAccountRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesUsdtGlobalLongShortAccountRatio>
{
    public FuturesUsdtGlobalLongShortAccountRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}

public sealed class FuturesCoinGlobalLongShortAccountRatioTableRoute : FuturesLongShortRatioTableRoute<FuturesCoinGlobalLongShortAccountRatio>
{
    public FuturesCoinGlobalLongShortAccountRatioTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
        : base(tableCreator, virtualDataSource)
    {
    }
}
