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
using System.Threading;

namespace CollectorModels.ShardingCore;

public class BinanceKlineVirtualTableRoute<T> : AbstractShardingOperatorVirtualTableRoute<T, string>
    where T : BinanceMarkIndexKline
{
    private readonly IShardingTableCreator tableCreator;
    private readonly IVirtualDataSource virtualDataSource;
    private readonly ConcurrentDictionary<string, byte> tails;
    private readonly string currentTableName;

    public BinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource)
    {
        currentTableName = ShardingTableRouteHelper.GetLogicalTableName<T>();
        tails = ShardingTableRouteHelper.GetOrCreateTailCache(virtualDataSource.DefaultConnectionString, currentTableName);
        this.tableCreator = tableCreator;
        this.virtualDataSource = virtualDataSource;
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
        => [.. tails.Keys];

    public override TableRouteUnit RouteWithValue(DataSourceRouteResult dataSourceRouteResult, object shardingKey)
    {
        string shardingKeyToTail = ShardingKeyToTail(shardingKey);
        ShardingTableRouteHelper.EnsureTableTail<T>(tableCreator, virtualDataSource.DefaultConnectionString, virtualDataSource.DefaultDataSourceName, currentTableName, tails, shardingKeyToTail);
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

public class FuturesUsdtBinanceIndexPriceKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesUsdtBinanceIndexPriceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesUsdtBinanceMarkPriceKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesUsdtBinanceMarkPriceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinanceKlineVirtualTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinanceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinancePremiumIndexKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinancePremiumIndexKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinanceIndexPriceKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinanceIndexPriceKline>(tableCreator, virtualDataSource)
{
}

public class FuturesCoinBinanceMarkPriceKlineTableRoute(IShardingTableCreator tableCreator, IVirtualDataSource virtualDataSource) : BinanceKlineVirtualTableRoute<FuturesCoinBinanceMarkPriceKline>(tableCreator, virtualDataSource)
{
}
