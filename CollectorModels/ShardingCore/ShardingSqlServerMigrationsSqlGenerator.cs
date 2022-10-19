using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using ShardingCore.Core.RuntimeContexts;
using ShardingCore.Helpers;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace CollectorModels.ShardingCore;

public class ShardingSqlServerMigrationsSqlGenerator : SqlServerMigrationsSqlGenerator
{
    private readonly IShardingRuntimeContext shardingRuntimeContext;

    public ShardingSqlServerMigrationsSqlGenerator(IShardingRuntimeContext shardingRuntimeContext, [NotNull] MigrationsSqlGeneratorDependencies dependencies, [NotNull] IRelationalAnnotationProvider migrationsAnnotations)
        : base(dependencies, migrationsAnnotations) => this.shardingRuntimeContext = shardingRuntimeContext;

    protected override void Generate(
    MigrationOperation operation,
    IModel model,
    MigrationCommandListBuilder builder)
    {
        var oldCmds = builder.GetCommandList().ToList();
        base.Generate(operation, model, builder);
        var newCmds = builder.GetCommandList().ToList();
        var addCmds = newCmds.Where(x => !oldCmds.Contains(x)).ToList();

        MigrationHelper.Generate(shardingRuntimeContext, operation, builder, Dependencies.SqlGenerationHelper, addCmds);
    }
}
