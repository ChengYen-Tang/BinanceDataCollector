using Binance.Net.Enums;
using CollectorModels.Models;
using CollectorModels.ValueConverter;
using Microsoft.EntityFrameworkCore;
using ShardingCore.Core.VirtualRoutes.TableRoutes.RouteTails.Abstractions;
using ShardingCore.Sharding;
using ShardingCore.Sharding.Abstractions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CollectorModels
{
    public class BinanceDbContext(DbContextOptions options) : AbstractShardingDbContext(options), IShardingTableDbContext
    {
        private readonly static EnumCollectionJsonValueConverter<FuturesOrderType> futuresOrderTypeConverter = new();
        private readonly static EnumCollectionJsonValueConverter<TimeInForce> timeInForceConverter = new();
        private readonly static EnumCollectionJsonValueConverter<SpotOrderType> spotOrderTypeConverter = new();
        private readonly static EnumCollectionJsonValueConverter<AccountType> accountTypeConverter = new();
        private readonly static StringCollectionJsonValueConverter stringConverter = new();
        private readonly static CollectionValueComparer<FuturesOrderType> futuresOrderTypeComparer = new();
        private readonly static CollectionValueComparer<TimeInForce> timeInForceComparer = new();
        private readonly static CollectionValueComparer<SpotOrderType> spotOrderTypeComparer = new();
        private readonly static CollectionValueComparer<AccountType> accountTypeComparer = new();
        private readonly static CollectionValueComparer<string> stringComparer = new();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            #region BinanceSymbolInfo
            modelBuilder.Entity<BinanceSymbolInfo>()
                .Property(e => e.Status)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceSymbolInfo>()
                .Property(item => item.OrderTypes)
                .HasConversion(spotOrderTypeConverter)
                .Metadata.SetValueComparer(spotOrderTypeComparer);
            modelBuilder.Entity<BinanceSymbolInfo>()
                .Property(item => item.Permissions)
                .HasConversion(accountTypeConverter)
                .Metadata.SetValueComparer(accountTypeComparer);
            modelBuilder.Entity<BinanceSymbolInfo>()
                .HasMany(item => item.BinanceKlines)
                .WithOne(item => item.SymbolInfo);
            #endregion
            #region BinanceFuturesCoinSymbolInfo
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(e => e.ContractType)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(e => e.UnderlyingType)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(e => e.Status)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(item => item.OrderTypes)
                .HasConversion(futuresOrderTypeConverter)
                .Metadata.SetValueComparer(futuresOrderTypeComparer);
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(item => item.TimeInForce)
                .HasConversion(timeInForceConverter)
                .Metadata.SetValueComparer(timeInForceComparer);
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .Property(item => item.UnderlyingSubType)
                .HasConversion(stringConverter)
                .Metadata.SetValueComparer(stringComparer);
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .HasMany(item => item.BinanceKlines)
                .WithOne(item => item.SymbolInfo);
            modelBuilder.Entity<BinanceFuturesCoinSymbolInfo>()
                .HasMany(item => item.BinancePremiumIndexKlines)
                .WithOne(item => item.SymbolInfo);
            #endregion
            #region BinanceFuturesUsdtSymbolInfo
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(e => e.ContractType)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(e => e.UnderlyingType)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(e => e.Status)
                .HasConversion<string>();
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(item => item.OrderTypes)
                .HasConversion(futuresOrderTypeConverter)
                .Metadata.SetValueComparer(futuresOrderTypeComparer);
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(item => item.TimeInForce)
                .HasConversion(timeInForceConverter)
                .Metadata.SetValueComparer(timeInForceComparer);
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .Property(item => item.UnderlyingSubType)
                .HasConversion(stringConverter)
                .Metadata.SetValueComparer(stringComparer);
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .HasMany(item => item.BinanceKlines)
                .WithOne(item => item.SymbolInfo);
            modelBuilder.Entity<BinanceFuturesUsdtSymbolInfo>()
                .HasMany(item => item.BinancePremiumIndexKlines)
                .WithOne(item => item.SymbolInfo);
            #endregion

            #region SpotBinanceKline
            modelBuilder.Entity<SpotBinanceKline>()
                .Property(e => e.Interval)
                .HasConversion<string>();
            modelBuilder.Entity<SpotBinanceKline>()
                .HasOne(item => item.SymbolInfo)
                .WithMany(item => item.BinanceKlines)
                .HasForeignKey(item => item.SymbolInfoId)
                .OnDelete(DeleteBehavior.Cascade);
            #endregion
            #region FuturesUsdtBinanceKline
            modelBuilder.Entity<FuturesUsdtBinanceKline>()
                .Property(e => e.Interval)
                .HasConversion<string>();
            modelBuilder.Entity<FuturesUsdtBinanceKline>()
                .HasOne(item => item.SymbolInfo)
                .WithMany(item => item.BinanceKlines)
                .HasForeignKey(item => item.SymbolInfoId)
                .OnDelete(DeleteBehavior.Cascade);
            #endregion
            #region FuturesUsdtBinancePremiumIndexKline
            modelBuilder.Entity<FuturesUsdtBinancePremiumIndexKline>()
                .Property(e => e.Interval)
                .HasConversion<string>();
            modelBuilder.Entity<FuturesUsdtBinancePremiumIndexKline>()
                .HasOne(item => item.SymbolInfo)
                .WithMany(item => item.BinancePremiumIndexKlines)
                .HasForeignKey(item => item.SymbolInfoId)
                .OnDelete(DeleteBehavior.Cascade);
            #endregion
            #region FuturesCoinBinanceKline
            modelBuilder.Entity<FuturesCoinBinanceKline>()
                .Property(e => e.Interval)
                .HasConversion<string>();
            modelBuilder.Entity<FuturesCoinBinanceKline>()
                .HasOne(item => item.SymbolInfo)
                .WithMany(item => item.BinanceKlines)
                .HasForeignKey(item => item.SymbolInfoId)
                .OnDelete(DeleteBehavior.Cascade);
            #endregion
            #region FuturesCoinBinancePremiumIndexKline
            modelBuilder.Entity<FuturesCoinBinancePremiumIndexKline>()
                .Property(e => e.Interval)
                .HasConversion<string>();
            modelBuilder.Entity<FuturesCoinBinancePremiumIndexKline>()
                .HasOne(item => item.SymbolInfo)
                .WithMany(item => item.BinancePremiumIndexKlines)
                .HasForeignKey(item => item.SymbolInfoId)
                .OnDelete(DeleteBehavior.Cascade);
            #endregion
        }

        public virtual DbSet<BinanceSymbolInfo> BinanceSymbolInfos { get; set; }
        public virtual DbSet<BinanceFuturesCoinSymbolInfo> BinanceFuturesCoinSymbolInfos { get; set; }
        public virtual DbSet<BinanceFuturesUsdtSymbolInfo> BinanceFuturesUsdtSymbolInfos { get; set; }
        public virtual DbSet<SpotBinanceKline> SpotBinanceKlines { get; set; }
        public virtual DbSet<FuturesUsdtBinanceKline> FuturesUsdtBinanceKlines { get; set; }
        public virtual DbSet<FuturesUsdtBinancePremiumIndexKline> FuturesUsdtBinancePremiumIndexKlines { get; set; }
        public virtual DbSet<FuturesCoinBinanceKline> FuturesCoinBinanceKlines { get; set; }
        public virtual DbSet<FuturesCoinBinancePremiumIndexKline> FuturesCoinBinancePremiumIndexKlines { get; set; }

        public IRouteTail RouteTail { get; set; }
    }

    public class CoinModel
    {
        public CoinModel()
            => (Id, CoinData) = (Guid.NewGuid(), new List<CoinDataModel>());

        [Key]
        [Required]
        public Guid Id { get; set; }
        [Required]
        public string Name { get; set; }

        public ICollection<CoinDataModel> CoinData { get; set; }
    }

    [Index(nameof(CoinId))]
    public class CoinDataModel
    {
        [Key]
        [Required]
        public string Key { get; set; }
        [Required]
        [Column(TypeName = "datetime")]
        public DateTime Date { get; set; }
        [Required]
        [Column(TypeName = "decimal(38, 19)")]
        public decimal Open { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal High { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal Low { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal Close { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal Volume { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal Money { get; set; }
        [Required]
        public int TradeCount { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal TakerBuyBaseVolume { get; set; }
        [Column(TypeName = "decimal(38, 19)")]
        [Required]
        public decimal TakerBuyQuoteVolume { get; set; }

        [Required]
        public Guid CoinId { get; set; }
        [Required]
        public virtual CoinModel Coin { get; set; }
    }
}
