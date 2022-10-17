﻿// <auto-generated />
using System;
using CollectorModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

#nullable disable

namespace CollectorModels.Migrations.ShardingMigrations
{
    [DbContext(typeof(BinanceDbContext))]
    partial class BinanceDbContextModelSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("ProductVersion", "6.0.10")
                .HasAnnotation("Relational:MaxIdentifierLength", 128);

            SqlServerModelBuilderExtensions.UseIdentityColumns(modelBuilder, 1L, 1);

            modelBuilder.Entity("CollectorModels.Models.BinanceFuturesCoinSymbolInfo", b =>
                {
                    b.Property<string>("Name")
                        .HasColumnType("nvarchar(450)");

                    b.Property<string>("BaseAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("BaseAssetPrecision")
                        .HasColumnType("int");

                    b.Property<int>("ContractSize")
                        .HasColumnType("int");

                    b.Property<string>("ContractType")
                        .HasColumnType("nvarchar(max)");

                    b.Property<DateTime>("DeliveryDate")
                        .HasColumnType("datetime2");

                    b.Property<int>("EqualQuantityPrecision")
                        .HasColumnType("int");

                    b.Property<double>("LiquidationFee")
                        .HasColumnType("float");

                    b.Property<DateTime>("ListingDate")
                        .HasColumnType("datetime2");

                    b.Property<double>("MaintMarginPercent")
                        .HasColumnType("float");

                    b.Property<string>("MarginAsset")
                        .HasColumnType("nvarchar(max)");

                    b.Property<double>("MarketTakeBound")
                        .HasColumnType("float");

                    b.Property<string>("OrderTypes")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("Pair")
                        .HasColumnType("nvarchar(max)");

                    b.Property<int>("PricePrecision")
                        .HasColumnType("int");

                    b.Property<int>("QuantityPrecision")
                        .HasColumnType("int");

                    b.Property<string>("QuoteAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("QuoteAssetPrecision")
                        .HasColumnType("int");

                    b.Property<double>("RequiredMarginPercent")
                        .HasColumnType("float");

                    b.Property<string>("Status")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("TimeInForce")
                        .HasColumnType("nvarchar(max)");

                    b.Property<double>("TriggerProtect")
                        .HasColumnType("float");

                    b.Property<string>("UnderlyingSubType")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("UnderlyingType")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.HasKey("Name");

                    b.HasIndex("BaseAsset");

                    b.HasIndex("QuoteAsset");

                    b.HasIndex("BaseAsset", "QuoteAsset");

                    b.ToTable("BinanceFuturesCoinSymbolInfos");
                });

            modelBuilder.Entity("CollectorModels.Models.BinanceFuturesUsdtSymbolInfo", b =>
                {
                    b.Property<string>("Name")
                        .HasColumnType("nvarchar(450)");

                    b.Property<string>("BaseAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("BaseAssetPrecision")
                        .HasColumnType("int");

                    b.Property<string>("ContractType")
                        .HasColumnType("nvarchar(max)");

                    b.Property<DateTime>("DeliveryDate")
                        .HasColumnType("datetime2");

                    b.Property<double>("LiquidationFee")
                        .HasColumnType("float");

                    b.Property<DateTime>("ListingDate")
                        .HasColumnType("datetime2");

                    b.Property<double>("MaintMarginPercent")
                        .HasColumnType("float");

                    b.Property<string>("MarginAsset")
                        .HasColumnType("nvarchar(max)");

                    b.Property<double>("MarketTakeBound")
                        .HasColumnType("float");

                    b.Property<string>("OrderTypes")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("Pair")
                        .HasColumnType("nvarchar(max)");

                    b.Property<int>("PricePrecision")
                        .HasColumnType("int");

                    b.Property<int>("QuantityPrecision")
                        .HasColumnType("int");

                    b.Property<string>("QuoteAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("QuoteAssetPrecision")
                        .HasColumnType("int");

                    b.Property<double>("RequiredMarginPercent")
                        .HasColumnType("float");

                    b.Property<double>("SettlePlan")
                        .HasColumnType("float");

                    b.Property<string>("Status")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("TimeInForce")
                        .HasColumnType("nvarchar(max)");

                    b.Property<double>("TriggerProtect")
                        .HasColumnType("float");

                    b.Property<string>("UnderlyingSubType")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("UnderlyingType")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.HasKey("Name");

                    b.HasIndex("BaseAsset");

                    b.HasIndex("QuoteAsset");

                    b.HasIndex("BaseAsset", "QuoteAsset");

                    b.ToTable("BinanceFuturesUsdtSymbolInfos");
                });

            modelBuilder.Entity("CollectorModels.Models.BinanceSymbolInfo", b =>
                {
                    b.Property<string>("Name")
                        .HasColumnType("nvarchar(450)");

                    b.Property<bool>("AllowTrailingStop")
                        .HasColumnType("bit");

                    b.Property<string>("BaseAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("BaseAssetPrecision")
                        .HasColumnType("int");

                    b.Property<int>("BaseFeePrecision")
                        .HasColumnType("int");

                    b.Property<bool>("CancelReplaceAllowed")
                        .HasColumnType("bit");

                    b.Property<bool>("IceBergAllowed")
                        .HasColumnType("bit");

                    b.Property<bool>("IsMarginTradingAllowed")
                        .HasColumnType("bit");

                    b.Property<bool>("IsSpotTradingAllowed")
                        .HasColumnType("bit");

                    b.Property<bool>("OCOAllowed")
                        .HasColumnType("bit");

                    b.Property<string>("OrderTypes")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("Permissions")
                        .HasColumnType("nvarchar(max)");

                    b.Property<string>("QuoteAsset")
                        .HasColumnType("nvarchar(450)");

                    b.Property<int>("QuoteAssetPrecision")
                        .HasColumnType("int");

                    b.Property<int>("QuoteFeePrecision")
                        .HasColumnType("int");

                    b.Property<bool>("QuoteOrderQuantityMarketAllowed")
                        .HasColumnType("bit");

                    b.Property<string>("Status")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.HasKey("Name");

                    b.HasIndex("BaseAsset");

                    b.HasIndex("QuoteAsset");

                    b.HasIndex("BaseAsset", "QuoteAsset");

                    b.ToTable("BinanceSymbolInfos");
                });

            modelBuilder.Entity("CollectorModels.Models.FuturesCoinBinanceKline", b =>
                {
                    b.Property<string>("Id")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("ClosePrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("CloseTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("HighPrice")
                        .HasColumnType("float");

                    b.Property<string>("Interval")
                        .IsRequired()
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("LowPrice")
                        .HasColumnType("float");

                    b.Property<double>("OpenPrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("OpenTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("QuoteVolume")
                        .HasColumnType("float");

                    b.Property<string>("SymbolInfoId")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("TakerBuyBaseVolume")
                        .HasColumnType("float");

                    b.Property<double>("TakerBuyQuoteVolume")
                        .HasColumnType("float");

                    b.Property<int>("TradeCount")
                        .HasColumnType("int");

                    b.Property<double>("Volume")
                        .HasColumnType("float");

                    b.HasKey("Id");

                    b.HasIndex("CloseTime");

                    b.HasIndex("Interval");

                    b.HasIndex("OpenTime");

                    b.HasIndex("SymbolInfoId");

                    b.HasIndex("CloseTime", "Interval");

                    b.HasIndex("OpenTime", "Interval");

                    b.ToTable("FuturesCoinBinanceKlines");
                });

            modelBuilder.Entity("CollectorModels.Models.FuturesUsdtBinanceKline", b =>
                {
                    b.Property<string>("Id")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("ClosePrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("CloseTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("HighPrice")
                        .HasColumnType("float");

                    b.Property<string>("Interval")
                        .IsRequired()
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("LowPrice")
                        .HasColumnType("float");

                    b.Property<double>("OpenPrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("OpenTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("QuoteVolume")
                        .HasColumnType("float");

                    b.Property<string>("SymbolInfoId")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("TakerBuyBaseVolume")
                        .HasColumnType("float");

                    b.Property<double>("TakerBuyQuoteVolume")
                        .HasColumnType("float");

                    b.Property<int>("TradeCount")
                        .HasColumnType("int");

                    b.Property<double>("Volume")
                        .HasColumnType("float");

                    b.HasKey("Id");

                    b.HasIndex("CloseTime");

                    b.HasIndex("Interval");

                    b.HasIndex("OpenTime");

                    b.HasIndex("SymbolInfoId");

                    b.HasIndex("CloseTime", "Interval");

                    b.HasIndex("OpenTime", "Interval");

                    b.HasIndex("CloseTime", "Interval", "ClosePrice");

                    b.HasIndex("CloseTime", "Interval", "HighPrice");

                    b.HasIndex("CloseTime", "Interval", "LowPrice");

                    b.HasIndex("CloseTime", "Interval", "OpenPrice");

                    b.HasIndex("CloseTime", "Interval", "QuoteVolume");

                    b.HasIndex("CloseTime", "Interval", "Volume");

                    b.HasIndex("OpenTime", "Interval", "ClosePrice");

                    b.HasIndex("OpenTime", "Interval", "HighPrice");

                    b.HasIndex("OpenTime", "Interval", "LowPrice");

                    b.HasIndex("OpenTime", "Interval", "OpenPrice");

                    b.HasIndex("OpenTime", "Interval", "QuoteVolume");

                    b.HasIndex("OpenTime", "Interval", "Volume");

                    b.ToTable("FuturesUsdtBinanceKlines");
                });

            modelBuilder.Entity("CollectorModels.Models.SpotBinanceKline", b =>
                {
                    b.Property<string>("Id")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("ClosePrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("CloseTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("HighPrice")
                        .HasColumnType("float");

                    b.Property<string>("Interval")
                        .IsRequired()
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("LowPrice")
                        .HasColumnType("float");

                    b.Property<double>("OpenPrice")
                        .HasColumnType("float");

                    b.Property<DateTime>("OpenTime")
                        .HasColumnType("datetime2");

                    b.Property<double>("QuoteVolume")
                        .HasColumnType("float");

                    b.Property<string>("SymbolInfoId")
                        .HasColumnType("nvarchar(450)");

                    b.Property<double>("TakerBuyBaseVolume")
                        .HasColumnType("float");

                    b.Property<double>("TakerBuyQuoteVolume")
                        .HasColumnType("float");

                    b.Property<int>("TradeCount")
                        .HasColumnType("int");

                    b.Property<double>("Volume")
                        .HasColumnType("float");

                    b.HasKey("Id");

                    b.HasIndex("CloseTime");

                    b.HasIndex("Interval");

                    b.HasIndex("OpenTime");

                    b.HasIndex("SymbolInfoId");

                    b.HasIndex("CloseTime", "Interval");

                    b.HasIndex("OpenTime", "Interval");

                    b.ToTable("SpotBinanceKlines");
                });

            modelBuilder.Entity("CollectorModels.Models.FuturesCoinBinanceKline", b =>
                {
                    b.HasOne("CollectorModels.Models.BinanceFuturesCoinSymbolInfo", "SymbolInfo")
                        .WithMany("BinanceKlines")
                        .HasForeignKey("SymbolInfoId")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.Navigation("SymbolInfo");
                });

            modelBuilder.Entity("CollectorModels.Models.FuturesUsdtBinanceKline", b =>
                {
                    b.HasOne("CollectorModels.Models.BinanceFuturesUsdtSymbolInfo", "SymbolInfo")
                        .WithMany("BinanceKlines")
                        .HasForeignKey("SymbolInfoId")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.Navigation("SymbolInfo");
                });

            modelBuilder.Entity("CollectorModels.Models.SpotBinanceKline", b =>
                {
                    b.HasOne("CollectorModels.Models.BinanceSymbolInfo", "SymbolInfo")
                        .WithMany("BinanceKlines")
                        .HasForeignKey("SymbolInfoId")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.Navigation("SymbolInfo");
                });

            modelBuilder.Entity("CollectorModels.Models.BinanceFuturesCoinSymbolInfo", b =>
                {
                    b.Navigation("BinanceKlines");
                });

            modelBuilder.Entity("CollectorModels.Models.BinanceFuturesUsdtSymbolInfo", b =>
                {
                    b.Navigation("BinanceKlines");
                });

            modelBuilder.Entity("CollectorModels.Models.BinanceSymbolInfo", b =>
                {
                    b.Navigation("BinanceKlines");
                });
#pragma warning restore 612, 618
        }
    }
}
