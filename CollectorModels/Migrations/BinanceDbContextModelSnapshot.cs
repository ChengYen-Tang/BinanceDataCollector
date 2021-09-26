﻿// <auto-generated />
using System;
using CollectorModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CollectorModels.Migrations
{
    [DbContext(typeof(BinanceDbContext))]
    partial class BinanceDbContextModelSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("Relational:MaxIdentifierLength", 128)
                .HasAnnotation("ProductVersion", "5.0.10")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("CollectorModels.CoinDataModel", b =>
                {
                    b.Property<string>("Key")
                        .HasColumnType("nvarchar(450)");

                    b.Property<decimal>("Close")
                        .HasColumnType("decimal(18,2)");

                    b.Property<Guid>("CoinId")
                        .HasColumnType("uniqueidentifier");

                    b.Property<DateTime>("Date")
                        .HasColumnType("datetime2");

                    b.Property<decimal>("High")
                        .HasColumnType("decimal(18,2)");

                    b.Property<decimal>("Low")
                        .HasColumnType("decimal(18,2)");

                    b.Property<decimal>("Money")
                        .HasColumnType("decimal(18,2)");

                    b.Property<decimal>("Open")
                        .HasColumnType("decimal(18,2)");

                    b.Property<decimal>("TakerBuyBaseVolume")
                        .HasColumnType("decimal(18,2)");

                    b.Property<decimal>("TakerBuyQuoteVolume")
                        .HasColumnType("decimal(18,2)");

                    b.Property<int>("TradeCount")
                        .HasColumnType("int");

                    b.Property<decimal>("Volume")
                        .HasColumnType("decimal(18,2)");

                    b.HasKey("Key");

                    b.HasIndex("CoinId");

                    b.ToTable("CoinData");
                });

            modelBuilder.Entity("CollectorModels.CoinModel", b =>
                {
                    b.Property<Guid>("Id")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("uniqueidentifier");

                    b.Property<string>("Name")
                        .IsRequired()
                        .HasColumnType("nvarchar(max)");

                    b.HasKey("Id");

                    b.ToTable("Coins");
                });

            modelBuilder.Entity("CollectorModels.CoinDataModel", b =>
                {
                    b.HasOne("CollectorModels.CoinModel", "Coin")
                        .WithMany("CoinData")
                        .HasForeignKey("CoinId")
                        .OnDelete(DeleteBehavior.Cascade)
                        .IsRequired();

                    b.Navigation("Coin");
                });

            modelBuilder.Entity("CollectorModels.CoinModel", b =>
                {
                    b.Navigation("CoinData");
                });
#pragma warning restore 612, 618
        }
    }
}