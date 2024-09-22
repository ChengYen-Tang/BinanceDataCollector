using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "BinanceFuturesCoinSymbolInfos",
                columns: table => new
                {
                    Name = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    ContractSize = table.Column<int>(type: "int", nullable: false),
                    EqualQuantityPrecision = table.Column<int>(type: "int", nullable: false),
                    ContractType = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    MaintMarginPercent = table.Column<double>(type: "float", nullable: false),
                    PricePrecision = table.Column<int>(type: "int", nullable: false),
                    QuantityPrecision = table.Column<int>(type: "int", nullable: false),
                    RequiredMarginPercent = table.Column<double>(type: "float", nullable: false),
                    BaseAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    MarginAsset = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    QuoteAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    BaseAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    QuoteAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    OrderTypes = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    Pair = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    DeliveryDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    ListingDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    TriggerProtect = table.Column<double>(type: "float", nullable: false),
                    UnderlyingType = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UnderlyingSubType = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    LiquidationFee = table.Column<double>(type: "float", nullable: false),
                    MarketTakeBound = table.Column<double>(type: "float", nullable: false),
                    TimeInForce = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    Status = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BinanceFuturesCoinSymbolInfos", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "BinanceFuturesUsdtSymbolInfos",
                columns: table => new
                {
                    Name = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SettlePlan = table.Column<double>(type: "float", nullable: false),
                    ContractType = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    MaintMarginPercent = table.Column<double>(type: "float", nullable: false),
                    PricePrecision = table.Column<int>(type: "int", nullable: false),
                    QuantityPrecision = table.Column<int>(type: "int", nullable: false),
                    RequiredMarginPercent = table.Column<double>(type: "float", nullable: false),
                    BaseAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    MarginAsset = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    QuoteAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    BaseAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    QuoteAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    OrderTypes = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    Pair = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    DeliveryDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    ListingDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    TriggerProtect = table.Column<double>(type: "float", nullable: false),
                    UnderlyingType = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UnderlyingSubType = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    LiquidationFee = table.Column<double>(type: "float", nullable: false),
                    MarketTakeBound = table.Column<double>(type: "float", nullable: false),
                    TimeInForce = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    Status = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BinanceFuturesUsdtSymbolInfos", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "BinanceSymbolInfos",
                columns: table => new
                {
                    Name = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Status = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    BaseAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    BaseAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    QuoteAsset = table.Column<string>(type: "nvarchar(450)", nullable: true),
                    QuoteAssetPrecision = table.Column<int>(type: "int", nullable: false),
                    OrderTypes = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    IcebergAllowed = table.Column<bool>(type: "bit", nullable: false),
                    CancelReplaceAllowed = table.Column<bool>(type: "bit", nullable: false),
                    IsSpotTradingAllowed = table.Column<bool>(type: "bit", nullable: false),
                    AllowTrailingStop = table.Column<bool>(type: "bit", nullable: false),
                    IsMarginTradingAllowed = table.Column<bool>(type: "bit", nullable: false),
                    OCOAllowed = table.Column<bool>(type: "bit", nullable: false),
                    QuoteOrderQuantityMarketAllowed = table.Column<bool>(type: "bit", nullable: false),
                    BaseFeePrecision = table.Column<int>(type: "int", nullable: false),
                    QuoteFeePrecision = table.Column<int>(type: "int", nullable: false),
                    Permissions = table.Column<string>(type: "nvarchar(max)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BinanceSymbolInfos", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinBinanceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    Volume = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    QuoteVolume = table.Column<double>(type: "float", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<double>(type: "float", nullable: false),
                    TakerBuyQuoteVolume = table.Column<double>(type: "float", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinBinanceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinBinanceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinBinancePremiumIndexKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    Volume = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    QuoteVolume = table.Column<double>(type: "float", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<double>(type: "float", nullable: false),
                    TakerBuyQuoteVolume = table.Column<double>(type: "float", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinBinancePremiumIndexKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinBinancePremiumIndexKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtBinanceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    Volume = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    QuoteVolume = table.Column<double>(type: "float", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<double>(type: "float", nullable: false),
                    TakerBuyQuoteVolume = table.Column<double>(type: "float", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtBinanceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtBinanceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtBinancePremiumIndexKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    Volume = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    QuoteVolume = table.Column<double>(type: "float", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<double>(type: "float", nullable: false),
                    TakerBuyQuoteVolume = table.Column<double>(type: "float", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtBinancePremiumIndexKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtBinancePremiumIndexKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SpotBinanceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    Volume = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    QuoteVolume = table.Column<double>(type: "float", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<double>(type: "float", nullable: false),
                    TakerBuyQuoteVolume = table.Column<double>(type: "float", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SpotBinanceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SpotBinanceKlines_BinanceSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesCoinSymbolInfos_BaseAsset",
                table: "BinanceFuturesCoinSymbolInfos",
                column: "BaseAsset");

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesCoinSymbolInfos_BaseAsset_QuoteAsset",
                table: "BinanceFuturesCoinSymbolInfos",
                columns: new[] { "BaseAsset", "QuoteAsset" });

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesCoinSymbolInfos_QuoteAsset",
                table: "BinanceFuturesCoinSymbolInfos",
                column: "QuoteAsset");

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesUsdtSymbolInfos_BaseAsset",
                table: "BinanceFuturesUsdtSymbolInfos",
                column: "BaseAsset");

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesUsdtSymbolInfos_BaseAsset_QuoteAsset",
                table: "BinanceFuturesUsdtSymbolInfos",
                columns: new[] { "BaseAsset", "QuoteAsset" });

            migrationBuilder.CreateIndex(
                name: "IX_BinanceFuturesUsdtSymbolInfos_QuoteAsset",
                table: "BinanceFuturesUsdtSymbolInfos",
                column: "QuoteAsset");

            migrationBuilder.CreateIndex(
                name: "IX_BinanceSymbolInfos_BaseAsset",
                table: "BinanceSymbolInfos",
                column: "BaseAsset");

            migrationBuilder.CreateIndex(
                name: "IX_BinanceSymbolInfos_BaseAsset_QuoteAsset",
                table: "BinanceSymbolInfos",
                columns: new[] { "BaseAsset", "QuoteAsset" });

            migrationBuilder.CreateIndex(
                name: "IX_BinanceSymbolInfos_QuoteAsset",
                table: "BinanceSymbolInfos",
                column: "QuoteAsset");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_CloseTime",
                table: "FuturesCoinBinanceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_CloseTime_Interval",
                table: "FuturesCoinBinanceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_Interval",
                table: "FuturesCoinBinanceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_OpenTime",
                table: "FuturesCoinBinanceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_OpenTime_Interval",
                table: "FuturesCoinBinanceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceKlines_SymbolInfoId",
                table: "FuturesCoinBinanceKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_CloseTime",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_CloseTime_Interval",
                table: "FuturesCoinBinancePremiumIndexKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_Interval",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_OpenTime",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_OpenTime_Interval",
                table: "FuturesCoinBinancePremiumIndexKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinancePremiumIndexKlines_SymbolInfoId",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_CloseTime",
                table: "FuturesUsdtBinanceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_CloseTime_Interval",
                table: "FuturesUsdtBinanceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_Interval",
                table: "FuturesUsdtBinanceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_OpenTime",
                table: "FuturesUsdtBinanceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_OpenTime_Interval",
                table: "FuturesUsdtBinanceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceKlines_SymbolInfoId",
                table: "FuturesUsdtBinanceKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_CloseTime",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_CloseTime_Interval",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_Interval",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_OpenTime",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_OpenTime_Interval",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinancePremiumIndexKlines_SymbolInfoId",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_CloseTime",
                table: "SpotBinanceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_CloseTime_Interval",
                table: "SpotBinanceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_Interval",
                table: "SpotBinanceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_OpenTime",
                table: "SpotBinanceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_OpenTime_Interval",
                table: "SpotBinanceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_SpotBinanceKlines_SymbolInfoId",
                table: "SpotBinanceKlines",
                column: "SymbolInfoId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinBinanceKlines");

            migrationBuilder.DropTable(
                name: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropTable(
                name: "FuturesUsdtBinanceKlines");

            migrationBuilder.DropTable(
                name: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropTable(
                name: "SpotBinanceKlines");

            migrationBuilder.DropTable(
                name: "BinanceFuturesCoinSymbolInfos");

            migrationBuilder.DropTable(
                name: "BinanceFuturesUsdtSymbolInfos");

            migrationBuilder.DropTable(
                name: "BinanceSymbolInfos");
        }
    }
}
