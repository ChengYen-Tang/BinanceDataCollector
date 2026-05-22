using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class AddFuturesTakerLongShortRatios : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "FuturesCoinTakerLongShortRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    BuySellRatio = table.Column<double>(type: "float", nullable: true),
                    BuyVolume = table.Column<double>(type: "float", nullable: false),
                    SellVolume = table.Column<double>(type: "float", nullable: false),
                    BuyVolumeValue = table.Column<double>(type: "float", nullable: true),
                    SellVolumeValue = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinTakerLongShortRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinTakerLongShortRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtTakerLongShortRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    BuySellRatio = table.Column<double>(type: "float", nullable: true),
                    BuyVolume = table.Column<double>(type: "float", nullable: false),
                    SellVolume = table.Column<double>(type: "float", nullable: false),
                    BuyVolumeValue = table.Column<double>(type: "float", nullable: true),
                    SellVolumeValue = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtTakerLongShortRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtTakerLongShortRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTakerLongShortRatios_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTakerLongShortRatios_Timestamp",
                table: "FuturesCoinTakerLongShortRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTakerLongShortRatios_Timestamp_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTakerLongShortRatios_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTakerLongShortRatios_Timestamp",
                table: "FuturesUsdtTakerLongShortRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTakerLongShortRatios_Timestamp_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinTakerLongShortRatios");

            migrationBuilder.DropTable(
                name: "FuturesUsdtTakerLongShortRatios");
        }
    }
}
