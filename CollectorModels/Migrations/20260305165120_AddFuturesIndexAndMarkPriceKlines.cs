using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class AddFuturesIndexAndMarkPriceKlines : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "FuturesCoinBinanceIndexPriceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinBinanceIndexPriceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinBinanceIndexPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinBinanceMarkPriceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinBinanceMarkPriceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinBinanceMarkPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtBinanceIndexPriceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtBinanceIndexPriceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtBinanceIndexPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtBinanceMarkPriceKlines",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    OpenTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    OpenPrice = table.Column<double>(type: "float", nullable: false),
                    HighPrice = table.Column<double>(type: "float", nullable: false),
                    LowPrice = table.Column<double>(type: "float", nullable: false),
                    ClosePrice = table.Column<double>(type: "float", nullable: false),
                    CloseTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Interval = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtBinanceMarkPriceKlines", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtBinanceMarkPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_CloseTime",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_CloseTime_Interval",
                table: "FuturesCoinBinanceIndexPriceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_Interval",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_OpenTime",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_OpenTime_Interval",
                table: "FuturesCoinBinanceIndexPriceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceIndexPriceKlines_SymbolInfoId",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_CloseTime",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_CloseTime_Interval",
                table: "FuturesCoinBinanceMarkPriceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_Interval",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_OpenTime",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_OpenTime_Interval",
                table: "FuturesCoinBinanceMarkPriceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBinanceMarkPriceKlines_SymbolInfoId",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_CloseTime",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_CloseTime_Interval",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_Interval",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_OpenTime",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_OpenTime_Interval",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceIndexPriceKlines_SymbolInfoId",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_CloseTime",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "CloseTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_CloseTime_Interval",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                columns: new[] { "CloseTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_Interval",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "Interval");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_OpenTime",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "OpenTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_OpenTime_Interval",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                columns: new[] { "OpenTime", "Interval" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBinanceMarkPriceKlines_SymbolInfoId",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "SymbolInfoId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinBinanceIndexPriceKlines");

            migrationBuilder.DropTable(
                name: "FuturesCoinBinanceMarkPriceKlines");

            migrationBuilder.DropTable(
                name: "FuturesUsdtBinanceIndexPriceKlines");

            migrationBuilder.DropTable(
                name: "FuturesUsdtBinanceMarkPriceKlines");
        }
    }
}
