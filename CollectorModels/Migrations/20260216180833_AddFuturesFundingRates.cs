using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class AddFuturesFundingRates : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "FuturesCoinFundingRates",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    FundingTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    FundingRate = table.Column<double>(type: "float", nullable: false),
                    MarkPrice = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinFundingRates", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinFundingRates_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtFundingRates",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    FundingTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    FundingRate = table.Column<double>(type: "float", nullable: false),
                    MarkPrice = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtFundingRates", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtFundingRates_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinFundingRates_FundingTime",
                table: "FuturesCoinFundingRates",
                column: "FundingTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinFundingRates_FundingTime_SymbolInfoId",
                table: "FuturesCoinFundingRates",
                columns: new[] { "FundingTime", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinFundingRates_SymbolInfoId",
                table: "FuturesCoinFundingRates",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtFundingRates_FundingTime",
                table: "FuturesUsdtFundingRates",
                column: "FundingTime");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtFundingRates_FundingTime_SymbolInfoId",
                table: "FuturesUsdtFundingRates",
                columns: new[] { "FundingTime", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtFundingRates_SymbolInfoId",
                table: "FuturesUsdtFundingRates",
                column: "SymbolInfoId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinFundingRates");

            migrationBuilder.DropTable(
                name: "FuturesUsdtFundingRates");
        }
    }
}
