using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class AddFuturesBasis : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "FuturesCoinBasisHistories",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    FuturesPrice = table.Column<double>(type: "float", nullable: false),
                    IndexPrice = table.Column<double>(type: "float", nullable: false),
                    BasisValue = table.Column<double>(type: "float", nullable: false),
                    BasisRate = table.Column<double>(type: "float", nullable: false),
                    AnnualizedBasisRate = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinBasisHistories", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinBasisHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtBasisHistories",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    FuturesPrice = table.Column<double>(type: "float", nullable: false),
                    IndexPrice = table.Column<double>(type: "float", nullable: false),
                    BasisValue = table.Column<double>(type: "float", nullable: false),
                    BasisRate = table.Column<double>(type: "float", nullable: false),
                    AnnualizedBasisRate = table.Column<double>(type: "float", nullable: true),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtBasisHistories", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtBasisHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBasisHistories_SymbolInfoId",
                table: "FuturesCoinBasisHistories",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBasisHistories_Timestamp",
                table: "FuturesCoinBasisHistories",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinBasisHistories_Timestamp_SymbolInfoId",
                table: "FuturesCoinBasisHistories",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBasisHistories_SymbolInfoId",
                table: "FuturesUsdtBasisHistories",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBasisHistories_Timestamp",
                table: "FuturesUsdtBasisHistories",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtBasisHistories_Timestamp_SymbolInfoId",
                table: "FuturesUsdtBasisHistories",
                columns: new[] { "Timestamp", "SymbolInfoId" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinBasisHistories");

            migrationBuilder.DropTable(
                name: "FuturesUsdtBasisHistories");
        }
    }
}
