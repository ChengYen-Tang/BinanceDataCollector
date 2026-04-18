using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class AddFuturesLongShortRatios : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "FuturesCoinGlobalLongShortAccountRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinGlobalLongShortAccountRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinGlobalLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinOpenInterestHistories",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    SumOpenInterest = table.Column<double>(type: "float", nullable: false),
                    SumOpenInterestValue = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinOpenInterestHistories", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinOpenInterestHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinTopLongShortAccountRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinTopLongShortAccountRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinTopLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesCoinTopLongShortPositionRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesCoinTopLongShortPositionRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesCoinTopLongShortPositionRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesCoinSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtGlobalLongShortAccountRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtGlobalLongShortAccountRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtGlobalLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtOpenInterestHistories",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    SumOpenInterest = table.Column<double>(type: "float", nullable: false),
                    SumOpenInterestValue = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtOpenInterestHistories", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtOpenInterestHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtTopLongShortAccountRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtTopLongShortAccountRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtTopLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "FuturesUsdtTopLongShortPositionRatios",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Timestamp = table.Column<DateTime>(type: "datetime2", nullable: false),
                    LongShortRatio = table.Column<double>(type: "float", nullable: false),
                    LongAccount = table.Column<double>(type: "float", nullable: false),
                    ShortAccount = table.Column<double>(type: "float", nullable: false),
                    SymbolInfoId = table.Column<string>(type: "nvarchar(450)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FuturesUsdtTopLongShortPositionRatios", x => x.Id);
                    table.ForeignKey(
                        name: "FK_FuturesUsdtTopLongShortPositionRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                        column: x => x.SymbolInfoId,
                        principalTable: "BinanceFuturesUsdtSymbolInfos",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinGlobalLongShortAccountRatios_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinGlobalLongShortAccountRatios_Timestamp",
                table: "FuturesCoinGlobalLongShortAccountRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinGlobalLongShortAccountRatios_Timestamp_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinOpenInterestHistories_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinOpenInterestHistories_Timestamp",
                table: "FuturesCoinOpenInterestHistories",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinOpenInterestHistories_Timestamp_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortAccountRatios_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortAccountRatios_Timestamp",
                table: "FuturesCoinTopLongShortAccountRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortAccountRatios_Timestamp_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortPositionRatios_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortPositionRatios_Timestamp",
                table: "FuturesCoinTopLongShortPositionRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesCoinTopLongShortPositionRatios_Timestamp_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtGlobalLongShortAccountRatios_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtGlobalLongShortAccountRatios_Timestamp",
                table: "FuturesUsdtGlobalLongShortAccountRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtGlobalLongShortAccountRatios_Timestamp_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtOpenInterestHistories_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtOpenInterestHistories_Timestamp",
                table: "FuturesUsdtOpenInterestHistories",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtOpenInterestHistories_Timestamp_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortAccountRatios_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortAccountRatios_Timestamp",
                table: "FuturesUsdtTopLongShortAccountRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortAccountRatios_Timestamp_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortPositionRatios_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios",
                column: "SymbolInfoId");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortPositionRatios_Timestamp",
                table: "FuturesUsdtTopLongShortPositionRatios",
                column: "Timestamp");

            migrationBuilder.CreateIndex(
                name: "IX_FuturesUsdtTopLongShortPositionRatios_Timestamp_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios",
                columns: new[] { "Timestamp", "SymbolInfoId" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "FuturesCoinGlobalLongShortAccountRatios");

            migrationBuilder.DropTable(
                name: "FuturesCoinOpenInterestHistories");

            migrationBuilder.DropTable(
                name: "FuturesCoinTopLongShortAccountRatios");

            migrationBuilder.DropTable(
                name: "FuturesCoinTopLongShortPositionRatios");

            migrationBuilder.DropTable(
                name: "FuturesUsdtGlobalLongShortAccountRatios");

            migrationBuilder.DropTable(
                name: "FuturesUsdtOpenInterestHistories");

            migrationBuilder.DropTable(
                name: "FuturesUsdtTopLongShortAccountRatios");

            migrationBuilder.DropTable(
                name: "FuturesUsdtTopLongShortPositionRatios");
        }
    }
}
