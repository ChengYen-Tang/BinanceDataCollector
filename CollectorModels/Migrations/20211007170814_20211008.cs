using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CollectorModels.Migrations
{
    public partial class _20211008 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Coins",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Coins", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "CoinData",
                columns: table => new
                {
                    Key = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Date = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Open = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    High = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    Low = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    Close = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    Volume = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    Money = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    TradeCount = table.Column<int>(type: "int", nullable: false),
                    TakerBuyBaseVolume = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    TakerBuyQuoteVolume = table.Column<decimal>(type: "decimal(38,19)", nullable: false),
                    CoinId = table.Column<Guid>(type: "uniqueidentifier", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoinData", x => x.Key);
                    table.ForeignKey(
                        name: "FK_CoinData_Coins_CoinId",
                        column: x => x.CoinId,
                        principalTable: "Coins",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_CoinData_CoinId",
                table: "CoinData",
                column: "CoinId");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "CoinData");

            migrationBuilder.DropTable(
                name: "Coins");
        }
    }
}
