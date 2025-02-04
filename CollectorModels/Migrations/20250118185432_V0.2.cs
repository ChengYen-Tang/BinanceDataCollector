using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class V02 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "QuoteVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TakerBuyBaseVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TakerBuyQuoteVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TradeCount",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "Volume",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "QuoteVolume",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TakerBuyBaseVolume",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TakerBuyQuoteVolume",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "TradeCount",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropColumn(
                name: "Volume",
                table: "FuturesCoinBinancePremiumIndexKlines");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<double>(
                name: "QuoteVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "TakerBuyBaseVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "TakerBuyQuoteVolume",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<int>(
                name: "TradeCount",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                type: "int",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<double>(
                name: "Volume",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "QuoteVolume",
                table: "FuturesCoinBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "TakerBuyBaseVolume",
                table: "FuturesCoinBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "TakerBuyQuoteVolume",
                table: "FuturesCoinBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<int>(
                name: "TradeCount",
                table: "FuturesCoinBinancePremiumIndexKlines",
                type: "int",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<double>(
                name: "Volume",
                table: "FuturesCoinBinancePremiumIndexKlines",
                type: "float",
                nullable: false,
                defaultValue: 0.0);
        }
    }
}
