using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CollectorModels.Migrations
{
    /// <inheritdoc />
    public partial class RemoveFuturesSymbolCascadeDelete : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBasisHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBasisHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceIndexPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceIndexPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceMarkPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceMarkPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinancePremiumIndexKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinFundingRates_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinFundingRates");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinGlobalLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinOpenInterestHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTakerLongShortRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTopLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTopLongShortPositionRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBasisHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBasisHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceIndexPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceIndexPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceMarkPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceMarkPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinancePremiumIndexKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtFundingRates_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtFundingRates");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtGlobalLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtOpenInterestHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTakerLongShortRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTopLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTopLongShortPositionRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBasisHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBasisHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceIndexPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceMarkPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinancePremiumIndexKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinFundingRates_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinFundingRates",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinGlobalLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinOpenInterestHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTakerLongShortRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTopLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTopLongShortPositionRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBasisHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBasisHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceIndexPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceMarkPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinancePremiumIndexKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtFundingRates_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtFundingRates",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtGlobalLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtOpenInterestHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTakerLongShortRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTopLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTopLongShortPositionRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBasisHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBasisHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceIndexPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceIndexPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinanceMarkPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceMarkPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinBinancePremiumIndexKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinancePremiumIndexKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinFundingRates_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinFundingRates");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinGlobalLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinOpenInterestHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTakerLongShortRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTopLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesCoinTopLongShortPositionRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBasisHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBasisHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceIndexPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceIndexPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinanceMarkPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceMarkPriceKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtBinancePremiumIndexKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinancePremiumIndexKlines");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtFundingRates_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtFundingRates");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtGlobalLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtOpenInterestHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTakerLongShortRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTopLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios");

            migrationBuilder.DropForeignKey(
                name: "FK_FuturesUsdtTopLongShortPositionRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios");

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBasisHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBasisHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceIndexPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceIndexPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinanceMarkPriceKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinanceMarkPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinBinancePremiumIndexKlines_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinBinancePremiumIndexKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinFundingRates_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinFundingRates",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinGlobalLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinGlobalLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinOpenInterestHistories_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinOpenInterestHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTakerLongShortRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTakerLongShortRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTopLongShortAccountRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesCoinTopLongShortPositionRatios_BinanceFuturesCoinSymbolInfos_SymbolInfoId",
                table: "FuturesCoinTopLongShortPositionRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesCoinSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBasisHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBasisHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceIndexPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceIndexPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinanceMarkPriceKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinanceMarkPriceKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtBinancePremiumIndexKlines_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtBinancePremiumIndexKlines",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtFundingRates_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtFundingRates",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtGlobalLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtGlobalLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtOpenInterestHistories_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtOpenInterestHistories",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTakerLongShortRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTakerLongShortRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTopLongShortAccountRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortAccountRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_FuturesUsdtTopLongShortPositionRatios_BinanceFuturesUsdtSymbolInfos_SymbolInfoId",
                table: "FuturesUsdtTopLongShortPositionRatios",
                column: "SymbolInfoId",
                principalTable: "BinanceFuturesUsdtSymbolInfos",
                principalColumn: "Name",
                onDelete: ReferentialAction.Cascade);
        }
    }
}
