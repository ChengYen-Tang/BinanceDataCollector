namespace CollectorModels.Models.Storage;

public class SymbolInfoCsv
{
    public string Name { get; set; }

    public string Status { get; set; }

    public string BaseAsset { get; set; }

    public string QuoteAsset { get; set; }

    public string MarginAsset { get; set; }

    public string Pair { get; set; }

    public int BaseAssetPrecision { get; set; }

    public int QuoteAssetPrecision { get; set; }

    public int? BaseFeePrecision { get; set; }

    public int? QuoteFeePrecision { get; set; }

    public int? PricePrecision { get; set; }

    public int? QuantityPrecision { get; set; }

    public string ContractType { get; set; }

    public string UnderlyingType { get; set; }

    public string UnderlyingSubType { get; set; }

    public string OrderTypes { get; set; }

    public string Permissions { get; set; }

    public string TimeInForce { get; set; }

    public bool? IcebergAllowed { get; set; }

    public bool? CancelReplaceAllowed { get; set; }

    public bool? IsSpotTradingAllowed { get; set; }

    public bool? AllowTrailingStop { get; set; }

    public bool? IsMarginTradingAllowed { get; set; }

    public bool? OCOAllowed { get; set; }

    public bool? QuoteOrderQuantityMarketAllowed { get; set; }

    public double? MaintMarginPercent { get; set; }

    public double? RequiredMarginPercent { get; set; }

    public double? TriggerProtect { get; set; }

    public double? LiquidationFee { get; set; }

    public double? MarketTakeBound { get; set; }

    public long? ListingDate { get; set; }

    public long? DeliveryDate { get; set; }
}
