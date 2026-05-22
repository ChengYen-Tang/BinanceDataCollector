namespace CollectorModels.Models.Csv;

public class TakerLongShortRatioCsv
{
    public long Timestamp { get; set; }

    public double? BuySellRatio { get; set; }

    public double BuyVolume { get; set; }

    public double SellVolume { get; set; }

    public double? BuyVolumeValue { get; set; }

    public double? SellVolumeValue { get; set; }
}
