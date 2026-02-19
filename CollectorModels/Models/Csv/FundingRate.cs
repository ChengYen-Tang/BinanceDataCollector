namespace CollectorModels.Models.Csv;

public class FundingRate
{
    /// <summary>
    /// The time the funding rate is applied
    /// </summary>
    public long FundingTime { get; set; }

    /// <summary>
    /// The funding rate for the given symbol and time
    /// </summary>
    public double Rate { get; set; }

    /// <summary>
    /// The mark price
    /// </summary>
    public double? MarkPrice { get; set; }
}
