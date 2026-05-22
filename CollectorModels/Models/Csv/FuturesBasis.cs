namespace CollectorModels.Models.Csv;

public class FuturesBasisCsv
{
    public long Timestamp { get; set; }

    public double FuturesPrice { get; set; }

    public double IndexPrice { get; set; }

    public double BasisValue { get; set; }

    public double BasisRate { get; set; }

    public double? AnnualizedBasisRate { get; set; }
}
