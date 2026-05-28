using System;

namespace CollectorModels.Models.Storage;

public class BookDepth
{
    public DateTime SnapshotTime { get; set; }
    public decimal Percentage { get; set; }
    public double Depth { get; set; }
    public double Notional { get; set; }
}
