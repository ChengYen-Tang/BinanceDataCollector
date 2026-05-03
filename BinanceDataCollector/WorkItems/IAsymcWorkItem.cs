namespace BinanceDataCollector.WorkItems;

internal interface IAsymcWorkItem
{
    string Description { get; }
    Task Run();
}
