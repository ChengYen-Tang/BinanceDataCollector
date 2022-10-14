namespace BinanceDataCollector.WorkItems;

internal class AsyncWorkItem : IAsymcWorkItem
{
    private readonly Func<CancellationToken, Task> func;
    private readonly CancellationToken ct;

    public AsyncWorkItem(Func<CancellationToken, Task> func, CancellationToken ct)
        => (this.func, this.ct) = (func, ct);

    public Task Run()
        => func(ct);
}

internal class AsyncWorkItem<T> : IAsymcWorkItem
{
    private readonly Func<T, CancellationToken, Task> func;
    private readonly T parameter;
    private readonly CancellationToken ct;

    public AsyncWorkItem(Func<T, CancellationToken, Task> func, T parameter, CancellationToken ct)
        => (this.func, this.parameter, this.ct) = (func, parameter, ct);

    public Task Run()
        => func(parameter, ct);
}

internal class AsyncWorkItem<T, T1, T2> : IAsymcWorkItem
{
    private readonly Func<T, T1, T2, CancellationToken, Task> func;
    private readonly T parameter;
    private readonly T1 parameter1;
    private readonly T2 parameter2;
    private readonly CancellationToken ct;

    public AsyncWorkItem(Func<T, T1, T2, CancellationToken, Task> func, T parameter, T1 parameter1, T2 parameter2, CancellationToken ct)
        => (this.func, this.parameter, this.parameter1, this.parameter2, this.ct)
        = (func, parameter, parameter1, parameter2, ct);

    public Task Run()
        => func(parameter, parameter1, parameter2, ct);
}
