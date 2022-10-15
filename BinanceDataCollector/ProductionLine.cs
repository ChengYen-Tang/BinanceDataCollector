using BinanceDataCollector.WorkItems;
using System.Threading.Channels;

namespace BinanceDataCollector;

internal class ProductionLine
{
    private readonly IConfiguration configuration;
    private readonly ILogger<ProductionLine> logger;
    private readonly List<Task> tasks;
    private readonly ManualResetEvent deleteWaitEvent;
    private readonly ManualResetEvent productionLineWaitEvent;
    private bool isRunning;
    private CancellationTokenSource cancellationTokenSource = null!;
    private ProcessState[] lastProcessState = null!;
    private ProcessState[] deleteProcessState = null!;

    public readonly Channel<IAsymcWorkItem> GetLastTimeChannel;
    public readonly Channel<IAsymcWorkItem> GatherKlineChannel;
    public readonly Channel<IAsymcWorkItem> InsertKlineChannel;
    public readonly Channel<IAsymcWorkItem> DeleteKlineChannel;

    public ProductionLine(IConfiguration configuration, ILogger<ProductionLine> logger)
        => (this.configuration, this.logger, GetLastTimeChannel, GatherKlineChannel, InsertKlineChannel, DeleteKlineChannel, tasks, isRunning, deleteWaitEvent, productionLineWaitEvent)
        = (configuration, logger, Channel.CreateUnbounded<IAsymcWorkItem>(), Channel.CreateUnbounded<IAsymcWorkItem>(), Channel.CreateBounded<IAsymcWorkItem>(10), Channel.CreateUnbounded<IAsymcWorkItem>(), new(), false, new(false), new(false));

    public void Start()
    {
        if (isRunning)
            return;
        int getLastTimeWorkerCount = configuration.GetValue("ProductionLine:GetLastTimeWorkerCount", 1);
        int insertKlineWorkerCount = configuration.GetValue("ProductionLine:InsertKlineWorkerCount", 1);
        int deleteKlineWorkerCount = configuration.GetValue("ProductionLine:DeleteKlineWorkerCount", 1);
        cancellationTokenSource = new();
        for (int i = 0; i < getLastTimeWorkerCount; i++)
            tasks.Add(Task.Factory.StartNew(GetLastTimeProcessAsync, TaskCreationOptions.LongRunning));
        tasks.Add(Task.Factory.StartNew(GatherKlineProcessAsync, TaskCreationOptions.LongRunning));
        lastProcessState = new ProcessState[insertKlineWorkerCount];
        for (int i = 0; i < insertKlineWorkerCount; i++)
        {
            int index = i;
            tasks.Add(Task.Factory.StartNew(() => InsertKlineProcessAsync(index), TaskCreationOptions.LongRunning));
            lastProcessState[i] = new();
        }
        deleteProcessState = new ProcessState[deleteKlineWorkerCount];
        for (int i = 0; i < deleteKlineWorkerCount; i++)
        {
            int index = i;
            tasks.Add(Task.Factory.StartNew(() => DeleteKlineProcessAsync(index), TaskCreationOptions.LongRunning));
            deleteProcessState[i] = new();
        }

        isRunning = true;
    }
    
    public void Stop()
    {
        if (!isRunning)
            return;
        cancellationTokenSource.Cancel();
        Task.WaitAll(tasks.ToArray());
        cancellationTokenSource.Dispose();
        tasks.Clear();
        isRunning = false;
    }
    
    public void ResetEvent()
    {
        deleteWaitEvent.Reset();
        productionLineWaitEvent.Reset();
    }
    
    public void Wait()
        => productionLineWaitEvent.WaitOne();

    private async Task GetLastTimeProcessAsync()
    {
        logger.LogInformation("GetLastTimeProcessAsync started");
        while (await GetLastTimeChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
        {
            if (GetLastTimeChannel.Reader.TryRead(out IAsymcWorkItem? item))
            {
                await item.Run();
            }
            logger.LogInformation($"The number of pending tasks:: GetLastTimeChannel:{GetLastTimeChannel.Reader.Count}, GatherKlineChannel:{GatherKlineChannel.Reader.Count}, InsertKlineChannel:{InsertKlineChannel.Reader.Count}, DeleteKlineChannel:{DeleteKlineChannel.Reader.Count}");
        }
        logger.LogInformation("GetLastTimeProcessAsync stopped");
    }

    private async Task GatherKlineProcessAsync()
    {
        logger.LogInformation("GatherKlineProcessAsync started");
        while (await GatherKlineChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
        {
            if (GatherKlineChannel.Reader.TryRead(out IAsymcWorkItem? item))
            {
                await item.Run();
            }
            logger.LogInformation($"The number of pending tasks:: GetLastTimeChannel:{GetLastTimeChannel.Reader.Count}, GatherKlineChannel:{GatherKlineChannel.Reader.Count}, InsertKlineChannel:{InsertKlineChannel.Reader.Count}, DeleteKlineChannel:{DeleteKlineChannel.Reader.Count}");
        }
        logger.LogInformation("GatherKlineProcessAsync stopped");
    }

    private async Task InsertKlineProcessAsync(int index)
    {
        logger.LogInformation("InsertKlineProcessAsync started");
        while (await InsertKlineChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
        {
            if (InsertKlineChannel.Reader.TryRead(out IAsymcWorkItem? item))
            {
                lock(lastProcessState[index].Lock)
                    lastProcessState[index].State = true;
                await item.Run();
                lock (lastProcessState[index].Lock)
                    lastProcessState[index].State = false;

                logger.LogInformation($"The number of pending tasks:: GetLastTimeChannel:{GetLastTimeChannel.Reader.Count}, GatherKlineChannel:{GatherKlineChannel.Reader.Count}, InsertKlineChannel:{InsertKlineChannel.Reader.Count}, DeleteKlineChannel:{DeleteKlineChannel.Reader.Count}");

                if (lastProcessState.All(x => { lock (x.Lock) return !x.State; }) && InsertKlineChannel.Reader.Count == 0 && GatherKlineChannel.Reader.Count == 0 && GetLastTimeChannel.Reader.Count == 0)
                    deleteWaitEvent.Set();
            }
        }
        logger.LogInformation("InsertKlineProcessAsync stopped");
    }

    private async Task DeleteKlineProcessAsync(int index)
    {
        logger.LogInformation("DeleteKlineProcessAsync started");
        while (await DeleteKlineChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
        {
            deleteWaitEvent.WaitOne();
            if (DeleteKlineChannel.Reader.TryRead(out IAsymcWorkItem? item))
            {
                lock (deleteProcessState[index].Lock)
                    deleteProcessState[index].State = true;
                await item.Run();
                lock (deleteProcessState[index].Lock)
                    deleteProcessState[index].State = false;

                logger.LogInformation($"The number of pending tasks:: GetLastTimeChannel:{GetLastTimeChannel.Reader.Count}, GatherKlineChannel:{GatherKlineChannel.Reader.Count}, InsertKlineChannel:{InsertKlineChannel.Reader.Count}, DeleteKlineChannel:{DeleteKlineChannel.Reader.Count}");

                if (deleteProcessState.All(x => { lock (x.Lock) return !x.State; }) && DeleteKlineChannel.Reader.Count == 0)
                    productionLineWaitEvent.Set();
            }
        }
        logger.LogInformation("DeleteKlineProcessAsync stopped");
    }

    internal class ProcessState
    {
        public bool State { get; set; }
        public object Lock { get; } = new();
    }
}
