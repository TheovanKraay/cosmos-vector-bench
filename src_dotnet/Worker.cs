using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace CosmosVectorBench;

/// <summary>
/// Per-worker (logical client) execution helpers and the async batch adapters used by both data modes.
/// Replaces the Python multi-process worker model with in-process async worker loops.
/// </summary>
public static class Worker
{
    /// <summary>Runs one worker: it consumes document bulks through the insert scheduler under its own concurrency semaphore.</summary>
    public static async Task RunAsync(
        CosmosWriter writer,
        IAsyncEnumerable<List<JsonObject>> batches,
        WorkerMetrics metrics,
        int maxInFlight,
        CancellationToken cancellationToken)
    {
        using var sem = new SemaphoreSlim(maxInFlight, maxInFlight);
        await writer.InsertDocBatchesAsync(batches, sem, metrics, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one worker against a caller-owned semaphore. When every worker is handed the same
    /// instance, total outstanding operations stay fixed instead of scaling with client count.
    /// </summary>
    public static async Task RunAsync(
        CosmosWriter writer,
        IAsyncEnumerable<List<JsonObject>> batches,
        WorkerMetrics metrics,
        SemaphoreSlim sharedInFlight,
        CancellationToken cancellationToken)
    {
        await writer.InsertDocBatchesAsync(batches, sharedInFlight, metrics, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs one worker over raw UTF-8 document batches using the stream-write hot path.</summary>
    public static async Task RunRawAsync(
        CosmosWriter writer,
        IAsyncEnumerable<List<DataSource.RawDocument>> batches,
        WorkerMetrics metrics,
        int maxInFlight,
        CancellationToken cancellationToken)
    {
        using var sem = new SemaphoreSlim(maxInFlight, maxInFlight);
        await writer.InsertRawBatchesAsync(batches, sem, metrics, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Raw-batch worker sharing a caller-owned in-flight semaphore across all workers.</summary>
    public static async Task RunRawAsync(
        CosmosWriter writer,
        IAsyncEnumerable<List<DataSource.RawDocument>> batches,
        WorkerMetrics metrics,
        SemaphoreSlim sharedInFlight,
        CancellationToken cancellationToken)
    {
        await writer.InsertRawBatchesAsync(batches, sharedInFlight, metrics, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Adapts a synchronous bulk generator (fake mode) to the async batch interface.</summary>
    public static async IAsyncEnumerable<List<JsonObject>> ToAsync(
        IEnumerable<List<JsonObject>> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (List<JsonObject> bulk in source)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return bulk;
            await Task.Yield();
        }
    }

    /// <summary>
    /// Reads file-loaded document batches from a channel and regroups them into local bulks of <paramref name="bulkSize"/>,
    /// mirroring the Python <c>_iter_queue_doc_bulks</c> buffering behavior.
    /// </summary>
    public static async IAsyncEnumerable<List<T>> QueueBulks<T>(
        ChannelReader<List<T>> reader,
        int bulkSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var buffered = new List<T>(bulkSize);
        await foreach (List<T> batch in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            buffered.AddRange(batch);
            while (buffered.Count >= bulkSize)
            {
                var bulk = buffered.GetRange(0, bulkSize);
                buffered.RemoveRange(0, bulkSize);
                yield return bulk;
            }
        }

        if (buffered.Count > 0)
        {
            yield return buffered;
        }
    }

    /// <summary>Splits a fake-document workload across logical clients, returning (index, start, count) slices.</summary>
    public static List<(int Index, int Start, int Count)> WorkerSlices(int totalDocs, int clientProcesses)
    {
        int baseCount = totalDocs / clientProcesses;
        int remainder = totalDocs % clientProcesses;
        int start = 0;
        var slices = new List<(int, int, int)>(clientProcesses);
        for (int index = 0; index < clientProcesses; index++)
        {
            int count = baseCount + (index < remainder ? 1 : 0);
            slices.Add((index, start, count));
            start += count;
        }

        return slices;
    }
}
