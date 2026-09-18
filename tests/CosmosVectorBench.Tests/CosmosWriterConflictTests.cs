using CosmosVectorBench;
using Xunit;

namespace CosmosVectorBench.Tests;

public sealed class CosmosWriterConflictTests
{
    [Fact]
    public void ClassifyFailureStatus_TreatsConflictAsSkippedDuplicate()
    {
        Assert.Equal(CreateFailureDisposition.Conflict, CosmosWriter.ClassifyFailureStatus(409));
    }

    [Theory]
    [InlineData(408)]
    [InlineData(429)]
    [InlineData(449)]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public void ClassifyFailureStatus_PreservesRetryableStatuses(int statusCode)
    {
        Assert.Equal(CreateFailureDisposition.Retryable, CosmosWriter.ClassifyFailureStatus(statusCode));
    }

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(404)]
    [InlineData(412)]
    public void ClassifyFailureStatus_TreatsOtherTerminalFailuresAsErrors(int statusCode)
    {
        Assert.Equal(CreateFailureDisposition.Error, CosmosWriter.ClassifyFailureStatus(statusCode));
    }
}

public sealed class ConflictMetricsTests
{
    [Fact]
    public async Task RecordConflict_IsThreadSafeAndDoesNotIncrementErrors()
    {
        var metrics = new WorkerMetrics(CreateConfig());

        Task[] callers = Enumerable.Range(0, 20)
            .Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < 500; i++)
                {
                    metrics.RecordConflict();
                }
            }))
            .ToArray();

        await Task.WhenAll(callers);

        MetricSnapshot snapshot = metrics.LiveSnapshot();
        Assert.Equal(10_000, snapshot.Conflicts);
        Assert.Equal(0, snapshot.Success);
        Assert.Equal(0, snapshot.Errors);
        Assert.Equal(0, metrics.ErrorCountSnapshot());
    }

    [Fact]
    public void BuildAggregateLine_CountsConflictsAsCompletedWithoutInflatingThroughput()
    {
        var reporter = new MetricsReporter(CreateConfig());
        List<double> throughputSamples = [];
        MetricSnapshot snapshot = CreateSnapshot(success: 2, conflicts: 7, errors: 1, currentDocsPerSec: 2.5);

        string output = reporter.BuildAggregateLine([snapshot], totalDocs: 10, clientProcesses: 1, throughputSamples);

        Assert.Contains("completed=10/10", output);
        Assert.Contains("current_docs/sec=2.50", output);
        Assert.Contains("success=2, conflicts_skipped=7, errors=1", output);
    }

    private static BenchmarkConfig CreateConfig()
        => (BenchmarkConfig)Activator.CreateInstance(typeof(BenchmarkConfig), nonPublic: true)!;

    private static MetricSnapshot CreateSnapshot(long success, long conflicts, long errors, double currentDocsPerSec)
        => new()
        {
            Started = true,
            StartedEpoch = Clock.Epoch,
            Success = success,
            Conflicts = conflicts,
            Errors = errors,
            ThrottlesWithRetry = 0,
            CreateItemAttempts = success + conflicts + errors,
            CurrentDocsPerSec = currentDocsPerSec,
            ThroughputSampleCount = 1,
            ServiceTimeMeanMs = 0,
            ServiceTimeP50Ms = 0,
            ServiceTimeP90Ms = 0,
            ServiceTimeP99Ms = 0,
            RequestChargeTotal = 0,
            RequestChargeObservations = 0,
            PartitionKeyRangeRequestsPerSec = new Dictionary<string, double>(),
            PartitionKeyRangeRequestTotals = new Dictionary<string, long>(),
            PartitionKeyRangeThrottleTotals = new Dictionary<string, long>(),
            PartitionKeyRangeRuTotals = new Dictionary<string, double>(),
            PartitionKeyRangeMissingHeaderCount = 0,
        };
}