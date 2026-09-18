using System.Diagnostics;

namespace CosmosVectorBench;

/// <summary>Monotonic and wall-clock time helpers, mirroring Python's perf_counter and time.time.</summary>
public static class Clock
{
    private static readonly double TicksToSeconds = 1.0 / Stopwatch.Frequency;

    /// <summary>Monotonic seconds (analogous to <c>time.perf_counter()</c>).</summary>
    public static double Now => Stopwatch.GetTimestamp() * TicksToSeconds;

    /// <summary>Unix epoch seconds (analogous to <c>time.time()</c>).</summary>
    public static double Epoch => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
}

/// <summary>Immutable summary statistics helpers shared by snapshots and the reporter.</summary>
public static class Stats
{
    public static double Mean(IReadOnlyList<double> values)
    {
        if (values.Count == 0)
        {
            return 0.0;
        }

        double sum = 0.0;
        for (int i = 0; i < values.Count; i++)
        {
            sum += values[i];
        }

        return sum / values.Count;
    }

    public static double Max(IReadOnlyList<double> values)
    {
        if (values.Count == 0)
        {
            return 0.0;
        }

        double max = values[0];
        for (int i = 1; i < values.Count; i++)
        {
            if (values[i] > max)
            {
                max = values[i];
            }
        }

        return max;
    }

    /// <summary>Percentile using the same index formula as the Python benchmark: ceil(n*ratio)-1, clamped.</summary>
    public static double Percentile(List<double> sortedValues, double ratio)
    {
        if (sortedValues.Count == 0)
        {
            return 0.0;
        }

        if (ratio >= 1)
        {
            return sortedValues[^1];
        }

        int index = (int)Math.Ceiling(sortedValues.Count * ratio) - 1;
        index = Math.Min(Math.Max(index, 0), sortedValues.Count - 1);
        return sortedValues[index];
    }

    public static double SafeDiv(double numerator, double denominator) => denominator <= 0 ? 0.0 : numerator / denominator;
}

/// <summary>Live per-worker snapshot used to build the aggregate progress line.</summary>
public sealed record MetricSnapshot
{
    public required bool Started { get; init; }
    public required double? StartedEpoch { get; init; }
    public required long Success { get; init; }
    public required long Conflicts { get; init; }
    public required long Errors { get; init; }
    public required long ThrottlesWithRetry { get; init; }
    public required long CreateItemAttempts { get; init; }
    public required double CurrentDocsPerSec { get; init; }
    public required int ThroughputSampleCount { get; init; }
    public required double ServiceTimeMeanMs { get; init; }
    public required double ServiceTimeP50Ms { get; init; }
    public required double ServiceTimeP90Ms { get; init; }
    public required double ServiceTimeP99Ms { get; init; }
    public required double RequestChargeTotal { get; init; }
    public required long RequestChargeObservations { get; init; }
    public required IReadOnlyDictionary<string, double> PartitionKeyRangeRequestsPerSec { get; init; }
    public required IReadOnlyDictionary<string, long> PartitionKeyRangeRequestTotals { get; init; }
    public required IReadOnlyDictionary<string, long> PartitionKeyRangeThrottleTotals { get; init; }
    public required IReadOnlyDictionary<string, double> PartitionKeyRangeRuTotals { get; init; }
    public required long PartitionKeyRangeMissingHeaderCount { get; init; }
}

/// <summary>Final per-worker result snapshot used to build the aggregate parent result and CSV row.</summary>
public sealed record ResultSnapshot
{
    public required double? StartedEpoch { get; init; }
    public required double? FinishedEpoch { get; init; }
    public required long Success { get; init; }
    public required long Conflicts { get; init; }
    public required long Errors { get; init; }
    public required long ThrottlesWithRetry { get; init; }
    public required long CreateItemAttempts { get; init; }
    public required long CreateItemFailureAttempts { get; init; }
    public required List<double> ThroughputDocsPerSecSamples { get; init; }
    public required List<double> ServiceTimeMsSamples { get; init; }
    public required long BulksStarted { get; init; }
    public required long BulksCompleted { get; init; }
    public required long BulkSuccess { get; init; }
    public required long BulkErrors { get; init; }
    public required long BulkDocsAttempted { get; init; }
    public required long BulkDocsSampled { get; init; }
    public required double RequestChargeTotal { get; init; }
    public required long RequestChargeObservations { get; init; }
}

/// <summary>
/// Tracks per-worker (logical client) benchmark metrics. Thread-safe because a worker fires multiple concurrent
/// item creates. Mirrors the counters and sampling logic of the Python <c>src/metrics.py</c> module.
/// </summary>
public sealed class WorkerMetrics
{
    private readonly BenchmarkConfig _config;
    private readonly object _sync = new();
    private readonly double _totalStartedAt = Clock.Now;

    private long _success;
    private long _conflicts;
    private long _errors;
    private long _throttlesWithRetry;
    private long _createItemAttempts;
    private long _createItemFailureAttempts;

    private double? _startedAt;
    private double? _startedEpoch;
    private double? _finishedAt;
    private double? _finishedEpoch;

    private double? _throughputLastSampleAt;
    private long _throughputLastSampleSuccess;
    private readonly List<double> _throughputSamples = [];

    private readonly List<double> _serviceTimeSamples = [];
    private long _bulkTimingObservations;
    private long _bulkDocsSampled;
    private long _bulksStarted;
    private long _bulksCompleted;
    private long _bulkSuccess;
    private long _bulkErrors;
    private long _bulkDocsAttempted;

    private double _requestChargeTotal;
    private long _requestChargeObservations;

    private readonly Dictionary<string, long> _partitionKeyRangeRequestCounts = [];
    private readonly Dictionary<string, long> _partitionKeyRangeThrottleCounts = [];
    private readonly Dictionary<string, double> _partitionKeyRangeRuTotals = [];
    private Dictionary<string, long> _partitionKeyRangeLastSampleCounts = [];
    private Dictionary<string, double> _partitionKeyRangeRequestsPerSec = [];
    private long _partitionKeyRangeMissingHeaderCount;

    public int CosmosErrorSamplesLogged;

    public WorkerMetrics(BenchmarkConfig config) => _config = config;

    public void RecordUploadStarted(double startedAt, double startedEpoch)
    {
        lock (_sync)
        {
            if (_startedAt is null)
            {
                _startedAt = startedAt;
                _startedEpoch = startedEpoch;
                _throughputLastSampleAt = startedAt;
                _throughputLastSampleSuccess = _success;
                _partitionKeyRangeLastSampleCounts = new Dictionary<string, long>(_partitionKeyRangeRequestCounts);
            }
        }
    }

    public void RecordUploadFinished(double finishedAt, double finishedEpoch)
    {
        lock (_sync)
        {
            _finishedAt = finishedAt;
            _finishedEpoch = finishedEpoch;
        }
    }

    public void RecordCreateItemAttempt()
    {
        lock (_sync)
        {
            _createItemAttempts++;
        }
    }

    public void RecordCreateItemFailure()
    {
        lock (_sync)
        {
            _createItemFailureAttempts++;
        }
    }

    public void RecordSuccess()
    {
        lock (_sync)
        {
            _success++;
        }
    }

    public void RecordConflict()
    {
        lock (_sync)
        {
            _conflicts++;
        }
    }

    public void RecordError()
    {
        lock (_sync)
        {
            _errors++;
        }
    }

    public void RecordThrottle()
    {
        lock (_sync)
        {
            _throttlesWithRetry++;
        }
    }

    public void RecordRequestCharge(double charge)
    {
        if (charge <= 0)
        {
            return;
        }

        lock (_sync)
        {
            _requestChargeTotal += charge;
            _requestChargeObservations++;
        }
    }

    /// <summary>Records one create attempt against its Cosmos partition key range, or a missing-header observation.</summary>
    public void RecordPartitionKeyRangeRequest(string? partitionKeyRangeId, bool throttled, double requestCharge)
    {
        lock (_sync)
        {
            if (string.IsNullOrEmpty(partitionKeyRangeId))
            {
                _partitionKeyRangeMissingHeaderCount++;
                return;
            }

            _partitionKeyRangeRequestCounts.TryGetValue(partitionKeyRangeId, out long current);
            _partitionKeyRangeRequestCounts[partitionKeyRangeId] = current + 1;

            if (throttled)
            {
                _partitionKeyRangeThrottleCounts.TryGetValue(partitionKeyRangeId, out long throttles);
                _partitionKeyRangeThrottleCounts[partitionKeyRangeId] = throttles + 1;
            }

            if (requestCharge > 0)
            {
                _partitionKeyRangeRuTotals.TryGetValue(partitionKeyRangeId, out double ru);
                _partitionKeyRangeRuTotals[partitionKeyRangeId] = ru + requestCharge;
            }
        }
    }

    public void RecordBulkStarted(int docCount)
    {
        lock (_sync)
        {
            _bulksStarted++;
            _bulkDocsAttempted += docCount;
        }
    }

    public void RecordBulkCompleted(bool hadError)
    {
        lock (_sync)
        {
            _bulksCompleted++;
            if (hadError)
            {
                _bulkErrors++;
            }
            else
            {
                _bulkSuccess++;
            }
        }
    }

    public long ErrorCountSnapshot()
    {
        lock (_sync)
        {
            return _errors;
        }
    }

    /// <summary>Records a bulk's service-time samples, applying warmup and timing-sample-interval gating.</summary>
    public void RecordBulkSample(int docsCount, IReadOnlyList<double> serviceTimeMsSamples, double finishedAt)
    {
        lock (_sync)
        {
            _bulkTimingObservations++;
            if (!AfterWarmupUnlocked(finishedAt))
            {
                return;
            }

            if (_bulkTimingObservations % _config.MetricsTimingSampleInterval != 0)
            {
                return;
            }

            _bulkDocsSampled += docsCount;
            _serviceTimeSamples.AddRange(serviceTimeMsSamples);
        }
    }

    /// <summary>Records a throughput sample (success delta over elapsed) honoring warmup and sample interval.</summary>
    public void RecordThroughputSample(bool force = false)
    {
        lock (_sync)
        {
            if (_startedAt is null)
            {
                return;
            }

            double now = Clock.Now;
            long success = _success;
            double? lastSampleAt = _throughputLastSampleAt;
            if (lastSampleAt is null)
            {
                _throughputLastSampleAt = now;
                _throughputLastSampleSuccess = success;
                _partitionKeyRangeLastSampleCounts = new Dictionary<string, long>(_partitionKeyRangeRequestCounts);
                return;
            }

            if (!AfterWarmupUnlocked(now) || !AfterWarmupUnlocked(lastSampleAt.Value))
            {
                _throughputLastSampleAt = now;
                _throughputLastSampleSuccess = success;
                _partitionKeyRangeLastSampleCounts = new Dictionary<string, long>(_partitionKeyRangeRequestCounts);
                _partitionKeyRangeRequestsPerSec = [];
                return;
            }

            double elapsed = now - lastSampleAt.Value;
            if (elapsed < _config.MetricsSampleIntervalSec && !force)
            {
                return;
            }

            long successDelta = success - _throughputLastSampleSuccess;
            if (elapsed > 0)
            {
                _throughputSamples.Add(successDelta / elapsed);
            }

            if (_config.PartitionKeyRangeRpsEnabled && elapsed > 0)
            {
                var perSec = new Dictionary<string, double>();
                foreach ((string rangeId, long count) in _partitionKeyRangeRequestCounts)
                {
                    _partitionKeyRangeLastSampleCounts.TryGetValue(rangeId, out long last);
                    long delta = count - last;
                    if (delta > 0)
                    {
                        perSec[rangeId] = delta / elapsed;
                    }
                }

                _partitionKeyRangeRequestsPerSec = perSec;
            }

            _throughputLastSampleAt = now;
            _throughputLastSampleSuccess = success;
            _partitionKeyRangeLastSampleCounts = new Dictionary<string, long>(_partitionKeyRangeRequestCounts);
        }
    }

    private bool AfterWarmupUnlocked(double sampleAt)
    {
        if (_startedAt is null)
        {
            return false;
        }

        return sampleAt >= _startedAt.Value + _config.MetricsWarmupSec;
    }

    private double ElapsedUploadTimeUnlocked(bool live)
    {
        if (_startedAt is null)
        {
            return 0.0;
        }

        double endpoint = (live || _finishedAt is null) ? Clock.Now : _finishedAt.Value;
        return Math.Max(endpoint - _startedAt.Value, 0.000001);
    }

    private List<double> SamplesOrFallbackUnlocked(List<double> samples, double fallback)
    {
        if (samples.Count > 0)
        {
            return [.. samples];
        }

        if (_config.MetricsWarmupSec > 0)
        {
            return [];
        }

        return fallback > 0 ? [fallback] : [];
    }

    public MetricSnapshot LiveSnapshot()
    {
        lock (_sync)
        {
            double elapsed = ElapsedUploadTimeUnlocked(live: true);
            double currentThroughput = _throughputSamples.Count > 0
                ? _throughputSamples[^1]
                : Stats.SafeDiv(_success, elapsed);

            var sortedServiceTimes = new List<double>(_serviceTimeSamples);
            sortedServiceTimes.Sort();

            return new MetricSnapshot
            {
                Started = _startedAt is not null,
                StartedEpoch = _startedEpoch,
                Success = _success,
                Conflicts = _conflicts,
                Errors = _errors,
                ThrottlesWithRetry = _throttlesWithRetry,
                CreateItemAttempts = _createItemAttempts,
                CurrentDocsPerSec = currentThroughput,
                ThroughputSampleCount = _throughputSamples.Count,
                ServiceTimeMeanMs = Stats.Mean(sortedServiceTimes),
                ServiceTimeP50Ms = Stats.Percentile(sortedServiceTimes, 0.50),
                ServiceTimeP90Ms = Stats.Percentile(sortedServiceTimes, 0.90),
                ServiceTimeP99Ms = Stats.Percentile(sortedServiceTimes, 0.99),
                RequestChargeTotal = _requestChargeTotal,
                RequestChargeObservations = _requestChargeObservations,
                PartitionKeyRangeRequestsPerSec = new Dictionary<string, double>(_partitionKeyRangeRequestsPerSec),
                PartitionKeyRangeRequestTotals = new Dictionary<string, long>(_partitionKeyRangeRequestCounts),
                PartitionKeyRangeThrottleTotals = new Dictionary<string, long>(_partitionKeyRangeThrottleCounts),
                PartitionKeyRangeRuTotals = new Dictionary<string, double>(_partitionKeyRangeRuTotals),
                PartitionKeyRangeMissingHeaderCount = _partitionKeyRangeMissingHeaderCount,
            };
        }
    }

    public ResultSnapshot ResultSnapshot()
    {
        RecordThroughputSample(force: true);
        lock (_sync)
        {
            double insertTime = ElapsedUploadTimeUnlocked(live: false);
            double fallbackThroughput = Stats.SafeDiv(_success, insertTime);
            return new ResultSnapshot
            {
                StartedEpoch = _startedEpoch,
                FinishedEpoch = _finishedEpoch,
                Success = _success,
                Conflicts = _conflicts,
                Errors = _errors,
                ThrottlesWithRetry = _throttlesWithRetry,
                CreateItemAttempts = _createItemAttempts,
                CreateItemFailureAttempts = _createItemFailureAttempts,
                ThroughputDocsPerSecSamples = SamplesOrFallbackUnlocked(_throughputSamples, fallbackThroughput),
                ServiceTimeMsSamples = [.. _serviceTimeSamples],
                BulksStarted = _bulksStarted,
                BulksCompleted = _bulksCompleted,
                BulkSuccess = _bulkSuccess,
                BulkErrors = _bulkErrors,
                BulkDocsAttempted = _bulkDocsAttempted,
                BulkDocsSampled = _bulkDocsSampled,
                RequestChargeTotal = _requestChargeTotal,
                RequestChargeObservations = _requestChargeObservations,
            };
        }
    }
}
