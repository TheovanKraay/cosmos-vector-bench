using System.Globalization;
using System.Text;

namespace CosmosVectorBench;

/// <summary>
/// Builds the live aggregate progress output and the final aggregated result, and writes the metrics CSV row.
/// Reproduces the column order, filename pattern, and excluded fields of the Python <c>_print_parent_result</c>
/// and <c>_write_metrics_csv</c> so .NET runs land in the same <c>results/</c> folder with comparable schema.
/// </summary>
public sealed class MetricsReporter
{
    private static readonly (string Name, double Ratio)[] Percentiles =
    [
        ("p50", 0.50),
        ("p90", 0.90),
        ("p99", 0.99),
    ];

    private readonly BenchmarkConfig _config;
    private int _lastLineCount;

    public MetricsReporter(BenchmarkConfig config) => _config = config;

    public void PrintLiveLine(string line, bool final = false)
    {
        var builder = new StringBuilder();
        builder.Append(ClearPreviousLiveBlock(_lastLineCount));
        builder.Append(line);
        if (final)
        {
            Console.WriteLine(builder.ToString());
        }
        else
        {
            Console.Write(builder.ToString());
        }

        _lastLineCount = Math.Max(1, line.Count(c => c == '\n') + 1);
    }

    private static string ClearPreviousLiveBlock(int lineCount)
    {
        if (lineCount <= 0)
        {
            return "";
        }

        var builder = new StringBuilder("\r\x1b[2K");
        for (int i = 0; i < lineCount - 1; i++)
        {
            builder.Append("\x1b[1A\r\x1b[2K");
        }

        return builder.ToString();
    }

    /// <summary>Builds the multi-line aggregate progress block from the latest per-worker snapshots.</summary>
    public string BuildAggregateLine(
        IReadOnlyList<MetricSnapshot> snapshots,
        long? totalDocs,
        int clientProcesses,
        List<double> aggregateThroughputSamples)
    {
        double elapsed = AggregateElapsed(snapshots);
        int activeClients = snapshots.Count;
        long successTotal = snapshots.Sum(s => s.Success);
        long conflictsTotal = snapshots.Sum(s => s.Conflicts);
        long errorsTotal = snapshots.Sum(s => s.Errors);
        long throttlesTotal = snapshots.Sum(s => s.ThrottlesWithRetry);
        long completedTotal = successTotal + conflictsTotal + errorsTotal;
        double throughputCurrent = snapshots.Sum(s => s.CurrentDocsPerSec);

        if (snapshots.Any(s => s.Started))
        {
            aggregateThroughputSamples.Add(throughputCurrent);
        }

        double meanThroughput = Stats.Mean(aggregateThroughputSamples);
        double maxThroughput = Stats.Max(aggregateThroughputSamples);
        double requestChargeTotal = snapshots.Sum(s => s.RequestChargeTotal);
        long requestChargeObservations = snapshots.Sum(s => s.RequestChargeObservations);
        double avgRu = Stats.SafeDiv(requestChargeTotal, requestChargeObservations);

        double serviceMean = Stats.Mean(snapshots.Select(s => s.ServiceTimeMeanMs).ToList());
        double serviceP50 = snapshots.Count > 0 ? snapshots.Max(s => s.ServiceTimeP50Ms) : 0.0;
        double serviceP90 = snapshots.Count > 0 ? snapshots.Max(s => s.ServiceTimeP90Ms) : 0.0;
        double serviceP99 = snapshots.Count > 0 ? snapshots.Max(s => s.ServiceTimeP99Ms) : 0.0;

        var lines = new List<string>
        {
            $"clients_active={activeClients}/{clientProcesses}",
            "  Progress",
            $"    elapsed={elapsed.ToString("F1", CultureInfo.InvariantCulture)}s",
            $"    clients_active={activeClients}/{clientProcesses}",
            $"    completed={FormatCompleted(completedTotal, totalDocs)}",
            "  Throughput",
            $"    current_docs/sec={F2(throughputCurrent)}, current_docs/sec/client={F2(Stats.SafeDiv(throughputCurrent, Math.Max(clientProcesses, 1)))}",
            $"    mean_docs/sec={F2(meanThroughput)}, mean_docs/sec/client={F2(Stats.SafeDiv(meanThroughput, Math.Max(clientProcesses, 1)))}",
            $"    max_docs/sec={F2(maxThroughput)}",
        };

        if (_config.PartitionKeyRangeRpsEnabled)
        {
            var rangeRps = new SortedDictionary<string, double>(StringComparer.Ordinal);
            var rangeRequests = new SortedDictionary<string, long>(StringComparer.Ordinal);
            var rangeThrottles = new SortedDictionary<string, long>(StringComparer.Ordinal);
            var rangeRu = new SortedDictionary<string, double>(StringComparer.Ordinal);
            long missingHeaderCount = 0;
            foreach (MetricSnapshot snapshot in snapshots)
            {
                missingHeaderCount += snapshot.PartitionKeyRangeMissingHeaderCount;
                foreach ((string rangeId, double rps) in snapshot.PartitionKeyRangeRequestsPerSec)
                {
                    rangeRps.TryGetValue(rangeId, out double current);
                    rangeRps[rangeId] = current + rps;
                }

                foreach ((string rangeId, long count) in snapshot.PartitionKeyRangeRequestTotals)
                {
                    rangeRequests.TryGetValue(rangeId, out long current);
                    rangeRequests[rangeId] = current + count;
                }

                foreach ((string rangeId, long count) in snapshot.PartitionKeyRangeThrottleTotals)
                {
                    rangeThrottles.TryGetValue(rangeId, out long current);
                    rangeThrottles[rangeId] = current + count;
                }

                foreach ((string rangeId, double ru) in snapshot.PartitionKeyRangeRuTotals)
                {
                    rangeRu.TryGetValue(rangeId, out double current);
                    rangeRu[rangeId] = current + ru;
                }
            }

            lines.Add("  Partition key range stats");
            if (rangeRequests.Count > 0)
            {
                double ruGrandTotal = rangeRu.Values.Sum();
                double partitionBudgetRu = _config.ContainerThroughputRu > 0
                    ? (double)_config.ContainerThroughputRu / rangeRequests.Count
                    : 0.0;

                foreach ((string rangeId, long requests) in rangeRequests)
                {
                    rangeRps.TryGetValue(rangeId, out double rps);
                    rangeThrottles.TryGetValue(rangeId, out long throttles);
                    rangeRu.TryGetValue(rangeId, out double ru);
                    double ruPerSec = Stats.SafeDiv(ru, elapsed);
                    string utilisation = partitionBudgetRu > 0
                        ? $", pct_of_partition_budget={F2(Stats.SafeDiv(ruPerSec * 100.0, partitionBudgetRu))}"
                        : "";
                    lines.Add(
                        $"    pkrange_{rangeId}=ops_per_sec={F2(rps)}, requests={requests}, " +
                        $"throttles={throttles}, throttle_pct={F2(Stats.SafeDiv(throttles * 100.0, requests))}, " +
                        $"ru_per_sec={F2(ruPerSec)}, pct_of_run_ru={F2(Stats.SafeDiv(ru * 100.0, ruGrandTotal))}{utilisation}");
                }

                var shares = rangeRu.Values.Select(ru => Stats.SafeDiv(ru * 100.0, ruGrandTotal)).ToList();
                if (shares.Count > 0)
                {
                    double evenShare = 100.0 / shares.Count;
                    string budgetNote = partitionBudgetRu > 0
                        ? $", partition_budget_ru_per_sec={F2(partitionBudgetRu)}"
                        : ", partition_budget_ru_per_sec=unknown (set CONTAINER_THROUGHPUT_RU)";
                    lines.Add(
                        $"    skew: ranges={shares.Count}, even_share_pct={F2(evenShare)}, " +
                        $"min_pct_of_run_ru={F2(shares.Min())}, max_pct_of_run_ru={F2(shares.Max())}, " +
                        $"spread_pts={F2(shares.Max() - shares.Min())}, " +
                        $"hot_cold_ratio={F2(Stats.SafeDiv(shares.Max(), shares.Min()))}{budgetNote}");
                }

                AppendPkRangeSeries(rangeRequests, rangeThrottles, rangeRu);
            }
            else
            {
                lines.Add("    observed_ranges=0");
            }

            lines.Add($"    missing_header_count={missingHeaderCount}");
        }

        lines.AddRange(
        [
            "  Timing",
            $"    service_time_ms_mean={F2(serviceMean)}, service_time_ms_p50={F2(serviceP50)}, service_time_ms_p90={F2(serviceP90)}, service_time_ms_p99={F2(serviceP99)}",
            "  Responses",
            $"    success={successTotal}, conflicts_skipped={conflictsTotal}, errors={errorsTotal}, throttles_w_retry={throttlesTotal}",
            $"    avg_ru_per_operation={F2(avgRu)}",
        ]);

        return string.Join("\n", lines);
    }

    /// <summary>Appends cumulative per-range counters each sample so post-processing can derive instantaneous skew.</summary>
    private void AppendPkRangeSeries(
        SortedDictionary<string, long> requests,
        SortedDictionary<string, long> throttles,
        SortedDictionary<string, double> ru)
    {
        if (string.IsNullOrEmpty(_config.PkRangeSeriesPath))
        {
            return;
        }

        try
        {
            bool writeHeader = !File.Exists(_config.PkRangeSeriesPath);
            using var writer = new StreamWriter(_config.PkRangeSeriesPath, append: true);
            if (writeHeader)
            {
                writer.WriteLine("epoch_s,range_id,requests_cum,throttles_cum,ru_cum");
            }

            double now = Clock.Epoch;
            foreach ((string rangeId, long count) in requests)
            {
                throttles.TryGetValue(rangeId, out long throttleCount);
                ru.TryGetValue(rangeId, out double ruTotal);
                writer.WriteLine(string.Create(
                    CultureInfo.InvariantCulture,
                    $"{now:F3},{rangeId},{count},{throttleCount},{ruTotal:F2}"));
            }
        }
        catch (IOException)
        {
            // Diagnostics only; never fail a benchmark because the series file is unavailable.
        }
    }

    public string BuildSearchAggregateLine(
        IReadOnlyList<SearchMetricSnapshot> snapshots,
        long totalQueries,
        int clientProcesses,
        List<double> aggregateThroughputSamples)
    {
        double elapsed = SearchAggregateElapsed(snapshots);
        int clientsActive = snapshots.Count(snapshot => snapshot.Started && !snapshot.Finished);
        int clientsCompleted = snapshots.Count(snapshot => snapshot.Finished);
        long completed = snapshots.Sum(snapshot => snapshot.Completed);
        long success = snapshots.Sum(snapshot => snapshot.Success);
        long errors = snapshots.Sum(snapshot => snapshot.Errors);
        double currentThroughput = snapshots.Sum(snapshot => snapshot.CurrentQueriesPerSec);
        if (snapshots.Any(snapshot => snapshot.Started))
        {
            aggregateThroughputSamples.Add(currentThroughput);
        }

        var totalTimes = snapshots.SelectMany(snapshot => snapshot.QueryTotalTimeMsSamples).Order().ToList();
        double requestCharge = snapshots.Sum(snapshot => snapshot.RequestChargeTotal);
        var lines = new List<string>
        {
            $"clients_active={clientsActive}/{clientProcesses}, clients_completed={clientsCompleted}/{clientProcesses}",
            "  Progress",
            $"    elapsed={F2(elapsed)}s",
            $"    completed={completed}/{totalQueries}",
            "  Throughput",
            $"    current_search_queries/sec={F2(currentThroughput)}, current_search_queries/sec/client={F2(Stats.SafeDiv(currentThroughput, Math.Max(clientProcesses, 1)))}",
            $"    mean_search_queries/sec={F2(Stats.Mean(aggregateThroughputSamples))}, mean_search_queries/sec/client={F2(Stats.SafeDiv(Stats.Mean(aggregateThroughputSamples), Math.Max(clientProcesses, 1)))}",
            $"    max_search_queries/sec={F2(Stats.Max(aggregateThroughputSamples))}",
            "  Query metrics",
            $"    query_total_time_ms_mean={F2(Stats.Mean(totalTimes))}, query_total_time_ms_p50={F2(Stats.Percentile(totalTimes, 0.50))}, query_total_time_ms_p90={F2(Stats.Percentile(totalTimes, 0.90))}, query_total_time_ms_p99={F2(Stats.Percentile(totalTimes, 0.99))}",
            "  Responses",
            $"    success={success}, errors={errors}",
            $"    avg_ru_per_search_query={F2(Stats.SafeDiv(requestCharge, completed))}",
        };

        return string.Join("\n", lines);
    }

    /// <summary>Aggregates final per-worker results, prints the completion banner and final metrics, and writes the CSV row.</summary>
    public void PrintParentResult(IReadOnlyList<ResultSnapshot> results, double totalElapsedTimeSec)
    {
        long successTotal = results.Sum(r => r.Success);
        long conflictsTotal = results.Sum(r => r.Conflicts);
        long errorsTotal = results.Sum(r => r.Errors);
        long docsCompleted = successTotal + conflictsTotal + errorsTotal;
        long throttlesTotal = results.Sum(r => r.ThrottlesWithRetry);
        int clientsCompleted = results.Count;

        double insertTimeSec = ResultElapsed(results);
        double fallbackThroughput = Stats.SafeDiv(successTotal, insertTimeSec);
        List<double> throughputSamples = SamplesOrFallback(AggregateThroughputSamples(results), fallbackThroughput);

        var serviceTimes = new List<double>();
        foreach (ResultSnapshot result in results)
        {
            serviceTimes.AddRange(result.ServiceTimeMsSamples);
        }

        double requestChargeTotal = results.Sum(r => r.RequestChargeTotal);
        long requestChargeObservations = results.Sum(r => r.RequestChargeObservations);

        double meanThroughput = Stats.Mean(throughputSamples);
        double maxThroughput = Stats.Max(throughputSamples);

        serviceTimes.Sort();

        var row = new (string Name, string Value)[]
        {
            ("total_elapsed_time_sec", F2(totalElapsedTimeSec)),
            ("insert_time_sec", F2(insertTimeSec)),
            ("metrics_sample_interval_sec", F2(_config.MetricsSampleIntervalSec)),
            ("metrics_timing_sample_interval", _config.MetricsTimingSampleInterval.ToString(CultureInfo.InvariantCulture)),
            ("capture_ru_charges", _config.CaptureRuCharges ? "true" : "false"),
            ("clients", _config.ClientProcesses.ToString(CultureInfo.InvariantCulture)),
            ("clients_completed", clientsCompleted.ToString(CultureInfo.InvariantCulture)),
            ("docs_completed", docsCompleted.ToString(CultureInfo.InvariantCulture)),
            ("success_total", successTotal.ToString(CultureInfo.InvariantCulture)),
            ("conflicts_skipped_total", conflictsTotal.ToString(CultureInfo.InvariantCulture)),
            ("errors_total", errorsTotal.ToString(CultureInfo.InvariantCulture)),
            ("throttles_w_retry_total", throttlesTotal.ToString(CultureInfo.InvariantCulture)),
            ("mean_docs_per_sec", F2(meanThroughput)),
            ("mean_docs_per_sec_per_client", F2(Stats.SafeDiv(meanThroughput, Math.Max(_config.ClientProcesses, 1)))),
            ("max_docs_per_sec", F2(maxThroughput)),
            ("service_time_ms_mean", F2(Stats.Mean(serviceTimes))),
            ("service_time_ms_p50", F2(Stats.Percentile(serviceTimes, 0.50))),
            ("service_time_ms_p90", F2(Stats.Percentile(serviceTimes, 0.90))),
            ("service_time_ms_p99", F2(Stats.Percentile(serviceTimes, 0.99))),
            ("bulk_size", _config.BulkSize.ToString(CultureInfo.InvariantCulture)),
            ("bulks_started", results.Sum(r => r.BulksStarted).ToString(CultureInfo.InvariantCulture)),
            ("bulks_completed", results.Sum(r => r.BulksCompleted).ToString(CultureInfo.InvariantCulture)),
            ("bulk_success", results.Sum(r => r.BulkSuccess).ToString(CultureInfo.InvariantCulture)),
            ("bulk_errors", results.Sum(r => r.BulkErrors).ToString(CultureInfo.InvariantCulture)),
            ("bulk_docs_attempted", results.Sum(r => r.BulkDocsAttempted).ToString(CultureInfo.InvariantCulture)),
            ("bulk_docs_sampled", results.Sum(r => r.BulkDocsSampled).ToString(CultureInfo.InvariantCulture)),
            ("session_pool_size", _config.DataType == "fake" ? _config.SessionPoolSize.ToString(CultureInfo.InvariantCulture) : ""),
            ("session_write_quantum_docs", _config.DataType == "fake" ? _config.SessionWriteQuantumDocs.ToString(CultureInfo.InvariantCulture) : ""),
            ("session_id_min_docs", _config.DataType == "fake" ? _config.SessionIdMinDocs.ToString(CultureInfo.InvariantCulture) : ""),
            ("session_id_max_docs", _config.DataType == "fake" ? _config.SessionIdMaxDocs.ToString(CultureInfo.InvariantCulture) : ""),
            ("request_charge_total", F2(requestChargeTotal)),
            ("request_charge_observations", requestChargeObservations.ToString(CultureInfo.InvariantCulture)),
            ("avg_ru_per_operation", F2(Stats.SafeDiv(requestChargeTotal, requestChargeObservations))),
            ("max_total_docs", _config.MaxTotalDocs?.ToString(CultureInfo.InvariantCulture) ?? ""),
        };

        PrintFinalMetrics(row);
        WriteMetricsCsv(row);
    }

    public void PrintSearchResult(
        IReadOnlyList<SearchResultSnapshot> results,
        double searchElapsedTimeSec)
    {
        long completed = results.Sum(result => result.Completed);
        long success = results.Sum(result => result.Success);
        long errors = results.Sum(result => result.Errors);
        int clientsCompleted = results.Count(result => result.FinishedEpoch is not null);
        List<double> totalTimes = results.SelectMany(result => result.QueryTotalTimeMsSamples).Order().ToList();
        List<double> throughputSamples = AggregateSearchThroughputSamples(results);
        double meanThroughput = Stats.SafeDiv(completed, searchElapsedTimeSec);
        if (throughputSamples.Count == 0 && meanThroughput > 0)
        {
            throughputSamples.Add(meanThroughput);
        }

        double requestCharge = results.Sum(result => result.RequestChargeTotal);
        var row = new (string Name, string Value)[]
        {
            ("search_elapsed_time_sec", F2(searchElapsedTimeSec)),
            ("clients", _config.ClientProcesses.ToString(CultureInfo.InvariantCulture)),
            ("clients_active", "0"),
            ("clients_completed", clientsCompleted.ToString(CultureInfo.InvariantCulture)),
            ("configured_queries_per_sec_per_client", _config.SearchQueriesPerSecond.ToString(CultureInfo.InvariantCulture)),
            ("configured_total_queries", _config.SearchTotalQueries.ToString(CultureInfo.InvariantCulture)),
            ("queries_completed", completed.ToString(CultureInfo.InvariantCulture)),
            ("success_total", success.ToString(CultureInfo.InvariantCulture)),
            ("errors_total", errors.ToString(CultureInfo.InvariantCulture)),
            ("mean_search_queries_per_sec", F2(meanThroughput)),
            ("mean_search_queries_per_sec_per_client", F2(Stats.SafeDiv(meanThroughput, Math.Max(_config.ClientProcesses, 1)))),
            ("max_search_queries_per_sec", F2(Stats.Max(throughputSamples))),
            ("query_total_time_ms_mean", F2(Stats.Mean(totalTimes))),
            ("query_total_time_ms_p50", F2(Stats.Percentile(totalTimes, 0.50))),
            ("query_total_time_ms_p90", F2(Stats.Percentile(totalTimes, 0.90))),
            ("query_total_time_ms_p99", F2(Stats.Percentile(totalTimes, 0.99))),
            ("request_charge_total", F2(requestCharge)),
            ("avg_ru_per_search_query", F2(Stats.SafeDiv(requestCharge, completed))),
        };

        PrintFinalMetrics(row);
        WriteSearchMetricsCsv(row);
    }

    private static void PrintFinalMetrics((string Name, string Value)[] row)
    {
        Console.WriteLine("\n***Benchmark completed!***\n");
        foreach ((string name, string value) in row)
        {
            Console.WriteLine($"     {name.Replace("_per_", "/")}={value}");
        }
    }

    private void WriteMetricsCsv((string Name, string Value)[] row)
    {
        if (!_config.CsvOutputEnabled)
        {
            return;
        }

        string fileName =
            $"{_config.RunStartedAt.ToString("MMddyy-HHmmss", CultureInfo.InvariantCulture)}" +
            $"-clients-{_config.ClientProcesses}" +
            $"-bulk-{_config.BulkSize}" +
            $"-maxdocs-{_config.TotalDocsLabel()}" +
            ".csv";

        string resultsRoot = Path.IsPathRooted(_config.TestResultsRoot)
            ? _config.TestResultsRoot
            : Path.Combine(_config.ProjectRoot, _config.TestResultsRoot);
        Directory.CreateDirectory(resultsRoot);
        string csvPath = Path.Combine(resultsRoot, fileName);

        bool writeHeader = !File.Exists(csvPath) || new FileInfo(csvPath).Length == 0;
        using var writer = new StreamWriter(csvPath, append: true, Encoding.UTF8);
        if (writeHeader)
        {
            writer.WriteLine(string.Join(",", row.Select(r => CsvField(r.Name))));
        }

        writer.WriteLine(string.Join(",", row.Select(r => CsvField(r.Value))));
        Console.WriteLine($"metrics_csv_path={csvPath}");
    }

    private void WriteSearchMetricsCsv((string Name, string Value)[] row)
    {
        if (!_config.CsvOutputEnabled)
        {
            return;
        }

        string fileName =
            $"search-{_config.RunStartedAt.ToString("MMddyy-HHmmss", CultureInfo.InvariantCulture)}" +
            $"-clients-{_config.ClientProcesses}" +
            $"-qps-{_config.SearchQueriesPerSecond}" +
            $"-queries-{_config.SearchTotalQueries}.csv";
        string resultsRoot = Path.IsPathRooted(_config.TestResultsRoot)
            ? _config.TestResultsRoot
            : Path.Combine(_config.ProjectRoot, _config.TestResultsRoot);
        Directory.CreateDirectory(resultsRoot);
        string csvPath = Path.Combine(resultsRoot, fileName);
        using var writer = new StreamWriter(csvPath, append: false, Encoding.UTF8);
        writer.WriteLine(string.Join(",", row.Select(field => CsvField(field.Name))));
        writer.WriteLine(string.Join(",", row.Select(field => CsvField(field.Value))));
        Console.WriteLine($"metrics_csv_path={csvPath}");
    }

    private static string CsvField(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
        {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }

        return value;
    }

    private static double AggregateElapsed(IReadOnlyList<MetricSnapshot> snapshots)
    {
        var startedEpochs = snapshots.Where(s => s.StartedEpoch is > 0).Select(s => s.StartedEpoch!.Value).ToList();
        if (startedEpochs.Count == 0)
        {
            return 0.0;
        }

        return Math.Max(Clock.Epoch - startedEpochs.Min(), 0.000001);
    }

    private static double SearchAggregateElapsed(IReadOnlyList<SearchMetricSnapshot> snapshots)
    {
        List<double> startedEpochs = snapshots
            .Where(snapshot => snapshot.StartedEpoch is > 0)
            .Select(snapshot => snapshot.StartedEpoch!.Value)
            .ToList();
        return startedEpochs.Count == 0 ? 0.0 : Math.Max(Clock.Epoch - startedEpochs.Min(), 0.000001);
    }

    private static double ResultElapsed(IReadOnlyList<ResultSnapshot> results)
    {
        var startedEpochs = results.Where(r => r.StartedEpoch is > 0).Select(r => r.StartedEpoch!.Value).ToList();
        var finishedEpochs = results.Where(r => r.FinishedEpoch is > 0).Select(r => r.FinishedEpoch!.Value).ToList();
        if (startedEpochs.Count == 0 || finishedEpochs.Count == 0)
        {
            return 0.0;
        }

        return Math.Max(finishedEpochs.Max() - startedEpochs.Min(), 0.000001);
    }

    private static List<double> AggregateThroughputSamples(IReadOnlyList<ResultSnapshot> results)
    {
        int maxLen = results.Count == 0 ? 0 : results.Max(r => r.ThroughputDocsPerSecSamples.Count);
        var aggregate = new List<double>(maxLen);
        for (int i = 0; i < maxLen; i++)
        {
            double sum = 0.0;
            bool any = false;
            foreach (ResultSnapshot result in results)
            {
                if (i < result.ThroughputDocsPerSecSamples.Count)
                {
                    sum += result.ThroughputDocsPerSecSamples[i];
                    any = true;
                }
            }

            if (any)
            {
                aggregate.Add(sum);
            }
        }

        return aggregate;
    }

    private static List<double> AggregateSearchThroughputSamples(IReadOnlyList<SearchResultSnapshot> results)
    {
        int maxLength = results.Count == 0 ? 0 : results.Max(result => result.ThroughputQueriesPerSecSamples.Count);
        var aggregate = new List<double>(maxLength);
        for (int index = 0; index < maxLength; index++)
        {
            double sum = 0.0;
            bool any = false;
            foreach (SearchResultSnapshot result in results)
            {
                if (index < result.ThroughputQueriesPerSecSamples.Count)
                {
                    sum += result.ThroughputQueriesPerSecSamples[index];
                    any = true;
                }
            }

            if (any)
            {
                aggregate.Add(sum);
            }
        }

        return aggregate;
    }

    private List<double> SamplesOrFallback(List<double> samples, double fallback)
    {
        if (samples.Count > 0)
        {
            return samples;
        }

        if (_config.MetricsWarmupSec > 0)
        {
            return [];
        }

        return fallback > 0 ? [fallback] : [];
    }

    private static string FormatCompleted(long completed, long? totalDocs)
    {
        return totalDocs is null
            ? completed.ToString(CultureInfo.InvariantCulture)
            : $"{completed}/{totalDocs}";
    }

    private static string F2(double value) => value.ToString("F2", CultureInfo.InvariantCulture);
}
