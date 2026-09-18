using System.Buffers;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Azure.Cosmos;

namespace CosmosVectorBench;

internal enum CreateFailureDisposition
{
    Conflict,
    Retryable,
    Error,
}

/// <summary>
/// The Cosmos write hot path. Mirrors the Python <c>insert_doc</c> / <c>insert_bulk</c> / <c>insert_doc_batches</c>
/// behavior: a per-worker concurrency semaphore, local <c>BULK_SIZE</c> grouping, bounded pending bulks,
/// per-create service-time and request-charge capture, and retry handling for transient Cosmos errors.
/// Because the client enables <c>AllowBulkExecution</c>, concurrent point creates are batched by the SDK.
/// </summary>
public sealed class CosmosWriter
{
    private static readonly HashSet<HttpStatusCode> RetryableStatusCodes =
    [
        HttpStatusCode.RequestTimeout,           // 408
        (HttpStatusCode)429,                     // TooManyRequests
        (HttpStatusCode)449,                     // RetryWith
        HttpStatusCode.InternalServerError,      // 500
        HttpStatusCode.BadGateway,               // 502
        HttpStatusCode.ServiceUnavailable,       // 503
        HttpStatusCode.GatewayTimeout,           // 504
    ];

    private static readonly string[] PartitionKeyRangeHeaderNames =
    [
        "x-ms-documentdb-partitionkeyrangeid",
        "x-ms-partition-key-range-id",
        "x-ms-documentdb-partition-key-range-id",
    ];

    private readonly BenchmarkConfig _config;
    private readonly Container _container;
    private readonly IReadOnlyList<string> _partitionKeyFields;

    public CosmosWriter(BenchmarkConfig config, Container container)
    {
        _config = config;
        _container = container;
        _partitionKeyFields = config.PartitionKeyFields;
    }

    /// <summary>Creates one Cosmos item and returns the per-attempt service-time windows in milliseconds.</summary>
    private async Task<List<double>> InsertDocAsync(JsonObject doc, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
    {
        await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
        double start = Clock.Now;
        double startEpoch = Clock.Epoch;
        double finish = start;
        double finishEpoch = startEpoch;
        var attemptWindows = new List<double>();
        metrics.RecordUploadStarted(start, startEpoch);
        double requestChargeTotal = 0.0;

        try
        {
            PartitionKey partitionKey = ResolvePartitionKey(doc);

            for (int attempt = 0; attempt <= _config.MaxInsertRetries; attempt++)
            {
                double attemptStart = Clock.Now;
                metrics.RecordCreateItemAttempt();
                try
                {
                    ItemResponse<JsonObject> response = await _container
                        .CreateItemAsync(doc, partitionKey, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    finish = Clock.Now;
                    finishEpoch = Clock.Epoch;
                    attemptWindows.Add((finish - attemptStart) * 1000.0);

                    if (_config.CaptureRuCharges)
                    {
                        requestChargeTotal += response.RequestCharge;
                    }

                    if (_config.PartitionKeyRangeRpsEnabled)
                    {
                        metrics.RecordPartitionKeyRangeRequest(
                            PartitionKeyRangeIdFromHeaders(response.Headers),
                            throttled: false,
                            requestCharge: response.RequestCharge);
                    }

                    metrics.RecordSuccess();

                    // Only the successful attempt's charge: accumulating retried attempts inflated
                    // our RU total ~31% above the service's own TotalRequestUnits.
                    metrics.RecordRequestCharge(response.RequestCharge);
                    break;
                }
                catch (CosmosException ex)
                {
                    finish = Clock.Now;
                    finishEpoch = Clock.Epoch;
                    attemptWindows.Add((finish - attemptStart) * 1000.0);
                    metrics.RecordCreateItemFailure();
                    if (_config.CaptureRuCharges)
                    {
                        requestChargeTotal += ex.RequestCharge;
                    }

                    if (_config.PartitionKeyRangeRpsEnabled)
                    {
                        metrics.RecordPartitionKeyRangeRequest(
                            PartitionKeyRangeIdFromHeaders(ex.Headers),
                            throttled: (int)ex.StatusCode == 429,
                            requestCharge: ex.RequestCharge);
                    }

                    if ((int)ex.StatusCode == 429)
                    {
                        metrics.RecordThrottle();
                    }

                    CreateFailureDisposition disposition = ClassifyFailureStatus((int)ex.StatusCode);
                    if (disposition == CreateFailureDisposition.Conflict)
                    {
                        metrics.RecordConflict();
                        metrics.RecordRequestCharge(requestChargeTotal);
                        break;
                    }

                    if (attempt < _config.MaxInsertRetries && disposition == CreateFailureDisposition.Retryable)
                    {
                        await Task.Delay(RetryDelay(ex, attempt + 1), cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    metrics.RecordError();
                    metrics.RecordRequestCharge(requestChargeTotal);

                    if (metrics.CosmosErrorSamplesLogged < _config.CosmosErrorSampleLimit)
                    {
                        metrics.CosmosErrorSamplesLogged++;
                        PrintErrorSample(ex, doc);
                    }

                    break;
                }
            }
        }
        finally
        {
            metrics.RecordUploadFinished(finish, finishEpoch);
            sem.Release();
        }

        return attemptWindows;
    }

    /// <summary>Inserts one doc and returns its per-attempt service-time windows in milliseconds.</summary>
    private delegate Task<List<double>> InsertOne<in T>(T doc, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken);

    /// <summary>Schedules one local bulk of item creates and records bulk service-time and success/error classification.</summary>
    private async Task InsertBulkAsync<T>(IReadOnlyList<T> docs, InsertOne<T> insertOne, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
    {
        if (docs.Count == 0)
        {
            return;
        }

        metrics.RecordBulkStarted(docs.Count);
        long errorsBefore = metrics.ErrorCountSnapshot();

        var tasks = new Task<List<double>>[docs.Count];
        for (int i = 0; i < docs.Count; i++)
        {
            tasks[i] = insertOne(docs[i], sem, metrics, cancellationToken);
        }

        List<double>[] windows = await Task.WhenAll(tasks).ConfigureAwait(false);

        var serviceTimeSamples = new List<double>();
        foreach (List<double> attemptWindows in windows)
        {
            serviceTimeSamples.AddRange(attemptWindows);
        }

        bool hadError = metrics.ErrorCountSnapshot() > errorsBefore;
        metrics.RecordBulkCompleted(hadError);
        metrics.RecordBulkSample(docs.Count, serviceTimeSamples, Clock.Now);
    }

    /// <summary>Consumes document bulks while bounding the number of pending bulk tasks via <c>MAX_PENDING_BULKS</c>.</summary>
    private async Task ScheduleBatchesAsync<T>(IAsyncEnumerable<List<T>> batches, InsertOne<T> insertOne, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
    {
        var pending = new List<Task>();
        await foreach (List<T> batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            pending.Add(InsertBulkAsync(batch, insertOne, sem, metrics, cancellationToken));
            if (pending.Count >= _config.MaxPendingBulks)
            {
                Task completed = await Task.WhenAny(pending).ConfigureAwait(false);
                await completed.ConfigureAwait(false);
                pending.Remove(completed);
            }
        }

        if (pending.Count > 0)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    /// <summary>Inserts batches of in-memory <see cref="JsonObject"/> documents (fake mode).</summary>
    public Task InsertDocBatchesAsync(IAsyncEnumerable<List<JsonObject>> batches, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
        => ScheduleBatchesAsync(batches, InsertDocAsync, sem, metrics, cancellationToken);

    /// <summary>Inserts batches of raw UTF-8 document bytes via stream writes (file mode fast path).</summary>
    public Task InsertRawBatchesAsync(IAsyncEnumerable<List<DataSource.RawDocument>> batches, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
        => ScheduleBatchesAsync(batches, InsertRawDocAsync, sem, metrics, cancellationToken);

    /// <summary>
    /// Creates one Cosmos item from raw UTF-8 bytes using <c>CreateItemStreamAsync</c>, avoiding a JsonObject round-trip.
    /// Parsing, partition-key/id preparation, and re-serialization happen here on the worker so they run in parallel.
    /// </summary>
    private async Task<List<double>> InsertRawDocAsync(DataSource.RawDocument rawDocument, SemaphoreSlim sem, WorkerMetrics metrics, CancellationToken cancellationToken)
    {
        await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
        double start = Clock.Now;
        double startEpoch = Clock.Epoch;
        double finish = start;
        double finishEpoch = startEpoch;
        var attemptWindows = new List<double>();
        metrics.RecordUploadStarted(start, startEpoch);
        double requestChargeTotal = 0.0;

        try
        {
            (byte[] payload, PartitionKey partitionKey) = PrepareRawDoc(rawDocument);

            for (int attempt = 0; attempt <= _config.MaxInsertRetries; attempt++)
            {
                double attemptStart = Clock.Now;
                metrics.RecordCreateItemAttempt();

                int statusCode;
                bool success;
                double charge = 0.0;
                TimeSpan? retryAfter = null;
                string? partitionKeyRangeId = null;

                try
                {
                    using var stream = new MemoryStream(payload, writable: false);
                    using ResponseMessage response = await _container
                        .CreateItemStreamAsync(stream, partitionKey, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    statusCode = (int)response.StatusCode;
                    success = response.IsSuccessStatusCode;
                    if (_config.CaptureRuCharges)
                    {
                        charge = response.Headers.RequestCharge;
                    }

                    if (_config.PartitionKeyRangeRpsEnabled)
                    {
                        partitionKeyRangeId = PartitionKeyRangeIdFromHeaders(response.Headers);
                    }

                    retryAfter = ParseRetryAfter(response.Headers);
                }
                catch (CosmosException ex)
                {
                    statusCode = (int)ex.StatusCode;
                    success = false;
                    if (_config.CaptureRuCharges)
                    {
                        charge = ex.RequestCharge;
                    }

                    if (_config.PartitionKeyRangeRpsEnabled)
                    {
                        partitionKeyRangeId = PartitionKeyRangeIdFromHeaders(ex.Headers);
                    }

                    retryAfter = ex.RetryAfter;
                }

                finish = Clock.Now;
                finishEpoch = Clock.Epoch;
                attemptWindows.Add((finish - attemptStart) * 1000.0);
                requestChargeTotal += charge;

                if (_config.PartitionKeyRangeRpsEnabled)
                {
                    metrics.RecordPartitionKeyRangeRequest(
                        partitionKeyRangeId,
                        throttled: statusCode == 429,
                        requestCharge: charge);
                }

                if (success)
                {
                    metrics.RecordSuccess();
                    metrics.RecordRequestCharge(requestChargeTotal);
                    break;
                }

                metrics.RecordCreateItemFailure();
                if (statusCode == 429)
                {
                    metrics.RecordThrottle();
                }

                CreateFailureDisposition disposition = ClassifyFailureStatus(statusCode);
                if (disposition == CreateFailureDisposition.Conflict)
                {
                    metrics.RecordConflict();
                    metrics.RecordRequestCharge(requestChargeTotal);
                    break;
                }

                if (attempt < _config.MaxInsertRetries && disposition == CreateFailureDisposition.Retryable)
                {
                    await Task.Delay(RetryDelay(retryAfter, attempt + 1), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                metrics.RecordError();
                metrics.RecordRequestCharge(requestChargeTotal);

                if (metrics.CosmosErrorSamplesLogged < _config.CosmosErrorSampleLimit)
                {
                    metrics.CosmosErrorSamplesLogged++;
                    PrintRawErrorSample(statusCode, payload);
                }

                break;
            }
        }
        finally
        {
            metrics.RecordUploadFinished(finish, finishEpoch);
            sem.Release();
        }

        return attemptWindows;
    }

    /// <summary>
    /// Parses raw record bytes and rewrites them into a Cosmos-ready payload, applying the same rules as
    /// <see cref="DataSource.PrepareLoadedDoc"/>: the partition key field is required and ids fall back to the
    /// partition key value.
    /// </summary>
    private (byte[] Payload, PartitionKey PartitionKey) PrepareRawDoc(DataSource.RawDocument rawDocument)
    {
        byte[] raw = rawDocument.Payload;
        using JsonDocument doc = JsonDocument.Parse(raw);
        JsonElement root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException($"Loaded record is {root.ValueKind}, expected a JSON object");
        }

        string[] missingFields = _partitionKeyFields.Where(field =>
            !root.TryGetProperty(field, out JsonElement element) || IsNullOrEmptyElement(element))
            .ToArray();
        if (missingFields.Length > 0)
        {
            string available = string.Join(", ", root.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).Take(20));
            throw new InvalidDataException(
                $"Loaded record is missing required partition key field(s) '{string.Join(", ", missingFields)}'. Available fields: {available}");
        }

        bool hasId = root.TryGetProperty("id", out JsonElement idElement) && !IsNullOrEmptyElement(idElement);
        bool hasFallbackElement = root.TryGetProperty(_config.DocumentIdFallbackField, out JsonElement fallbackElement)
            && !IsNullOrEmptyElement(fallbackElement);
        bool hasFallback = hasFallbackElement;
        if (!hasId && !hasFallback)
        {
            throw new InvalidDataException($"Loaded record is missing id and fallback field '{_config.DocumentIdFallbackField}'");
        }

        var buffer = new ArrayBufferWriter<byte>(raw.Length + 96);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();

            writer.WritePropertyName("id");
            if (hasId)
            {
                writer.WriteStringValue(ElementToString(idElement));
            }
            else
            {
                writer.WriteStringValue(ElementToString(fallbackElement));
            }

            foreach (JsonProperty property in root.EnumerateObject())
            {
                if (property.NameEquals("id"))
                {
                    continue;
                }

                property.WriteTo(writer);
            }

            writer.WriteEndObject();
        }

        byte[] payload = buffer.WrittenSpan.ToArray();
        var partitionKeyBuilder = new PartitionKeyBuilder();
        foreach (string field in _partitionKeyFields)
        {
            root.TryGetProperty(field, out JsonElement element);
            AddPartitionKeyComponent(partitionKeyBuilder, element);
        }
        PartitionKey partitionKey = partitionKeyBuilder.Build();
        return (payload, partitionKey);
    }

    private static void AddPartitionKeyComponent(PartitionKeyBuilder builder, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Number:
                builder.Add(element.GetDouble());
                break;
            case JsonValueKind.True:
                builder.Add(true);
                break;
            case JsonValueKind.False:
                builder.Add(false);
                break;
            default:
                builder.Add(element.ValueKind == JsonValueKind.String ? element.GetString() : element.GetRawText());
                break;
        }
    }

    private static string ElementToString(JsonElement element)
        => element.ValueKind == JsonValueKind.String ? element.GetString() ?? "" : element.GetRawText();

    private static bool IsNullOrEmptyElement(JsonElement element)
    {
        if (element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return true;
        }

        if (element.ValueKind == JsonValueKind.String)
        {
            return string.IsNullOrEmpty(element.GetString());
        }

        return false;
    }

    private PartitionKey ResolvePartitionKey(JsonObject doc)
    {
        if (_partitionKeyFields.Count == 0)
        {
            // Fake mode has no configured partition key field; partition on id.
            return new PartitionKey(doc["id"]!.ToString());
        }

        var builder = new PartitionKeyBuilder();
        foreach (string field in _partitionKeyFields)
        {
            if (!doc.TryGetPropertyValue(field, out JsonNode? node) || node is null)
            {
                throw new InvalidDataException($"Document is missing required partition key field '{field}'");
            }

            if (node is JsonValue value && value.TryGetValue(out double number))
            {
                builder.Add(number);
            }
            else if (node is JsonValue boolValue && boolValue.TryGetValue(out bool boolean))
            {
                builder.Add(boolean);
            }
            else
            {
                builder.Add(node.ToString());
            }
        }

        return builder.Build();
    }

    internal static CreateFailureDisposition ClassifyFailureStatus(int statusCode)
    {
        if (statusCode == (int)HttpStatusCode.Conflict)
        {
            return CreateFailureDisposition.Conflict;
        }

        return RetryableStatusCodes.Contains((HttpStatusCode)statusCode)
            ? CreateFailureDisposition.Retryable
            : CreateFailureDisposition.Error;
    }

    private static TimeSpan? ParseRetryAfter(Headers headers)
    {
        string? value = headers.Get("x-ms-retry-after-ms");
        if (!string.IsNullOrEmpty(value) && double.TryParse(value, out double ms))
        {
            return TimeSpan.FromMilliseconds(ms);
        }

        return null;
    }

    private static string? PartitionKeyRangeIdFromHeaders(Headers? headers)
    {
        if (headers is null)
        {
            return null;
        }

        foreach (string name in PartitionKeyRangeHeaderNames)
        {
            string? value = headers.Get(name);
            if (!string.IsNullOrEmpty(value))
            {
                return value;
            }
        }

        return null;
    }

    private TimeSpan RetryDelay(CosmosException ex, int attemptIndex)
    {
        if (ex.RetryAfter is { } retryAfter && retryAfter > TimeSpan.Zero)
        {
            return retryAfter;
        }

        return TimeSpan.FromMilliseconds(_config.InsertRetryDelayMs * Math.Max(attemptIndex, 1));
    }

    private TimeSpan RetryDelay(TimeSpan? retryAfter, int attemptIndex)
    {
        if (retryAfter is { } ra && ra > TimeSpan.Zero)
        {
            return ra;
        }

        return TimeSpan.FromMilliseconds(_config.InsertRetryDelayMs * Math.Max(attemptIndex, 1));
    }

    private void PrintRawErrorSample(int statusCode, byte[] payload)
    {
        string body = payload.Length <= 512
            ? System.Text.Encoding.UTF8.GetString(payload)
            : System.Text.Encoding.UTF8.GetString(payload, 0, 512) + "...";
        Console.WriteLine("\n[cosmos error sample]");
        Console.WriteLine($"status={statusCode}");
        Console.WriteLine($"partition_key_fields={string.Join(",", _partitionKeyFields)}");
        Console.WriteLine($"document={body}");
    }

    private void PrintErrorSample(CosmosException ex, JsonObject doc)
    {
        string partitionValue = string.Join(",", _partitionKeyFields.Select(field => doc[field]?.ToString() ?? ""));
        Console.WriteLine("\n[cosmos error sample]");
        Console.WriteLine($"status={(int)ex.StatusCode}");
        Console.WriteLine($"sub_status={ex.SubStatusCode}");
        Console.WriteLine($"id={doc["id"]}");
        Console.WriteLine($"partition_key_fields={string.Join(",", _partitionKeyFields)}");
        Console.WriteLine($"partition_key_value={partitionValue}");
        Console.WriteLine($"request_charge={ex.RequestCharge:F2}");
        Console.WriteLine($"activity_id={ex.ActivityId}");
        Console.WriteLine($"message={ex.Message}");
    }
}
