using System.Globalization;
using DotNetEnv;

namespace CosmosVectorBench;

/// <summary>
/// Loads and validates benchmark configuration from the root <c>.env</c> file and process environment.
/// Mirrors the behavior of the Python <c>src/config.py</c> module, including override=false semantics
/// (existing process environment variables win over <c>.env</c> values) and the same knob names and defaults.
/// </summary>
public sealed class BenchmarkConfig
{
    public const int SearchWarmupQueryCount = 1000;

    public string Endpoint { get; private init; } = "";
    public string Database { get; private init; } = "";
    public string Container { get; private init; } = "";
    public string CosmosKey { get; private init; } = "";

    public int TotalDocs { get; private init; }
    public int? MaxTotalDocs { get; private init; }
    public int ClientProcesses { get; private init; }
    public int BulkSize { get; private init; }
    public int MaxInFlight { get; private init; }

    /// <summary>
    /// Optional cap on concurrent operations across ALL workers combined (<c>MAX_IN_FLIGHT_TOTAL</c>).
    /// Zero keeps the legacy per-worker behaviour, where offered load is clients x MaxInFlight.
    /// </summary>
    public int MaxInFlightTotal { get; private init; }

    /// <summary>
    /// Optional client-side consistency level (<c>COSMOS_CONSISTENCY_LEVEL</c>). Cosmos only allows
    /// relaxing below the account default, never strengthening. Empty uses the account default.
    /// </summary>
    public string ConsistencyLevelOverride { get; private init; } = "";
    public int MaxPendingBulks { get; private init; }
    public int MaxInsertRetries { get; private init; }
    public int InsertRetryDelayMs { get; private init; }
    public bool CaptureRuCharges { get; private init; }
    public bool EnableContentResponseOnWrite { get; private init; }
    public bool UseStreamWriter { get; private init; }
    public bool PartitionKeyRangeRpsEnabled { get; private init; }

    /// <summary>Provisioned container RU/s, used only to express per-partition utilisation. 0 = unknown.</summary>
    public int ContainerThroughputRu { get; private init; }

    /// <summary>When set, appends a per-sample, per-partition-key-range CSV so instantaneous skew can be measured.</summary>
    public string PkRangeSeriesPath { get; private init; } = "";
    public int PayloadBytes { get; private init; }
    public int FakeDataVectorDim { get; private init; }
    public bool SessionIdEnabled { get; private init; }
    public int SessionIdMinDocs { get; private init; }
    public int SessionIdMaxDocs { get; private init; }
    public int SessionPoolSize { get; private init; }
    public int SessionWriteQuantumDocs { get; private init; }
    public bool SearchEnabled { get; private init; }
    public bool SearchWarmupEnabled { get; private init; }
    public int SearchQueriesPerSecond { get; private init; }
    public int SearchTotalQueries { get; private init; }
    public string CosmosVectorPath { get; private init; } = "/emb";
    public int CosmosVectorDimensions { get; private init; }

    public double LiveIntervalSec { get; private init; }
    public double MetricsSampleIntervalSec { get; private init; }
    public int MetricsTimingSampleInterval { get; private init; }
    public double MetricsWarmupSec { get; private init; }

    public string DataType { get; private init; } = "fake";
    public string DocJsonPath { get; private init; } = "";
    public string DocJsonFormat { get; private init; } = "jsonl";
    public int ReadBatchSize { get; private init; }
    public int DocQueueMultiplier { get; private init; }
    public string PartitionKeyField { get; private init; } = "";
    public IReadOnlyList<string> PartitionKeyFields { get; private init; } = [];
    public string DocumentIdFallbackField { get; private init; } = "docid";
    public int CosmosErrorSampleLimit { get; private init; }

    public int EffectiveTotalDocs { get; private init; }
    public bool CsvOutputEnabled { get; private init; }
    public string TestResultsRoot { get; private init; } = "results";
    public DateTime RunStartedAt { get; private init; }

    /// <summary>Absolute path of the repository root (parent of <c>src_dotnet</c>).</summary>
    public string ProjectRoot { get; private init; } = "";

    private BenchmarkConfig() { }

    /// <summary>
    /// Loads configuration by reading the root <c>.env</c> file without overriding existing process
    /// environment variables, then parsing and validating every benchmark knob.
    /// </summary>
    public static BenchmarkConfig Load()
    {
        string projectRoot = ResolveProjectRoot();
        string envPath = Path.Combine(projectRoot, ".env");
        if (File.Exists(envPath))
        {
            // override=false: existing process env vars win over .env values (matches python load_dotenv override=False).
            Env.Load(envPath, new LoadOptions(setEnvVars: true, clobberExistingVars: false, onlyExactPath: true));
        }

        string endpoint = RequireEnv("COSMOS_ENDPOINT");
        string database = RequireEnv("COSMOS_DATABASE_NAME");
        string container = RequireEnv("COSMOS_CONTAINER_NAME");

        int totalDocs = IntEnv("TOTAL_DOCS", 1_000_000);
        int? maxTotalDocs = OptionalIntEnv("MAX_TOTAL_DOCS");

        int clientProcesses = HasEnv("NUM_CLIENTS")
            ? IntEnv("NUM_CLIENTS", 1)
            : IntEnvAlias("CLIENTS", "CLIENT_PROCESSES", 1);

        int bulkSize = IntEnv("BULK_SIZE", 100);
        int maxInFlightAuto = (int)Math.Ceiling(bulkSize * 1.5);
        int maxInFlight = IntEnvAliasOrAuto("MAX_IN_FLIGHT", "MAX_CONCURRENCY", Math.Max(bulkSize * 2, 40), maxInFlightAuto);
        int maxInFlightTotal = IntEnv("MAX_IN_FLIGHT_TOTAL", 0, 0);
        int maxPendingBulksDefault = Math.Max(1, Math.Min(8, ((maxInFlight + bulkSize - 1) / bulkSize) * 2));
        int maxPendingBulks = IntEnv("MAX_PENDING_BULKS", maxPendingBulksDefault);

        string dataType = (GetEnv("DATA_TYPE") ?? "fake").Trim().ToLowerInvariant();
        string docJsonPath = (GetEnv("DOC_JSON_PATH") ?? GetEnv("DATA_FILE_PATH") ?? "./data/open_ai_corpus-initial-indexing.json").Trim();
        string docJsonFormat = (GetEnv("DOC_JSON_FORMAT") ?? "jsonl").Trim().ToLowerInvariant();
        string partitionKeyField = (GetEnv("PARTITION_KEY_FIELD") ?? "").Trim();
        string[] partitionKeyFields = (GetEnv("PARTITION_KEY_FIELDS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(field => field.TrimStart('/'))
            .Where(field => field.Length > 0)
            .ToArray();
        if (partitionKeyFields.Length == 0 && partitionKeyField.Length > 0)
        {
            partitionKeyFields = [partitionKeyField.TrimStart('/')];
        }

        bool hpkSessionMode = dataType == "fake"
            && partitionKeyFields.SequenceEqual(["sessionid", "docid"], StringComparer.OrdinalIgnoreCase);
        int sessionIdMinDocs = IntEnv("SESSION_ID_MIN_DOCS", 10, 1);
        int sessionIdMaxDocs = IntEnv("SESSION_ID_MAX_DOCS", 1000, 1);

        int effectiveTotalDocs = maxTotalDocs.HasValue ? Math.Min(totalDocs, maxTotalDocs.Value) : totalDocs;
        double liveInterval = FloatEnv("LIVE_INTERVAL_SEC", 1.0, 0.1);
        bool searchEnabled = BoolEnv("SEARCH_ENABLED", false);

        var config = new BenchmarkConfig
        {
            ProjectRoot = projectRoot,
            Endpoint = endpoint,
            Database = database,
            Container = container,
            CosmosKey = (GetEnv("COSMOS_KEY") ?? "").Trim(),
            TotalDocs = totalDocs,
            MaxTotalDocs = maxTotalDocs,
            ClientProcesses = clientProcesses,
            BulkSize = bulkSize,
            MaxInFlight = maxInFlight,
            MaxInFlightTotal = maxInFlightTotal,
            ConsistencyLevelOverride = (GetEnv("COSMOS_CONSISTENCY_LEVEL") ?? "").Trim(),
            MaxPendingBulks = maxPendingBulks,
            MaxInsertRetries = IntEnv("MAX_INSERT_RETRIES", 5, 0),
            InsertRetryDelayMs = IntEnv("INSERT_RETRY_DELAY_MS", 50, 0),
            CaptureRuCharges = BoolEnv("CAPTURE_RU_CHARGES", true),
            EnableContentResponseOnWrite = BoolEnv("ENABLE_CONTENT_RESPONSE_ON_WRITE", false),
            UseStreamWriter = BoolEnv("USE_STREAM_WRITER", true),
            PartitionKeyRangeRpsEnabled = BoolEnv("PARTITION_KEY_RANGE_RPS_ENABLED", false),
            ContainerThroughputRu = IntEnv("CONTAINER_THROUGHPUT_RU", 0, 0),
            PkRangeSeriesPath = (GetEnv("PKRANGE_SERIES_PATH") ?? "").Trim(),
            PayloadBytes = IntEnv("PAYLOAD_BYTES", 5000, 0),
            FakeDataVectorDim = IntEnv("FAKE_DATA_VECTOR_DIM", 1536, 0),
            SessionIdEnabled = BoolEnv("SESSION_ID_ENABLED", false),
            SessionIdMinDocs = sessionIdMinDocs,
            SessionIdMaxDocs = sessionIdMaxDocs,
            SessionPoolSize = IntEnv("SESSION_POOL_SIZE", hpkSessionMode ? 1000 : 1, 1),
            SessionWriteQuantumDocs = IntEnv("SESSION_WRITE_QUANTUM_DOCS", hpkSessionMode ? 10 : sessionIdMaxDocs, 1),
            SearchEnabled = searchEnabled,
            SearchWarmupEnabled = searchEnabled ? BoolEnv("SEARCH_WARMUP_ENABLED", true) : true,
            SearchQueriesPerSecond = searchEnabled ? IntEnv("SEARCH_QUERIES_PER_SECOND", 1, 1) : 1,
            SearchTotalQueries = searchEnabled ? IntEnv("SEARCH_TOTAL_QUERIES", 1000, 1) : 1000,
            CosmosVectorPath = (GetEnv("COSMOS_VECTOR_PATH") ?? "/emb").Trim(),
            CosmosVectorDimensions = searchEnabled ? IntEnv("COSMOS_VECTOR_DIMENSIONS", 1536, 1) : 1536,
            LiveIntervalSec = liveInterval,
            MetricsSampleIntervalSec = FloatEnv("METRICS_SAMPLE_INTERVAL_SEC", liveInterval, 0.1),
            MetricsTimingSampleInterval = IntEnv("METRICS_TIMING_SAMPLE_INTERVAL", 1),
            MetricsWarmupSec = FloatEnv("METRICS_WARMUP_SEC", 0.0),
            DataType = dataType,
            DocJsonPath = docJsonPath,
            DocJsonFormat = docJsonFormat,
            ReadBatchSize = IntEnv("READ_BATCH_SIZE", bulkSize),
            DocQueueMultiplier = IntEnv("DOC_QUEUE_MULTIPLIER", 4),
            PartitionKeyField = partitionKeyField,
            PartitionKeyFields = partitionKeyFields,
            DocumentIdFallbackField = (GetEnv("DOCUMENT_ID_FALLBACK_FIELD") ?? (partitionKeyField.Length > 0 ? partitionKeyField : "docid")).Trim().TrimStart('/'),
            CosmosErrorSampleLimit = IntEnv("COSMOS_ERROR_SAMPLE_LIMIT", 3, 0),
            EffectiveTotalDocs = effectiveTotalDocs,
            CsvOutputEnabled = BoolEnv("CSV_OUTPUT_ENABLED", true),
            TestResultsRoot = (GetEnv("TEST_RESULTS_ROOT")?.Trim() is { Length: > 0 } root) ? root : "results",
            RunStartedAt = ResolveRunStartedAt(),
        };

        config.Validate();
        return config;
    }

    private void Validate()
    {
        if (DataType is not ("fake" or "file"))
        {
            throw new InvalidOperationException("DATA_TYPE must be one of: fake, file");
        }

        if (SessionIdMaxDocs < SessionIdMinDocs)
        {
            throw new InvalidOperationException("SESSION_ID_MAX_DOCS must be greater than or equal to SESSION_ID_MIN_DOCS");
        }

        if (SearchQueriesPerSecond > 100)
        {
            throw new InvalidOperationException("SEARCH_QUERIES_PER_SECOND must be between 1 and 100");
        }

        if (SearchEnabled && !BoolEnv("PARTITION_KEY_MODE_EXPLICIT", false))
        {
            throw new InvalidOperationException("Search mode requires an explicit --partition-key-mode argument");
        }

        if (SearchEnabled && PartitionKeyFields.Count == 0)
        {
            throw new InvalidOperationException("Search mode requires partition-key fields");
        }

        if (SearchEnabled)
        {
            ValidateVectorPath(CosmosVectorPath);
        }

        if (DataType == "file" && string.IsNullOrEmpty(DocJsonPath))
        {
            throw new InvalidOperationException("DOC_JSON_PATH is required when DATA_TYPE=file");
        }

        if (DataType == "file" && PartitionKeyFields.Count == 0)
        {
            throw new InvalidOperationException("PARTITION_KEY_FIELDS or PARTITION_KEY_FIELD is required when DATA_TYPE=file");
        }

        if (string.IsNullOrEmpty(DocumentIdFallbackField))
        {
            throw new InvalidOperationException("DOCUMENT_ID_FALLBACK_FIELD must not be empty");
        }

        if (DocJsonFormat is not ("array" or "jsonl" or "multiple_values"))
        {
            throw new InvalidOperationException("DOC_JSON_FORMAT must be one of: array, jsonl, multiple_values");
        }
    }

    private static void ValidateVectorPath(string path)
    {
        if (!path.StartsWith('/') || path.Length < 2)
        {
            throw new InvalidOperationException("COSMOS_VECTOR_PATH must start with '/' and name at least one property");
        }

        string[] segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Any(segment => segment.Any(character => !char.IsLetterOrDigit(character) && character is not ('_' or '-'))))
        {
            throw new InvalidOperationException("COSMOS_VECTOR_PATH segments may contain only letters, digits, '_' and '-'");
        }
    }

    /// <summary>Builds the document-count label used in metrics CSV filenames.</summary>
    public string TotalDocsLabel()
    {
        if (MaxTotalDocs.HasValue)
        {
            return MaxTotalDocs.Value.ToString(CultureInfo.InvariantCulture);
        }

        return DataType == "fake" ? EffectiveTotalDocs.ToString(CultureInfo.InvariantCulture) : "all";
    }

    private static string ResolveProjectRoot()
    {
        // The published/run assembly lives under src_dotnet/bin/...; walk up to find the repo root (the folder containing .env or src_dotnet).
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, ".env")) ||
                Directory.Exists(Path.Combine(dir.FullName, "src_dotnet")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        return Directory.GetCurrentDirectory();
    }

    private static DateTime ResolveRunStartedAt()
    {
        const string envName = "BENCHMARK_RUN_STARTED_AT";
        string? raw = GetEnv(envName);
        if (!string.IsNullOrWhiteSpace(raw) && DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            return parsed;
        }

        var value = DateTime.Now;
        value = new DateTime(value.Year, value.Month, value.Day, value.Hour, value.Minute, value.Second, DateTimeKind.Local);
        Environment.SetEnvironmentVariable(envName, value.ToString("o", CultureInfo.InvariantCulture));
        return value;
    }

    // -------- environment parsing helpers (mirror src/config.py) --------

    private static string? GetEnv(string name)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return value;
    }

    private static bool HasEnv(string name) => !string.IsNullOrWhiteSpace(GetEnv(name));

    private static string RequireEnv(string name)
    {
        string value = (GetEnv(name) ?? "").Trim();
        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Missing required environment variable: {name}");
        }

        return value;
    }

    private static int IntEnv(string name, int defaultValue, int minimum = 1)
    {
        string raw = (GetEnv(name) ?? "").Trim();
        if (string.IsNullOrEmpty(raw))
        {
            return defaultValue;
        }

        if (!int.TryParse(raw.Replace("_", ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
        {
            throw new InvalidOperationException($"{name} must be an integer, got '{raw}'");
        }

        if (value < minimum)
        {
            throw new InvalidOperationException($"{name} must be >= {minimum}, got {value}");
        }

        return value;
    }

    private static int? OptionalIntEnv(string name, int minimum = 1)
    {
        string raw = (GetEnv(name) ?? "").Trim();
        if (string.IsNullOrEmpty(raw))
        {
            return null;
        }

        if (!int.TryParse(raw.Replace("_", ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
        {
            throw new InvalidOperationException($"{name} must be an integer, got '{raw}'");
        }

        if (value < minimum)
        {
            throw new InvalidOperationException($"{name} must be >= {minimum}, got {value}");
        }

        return value;
    }

    private static double FloatEnv(string name, double defaultValue, double minimum = 0.0)
    {
        string raw = (GetEnv(name) ?? "").Trim();
        if (string.IsNullOrEmpty(raw))
        {
            return defaultValue;
        }

        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
        {
            throw new InvalidOperationException($"{name} must be a number, got '{raw}'");
        }

        if (value < minimum)
        {
            throw new InvalidOperationException($"{name} must be >= {minimum}, got {value}");
        }

        return value;
    }

    private static bool BoolEnv(string name, bool defaultValue)
    {
        string raw = (GetEnv(name) ?? "").Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(raw))
        {
            return defaultValue;
        }

        if (raw is "1" or "true" or "yes" or "y" or "on")
        {
            return true;
        }

        if (raw is "0" or "false" or "no" or "n" or "off")
        {
            return false;
        }

        throw new InvalidOperationException($"{name} must be a boolean value, got '{raw}'");
    }

    private static int IntEnvAlias(string primary, string fallback, int defaultValue, int minimum = 1)
    {
        return HasEnv(primary) ? IntEnv(primary, defaultValue, minimum) : IntEnv(fallback, defaultValue, minimum);
    }

    private static int IntEnvAliasOrAuto(string primary, string fallback, int defaultValue, int autoValue)
    {
        string envName = HasEnv(primary) ? primary : fallback;
        string raw = (GetEnv(envName) ?? "").Trim();
        if (string.IsNullOrEmpty(raw))
        {
            return defaultValue;
        }

        if (!int.TryParse(raw.Replace("_", ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
        {
            throw new InvalidOperationException($"{envName} must be an integer, got '{raw}'");
        }

        return value < 1 ? autoValue : value;
    }
}
