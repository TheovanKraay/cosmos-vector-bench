using System.Globalization;

namespace CosmosVectorBench;

/// <summary>
/// Command entrypoint for the .NET Cosmos DB write benchmark.
/// Parses CLI overrides, applies them as environment variables (mirroring the Python <c>main.py</c>),
/// loads configuration, and dispatches to the fake-data or file-input benchmark mode.
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            CliArgs parsed = ParseArgs(args);
            if (parsed.Search == true && string.IsNullOrEmpty(parsed.PartitionKeyMode))
            {
                throw new ArgumentException("--search requires --partition-key-mode hpk|docid|sessionid");
            }

            ApplyOverrides(parsed);
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }

        BenchmarkConfig config;
        try
        {
            config = BenchmarkConfig.Load();
        }
        catch (InvalidOperationException ex)
        {
            Console.Error.WriteLine($"Configuration error: {ex.Message}");
            return 2;
        }

        Console.Write("\n");
        if (config.SearchEnabled)
        {
            Console.WriteLine(
                $"Starting search benchmark for num_clients={config.ClientProcesses}, " +
                $"queries_per_second_per_client={config.SearchQueriesPerSecond}, " +
                $"total_queries={config.SearchTotalQueries}, " +
                $"warmup_queries={(config.SearchWarmupEnabled ? BenchmarkConfig.SearchWarmupQueryCount : 0)}, " +
                $"partition_key_fields={string.Join(',', config.PartitionKeyFields)}");
        }
        else
        {
            string syntheticSessions = config.DataType == "fake"
                ? $", session_pool_size={config.SessionPoolSize}, " +
                  $"session_write_quantum_docs={config.SessionWriteQuantumDocs}, " +
                  $"session_docs={config.SessionIdMinDocs}-{config.SessionIdMaxDocs}"
                : "";
            string inFlight = config.MaxInFlightTotal > 0
                ? $", max_in_flight_total={config.MaxInFlightTotal} (shared)"
                : $", max_in_flight={config.MaxInFlight}/client (total {(long)config.ClientProcesses * config.MaxInFlight})";
            Console.WriteLine(
                $"Starting up benchmark run for num_clients={config.ClientProcesses}, " +
                $"bulk_size={config.BulkSize}, max_documents={config.EffectiveTotalDocs}, " +
                $"partition_key_fields={string.Join(',', config.PartitionKeyFields)}{inFlight}{syntheticSessions}");
        }

        var benchmark = new Benchmark(config);
        return await benchmark.RunAsync().ConfigureAwait(false);
    }

    private sealed class CliArgs
    {
        public int? NumClients { get; set; }
        public int? BulkSize { get; set; }
        public int? TotalDocs { get; set; }
        public string? DataType { get; set; }
        public string? DataPath { get; set; }
        public string? ContainerName { get; set; }
        public string? PartitionKeyMode { get; set; }
        public int? SessionPoolSize { get; set; }
        public int? SessionWriteQuantumDocs { get; set; }
        public bool? Search { get; set; }
        public bool? Warmup { get; set; }
        public int? QueriesPerSecond { get; set; }
        public int? TotalQueries { get; set; }
    }

    private static CliArgs ParseArgs(string[] args)
    {
        var parsed = new CliArgs();
        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            switch (arg)
            {
                case "--num-clients" or "--num_clients":
                    parsed.NumClients = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "--bulk-size" or "--bulk_size":
                    parsed.BulkSize = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "--total-docs" or "--total_docs":
                    parsed.TotalDocs = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "--data-type" or "--data_type":
                    parsed.DataType = DataTypeValue(NextValue(args, ref i, arg), arg);
                    break;
                case "--data-path" or "--data_path":
                    parsed.DataPath = NextValue(args, ref i, arg);
                    break;
                case "--container-name" or "--container_name":
                    parsed.ContainerName = NextValue(args, ref i, arg);
                    break;
                case "--partition-key-mode" or "--partition_key_mode":
                    parsed.PartitionKeyMode = PartitionKeyModeValue(NextValue(args, ref i, arg), arg);
                    break;
                case "--session-pool-size" or "--session_pool_size":
                    parsed.SessionPoolSize = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "--session-write-quantum-docs" or "--session_write_quantum_docs":
                    parsed.SessionWriteQuantumDocs = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "--search":
                    parsed.Search = true;
                    if (i + 1 < args.Length && !args[i + 1].StartsWith('-'))
                    {
                        parsed.Search = BooleanValue(NextValue(args, ref i, arg), arg);
                    }
                    break;
                case "--warmup":
                    parsed.Warmup = true;
                    if (i + 1 < args.Length && !args[i + 1].StartsWith('-'))
                    {
                        parsed.Warmup = BooleanValue(NextValue(args, ref i, arg), arg);
                    }
                    break;
                case "--queries-per-second" or "--queries_per_second":
                    parsed.QueriesPerSecond = IntInRange(NextValue(args, ref i, arg), arg, 1, 100);
                    break;
                case "--total-queries" or "--total_queries":
                    parsed.TotalQueries = PositiveInt(NextValue(args, ref i, arg), arg);
                    break;
                case "-h" or "--help":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
                default:
                    throw new ArgumentException($"Unknown argument: {arg}");
            }
        }

        return parsed;
    }

    private static void ApplyOverrides(CliArgs args)
    {
        if (args.NumClients is int numClients)
        {
            string value = numClients.ToString(CultureInfo.InvariantCulture);
            Environment.SetEnvironmentVariable("NUM_CLIENTS", value);
            Environment.SetEnvironmentVariable("CLIENTS", value);
            Environment.SetEnvironmentVariable("CLIENT_PROCESSES", value);
        }

        if (args.BulkSize is int bulkSize)
        {
            Environment.SetEnvironmentVariable("BULK_SIZE", bulkSize.ToString(CultureInfo.InvariantCulture));
        }

        if (args.TotalDocs is int totalDocs)
        {
            string value = totalDocs.ToString(CultureInfo.InvariantCulture);
            Environment.SetEnvironmentVariable("TOTAL_DOCS", value);
            Environment.SetEnvironmentVariable("MAX_TOTAL_DOCS", value);
        }

        if (!string.IsNullOrEmpty(args.DataType))
        {
            Environment.SetEnvironmentVariable("DATA_TYPE", args.DataType);
        }

        if (!string.IsNullOrEmpty(args.DataPath))
        {
            Environment.SetEnvironmentVariable("DATA_TYPE", "file");
            Environment.SetEnvironmentVariable("DOC_JSON_PATH", args.DataPath);
        }

        if (!string.IsNullOrEmpty(args.ContainerName))
        {
            Environment.SetEnvironmentVariable("COSMOS_CONTAINER_NAME", args.ContainerName);
        }

        if (!string.IsNullOrEmpty(args.PartitionKeyMode))
        {
            string partitionKeyFields = args.PartitionKeyMode switch
            {
                "hpk" => "sessionid,docid",
                "docid" => "docid",
                "sessionid" => "sessionid",
                _ => throw new InvalidOperationException($"Unexpected partition-key mode: {args.PartitionKeyMode}"),
            };
            Environment.SetEnvironmentVariable("SESSION_ID_ENABLED", args.PartitionKeyMode == "docid" ? "false" : "true");
            Environment.SetEnvironmentVariable("PARTITION_KEY_FIELDS", partitionKeyFields);
            Environment.SetEnvironmentVariable("DOCUMENT_ID_FALLBACK_FIELD", "docid");
            Environment.SetEnvironmentVariable("PARTITION_KEY_MODE_EXPLICIT", "true");
        }

        if (args.SessionPoolSize is int sessionPoolSize)
        {
            Environment.SetEnvironmentVariable("SESSION_POOL_SIZE", sessionPoolSize.ToString(CultureInfo.InvariantCulture));
        }

        if (args.SessionWriteQuantumDocs is int sessionWriteQuantumDocs)
        {
            Environment.SetEnvironmentVariable("SESSION_WRITE_QUANTUM_DOCS", sessionWriteQuantumDocs.ToString(CultureInfo.InvariantCulture));
        }

        if (args.Search is bool search)
        {
            Environment.SetEnvironmentVariable("SEARCH_ENABLED", search ? "true" : "false");
        }

        if (args.Warmup is bool warmup)
        {
            Environment.SetEnvironmentVariable("SEARCH_WARMUP_ENABLED", warmup ? "true" : "false");
        }

        if (args.QueriesPerSecond is int queriesPerSecond)
        {
            Environment.SetEnvironmentVariable("SEARCH_QUERIES_PER_SECOND", queriesPerSecond.ToString(CultureInfo.InvariantCulture));
        }

        if (args.TotalQueries is int totalQueries)
        {
            Environment.SetEnvironmentVariable("SEARCH_TOTAL_QUERIES", totalQueries.ToString(CultureInfo.InvariantCulture));
        }
    }

    private static string NextValue(string[] args, ref int i, string flag)
    {
        if (i + 1 >= args.Length)
        {
            throw new ArgumentException($"{flag} requires a value");
        }

        return args[++i];
    }

    private static int PositiveInt(string value, string flag)
    {
        if (!int.TryParse(value.Replace("_", "").Replace(",", ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
        {
            throw new ArgumentException($"{flag}: '{value}' must be an integer");
        }

        if (parsed < 1)
        {
            throw new ArgumentException($"{flag}: '{value}' must be >= 1");
        }

        return parsed;
    }

    private static int IntInRange(string value, string flag, int minimum, int maximum)
    {
        int parsed = PositiveInt(value, flag);
        if (parsed > maximum)
        {
            throw new ArgumentException($"{flag}: '{value}' must be between {minimum} and {maximum}");
        }

        return parsed;
    }

    private static bool BooleanValue(string value, string flag)
    {
        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => throw new ArgumentException($"{flag}: '{value}' must be true or false"),
        };
    }

    private static string DataTypeValue(string value, string flag)
    {
        string normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "fake" => "fake",
            "file" or "json" => "file",
            _ => throw new ArgumentException($"{flag}: '{value}' must be one of: fake, file, json"),
        };
    }

    private static string PartitionKeyModeValue(string value, string flag)
    {
        string normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "hpk" => "hpk",
            "docid" => "docid",
            "sessionid" => "sessionid",
            _ => throw new ArgumentException($"{flag}: '{value}' must be one of: hpk, docid, sessionid"),
        };
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Run the Cosmos DB write benchmark (.NET).");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --num-clients <n>      Override NUM_CLIENTS from .env.");
        Console.WriteLine("  --bulk-size <n>        Override BULK_SIZE from .env.");
        Console.WriteLine("  --total-docs <n>       Override TOTAL_DOCS and MAX_TOTAL_DOCS from .env.");
        Console.WriteLine("  --data-type <type>     Select data source: fake | file | json (json is an alias for file).");
        Console.WriteLine("  --data-path <path>     Override DOC_JSON_PATH and run with DATA_TYPE=file.");
        Console.WriteLine("  --container-name <name> Override COSMOS_CONTAINER_NAME from .env.");
        Console.WriteLine("  --partition-key-mode <mode> Select hpk (sessionid,docid), docid, or sessionid.");
        Console.WriteLine("  --session-pool-size <n> Fake mode: number of concurrently active generated session IDs.");
        Console.WriteLine("  --session-write-quantum-docs <n> Fake mode: docs per active session before rotation.");
        Console.WriteLine("  --search [true|false]    Run vector searches instead of document inserts; requires --partition-key-mode.");
        Console.WriteLine("  --warmup [true|false]    Run 1000 untimed vector search queries before the test (default true).");
        Console.WriteLine("  --queries-per-second <n> Target query starts per second per client (1-100, default 1).");
        Console.WriteLine("  --total-queries <n>      Total queries shared across all clients (default 1000).");
    }
}
