package com.azure.cosmos.bench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads benchmark configuration from the repo-root {@code .env} file and process environment.
 *
 * <p>Mirrors the knob names and defaults used by the Python ({@code src/config.py}) and .NET
 * ({@code src_dotnet/BenchmarkConfig.cs}) implementations so the three ports are comparable.
 * Process environment variables win over {@code .env} values (override=false semantics), matching
 * the other implementations.</p>
 */
public final class BenchmarkConfig {

    // Connection
    public final String endpoint;
    public final String key;            // empty => DefaultAzureCredential
    public final String database;
    public final String container;

    // Workload (fake mode) — same knobs as the .NET/Python versions
    public final int totalDocs;         // TOTAL_DOCS
    public final int vectorDim;         // FAKE_DATA_VECTOR_DIM
    public final int payloadBytes;      // PAYLOAD_BYTES
    public final String partitionKeyField; // COSMOS_PARTITION_KEY_FIELD (fake docs use "docid")

    // Ingest / bulk
    public final int bulkFlushMicroBatch; // maps to Cosmos bulk micro-batch target (BULK_SIZE analogue)
    public final int maxMicroBatchConcurrency; // per-partition in-flight batches (SDK default 1)
    public final int maxMicroBatchSize;        // ops per micro-batch (SDK cap 100 in direct mode)
    public final boolean useGatewayMode;  // needed for the HTTP-only emulator; DIRECT for production
    public final String preferredRegion;  // optional, empty => none

    // Throughput control — the whole point of this port
    public final boolean throughputControlEnabled;   // THROUGHPUT_CONTROL_ENABLED (default true)
    public final String throughputControlGroup;      // THROUGHPUT_CONTROL_GROUP_NAME
    public final Double targetThroughputThreshold;   // 0..1 fraction of provisioned RU, or null
    public final Integer targetThroughput;           // absolute RU/s, or null
    public final boolean globalControl;              // true => distributed global control container
    public final String controlContainer;            // THROUGHPUT_CONTROL container name

    // Optional container provisioning
    public final boolean createContainer;            // CREATE_CONTAINER (default false)
    public final int autoScaleMaxThroughput;         // AUTOSCALE_MAX_THROUGHPUT for provisioning

    private BenchmarkConfig(Map<String, String> c) {
        this.endpoint = req(c, "COSMOS_ENDPOINT");
        this.key = get(c, "COSMOS_KEY", "");
        this.database = req(c, "COSMOS_DATABASE_NAME");
        this.container = req(c, "COSMOS_CONTAINER_NAME");

        this.totalDocs = intVal(c, "TOTAL_DOCS", 1_000_000);
        this.vectorDim = intVal(c, "FAKE_DATA_VECTOR_DIM", 1536);
        this.payloadBytes = intVal(c, "PAYLOAD_BYTES", 1000);
        this.partitionKeyField = get(c, "COSMOS_PARTITION_KEY_FIELD", "docid");

        this.bulkFlushMicroBatch = intVal(c, "BULK_SIZE", 100);
        this.maxMicroBatchConcurrency = intVal(c, "MAX_MICRO_BATCH_CONCURRENCY", 8);
        this.maxMicroBatchSize = Math.min(intVal(c, "MAX_MICRO_BATCH_SIZE", 100), 100);
        this.useGatewayMode = boolVal(c, "USE_GATEWAY_MODE", false);
        this.preferredRegion = get(c, "COSMOS_PREFERRED_REGION", "");

        this.throughputControlEnabled = boolVal(c, "THROUGHPUT_CONTROL_ENABLED", true);
        this.throughputControlGroup = get(c, "THROUGHPUT_CONTROL_GROUP_NAME", "vectorBulkIngest");
        String threshold = get(c, "THROUGHPUT_CONTROL_TARGET_THRESHOLD", "0.95");
        String absolute = get(c, "THROUGHPUT_CONTROL_TARGET_RU", "");
        this.targetThroughput = absolute.isBlank() ? null : Integer.parseInt(absolute.trim());
        // If an absolute RU target is set, it takes precedence and threshold is ignored.
        this.targetThroughputThreshold = (this.targetThroughput != null || threshold.isBlank())
                ? null : Double.parseDouble(threshold.trim());
        this.globalControl = boolVal(c, "THROUGHPUT_CONTROL_GLOBAL", true);
        this.controlContainer = get(c, "THROUGHPUT_CONTROL_CONTAINER", "ThroughputControl");

        this.createContainer = boolVal(c, "CREATE_CONTAINER", false);
        this.autoScaleMaxThroughput = intVal(c, "AUTOSCALE_MAX_THROUGHPUT", 100_000);
    }

    /** Load from an optional {@code .env} file plus process environment (env wins). */
    public static BenchmarkConfig load(String envPath) {
        Map<String, String> merged = new HashMap<>();
        Path p = Path.of(envPath);
        if (Files.exists(p)) {
            try (BufferedReader r = new BufferedReader(new FileReader(envPath))) {
                String line;
                while ((line = r.readLine()) != null) {
                    String t = line.trim();
                    if (t.isEmpty() || t.startsWith("#")) continue;
                    int eq = t.indexOf('=');
                    if (eq <= 0) continue;
                    String k = t.substring(0, eq).trim();
                    String v = t.substring(eq + 1).trim();
                    if (v.length() >= 2 && ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'")))) {
                        v = v.substring(1, v.length() - 1);
                    }
                    merged.put(k, v);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read .env at " + envPath, e);
            }
        }
        // Process env overrides .env (override=false semantics: existing env wins).
        for (Map.Entry<String, String> e : System.getenv().entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }
        return new BenchmarkConfig(merged);
    }

    private static String req(Map<String, String> c, String k) {
        String v = c.get(k);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing required config: " + k);
        return v.trim();
    }
    private static String get(Map<String, String> c, String k, String d) {
        String v = c.get(k);
        return (v == null || v.isBlank()) ? d : v.trim();
    }
    private static int intVal(Map<String, String> c, String k, int d) {
        String v = c.get(k);
        try { return (v == null || v.isBlank()) ? d : Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return d; }
    }
    private static boolean boolVal(Map<String, String> c, String k, boolean d) {
        String v = c.get(k);
        if (v == null || v.isBlank()) return d;
        String t = v.trim().toLowerCase();
        return t.equals("1") || t.equals("true") || t.equals("yes") || t.equals("on");
    }
}
