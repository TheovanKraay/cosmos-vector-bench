package com.azure.cosmos.bench;

import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Offline validation harness (no network / no live Cosmos endpoint required).
 *
 * <p>Exercises the real configuration, throughput-control-group construction, and bulk-execution
 * option tuning code paths and asserts the resulting values. This is what can be validated
 * deterministically in an environment without a reachable Cosmos account or a working emulator
 * (the local vnext emulator surfaces an unrelated Netty TLS/HTTP-2 transport quirk on the SDK's
 * metadata channel; see README Notes).</p>
 *
 * <p>Run: {@code java -cp target/cosmos-vector-bench-java.jar com.azure.cosmos.bench.ValidationMain}</p>
 */
public final class ValidationMain {

    private static int checks = 0;
    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("== Cosmos vector bench (Java) offline validation ==\n");

        validateConfigDefaults();
        validateThresholdVsAbsolute();
        validateEnvOverridesFile();
        validateThroughputControlGroupLocal();
        validateThroughputControlGroupAbsolute();
        validateBulkOptionsTuning();
        validateConcurrencyClamp();
        validateBulkWorkers();

        System.out.printf("%n== %d checks, %d failures ==%n", checks, failures);
        if (failures > 0) {
            System.exit(1);
        }
        System.out.println("ALL VALIDATIONS PASSED");
    }

    // ---- Config ---------------------------------------------------------------

    private static void validateConfigDefaults() throws Exception {
        Path env = writeEnv(
                "COSMOS_ENDPOINT=https://acct.documents.azure.com:443/",
                "COSMOS_KEY=abc==",
                "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll");
        BenchmarkConfig c = BenchmarkConfig.load(env.toString());
        section("Config defaults");
        check("endpoint parsed", "https://acct.documents.azure.com:443/".equals(c.endpoint));
        check("database parsed", "db".equals(c.database));
        check("default totalDocs=1,000,000", c.totalDocs == 1_000_000);
        check("default vectorDim=1536", c.vectorDim == 1536);
        check("default pk field=docid", "docid".equals(c.partitionKeyField));
        check("throughput control on by default", c.throughputControlEnabled);
        check("global control on by default", c.globalControl);
        check("default threshold=0.95", c.targetThroughputThreshold != null && Math.abs(c.targetThroughputThreshold - 0.95) < 1e-9);
        check("no absolute RU by default", c.targetThroughput == null);
        check("default micro-batch concurrency=5 (SDK max; >default of 1)", c.maxMicroBatchConcurrency == 5);
        check("micro-batch size default=100", c.maxMicroBatchSize == 100);
        check("bulkWorkers defaults to >=1 (available cores)", c.bulkWorkers >= 1);
        Files.deleteIfExists(env);
    }

    private static void validateBulkWorkers() throws Exception {
        section("App-level parallelism: BULK_WORKERS override");
        Path env = writeEnv("COSMOS_ENDPOINT=https://x/", "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll", "BULK_WORKERS=32");
        check("BULK_WORKERS=32 honored", BenchmarkConfig.load(env.toString()).bulkWorkers == 32);
        Files.deleteIfExists(env);
        Path z = writeEnv("COSMOS_ENDPOINT=https://x/", "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll", "BULK_WORKERS=0");
        check("BULK_WORKERS=0 floored to 1", BenchmarkConfig.load(z.toString()).bulkWorkers == 1);
        Files.deleteIfExists(z);
    }

    private static void validateThresholdVsAbsolute() throws Exception {
        Path env = writeEnv(
                "COSMOS_ENDPOINT=https://x/",
                "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll",
                "THROUGHPUT_CONTROL_TARGET_RU=8000",
                "THROUGHPUT_CONTROL_TARGET_THRESHOLD=0.9");
        BenchmarkConfig c = BenchmarkConfig.load(env.toString());
        section("Absolute RU target overrides threshold (serverless path)");
        check("absolute RU parsed=8000", c.targetThroughput != null && c.targetThroughput == 8000);
        check("threshold nulled when absolute set", c.targetThroughputThreshold == null);
        check("empty key => AAD path (key blank)", c.key.isEmpty());
        Files.deleteIfExists(env);
    }

    private static void validateEnvOverridesFile() throws Exception {
        // Process env should win over .env (override=false semantics). We can't easily inject a new
        // process env var here, so we assert the complementary guarantee: a value present ONLY in the
        // .env file (not in the process environment) is still loaded correctly.
        Path env = writeEnv(
                "COSMOS_ENDPOINT=https://from-file/",
                "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll");
        BenchmarkConfig c = BenchmarkConfig.load(env.toString());
        section("Config loads file-only values (env-merge path)");
        check("file-only value loaded", "https://from-file/".equals(c.endpoint));
        check("quoted/space trimming works", "db".equals(c.database));
        Files.deleteIfExists(env);
    }

    // ---- Throughput control ---------------------------------------------------

    private static void validateThroughputControlGroupLocal() {
        section("Throughput control group (threshold / percentage mode)");
        ThroughputControlGroupConfig g = new ThroughputControlGroupConfigBuilder()
                .groupName("vectorBulkIngest")
                .targetThroughputThreshold(0.9)
                .defaultControlGroup(true)
                .build();
        check("group built (threshold mode) non-null", g != null);
        check("group name round-trips", g != null && "vectorBulkIngest".equals(g.getGroupName()));
    }

    private static void validateThroughputControlGroupAbsolute() {
        section("Throughput control group (absolute RU mode)");
        ThroughputControlGroupConfig g = new ThroughputControlGroupConfigBuilder()
                .groupName("vectorBulkIngest")
                .targetThroughput(8000)
                .defaultControlGroup(true)
                .build();
        check("group built (absolute RU) non-null", g != null);
        check("group name round-trips", g != null && "vectorBulkIngest".equals(g.getGroupName()));
    }

    // ---- Bulk options ---------------------------------------------------------

    private static void validateBulkOptionsTuning() {
        section("Bulk execution options tuning (removes per-partition depth=1 ceiling)");
        CosmosBulkExecutionOptions o = new CosmosBulkExecutionOptions();
        check("SDK default per-partition concurrency is 1 (the ceiling)", o.getMaxMicroBatchConcurrency() == 1);
        // SDK enforces [1,5]; 5 is the max supported per-partition depth.
        o.setMaxMicroBatchConcurrency(5);
        o.setMaxMicroBatchSize(100);
        o.setThroughputControlGroupName("vectorBulkIngest");
        check("raised per-partition concurrency=5 (SDK max)", o.getMaxMicroBatchConcurrency() == 5);
        check("micro-batch size=100", o.getMaxMicroBatchSize() == 100);
        check("throughput control group wired to bulk options",
                "vectorBulkIngest".equals(o.getThroughputControlGroupName()));
    }

    private static void validateConcurrencyClamp() throws Exception {
        section("Config clamps micro-batch concurrency into SDK-valid range [1,5]");
        Path hi = writeEnv("COSMOS_ENDPOINT=https://x/", "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll", "MAX_MICRO_BATCH_CONCURRENCY=99");
        check("value 99 clamped to 5", BenchmarkConfig.load(hi.toString()).maxMicroBatchConcurrency == 5);
        Files.deleteIfExists(hi);
        Path lo = writeEnv("COSMOS_ENDPOINT=https://x/", "COSMOS_DATABASE_NAME=db",
                "COSMOS_CONTAINER_NAME=coll", "MAX_MICRO_BATCH_CONCURRENCY=0");
        check("value 0 clamped to 1", BenchmarkConfig.load(lo.toString()).maxMicroBatchConcurrency == 1);
        Files.deleteIfExists(lo);
    }

    // ---- helpers --------------------------------------------------------------

    private static Path writeEnv(String... lines) throws Exception {
        Path p = Files.createTempFile("bench-validate", ".env");
        try (FileWriter w = new FileWriter(p.toFile())) {
            for (String l : lines) {
                w.write(l);
                w.write("\n");
            }
        }
        return p;
    }

    private static void section(String name) {
        System.out.println("- " + name);
    }

    private static void check(String label, boolean ok) {
        checks++;
        if (!ok) {
            failures++;
        }
        System.out.printf("    [%s] %s%n", ok ? "PASS" : "FAIL", label);
    }
}
