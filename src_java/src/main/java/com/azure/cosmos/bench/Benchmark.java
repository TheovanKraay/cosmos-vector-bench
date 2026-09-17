package com.azure.cosmos.bench;

import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.GlobalThroughputControlConfig;
import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkExecutionThresholdsState;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosVectorEmbedding;
import com.azure.cosmos.models.CosmosVectorEmbeddingPolicy;
import com.azure.cosmos.models.CosmosVectorDataType;
import com.azure.cosmos.models.CosmosVectorDistanceFunction;
import com.azure.cosmos.models.CosmosVectorIndexSpec;
import com.azure.cosmos.models.CosmosVectorIndexType;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java port of the Cosmos vector write benchmark that uses the <b>Cosmos Java SDK bulk API</b>
 * ({@code CosmosBulkOperations} / {@code executeBulkOperations}) together with the SDK's
 * <b>throughput control</b> feature.
 *
 * <p>Unlike the Python/.NET ports (which fire individual {@code createItem} calls and rely on manual
 * concurrency knobs with no RU governor), this port lets a single client drive right up to a target
 * fraction of the container's provisioned RU/s. When {@code globalControl} is enabled, the control
 * state is shared through a dedicated control container, so multiple clients/instances cooperatively
 * share the RU budget and the service spreads consumption across physical partitions.</p>
 *
 * <p>See {@code README.md} for what throughput control actually does under the hood.</p>
 */
public final class Benchmark {

    public static void main(String[] args) throws Exception {
        String envPath = args.length > 0 ? args[0] : "../.env";
        BenchmarkConfig cfg = BenchmarkConfig.load(envPath);

        System.out.printf(
                "Config: endpoint=%s db=%s container=%s totalDocs=%,d vectorDim=%d pk=/%s%n",
                cfg.endpoint, cfg.database, cfg.container, cfg.totalDocs, cfg.vectorDim, cfg.partitionKeyField);
        System.out.printf(
                "Throughput control: enabled=%s group=%s global=%s threshold=%s targetRU=%s%n",
                cfg.throughputControlEnabled, cfg.throughputControlGroup, cfg.globalControl,
                cfg.targetThroughputThreshold, cfg.targetThroughput);
        System.out.printf("Bulk tuning: initialMicroBatchSize=%d maxMicroBatchConcurrency=%d "
                + "maxMicroBatchSize=%d bulkWorkers=%d maxInFlight/worker=%d maxRetryCount=%d%n",
                cfg.initialMicroBatchSize, cfg.maxMicroBatchConcurrency, cfg.maxMicroBatchSize,
                cfg.bulkWorkers, cfg.maxInFlightPerWorker, cfg.maxRetryCount);

        CosmosAsyncClient client = buildClient(cfg);
        try {
            CosmosAsyncDatabase database = client.getDatabase(cfg.database);
            if (cfg.createContainer) {
                provisionContainer(client, cfg);
            }
            CosmosAsyncContainer container = database.getContainer(cfg.container);

            ThroughputControlGroupConfig groupConfig = null;
            if (cfg.throughputControlEnabled) {
                groupConfig = buildThroughputControlGroup(cfg);
                if (cfg.globalControl) {
                    // Distributed control: state lives in a dedicated control container so multiple
                    // clients share one RU budget cooperatively.
                    GlobalThroughputControlConfig global = client.createGlobalThroughputControlConfigBuilder(
                                    cfg.database, cfg.controlContainer)
                            .setControlItemRenewInterval(Duration.ofSeconds(5))
                            .setControlItemExpireInterval(Duration.ofSeconds(11))
                            .build();
                    container.enableGlobalThroughputControlGroup(groupConfig, global);
                } else {
                    // Local control: limits RU usage within this single client instance only.
                    container.enableLocalThroughputControlGroup(groupConfig);
                }
                System.out.println("Throughput control group enabled: " + cfg.throughputControlGroup);
            }

            runBulkIngest(container, cfg, groupConfig);
        } finally {
            client.close();
        }
    }

    private static CosmosAsyncClient buildClient(BenchmarkConfig cfg) {
        CosmosClientBuilder builder = new CosmosClientBuilder()
                .endpoint(cfg.endpoint)
                .contentResponseOnWriteEnabled(false); // fewer bytes back on writes, higher throughput

        if (!cfg.key.isBlank()) {
            builder.key(cfg.key);
        } else {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }
        if (cfg.useGatewayMode) {
            builder.gatewayMode();
        } else {
            builder.directMode();
        }
        if (!cfg.preferredRegion.isBlank()) {
            builder.preferredRegions(List.of(cfg.preferredRegion));
        }
        return builder.buildAsyncClient();
    }

    private static ThroughputControlGroupConfig buildThroughputControlGroup(BenchmarkConfig cfg) {
        ThroughputControlGroupConfigBuilder b = new ThroughputControlGroupConfigBuilder()
                .groupName(cfg.throughputControlGroup)
                // This group is the default for all requests from this client on the container.
                .defaultControlGroup(true);
        if (cfg.targetThroughput != null) {
            // Absolute RU/s cap. Required for serverless accounts (percentage not supported there).
            b.targetThroughput(cfg.targetThroughput);
        } else if (cfg.targetThroughputThreshold != null) {
            // Fraction (0,1] of the container's provisioned RU/s: the "saturate to N%" dial.
            b.targetThroughputThreshold(cfg.targetThroughputThreshold);
        }
        return b.build();
    }

    private static void runBulkIngest(CosmosAsyncContainer container, BenchmarkConfig cfg,
                                      ThroughputControlGroupConfig groupConfig) {
        String filler = "x".repeat(Math.max(cfg.payloadBytes, 1));
        AtomicLong success = new AtomicLong();
        AtomicLong throttled = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong totalRu = new AtomicLong(); // RU * 1000, fixed-point to stay lock-free

        // ---------------------------------------------------------------------------------------
        // ARCHITECTURE (faithful single-VM analogue of the Azure distributed-bulk sample):
        //
        //  * PARALLELISM is the engine. A single executeBulkOperations() consumes one input and its
        //    parallelism is bounded by (#physical partitions) x (per-partition micro-batch depth,
        //    max 5), NOT by CPU cores. So we run `bulkWorkers` INDEPENDENT pipelines (BulkWorker),
        //    one per shard, to actually use a large VM. Mirrors MAX_CONCURRENT_BATCHES_PER_MACHINE.
        //
        //  * Each worker feeds its bulk call from a LIVE Sinks.Many emitter (not a finite Flux) so
        //    the app-level RETRY path can re-inject throttled/transient failures into the SAME
        //    running pipeline, with jittered backoff honoring server Retry-After. A per-worker
        //    SEMAPHORE bounds in-flight ops to keep memory flat while the pipeline stays full.
        //
        //  * ADAPTIVE MICRO-BATCH SIZING: all workers share one JVM-scoped
        //    CosmosBulkExecutionThresholdsState, so the SDK's feedback loop grows batch size from
        //    initialMicroBatchSize toward the container's sweet spot instead of overshooting into
        //    429s. This is the SDK's own auto-tuner and is arguably the biggest saturation lever.
        //
        //  * THROUGHPUT CONTROL is the ceiling, not the engine. All workers share one control group
        //    (wired via bulk options) so aggregate RU is paced just under the container limit.
        // ---------------------------------------------------------------------------------------
        int workers = Math.max(1, cfg.bulkWorkers);
        long total = cfg.totalDocs;

        // One thresholds state shared across all workers => one adaptive-sizing feedback loop.
        CosmosBulkExecutionThresholdsState thresholds = new CosmosBulkExecutionThresholdsState();

        // A scheduled pool for jittered retries; a response scheduler so response handling never
        // blocks the SDK's IO threads; a launcher pool to run each blocking worker shard.
        Scheduler responseScheduler = Schedulers.newBoundedElastic(
                Math.max(workers, Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE),
                Integer.MAX_VALUE, "bulk-responses");
        ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(
                Math.max(1, workers / 4 + 1));
        ExecutorService launcher = Executors.newFixedThreadPool(workers);

        System.out.printf("Parallel bulk workers: %d (each: live sink + semaphore + app-level retry, "
                + "sharing one adaptive-thresholds state and throughput-control group)%n", workers);

        long start = System.nanoTime();
        try {
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < workers; w++) {
                long shardStart = (total * w) / workers;
                long shardEnd = (total * (w + 1)) / workers;
                BulkWorker worker = new BulkWorker(container, cfg, thresholds, retryScheduler,
                        responseScheduler, success, throttled, failed, totalRu);
                futures.add(launcher.submit(() -> worker.runShard(shardStart, shardEnd, filler)));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Bulk ingest failed", e);
        } finally {
            launcher.shutdownNow();
            retryScheduler.shutdownNow();
            responseScheduler.dispose();
        }

        double elapsedSec = (System.nanoTime() - start) / 1e9;
        double docsPerSec = success.get() / Math.max(elapsedSec, 1e-9);
        double ru = totalRu.get() / 1000.0;
        System.out.printf(
                "%nDone: succeeded=%,d throttled(429)=%,d failed=%,d in %.1fs => %,.0f docs/sec, %,.0f RU consumed (%.0f RU/s avg)%n",
                success.get(), throttled.get(), failed.get(), elapsedSec, docsPerSec, ru,
                ru / Math.max(elapsedSec, 1e-9));
    }

    /** Optionally create the target container with a vector embedding policy + quantizedFlat index on /emb. */
    private static void provisionContainer(CosmosAsyncClient client, BenchmarkConfig cfg) {
        client.createDatabaseIfNotExists(cfg.database).block();
        CosmosAsyncDatabase db = client.getDatabase(cfg.database);

        String vectorPath = "/emb";

        CosmosVectorEmbedding embedding = new CosmosVectorEmbedding();
        embedding.setPath(vectorPath);
        embedding.setDataType(CosmosVectorDataType.FLOAT32);
        embedding.setDimensions((long) cfg.vectorDim);
        embedding.setDistanceFunction(CosmosVectorDistanceFunction.COSINE);
        CosmosVectorEmbeddingPolicy embeddingPolicy = new CosmosVectorEmbeddingPolicy();
        embeddingPolicy.setCosmosVectorEmbeddings(List.of(embedding));

        CosmosVectorIndexSpec vectorIndex = new CosmosVectorIndexSpec();
        vectorIndex.setPath(vectorPath);
        vectorIndex.setType(CosmosVectorIndexType.QUANTIZED_FLAT.toString());

        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setIncludedPaths(List.of(new IncludedPath("/*")));
        indexingPolicy.setExcludedPaths(List.of(new ExcludedPath("/emb/*"), new ExcludedPath("/\"_etag\"/?")));
        indexingPolicy.setVectorIndexes(List.of(vectorIndex));

        CosmosContainerProperties props =
                new CosmosContainerProperties(cfg.container, "/" + cfg.partitionKeyField);
        props.setVectorEmbeddingPolicy(embeddingPolicy);
        props.setIndexingPolicy(indexingPolicy);

        ThroughputProperties throughput =
                ThroughputProperties.createAutoscaledThroughput(cfg.autoScaleMaxThroughput);
        db.createContainerIfNotExists(props, throughput).block();
        System.out.printf("Provisioned container %s (autoscale max %,d RU/s, quantizedFlat vector index on %s)%n",
                cfg.container, cfg.autoScaleMaxThroughput, vectorPath);
    }
}
