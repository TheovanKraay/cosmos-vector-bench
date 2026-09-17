package com.azure.cosmos.bench;

import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.GlobalThroughputControlConfig;
import com.azure.cosmos.ThroughputControlGroupConfig;
import com.azure.cosmos.ThroughputControlGroupConfigBuilder;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
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
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
        System.out.printf("Bulk tuning: maxMicroBatchConcurrency=%d maxMicroBatchSize=%d bulkWorkers=%d%n",
                cfg.maxMicroBatchConcurrency, cfg.maxMicroBatchSize, cfg.bulkWorkers);

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
            // Fraction (0,1] of the container's provisioned RU/s — the "saturate to N%" dial.
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
        // WHY MULTIPLE PARALLEL BULK PIPELINES (and not a single executeBulkOperations call):
        //
        // A single executeBulkOperations() consumes ONE input Flux. The bulk executor groups by
        // physical partition and paces each partition's micro-batch depth (SDK default 1, max 5).
        // It does NOT spin up worker threads proportional to CPU cores -- overall parallelism is
        // bounded by (#physical partitions) x (micro-batch concurrency <= 5), not by core count.
        // On a large VM (e.g. 96 cores) a single pipeline therefore leaves most cores idle, which is
        // exactly the saturation problem the .NET sample hits.
        //
        // The Azure distributed-bulk sample solves this with MAX_CONCURRENT_BATCHES_PER_MACHINE
        // ("~25%-100% of CPU cores") -- i.e. it runs MANY concurrent bulk executions per machine.
        // We do the same here: shard the workload across `bulkWorkers` independent pipelines, each
        // running its own executeBulkOperations, all bound to the SAME throughput-control group so
        // aggregate RU stays governed. Parallelism (the engine) comes from the workers; throughput
        // control (the ceiling) keeps them from collectively throttling the container.
        //
        // Throughput control is a ceiling, not an engine: it can only pace work that the pipelines
        // actually generate. Without enough parallel pipelines a high RU budget stays unused.
        // ---------------------------------------------------------------------------------------
        int workers = Math.max(1, cfg.bulkWorkers);
        long total = cfg.totalDocs;

        CosmosBulkExecutionOptions options = new CosmosBulkExecutionOptions();
        if (cfg.throughputControlEnabled && groupConfig != null) {
            options.setThroughputControlGroupName(cfg.throughputControlGroup);
        }
        // SDK per-partition micro-batch depth (default 1, valid range [1,5]); config clamps into range.
        options.setMaxMicroBatchConcurrency(cfg.maxMicroBatchConcurrency);
        options.setMaxMicroBatchSize(cfg.maxMicroBatchSize);

        // A dedicated bounded scheduler so the parallel pipelines don't starve the SDK's own I/O
        // schedulers. Sized to the worker count.
        Scheduler pool = Schedulers.newBoundedElastic(
                Math.max(workers, Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE),
                Integer.MAX_VALUE, "bulk-workers");

        System.out.printf("Parallel bulk workers: %d (each a separate executeBulkOperations pipeline, "
                + "sharing throughput-control group)%n", workers);

        long start = System.nanoTime();
        try {
            // Launch `workers` independent bulk pipelines, each owning a contiguous shard of the doc
            // range, and run them concurrently. Flux.merge with concurrency=workers subscribes to all.
            Flux.range(0, workers)
                    .flatMap(w -> {
                        long shardStart = (total * w) / workers;
                        long shardEnd = (total * (w + 1)) / workers;
                        Flux<CosmosItemOperation> ops =
                                buildOperations(shardStart, shardEnd, cfg, filler)
                                        .subscribeOn(pool);
                        return container.executeBulkOperations(ops, options)
                                .doOnNext(response -> record(response, success, throttled, failed, totalRu))
                                .subscribeOn(pool);
                    }, workers)
                    .then()
                    .block();
        } finally {
            pool.dispose();
        }

        double elapsedSec = (System.nanoTime() - start) / 1e9;
        double docsPerSec = success.get() / Math.max(elapsedSec, 1e-9);
        double ru = totalRu.get() / 1000.0;
        System.out.printf(
                "%nDone: succeeded=%,d throttled(429)=%,d failed=%,d in %.1fs => %,.0f docs/sec, %,.0f RU consumed (%.0f RU/s avg)%n",
                success.get(), throttled.get(), failed.get(), elapsedSec, docsPerSec, ru,
                ru / Math.max(elapsedSec, 1e-9));
    }

    /** Build a lazy stream of create operations for the half-open doc-index shard [start, end). */
    private static Flux<CosmosItemOperation> buildOperations(long start, long end, BenchmarkConfig cfg,
                                                             String filler) {
        return Flux.range(0, (int) (end - start))
                .map(offset -> {
                    long i = start + offset;
                    // Thread-local RNG: each pipeline runs on its own worker thread, so a shared
                    // Random would be a contention point. ThreadLocalRandom avoids that.
                    java.util.concurrent.ThreadLocalRandom rnd =
                            java.util.concurrent.ThreadLocalRandom.current();
                    List<Float> emb = new ArrayList<>(cfg.vectorDim);
                    for (int d = 0; d < cfg.vectorDim; d++) {
                        emb.add(rnd.nextFloat() * 2.0f - 1.0f); // [-1, 1]
                    }
                    String docid = UUID.randomUUID().toString();
                    VectorDoc doc = new VectorDoc(
                            UUID.randomUUID().toString(), docid, "Document " + i, filler, emb);
                    return CosmosBulkOperations.getCreateItemOperation(doc, new PartitionKey(docid));
                });
    }

    /** Tally one bulk item response into the shared counters. */
    private static void record(CosmosBulkOperationResponse<?> response, AtomicLong success,
                               AtomicLong throttled, AtomicLong failed, AtomicLong totalRu) {
        CosmosBulkItemResponse item = response.getResponse();
        if (item != null && item.isSuccessStatusCode()) {
            success.incrementAndGet();
            totalRu.addAndGet((long) (item.getRequestCharge() * 1000));
        } else {
            int status = item != null ? item.getStatusCode() : -1;
            if (status == 429) {
                throttled.incrementAndGet();
            } else {
                failed.incrementAndGet();
                if (failed.get() <= 5) {
                    Exception ex = response.getException();
                    System.out.printf("  [error sample] status=%d ex=%s%n",
                            status, ex != null ? ex.getMessage() : "n/a");
                }
            }
        }
        long done = success.get();
        if (done > 0 && done % 100_000 == 0) {
            System.out.printf("  ... %,d succeeded%n", done);
        }
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
