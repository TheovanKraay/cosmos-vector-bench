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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

        CosmosBulkExecutionOptions options = new CosmosBulkExecutionOptions();
        if (cfg.throughputControlEnabled && groupConfig != null) {
            options.setThroughputControlGroupName(cfg.throughputControlGroup);
        }

        // Stream operations lazily so we never materialise all docs in memory. The bulk executor
        // groups these into RU-aware micro-batches per physical partition automatically.
        Random rnd = new Random();
        Flux<CosmosItemOperation> operations = Flux.range(0, cfg.totalDocs)
                .map(i -> {
                    List<Float> emb = new ArrayList<>(cfg.vectorDim);
                    for (int d = 0; d < cfg.vectorDim; d++) {
                        emb.add(rnd.nextFloat() * 2.0f - 1.0f); // [-1, 1]
                    }
                    String docid = UUID.randomUUID().toString();
                    VectorDoc doc = new VectorDoc(
                            UUID.randomUUID().toString(), docid, "Document " + i, filler, emb);
                    return CosmosBulkOperations.getCreateItemOperation(doc, new PartitionKey(docid));
                });

        long start = System.nanoTime();
        container.executeBulkOperations(operations, options)
                .doOnNext(response -> {
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
                })
                .then()
                .block();

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
