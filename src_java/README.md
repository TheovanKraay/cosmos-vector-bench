# Cosmos DB Vector Write Benchmark: Java (throughput control)

A Java port of the .NET benchmark (`src_dotnet/`) that writes synthetic vector documents to
Azure Cosmos DB. Unlike the Python (`src/`) and .NET (`src_dotnet/`) implementations, which issue
individual `createItem` calls governed only by manual concurrency knobs, this version uses the
**Cosmos Java SDK bulk API** together with the SDK's **throughput control** feature.

It exists to answer a specific question: *why does a large VM (e.g. 96 cores) fail to saturate a
high-RU Cosmos container, and how do you fix it?*

## TL;DR

- **Parallel bulk pipelines** are the engine. A *single* `executeBulkOperations` call will **not**
  saturate a large VM (e.g. 96 cores): the bulk executor's parallelism is bounded by
  (number of physical partitions) x (per-partition micro-batch concurrency, max 5), **not** by your
  core count. So this sample runs `BULK_WORKERS` independent bulk pipelines concurrently (default =
  available cores), each ingesting a shard of the data, mirroring the Azure distributed-bulk sample's
  `MAX_CONCURRENT_BATCHES_PER_MACHINE`.
- **Throughput control** is the ceiling, not the engine. All parallel pipelines share one
  throughput-control group, so aggregate RU is paced just under the container's limit (avoiding mass
  429s) and, in global mode, one RU budget is shared fairly across multiple cooperating clients.

The key insight: **you need both.** Parallelism without throughput control throttles the container into
429 collapse; throughput control without parallelism paces work that was never generated, leaving a
high RU budget unused. A single-pipeline design (or a single-threaded bulk loop) leaves a big VM idle
regardless of RU budget, which is exactly the saturation problem seen with the .NET sample.

## Validation: does this actually fix the .NET saturation problem?

This was checked by reading the Cosmos Java SDK 4.68.0 bulk executor bytecode and by cross-checking the
official [Azure distributed-bulk sample](https://github.com/Azure/azure-cosmos-distributed-bulk-sample),
not assumed:

- **A single bulk pipeline does not scale to CPU cores.** `executeBulkOperations` consumes one input
  stream; the executor groups by physical partition and paces each partition's micro-batch depth
  (default 1, max 5). It does **not** spin up worker threads proportional to core count. The official
  distributed sample confirms this: it exposes `MAX_CONCURRENT_BATCHES_PER_MACHINE` ("~25% to 100% of
  CPU cores") and runs **many** concurrent bulk executions per machine. If one bulk call saturated a
  machine, that knob would not exist. This sample therefore runs `BULK_WORKERS` parallel pipelines.
- **Cross-partition fan-out within each pipeline is automatic.** `BulkExecutor` keys a
  `ConcurrentMap<partitionKeyRangeId, PartitionScopeThresholds>` and runs each partition group
  concurrently, so writes spread across physical partitions without app-side orchestration.
- **Per-partition micro-batch concurrency defaults to 1** (`DEFAULT_MAX_BULK_MICRO_BATCH_CONCURRENCY`),
  with `maxMicroBatchSize = 100`. This sample raises `maxMicroBatchConcurrency` to the SDK maximum
  (enforced range **[1, 5]**; default 5 here, tunable via `MAX_MICRO_BATCH_CONCURRENCY`, clamped into
  range). Throughput control still caps aggregate RU, so this cannot cause a 429 storm.

**Honest caveat about the .NET baseline:** the .NET app (`src_dotnet/`) already sets
`AllowBulkExecution = true`, so it is *not* missing bulk batching. It has two real gaps: (1) no
**throughput control** (confirmed by grepping the source: no `ThroughputControlGroupConfig` /
`enableLocalThroughputControlGroup` / `enableGlobalThroughputControlGroup` anywhere in `src_dotnet/`;
the only matches are inside the compiled SDK DLL), so it runs open-loop and oscillates into 429
collapse; and (2) if it drives bulk from insufficient app-level concurrency, a single pipeline cannot
use all cores of a large VM. This Java sample addresses both: parallel pipelines for the engine, and a
shared throughput-control group for the ceiling. Absolute throughput numbers still require a run
against a real Cosmos account; the local vnext emulator surfaces an unrelated HTTP/2 transport quirk
(see Notes).

## What throughput control actually does in the Java SDK

There are two distinct mechanisms, and it's worth being precise about which does what.

### 1. Client-side RU rate limiting (always)

Each client tracks its own RU consumption and **pre-emptively paces** requests to stay under a target,
rather than firing freely and absorbing 429s. You configure the target one of two ways:

- **`targetThroughput`**: an absolute RU/s cap (e.g. 8000). **Required for serverless accounts**,
  where percentage-based thresholds aren't supported.
- **`targetThroughputThreshold`**: a fraction in `(0, 1]` of the container's *provisioned* RU/s
  (e.g. `0.9` = "use up to 90%"). This is the "saturate to N%" dial.

This is genuine client-side flow control: the SDK delays/queues operations to hold the line, which
keeps latency stable and avoids the throughput collapse that mass throttling causes.

### 2. Fair RU sharing across *clients* (global mode)

With `enableGlobalThroughputControlGroup`, the SDK maintains a dedicated **control container** in which
every client writes a small heartbeat/ledger item (renewed ~every 5s, expiring ~11s). Clients read
each other's entries and **divide the RU budget among themselves**. So 10 clients sharing a
100,000 RU/s budget converge to ~10,000 RU/s each, instead of each independently assuming it owns the
full budget and collectively hammering the container into throttling.

Use **local** control (`enableLocalThroughputControlGroup`) when a single client/instance should be
limited on its own; use **global** control when you scale out across nodes and want them to cooperate.

### Nuance: it's an RU *budget governor*, not a per-partition scheduler

A common misconception is that throughput control assigns RU quotas per physical partition and thereby
spreads load evenly across partitions. It does **not**. Throughput control governs the *aggregate* RU
budget (per client, or shared across clients).

**What makes writes land uniformly across partitions is the bulk executor**, and **what makes a large
VM fully utilised is running enough parallel bulk pipelines** (`BULK_WORKERS`). `executeBulkOperations`
groups operations into **per-partition-key-range micro-batches** and dispatches them in parallel across
all physical partitions. Throughput control then sits on top of that, pacing the overall rate so you
ride just under the RU ceiling.

So saturating a high-RU container from a big VM is **three cooperating layers**:

| Layer | Responsibility |
| --- | --- |
| Parallel bulk pipelines (`BULK_WORKERS`) | App-level parallelism to actually use all CPU cores |
| Bulk executor | Parallelism + per-partition-range batching for uniform partition utilization |
| Throughput control | Aggregate RU pacing + fair cross-client budget sharing to avoid throttling collapse |

Together they let one large VM drive a container to its RU ceiling, and let many clients scale out
without stepping on each other. Parallelism is the engine; throughput control is the ceiling. Neither
alone is enough.

## Layout

```
src_java/
  pom.xml                                   Maven build (shaded runnable jar)
  src/main/java/com/azure/cosmos/bench/
    Benchmark.java                          Entry point: client, throughput control, parallel bulk ingest
    BenchmarkConfig.java                    .env + environment configuration loader
    VectorDoc.java                          Synthetic document shape (id, docid, title, text, emb)
    ValidationMain.java                     Offline (no-network) validation harness
```

## Configuration

Reads a `.env` file (path via first CLI arg, default `../.env`) plus process environment
(environment wins). Knob names mirror the Python/.NET implementations where they overlap.

| Variable | Default | Meaning |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | *(required)* | Account endpoint URI |
| `COSMOS_KEY` | *(empty)* | Account key; empty means `DefaultAzureCredential` (AAD) |
| `COSMOS_DATABASE_NAME` | *(required)* | Database id |
| `COSMOS_CONTAINER_NAME` | *(required)* | Container id |
| `TOTAL_DOCS` | `1000000` | Number of docs to write |
| `FAKE_DATA_VECTOR_DIM` | `1536` | Embedding dimensionality |
| `PAYLOAD_BYTES` | `1000` | Filler size of the `text` field |
| `COSMOS_PARTITION_KEY_FIELD` | `docid` | Partition key field (path `/docid`) |
| `BULK_SIZE` | `100` | Micro-batch target hint (analogue of the other ports' `BULK_SIZE`) |
| `MAX_MICRO_BATCH_CONCURRENCY` | `5` | Per-partition in-flight batches (SDK default 1, valid range [1,5]) |
| `MAX_MICRO_BATCH_SIZE` | `100` | Ops per micro-batch (SDK direct-mode cap is 100) |
| `BULK_WORKERS` | *(available cores)* | Number of concurrent bulk pipelines (app-level parallelism to use all cores) |
| `USE_GATEWAY_MODE` | `false` | Gateway vs. direct transport |
| `COSMOS_PREFERRED_REGION` | *(empty)* | Optional preferred region |
| `THROUGHPUT_CONTROL_ENABLED` | `true` | Enable throughput control |
| `THROUGHPUT_CONTROL_GROUP_NAME` | `vectorBulkIngest` | Control group name |
| `THROUGHPUT_CONTROL_GLOBAL` | `true` | Global (distributed) vs. local control |
| `THROUGHPUT_CONTROL_TARGET_THRESHOLD` | `0.95` | Fraction (0,1] of provisioned RU/s |
| `THROUGHPUT_CONTROL_TARGET_RU` | *(empty)* | Absolute RU/s cap; **overrides** threshold; required for serverless |
| `THROUGHPUT_CONTROL_CONTAINER` | `ThroughputControl` | Control container (global mode) |
| `CREATE_CONTAINER` | `false` | Create DB/container (vector policy + quantizedFlat index on `/emb`) |
| `AUTOSCALE_MAX_THROUGHPUT` | `100000` | Autoscale max RU/s when provisioning |

For serverless accounts, set `THROUGHPUT_CONTROL_TARGET_RU` to an absolute RU/s value
(percentage thresholds are not supported there).

## Build & run

Requires JDK 17+ and Maven.

```bash
cd src_java
mvn -B package -DskipTests
java -jar target/cosmos-vector-bench-java.jar ../.env
```

The build produces a shaded (fat) jar with `com.azure.cosmos.bench.Benchmark` as the main class.

## Offline validation

Because a reachable Cosmos account (or working emulator) may not be available in every environment,
an offline validation harness exercises the configuration, throughput-control-group construction, and
bulk-option tuning code paths and asserts the resulting values, with no network required:

```bash
java -cp target/cosmos-vector-bench-java.jar com.azure.cosmos.bench.ValidationMain
```

It verifies, among other things, that per-partition micro-batch concurrency is set within the SDK's
enforced `[1, 5]` range (out-of-range values are clamped, so the app never throws at runtime), that an
absolute RU target correctly overrides the percentage threshold (serverless path), and that both
threshold and absolute-RU throughput-control groups build successfully. Exit code is non-zero if any
check fails.

## Notes

- Writes use `contentResponseOnWriteEnabled(false)` to minimize response bytes and maximize write
  throughput.
- Optional container provisioning creates a vector embedding policy (`FLOAT32`, cosine) and a
  `quantizedFlat` vector index on `/emb`, matching the benchmark's vector-search intent.
- Against the local Cosmos DB emulator (vnext), the underlying SDK transport can currently surface an
  HTTP/2 / TLS handshake quirk (`not an SSL/TLS record`) unrelated to this code; validate at scale
  against a real account or the classic emulator.
