# Cosmos DB Vector Write Benchmark — Java (throughput control)

A Java port of the .NET benchmark (`src_dotnet/`) that writes synthetic vector documents to
Azure Cosmos DB. Unlike the Python (`src/`) and .NET (`src_dotnet/`) implementations — which issue
individual `createItem` calls governed only by manual concurrency knobs — this version uses the
**Cosmos Java SDK bulk API** together with the SDK's **throughput control** feature.

It exists to answer a specific question: *why does a large VM (e.g. 96 cores) fail to saturate a
high-RU Cosmos container, and how do you fix it?*

## TL;DR

- **Bulk executor** (`executeBulkOperations`) → parallelism across physical partitions. This is what
  actually saturates a container uniformly.
- **Throughput control** → paces aggregate RU consumption just under the container's ceiling (avoiding
  mass 429s) and, in global mode, fairly shares one RU budget across multiple cooperating clients.

The single async-loop-per-worker design used by the Python/.NET apps has *neither* a real
partition-aware batching layer *nor* an RU governor, which is exactly why extra CPU sits idle: the
bottleneck is not CPU, it's how requests are dispatched and paced.

## Validation: does this actually fix the .NET saturation problem?

This was checked by reading the Cosmos Java SDK 4.68.0 bulk executor bytecode, not assumed:

- **Cross-partition fan-out is automatic.** `BulkExecutor` keys a `ConcurrentMap<partitionKeyRangeId,
  PartitionScopeThresholds>` and runs each partition group concurrently, so writes spread across
  physical partitions without app-side orchestration.
- **Per-partition micro-batch concurrency defaults to 1** (`Configs.DEFAULT_MAX_BULK_MICRO_BATCH_CONCURRENCY = 1`),
  with `maxMicroBatchSize = 100`. On a container with few physical partitions this per-partition depth
  of 1 can under-drive a high-RU container. This benchmark therefore **explicitly raises**
  `maxMicroBatchConcurrency` (default 8 here, via `MAX_MICRO_BATCH_CONCURRENCY`) so each partition keeps
  several batches in flight. Throughput control still caps aggregate RU, so this cannot cause a 429 storm.

**Honest caveat about the .NET baseline:** the .NET app (`src_dotnet/`) already sets
`AllowBulkExecution = true`, so it is *not* missing bulk batching. What it *is* missing — confirmed by
grepping the source: no `ThroughputControlGroupConfig` / `enableLocalThroughputControlGroup` /
`enableGlobalThroughputControlGroup` anywhere in `src_dotnet/` (the only matches are inside the
compiled SDK DLL) — is **throughput control**. The Python app (`src/`) lacks it too. Without an RU
governor the app runs open-loop: it either under-drives the container (starvation) or overshoots into
429s and burns time on rate-limit retry backoff, so effective throughput collapses and a bigger VM
doesn't help. **Throughput control is the primary fix.** The explicit per-partition micro-batch depth
bump above is a useful secondary optimization. Absolute throughput numbers still require a run against
a real Cosmos account; the local vnext emulator surfaces an unrelated HTTP/2 transport quirk (see Notes).

## What throughput control actually does in the Java SDK

There are two distinct mechanisms, and it's worth being precise about which does what.

### 1. Client-side RU rate limiting (always)

Each client tracks its own RU consumption and **pre-emptively paces** requests to stay under a target,
rather than firing freely and absorbing 429s. You configure the target one of two ways:

- **`targetThroughput`** — an absolute RU/s cap (e.g. 8000). **Required for serverless accounts**,
  where percentage-based thresholds aren't supported.
- **`targetThroughputThreshold`** — a fraction in `(0, 1]` of the container's *provisioned* RU/s
  (e.g. `0.9` = "use up to 90%"). This is the "saturate to N%" dial.

This is genuine client-side flow control: the SDK delays/queues operations to hold the line, which
keeps latency stable and avoids the throughput collapse that mass throttling causes.

### 2. Fair RU sharing across *clients* (global mode)

With `enableGlobalThroughputControlGroup`, the SDK maintains a dedicated **control container** in which
every client writes a small heartbeat/ledger item (renewed ~every 5s, expiring ~11s). Clients read
each other's entries and **divide the RU budget among themselves**. So 10 clients sharing a
100,000 RU/s budget converge to ~10,000 RU/s each — instead of each independently assuming it owns the
full budget and collectively hammering the container into throttling.

Use **local** control (`enableLocalThroughputControlGroup`) when a single client/instance should be
limited on its own; use **global** control when you scale out across nodes and want them to cooperate.

### Nuance: it's an RU *budget governor*, not a per-partition scheduler

A common misconception is that throughput control assigns RU quotas per physical partition and thereby
spreads load evenly across partitions. It does **not**. Throughput control governs the *aggregate* RU
budget (per client, or shared across clients).

**What makes writes land uniformly across partitions is the bulk executor.** `executeBulkOperations`
groups operations into **per-partition-key-range micro-batches** and dispatches them in parallel across
all physical partitions. Throughput control then sits on top of that, pacing the overall rate so you
ride just under the RU ceiling.

So partition-uniform saturation and RU-budget governance are **two cooperating layers**:

| Layer | Responsibility |
| --- | --- |
| Bulk executor | Parallelism + per-partition-range batching → uniform partition utilization |
| Throughput control | Aggregate RU pacing + fair cross-client budget sharing → no throttling collapse |

Together they let a single modest client drive a container to its RU ceiling — and let many clients
scale out without stepping on each other. That, not more CPU, is the fix for the idle-VM problem.

## Layout

```
src_java/
  pom.xml                                   Maven build (shaded runnable jar)
  src/main/java/com/azure/cosmos/bench/
    Benchmark.java                          Entry point: client, throughput control, bulk ingest
    BenchmarkConfig.java                    .env + environment configuration loader
    VectorDoc.java                          Synthetic document shape (id, docid, title, text, emb)
```

## Configuration

Reads a `.env` file (path via first CLI arg, default `../.env`) plus process environment
(environment wins). Knob names mirror the Python/.NET implementations where they overlap.

| Variable | Default | Meaning |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | *(required)* | Account endpoint URI |
| `COSMOS_KEY` | *(empty)* | Account key; empty → `DefaultAzureCredential` (AAD) |
| `COSMOS_DATABASE_NAME` | *(required)* | Database id |
| `COSMOS_CONTAINER_NAME` | *(required)* | Container id |
| `TOTAL_DOCS` | `1000000` | Number of docs to write |
| `FAKE_DATA_VECTOR_DIM` | `1536` | Embedding dimensionality |
| `PAYLOAD_BYTES` | `1000` | Filler size of the `text` field |
| `COSMOS_PARTITION_KEY_FIELD` | `docid` | Partition key field (path `/docid`) |
| `BULK_SIZE` | `100` | Micro-batch target hint (analogue of the other ports' `BULK_SIZE`) |
| `MAX_MICRO_BATCH_CONCURRENCY` | `8` | Per-partition in-flight batches (SDK default is 1) |
| `MAX_MICRO_BATCH_SIZE` | `100` | Ops per micro-batch (SDK direct-mode cap is 100) |
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

## Notes

- Writes use `contentResponseOnWriteEnabled(false)` to minimize response bytes and maximize write
  throughput.
- Optional container provisioning creates a vector embedding policy (`FLOAT32`, cosine) and a
  `quantizedFlat` vector index on `/emb`, matching the benchmark's vector-search intent.
- Against the local Cosmos DB emulator (vnext), the underlying SDK transport can currently surface an
  HTTP/2 / TLS handshake quirk (`not an SSL/TLS record`) unrelated to this code; validate at scale
  against a real account or the classic emulator.
