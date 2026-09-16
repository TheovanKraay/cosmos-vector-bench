# Cosmos DB Vector Bulk Ingest: Spark Connector edition (`src_spark/`)

A third alternative implementation of this repo's write benchmark, alongside the reference
**Python** (`src/`, `main.py`) and the **.NET** port (`src_dotnet/`). Instead of driving the Cosmos
bulk SDK from a single process, it uses the **Azure Cosmos DB Spark 3 connector** in **bulk mode**
with the **throughput control** feature: a Databricks/Spark notebook that reproduces the same write
workload but scales ingestion horizontally across a cluster.

## Why this exists

The upstream benchmark drives the Cosmos **.NET/Python bulk SDK from a single process**. That
makes the *client* the bottleneck; one machine's CPU, NIC, and thread scheduling cap out long
before a high-RU container is saturated. Customers with large RU budgets struggle to actually
consume them.

The Spark connector solves this by:

- **Fanning ingest across every executor core** in the cluster (scale writers horizontally).
- **Auto-tuning bulk micro-batches** (`spark.cosmos.write.bulk.enabled=true`) so batch size adapts
  to the observed throttling rate.
- **Throughput control** (`spark.cosmos.throughputControl.*`) which lets you *deliberately drive at
  ~95% of provisioned RU/s* (or a fixed RU number), i.e. saturate the container on purpose while
  leaving headroom.

## Files

| File | Purpose |
|------|---------|
| `cosmos_vector_bulk_ingest.scala` | **The deliverable.** Databricks notebook source (`// COMMAND ----------` cells). Import into Databricks or run any cell in `spark-shell`. |
| `local_test.scala` | Small-volume smoke test against a local emulator (HTTPS + gateway mode). |
| `gen_test.scala` | Cosmos-free test of the data-generation logic / doc shape (runs anywhere). |
| `emu_rest_test.py` | REST data-plane probe proving the emulator accepts the benchmark doc shape. |

## The notebook

`cosmos_vector_bulk_ingest.scala` reproduces the benchmark's **fake document** exactly:

```
id     : string (uuid)
docid  : string (uuid)   -- partition key /docid
title  : string
text   : string          -- ~PAYLOAD_BYTES filler
emb    : array<float>    -- FAKE_DATA_VECTOR_DIM floats in [-1, 1] (default 1536)
```

Flow:
1. **Config**: endpoint/key/db/container + workload knobs mirroring the benchmark's `.env`
   (`TOTAL_DOCS`, `FAKE_DATA_VECTOR_DIM`, `PAYLOAD_BYTES`, partition key path).
2. **(Optional) create container** via the connector catalog API, with a **vector embedding policy**
   and **quantizedFlat vector index** on `/emb` (matching the benchmark's Bicep), plus a high
   autoscale RU ceiling to have throughput to saturate.
3. **Distributed generation**: `spark.range(...).mapPartitions` builds documents on the executors
   (the heavy 1536-dim vector payload is never materialised on the driver).
4. **Bulk write** with `cosmos.oltp` + bulk enabled + throughput control at `0.95` of provisioned RU.
5. **Read-back count** to verify.

### Cluster setup

- Spark **3.5** (e.g. Databricks Runtime 15.4 LTS). For other versions install the matching
  connector artifact (`_3-4_`, `_3-3_`, ...).
- Install the connector from Maven onto the cluster:
  `com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.49.2` (or later 4.x).
- Prefer Databricks **secret scopes** for the account key.

### Key throughput-control options

```
spark.cosmos.write.bulk.enabled                        = true
spark.cosmos.throughputControl.enabled                 = true
spark.cosmos.throughputControl.name                    = vectorBulkIngest
spark.cosmos.throughputControl.targetThroughputThreshold = 0.95   # fraction of provisioned RU/s
spark.cosmos.throughputControl.globalControl.database   = <db>
spark.cosmos.throughputControl.globalControl.container  = ThroughputControl
```

`targetThroughputThreshold` is the saturation dial (0.0–1.0). Raise toward `1.0` to push harder;
lower it if the container is shared. You can instead set an absolute
`spark.cosmos.throughputControl.targetThroughput` in RU/s.

## Verification performed

Verified locally on the Cosmos DB **vnext** emulator + Apache **Spark 3.5.3** with connector
`4.49.2`:

1. **Data-generation logic** (`gen_test.scala`): PASSED. Confirmed:
   - 500 docs across the requested 4 Spark partitions,
   - vector length == configured dim, values within `[-1, 1]`,
   - exact field set `id,docid,title,text,emb`.
   This exercises the same schema + `mapPartitions` generation the notebook uses.

2. **Emulator data plane** (`emu_rest_test.py`): PASSED. Inserted 5 benchmark-shaped documents
   into a `/docid`-partitioned container and confirmed `SELECT VALUE COUNT(1)` returns 5, proving
   the doc shape / partition key are accepted by Cosmos.

3. **Connector compile + execute**: the notebook's Scala compiled and executed in `spark-shell`
   with the connector on the classpath (DataFrame built, bulk write invoked).

### Emulator caveat (why the connector smoke test can't fully round-trip here)

The Cosmos DB **vnext** emulator's gateway negotiates **HTTP/2**, while the Java Cosmos SDK bundled
in the current Spark connector uses an **HTTP/1.1** gateway client. The gateway answers the SDK's
metadata request with `HTTP/1.1 400 Bad Request`, which surfaces as a `NotSslRecordException` /
`503 (10001)` in the connector. This is a **vnext-emulator vs. SDK compatibility limitation**, not a
defect in the notebook.

The notebook targets **real Azure Cosmos DB** (and the classic HTTPS emulator), both of which the
Spark connector fully supports. To smoke-test the connector end-to-end locally, use the **classic**
`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator` image (HTTPS/1.1) rather than vnext, or
point `local_test.scala` at a real Cosmos account.

## Tuning to saturate a high-RU container

- **Scale executors, not just cores**: bulk throughput scales with total executor cores; add
  workers before touching internals.
- **`numInputPartitions`** ≈ a small multiple of total cores so every core writes.
- **`targetThroughputThreshold`** is the RU dial; raise toward `1.0` to saturate.
- **Co-locate** the Spark cluster in the Cosmos account's region and set
  `spark.cosmos.preferredRegionsList`.
- **Partition spread**: `docid` is a GUID, so writes fan out evenly (ideal). A hot/low-cardinality
  key would bottleneck regardless of RU.
- For much larger docs, raise `spark.cosmos.write.bulk.targetedPayloadSizeInBytes` toward ~1.5 MB.
