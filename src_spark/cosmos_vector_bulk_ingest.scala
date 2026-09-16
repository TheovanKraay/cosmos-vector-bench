// Databricks notebook source
// MAGIC %md
// MAGIC # Cosmos DB Vector Bulk Ingest: Spark Connector + Throughput Control
// MAGIC
// MAGIC This notebook reproduces the write workload of the
// MAGIC [`cosmos-vector-bench`](https://github.com/TheovanKraay/cosmos-vector-bench) benchmark
// MAGIC (which generates synthetic vector documents and bulk-inserts them into Azure Cosmos DB),
// MAGIC but does the ingest with the **Azure Cosmos DB Spark 3 connector** using **bulk mode**
// MAGIC and the **throughput control** feature.
// MAGIC
// MAGIC **Why the Spark connector for this?**
// MAGIC The original benchmark drives the .NET/Python bulk SDK from a single process, so client-side
// MAGIC CPU, network, and thread scheduling become the bottleneck long before a high-RU container is
// MAGIC saturated. The Spark connector spreads bulk ingestion across every executor core, and its
// MAGIC auto-tuning micro-batching (combined with throughput control) pushes writes right up to the
// MAGIC RU budget you allow it, making it far easier to saturate a high-RU container.
// MAGIC
// MAGIC **Document shape produced here matches the benchmark's fake mode:**
// MAGIC `id`, `docid`, `title`, `text`, `emb` (a float vector, default 1536 dims, values in `[-1, 1]`).
// MAGIC
// MAGIC ### Cluster requirements
// MAGIC - Spark **3.5** (e.g. Databricks Runtime 15.4 LTS). For other Spark versions, install the
// MAGIC   matching connector (`azure-cosmos-spark_3-5_2-12`, `_3-4_`, `_3-3_`, ...).
// MAGIC - Install the connector from Maven onto the cluster, e.g.
// MAGIC   `com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.37.0`
// MAGIC   (or a later 4.x). This notebook does not `%pip`/`--packages` install it for you.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Configuration
// MAGIC
// MAGIC Set your account endpoint/key and the target database/container. In Databricks, prefer
// MAGIC secret scopes over inline keys:
// MAGIC ```scala
// MAGIC val cosmosKey = dbutils.secrets.get(scope = "cosmos", key = "masterKey")
// MAGIC ```

// COMMAND ----------

// ---- Connection ----
val cosmosEndpoint  = "https://<account>.documents.azure.com:443/"
val cosmosKey       = "<account-key>"           // prefer dbutils.secrets.get(...)
val cosmosDatabase  = "benchmark"
val cosmosContainer = "vectors"

// ---- Workload (mirrors cosmos-vector-bench fake mode) ----
val totalDocs        = 1000000                  // TOTAL_DOCS
val vectorDim        = 1536                     // FAKE_DATA_VECTOR_DIM
val payloadTextBytes = 1000                     // PAYLOAD_BYTES (approx size of `text`)
val partitionKeyPath = "/docid"                 // container partition key path

// ---- Ingest parallelism ----
// Number of Spark partitions the generated data is split into. Rule of thumb: a small multiple
// of total executor cores so every core stays busy. More partitions => more concurrent bulk writers.
val numInputPartitions = (spark.sparkContext.defaultParallelism * 4).max(8)

// ---- Throughput control ----
// targetThroughputThreshold is a FRACTION (0.0–1.0) of the container's provisioned RU/s that this
// job is allowed to consume. 0.95 tells the connector to drive at ~95% of the container RUs, i.e.
// deliberately saturate it while leaving a little headroom. Use a lower value if the container is
// shared with other workloads.
val throughputControlName = "vectorBulkIngest"
val targetThroughputFraction = "0.95"

println(s"Will generate $totalDocs docs (${vectorDim}-dim vectors) across $numInputPartitions Spark partitions")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. (Optional) Create database & container with a vector policy
// MAGIC
// MAGIC The connector's catalog API can create the container. We attach a **vector embedding policy**
// MAGIC and **vector index** on `/emb` so the container matches what the benchmark provisions via Bicep
// MAGIC (quantizedFlat index, float32, cosine, `/emb` excluded from the normal index).
// MAGIC
// MAGIC Set a high manual RU (or autoscale max) here so there is throughput to saturate. Skip this cell
// MAGIC if the container already exists.

// COMMAND ----------

val autoscaleMaxThroughput = "100000"   // provisioned RU/s to saturate (adjust to your test target)

spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosKey)

spark.sql(s"CREATE DATABASE IF NOT EXISTS cosmosCatalog.`$cosmosDatabase`;")

// Container with autoscale + vector embedding policy + vector index on /emb.
val vectorEmbeddingPolicy =
  s"""{"vectorEmbeddings":[{"path":"/emb","dataType":"float32","distanceFunction":"cosine","dimensions":$vectorDim}]}"""
val indexingPolicy =
  """{"indexingMode":"consistent","automatic":true,"includedPaths":[{"path":"/*"}],""" +
  """"excludedPaths":[{"path":"/\"_etag\"/?"},{"path":"/emb/*"}],""" +
  """"vectorIndexes":[{"path":"/emb","type":"quantizedFlat"}]}"""

spark.sql(s"""
CREATE TABLE IF NOT EXISTS cosmosCatalog.`$cosmosDatabase`.`$cosmosContainer`
USING cosmos.oltp
TBLPROPERTIES (
  partitionKeyPath = '$partitionKeyPath',
  autoScaleMaxThroughput = '$autoscaleMaxThroughput',
  indexingPolicy = '$indexingPolicy',
  vectorEmbeddingPolicy = '$vectorEmbeddingPolicy'
)
""")

println("Database/container ensured.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Generate synthetic vector documents (distributed)
// MAGIC
// MAGIC Documents are generated in parallel on the executors, one Spark partition at a time, so the
// MAGIC driver never has to hold or produce the whole dataset. Each row matches the benchmark's fake doc.

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import scala.util.Random

val docSchema = StructType(Seq(
  StructField("id",    StringType, nullable = false),
  StructField("docid", StringType, nullable = false),
  StructField("title", StringType, nullable = false),
  StructField("text",  StringType, nullable = false),
  StructField("emb",   ArrayType(FloatType, containsNull = false), nullable = false)
))

// Seed rows: one Long per document, distributed across numInputPartitions. mapPartitions then turns
// each into a full document, generating the heavy vector payload on the executor.
val seedDf = spark.range(0L, totalDocs.toLong, 1L, numInputPartitions).toDF("i")

val rowsRdd = seedDf.rdd.mapPartitions { it =>
  val rnd = new Random()
  // ~payloadTextBytes of filler text (matches PAYLOAD_BYTES intent for the `text` field).
  val filler = "x" * math.max(payloadTextBytes, 1)
  it.map { r =>
    val i = r.getLong(0)
    val emb = Array.fill(vectorDim)(rnd.nextFloat() * 2.0f - 1.0f)  // [-1, 1], matches MakeDoc
    Row(
      java.util.UUID.randomUUID().toString,   // id
      java.util.UUID.randomUUID().toString,   // docid (partition key)
      s"Document $i",                          // title
      filler,                                  // text
      emb                                      // emb
    )
  }
}

val docsDf: DataFrame = spark.createDataFrame(rowsRdd, docSchema)
println(s"Prepared DataFrame with ${docsDf.rdd.getNumPartitions} partitions.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Bulk write with throughput control
// MAGIC
// MAGIC Key options:
// MAGIC - `spark.cosmos.write.bulk.enabled=true`: connector bulk mode (auto-tunes micro-batch size).
// MAGIC - `spark.cosmos.write.strategy=ItemOverwrite`: upsert (idempotent re-runs). Use `ItemAppend`
// MAGIC   for pure inserts if you never re-run over the same ids.
// MAGIC - `spark.cosmos.throughputControl.*`: caps/steers RU usage to the target fraction of the
// MAGIC   container's provisioned throughput. The connector stores its control state in a small
// MAGIC   dedicated container (`ThroughputControl`) it creates in the same database.

// COMMAND ----------

val writeConfig = Map(
  "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey"      -> cosmosKey,
  "spark.cosmos.database"        -> cosmosDatabase,
  "spark.cosmos.container"       -> cosmosContainer,

  // Bulk ingest
  "spark.cosmos.write.strategy"     -> "ItemOverwrite",
  "spark.cosmos.write.bulk.enabled" -> "true",
  "spark.cosmos.write.maxRetryCount"-> "10",

  // Throughput control: drive at ~95% of provisioned RU/s
  "spark.cosmos.throughputControl.enabled"                     -> "true",
  "spark.cosmos.throughputControl.name"                        -> throughputControlName,
  "spark.cosmos.throughputControl.targetThroughputThreshold"   -> targetThroughputFraction,
  "spark.cosmos.throughputControl.globalControl.database"      -> cosmosDatabase,
  "spark.cosmos.throughputControl.globalControl.container"     -> "ThroughputControl"
)

val t0 = System.nanoTime()

docsDf.write
  .format("cosmos.oltp")
  .options(writeConfig)
  .mode("APPEND")
  .save()

val elapsedSec = (System.nanoTime() - t0) / 1e9
val docsPerSec = totalDocs / elapsedSec
println(f"Ingested $totalDocs%,d docs in $elapsedSec%.1f s  =>  $docsPerSec%,.0f docs/sec")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5. Verify count
// MAGIC
// MAGIC Quick read-back to confirm the documents landed. (For very large loads this count query itself
// MAGIC consumes RU, run it after ingest, or check the row count in the portal metrics instead.)

// COMMAND ----------

val readConfig = Map(
  "spark.cosmos.accountEndpoint"           -> cosmosEndpoint,
  "spark.cosmos.accountKey"                -> cosmosKey,
  "spark.cosmos.database"                  -> cosmosDatabase,
  "spark.cosmos.container"                 -> cosmosContainer,
  "spark.cosmos.read.inferSchema.enabled"  -> "false"
)

val readback = spark.read.format("cosmos.oltp").options(readConfig).load()
println(s"Row count in container: ${readback.count()}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tuning notes to saturate a high-RU container
// MAGIC
// MAGIC - **Scale executors, not just cores.** Bulk throughput scales with total executor cores. If you
// MAGIC   cannot hit the RU ceiling, add workers before touching connector internals.
// MAGIC - **`numInputPartitions`** should be a small multiple of total cores so every core writes.
// MAGIC - **`targetThroughputThreshold`** is the RU-saturation dial. Raise toward `1.0` to push harder;
// MAGIC   lower it when the container is shared. You can also use the absolute
// MAGIC   `spark.cosmos.throughputControl.targetThroughput` (RU/s) instead of the fraction.
// MAGIC - **Co-locate** the Spark cluster in the same Azure region as the Cosmos account, and set
// MAGIC   `spark.cosmos.preferredRegionsList` to that region to cut latency.
// MAGIC - **Partition key spread:** `docid` is a GUID here, so writes fan out evenly across physical
// MAGIC   partitions, ideal for saturation. A low-cardinality/hot key would bottleneck regardless of RU.
// MAGIC - **`spark.cosmos.write.bulk.targetedPayloadSizeInBytes`:** 1536-dim float vectors are ~6 KB+;
// MAGIC   the default 220 KB batch target is fine, but for much larger docs raise it toward ~1.5 MB.
