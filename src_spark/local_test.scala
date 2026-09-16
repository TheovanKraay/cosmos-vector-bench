// Local emulator test: small-volume version of the notebook logic.
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import scala.util.Random

val cosmosEndpoint  = "https://localhost:18081/"
val cosmosKey       = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
val cosmosDatabase  = "benchmark"
val cosmosContainer = "vectors"

val totalDocs        = 500        // small volume
val vectorDim        = 16         // small vectors for the smoke test
val payloadTextBytes = 200
val partitionKeyPath = "/docid"
val numInputPartitions = 4

// ---- catalog: create db + container ----
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosKey)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.useGatewayMode", "true")

spark.sql(s"CREATE DATABASE IF NOT EXISTS cosmosCatalog.`$cosmosDatabase`;")
spark.sql(s"""
CREATE TABLE IF NOT EXISTS cosmosCatalog.`$cosmosDatabase`.`$cosmosContainer`
USING cosmos.oltp
TBLPROPERTIES (
  partitionKeyPath = '$partitionKeyPath',
  manualThroughput = '10000'
)
""")
println(">>> container ensured")

// ---- generate ----
val docSchema = StructType(Seq(
  StructField("id",    StringType, false),
  StructField("docid", StringType, false),
  StructField("title", StringType, false),
  StructField("text",  StringType, false),
  StructField("emb",   ArrayType(FloatType, false), false)
))
val seedDf = spark.range(0L, totalDocs.toLong, 1L, numInputPartitions).toDF("i")
val rowsRdd = seedDf.rdd.mapPartitions { it =>
  val rnd = new Random()
  val filler = "x" * math.max(payloadTextBytes, 1)
  it.map { r =>
    val i = r.getLong(0)
    val emb = Array.fill(vectorDim)(rnd.nextFloat() * 2.0f - 1.0f)
    Row(java.util.UUID.randomUUID().toString, java.util.UUID.randomUUID().toString,
        s"Document $i", filler, emb)
  }
}
val docsDf = spark.createDataFrame(rowsRdd, docSchema)
println(s">>> generated ${docsDf.count()} docs in ${docsDf.rdd.getNumPartitions} partitions")

// ---- bulk write with throughput control ----
val writeConfig = Map(
  "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey"      -> cosmosKey,
  "spark.cosmos.database"        -> cosmosDatabase,
  "spark.cosmos.container"       -> cosmosContainer,
  "spark.cosmos.useGatewayMode"  -> "true",
  "spark.cosmos.write.strategy"     -> "ItemOverwrite",
  "spark.cosmos.write.bulk.enabled" -> "true",
  "spark.cosmos.throughputControl.enabled"                   -> "true",
  "spark.cosmos.throughputControl.name"                      -> "vectorBulkIngest",
  "spark.cosmos.throughputControl.targetThroughputThreshold" -> "0.95",
  "spark.cosmos.throughputControl.globalControl.database"    -> cosmosDatabase,
  "spark.cosmos.throughputControl.globalControl.container"   -> "ThroughputControl"
)
docsDf.write.format("cosmos.oltp").options(writeConfig).mode("APPEND").save()
println(">>> bulk write done")

// ---- verify ----
val readConfig = Map(
  "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey"      -> cosmosKey,
  "spark.cosmos.database"        -> cosmosDatabase,
  "spark.cosmos.container"       -> cosmosContainer,
  "spark.cosmos.useGatewayMode"  -> "true",
  "spark.cosmos.read.inferSchema.enabled" -> "false"
)
val cnt = spark.read.format("cosmos.oltp").options(readConfig).load().count()
println(s">>> READBACK COUNT = $cnt (expected $totalDocs)")
assert(cnt == totalDocs, s"count mismatch: $cnt != $totalDocs")
println(">>> TEST PASSED")
System.exit(0)
