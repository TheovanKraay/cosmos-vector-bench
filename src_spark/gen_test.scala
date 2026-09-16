// Validate generation logic + doc shape WITHOUT Cosmos (compile + execute check).
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import scala.util.Random

val totalDocs = 500; val vectorDim = 16; val payloadTextBytes = 200; val numInputPartitions = 4

val docSchema = StructType(Seq(
  StructField("id", StringType, false), StructField("docid", StringType, false),
  StructField("title", StringType, false), StructField("text", StringType, false),
  StructField("emb", ArrayType(FloatType, false), false)))

val seedDf = spark.range(0L, totalDocs.toLong, 1L, numInputPartitions).toDF("i")
val rowsRdd = seedDf.rdd.mapPartitions { it =>
  val rnd = new Random(); val filler = "x" * math.max(payloadTextBytes, 1)
  it.map { r =>
    val i = r.getLong(0)
    val emb = Array.fill(vectorDim)(rnd.nextFloat() * 2.0f - 1.0f)
    Row(java.util.UUID.randomUUID().toString, java.util.UUID.randomUUID().toString,
        s"Document $i", filler, emb) }
}
val docsDf = spark.createDataFrame(rowsRdd, docSchema)
val n = docsDf.count()
val parts = docsDf.rdd.getNumPartitions
val sample = docsDf.head()
val embLen = sample.getAs[Seq[Float]]("emb").length
val embMin = docsDf.selectExpr("array_min(emb) as m").agg(org.apache.spark.sql.functions.min("m")).head().getFloat(0)
val embMax = docsDf.selectExpr("array_max(emb) as m").agg(org.apache.spark.sql.functions.max("m")).head().getFloat(0)
println(s">>> GEN count=$n parts=$parts embLen=$embLen embRange=[$embMin,$embMax]")
val fieldStr = docSchema.fieldNames.mkString(",")
println(s">>> FIELDS=$fieldStr")
assert(n == totalDocs, "count"); assert(embLen == vectorDim, "embLen")
assert(parts == numInputPartitions, "parts")
assert(embMin >= -1.0f && embMax <= 1.0f, "emb range")
println(">>> GEN TEST PASSED")
System.exit(0)
