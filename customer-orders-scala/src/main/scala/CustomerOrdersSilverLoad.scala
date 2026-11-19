import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Customer Orders daily batch: CSV -> Bronze -> Quarantine -> Silver (Delta).
 *
 * Arguments:
 *   args(0) = run_date  (yyyy-MM-dd)
 *   args(1) = base_path (abfss://<container>@<account>.dfs.core.windows.net/<root>)
 *
 * Layout under base_path:
 *   landing/customer_orders/ingest_date=<run_date>/
 *   bronze/customer_orders, quarantine/customer_orders,
 *   silver/customer_orders, audit/customer_orders
 */
object CustomerOrdersSilverLoad {

  private val MaxRejectRate = 0.30

  def main(args: Array[String]): Unit = {
    require(args.length >= 2,
      "Usage: CustomerOrdersSilverLoad <run_date yyyy-MM-dd> <base_path>")

    val runDate  = LocalDate.parse(args(0).trim, DateTimeFormatter.ISO_LOCAL_DATE).toString
    val basePath = args(1).trim.stripSuffix("/")
    val startNs  = System.nanoTime()

    val spark = SparkSession.builder()
      .appName("CustomerOrdersSilverLoad")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val landing    = s"$basePath/landing/customer_orders/ingest_date=$runDate/"
    val bronze     = s"$basePath/bronze/customer_orders"
    val quarantine = s"$basePath/quarantine/customer_orders"
    val silver     = s"$basePath/silver/customer_orders"
    val auditPath  = s"$basePath/audit/customer_orders"

    println(s"RUN_DATE = $runDate")
    println(s"LANDING  = $landing")

    // Fail loudly (job retries/alerts) if the day's folder is missing or empty.
    val landingPath = new Path(landing)
    val fs = landingPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    require(fs.exists(landingPath) && fs.listStatus(landingPath).nonEmpty,
      s"No landing files found for run_date=$runDate at $landing")

    // ---------------- READ CSV ----------------
    val sourceSchema = StructType(Seq(
      StructField("order_id", StringType, true),
      StructField("customer_id", StringType, true),
      StructField("order_ts", StringType, true),
      StructField("amount", StringType, true),
      StructField("status", StringType, true),
      StructField("source_updated_at", StringType, true),
      StructField("op", StringType, true),
      StructField("_corrupt_record", StringType, true)
    ))

    val raw = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(sourceSchema)
      .load(landing)

    val rawWithMeta = raw
      .withColumn("_source_file", col("_metadata.file_path"))
      .withColumn("_ingested_at", current_timestamp())
      .withColumn("_run_date", lit(runDate))
      .cache()

    val rowsRead = rawWithMeta.count()

    // ---------------- BRONZE (idempotent MERGE) ----------------
    val bronzeRows = rawWithMeta.withColumn(
      "_record_hash",
      sha2(concat_ws("||",
        coalesce(col("order_id"), lit("")),
        coalesce(col("customer_id"), lit("")),
        coalesce(col("order_ts"), lit("")),
        coalesce(col("amount"), lit("")),
        coalesce(col("status"), lit("")),
        coalesce(col("source_updated_at"), lit("")),
        coalesce(col("op"), lit("")),
        coalesce(col("_corrupt_record"), lit(""))
      ), 256)
    )

    if (!DeltaTable.isDeltaTable(spark, bronze)) {
      bronzeRows.write.format("delta").mode("overwrite").save(bronze)
    } else {
      DeltaTable.forPath(spark, bronze).as("t")
        .merge(bronzeRows.as("s"),
          "t._source_file = s._source_file AND t._record_hash = s._record_hash")
        .whenNotMatched().insertAll()
        .execute()
    }

    // ---------------- PARSER REJECTS + TYPING ----------------
    val parserBad = rawWithMeta
      .filter(col("_corrupt_record").isNotNull)
      .withColumn("_reject_reason", lit("CSV_PARSER_CORRUPT_RECORD"))

    def tryTs(c: String) =
      expr(s"""try_to_timestamp($c, "yyyy-MM-dd'T'HH:mm:ssX")""")

    val typed = rawWithMeta
      .filter(col("_corrupt_record").isNull)
      .withColumn("order_id", trim(col("order_id")))
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("status", upper(trim(col("status"))))
      .withColumn("op", upper(coalesce(trim(col("op")), lit("I"))))
      .withColumn("order_ts", tryTs("order_ts"))
      .withColumn("source_updated_at", tryTs("source_updated_at"))
      .withColumn("amount", expr("try_cast(amount as DECIMAL(18,2))"))

    // ---------------- BUSINESS VALIDATION ----------------
    val rejectReason = concat_ws(";",
      when(col("order_id").isNull || col("order_id") === "", lit("MISSING_ORDER_ID")),
      when(col("customer_id").isNull || col("customer_id") === "", lit("MISSING_CUSTOMER_ID")),
      when(col("order_ts").isNull, lit("INVALID_ORDER_TIMESTAMP")),
      when(col("source_updated_at").isNull, lit("INVALID_SOURCE_UPDATED_AT")),
      when(col("amount").isNull, lit("INVALID_AMOUNT")),
      when(col("amount") < 0, lit("NEGATIVE_AMOUNT")),
      when(!col("op").isin("I", "U", "D"), lit("INVALID_CDC_OPERATION"))
    )

    val validated   = typed.withColumn("_reject_reason", rejectReason)
    val businessBad = validated.filter(col("_reject_reason") =!= "")
    val valid       = validated.filter(col("_reject_reason") === "").drop("_reject_reason")

    val parserRejects   = parserBad.count()
    val businessRejects = businessBad.count()
    val totalRejects    = parserRejects + businessRejects
    val validRows       = rowsRead - totalRejects

    // Reject-rate gate: stop before touching Quarantine/Silver if the file is mostly bad.
    val rejectRate = if (rowsRead == 0) 0.0 else totalRejects.toDouble / rowsRead
    println(f"Rows read=$rowsRead, parser rejects=$parserRejects, business rejects=$businessRejects, reject rate=${rejectRate * 100}%.1f%%")
    require(rejectRate <= MaxRejectRate,
      f"Reject rate ${rejectRate * 100}%.1f%% exceeds threshold ${MaxRejectRate * 100}%.0f%% for run_date=$runDate")

    // ---------------- QUARANTINE (idempotent MERGE) ----------------
    val quarantineRows = parserBad
      .select(
        col("order_id"), col("customer_id"),
        col("order_ts").cast(StringType).as("order_ts"),
        col("amount").cast(StringType).as("amount"),
        col("status"),
        col("source_updated_at").cast(StringType).as("source_updated_at"),
        col("op"), col("_corrupt_record"), col("_source_file"),
        col("_ingested_at"), col("_run_date"), col("_reject_reason"))
      .unionByName(
        businessBad.select(
          col("order_id"), col("customer_id"),
          col("order_ts").cast(StringType).as("order_ts"),
          col("amount").cast(StringType).as("amount"),
          col("status"),
          col("source_updated_at").cast(StringType).as("source_updated_at"),
          col("op"), col("_corrupt_record"), col("_source_file"),
          col("_ingested_at"), col("_run_date"), col("_reject_reason")))
      .withColumn("_row_fingerprint",
        sha2(concat_ws("||",
          coalesce(col("_source_file"), lit("")),
          coalesce(col("_corrupt_record"), lit("")),
          coalesce(col("order_id"), lit("")),
          coalesce(col("customer_id"), lit("")),
          coalesce(col("amount"), lit("")),
          coalesce(col("_reject_reason"), lit(""))
        ), 256))

    if (!DeltaTable.isDeltaTable(spark, quarantine)) {
      quarantineRows.write.format("delta").mode("overwrite").save(quarantine)
    } else {
      DeltaTable.forPath(spark, quarantine).as("t")
        .merge(quarantineRows.as("s"), "t._row_fingerprint = s._row_fingerprint")
        .whenNotMatched().insertAll()
        .execute()
    }

    // ---------------- SILVER: dedup, CDC deletes, upserts ----------------
    val sourceWindow = Window
      .partitionBy("order_id")
      .orderBy(col("source_updated_at").desc, col("_ingested_at").desc)

    val latest = valid
      .withColumn("_rn", row_number().over(sourceWindow))
      .filter(col("_rn") === 1)
      .drop("_rn")

    val deletes = latest.filter(col("op") === "D")
      .select("order_id", "source_updated_at")

    val upserts = latest.filter(col("op").isin("I", "U"))
      .drop("op", "_corrupt_record")

    if (!DeltaTable.isDeltaTable(spark, silver)) {
      upserts.limit(0).write.format("delta").mode("overwrite").save(silver)
    }
    val silverTable = DeltaTable.forPath(spark, silver)

    // Delete only if the delete event is not older than the Silver row.
    silverTable.as("t")
      .merge(deletes.as("s"), "t.order_id = s.order_id")
      .whenMatched("s.source_updated_at >= t.source_updated_at")
      .delete()
      .execute()

    // Insert new orders; update existing only when the source version is newer.
    silverTable.as("t")
      .merge(upserts.as("s"), "t.order_id = s.order_id")
      .whenMatched("s.source_updated_at > t.source_updated_at")
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    // ---------------- VALIDATE + AUDIT ----------------
    val bronzeCount     = spark.read.format("delta").load(bronze).count()
    val quarantineCount = spark.read.format("delta").load(quarantine).count()
    val silverDF        = spark.read.format("delta").load(silver)
    val silverCount     = silverDF.count()
    val duplicateKeys   = silverDF.groupBy("order_id").count()
      .filter(col("count") > 1).count()

    require(duplicateKeys == 0, s"Silver has $duplicateKeys duplicate order_id values")

    val durationSec = (System.nanoTime() - startNs) / 1e9
    Seq((runDate, "SUCCESS", rowsRead, parserRejects, businessRejects,
         bronzeCount, quarantineCount, silverCount, duplicateKeys, durationSec))
      .toDF("run_date", "status", "rows_read", "parser_rejects", "business_rejects",
            "bronze_total", "quarantine_total", "silver_total", "duplicate_keys",
            "duration_sec")
      .withColumn("finished_at", current_timestamp())
      .write.format("delta").mode("append").save(auditPath)

    println("PIPELINE SUCCESS")
    println(s"run_date=$runDate")
    println(s"input=$rowsRead")
    println(s"quarantine=$totalRejects")
    println(s"valid=$validRows")
    println(s"silver=$silverCount")
    println(s"bronze_total=$bronzeCount quarantine_total=$quarantineCount duplicate_keys=$duplicateKeys")

    rawWithMeta.unpersist()
    // Do not call spark.stop(): on Databricks the platform manages the session.
  }
}
