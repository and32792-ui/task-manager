package com.taskmanager.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

/**
 * Spark ETL Batch Job: Raw Avro → Curated Parquet
 *
 * Reads raw events from Ozone (Avro format), transforms and validates,
 * then writes to curated-data bucket in Parquet format partitioned by date.
 *
 * Scheduled hourly via Airflow.
 *
 * Usage:
 *   spark-submit --class com.taskmanager.spark.EtlBatchJob task-manager-spark.jar [date]
 *
 * Arguments:
 *   date - Optional date to process (yyyy-MM-dd). Defaults to yesterday.
 */
object EtlBatchJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TaskManager-ETL-Batch")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    OzoneConfig.configureS3A(spark)

    import spark.implicits._

    // Determine processing date
    val processingDate = if (args.length > 0) {
      args(0)
    } else {
      // Default to yesterday
      java.time.LocalDate.now().minusDays(1).toString
    }

    println(s"ETL Batch Job starting for date: $processingDate")

    // ─── Step 1: Read Raw Task Events ──────────────────────────────
    val rawTaskEvents = readRawAvro(spark, s"${OzoneConfig.rawEventsPath}/task-events", processingDate)

    if (rawTaskEvents.isEmpty) {
      println(s"No task events found for $processingDate. Exiting.")
      spark.stop()
      return
    }

    // ─── Step 2: Transform and Validate ────────────────────────────
    val curatedTaskEvents = transformTaskEvents(rawTaskEvents)

    // ─── Step 3: Deduplicate ───────────────────────────────────────
    val dedupedEvents = deduplicateEvents(curatedTaskEvents)

    // ─── Step 4: Write Curated Parquet ─────────────────────────────
    writeCuratedParquet(dedupedEvents, s"${OzoneConfig.curatedDataPath}/task-events", processingDate)

    // ─── Step 5: Process CDC Events ────────────────────────────────
    val rawCdcEvents = readRawAvro(spark, s"${OzoneConfig.rawEventsPath}/task-cdc", processingDate)
    if (!rawCdcEvents.isEmpty) {
      val curatedCdc = transformCdcEvents(rawCdcEvents)
      writeCuratedParquet(curatedCdc, s"${OzoneConfig.curatedDataPath}/task-cdc", processingDate)
    }

    println(s"ETL Batch Job completed for date: $processingDate")
    spark.stop()
  }

  /**
   * Read raw Avro files from Ozone for a specific date partition.
   */
  private def readRawAvro(spark: SparkSession, basePath: String, date: String): DataFrame = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7)
    val day = date.substring(8, 10)

    val path = s"$basePath/year=$year/month=$month/day=$day"

    try {
      spark.read
        .format("avro")
        .load(path)
    } catch {
      case e: org.apache.spark.sql.AnalysisException if e.getMessage.contains("Path does not exist") =>
        println(s"Warning: Path does not exist: $path")
        spark.emptyDataFrame
    }
  }

  /**
   * Transform raw task events to curated schema.
   */
  private def transformTaskEvents(df: DataFrame): DataFrame = {
    df.select(
        col("eventId"),
        col("eventType"),
        to_timestamp(col("timestamp")).as("eventTimestamp"),
        col("source"),
        col("data.task.id").as("taskId"),
        col("data.task.title").as("taskTitle"),
        col("data.task.done").as("taskDone"),
        to_timestamp(col("data.task.createdAt")).as("taskCreatedAt"),
        to_timestamp(col("data.task.updatedAt")).as("taskUpdatedAt"),
        col("data.previousState").as("previousState"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp")
      )
      // Validate: filter out records with null required fields
      .filter(col("eventId").isNotNull && col("taskId").isNotNull)
      // Add processing metadata
      .withColumn("processedAt", current_timestamp())
      .withColumn("year", year(col("eventTimestamp")).cast(StringType))
      .withColumn("month", format_string("%02d", month(col("eventTimestamp"))))
      .withColumn("day", format_string("%02d", dayofmonth(col("eventTimestamp"))))
  }

  /**
   * Transform CDC events to curated schema.
   */
  private def transformCdcEvents(df: DataFrame): DataFrame = {
    df.select(
        col("cdc_key"),
        col("cdc_value"),
        col("topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        to_timestamp(col("kafka_timestamp")).as("eventTimestamp"),
        current_timestamp().as("processedAt")
      )
      .withColumn("year", year(col("eventTimestamp")).cast(StringType))
      .withColumn("month", format_string("%02d", month(col("eventTimestamp"))))
      .withColumn("day", format_string("%02d", dayofmonth(col("eventTimestamp"))))
  }

  /**
   * Deduplicate events based on eventId, keeping the latest by kafka_offset.
   */
  private def deduplicateEvents(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("eventId").orderBy(col("kafka_offset").desc)
    df.withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") === 1)
      .drop("row_num")
  }

  /**
   * Write curated data to Parquet with date partitioning.
   */
  private def writeCuratedParquet(df: DataFrame, basePath: String, date: String): Unit = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7)
    val day = date.substring(8, 10)

    df.write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .option("compression", "snappy")
      .save(basePath)

    println(s"Written curated Parquet to $basePath for date $date")
  }
}
