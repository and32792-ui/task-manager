package com.taskmanager.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Spark Structured Streaming job: Kafka → Ozone (raw-events bucket).
 *
 * Consumes from Kafka topics (task-events, task-cdc) and writes
 * raw events to Apache Ozone in Avro format, partitioned by date/hour.
 *
 * Usage:
 *   spark-submit --class com.taskmanager.spark.StreamingIngestion task-manager-spark.jar
 */
object StreamingIngestion {

  // Schema for task events produced by the Node.js API
  val taskEventSchema: StructType = StructType(Seq(
    StructField("eventId", StringType, nullable = false),
    StructField("eventType", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("source", StringType, nullable = false),
    StructField("data", StructType(Seq(
      StructField("task", StructType(Seq(
        StructField("id", StringType, nullable = false),
        StructField("title", StringType, nullable = false),
        StructField("done", BooleanType, nullable = false),
        StructField("createdAt", StringType, nullable = false),
        StructField("updatedAt", StringType, nullable = true)
      )), nullable = false),
      StructField("previousState", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("title", StringType, nullable = true),
        StructField("done", BooleanType, nullable = true),
        StructField("createdAt", StringType, nullable = true),
        StructField("updatedAt", StringType, nullable = true)
      )), nullable = true)
    )), nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TaskManager-StreamingIngestion")
      .config("spark.sql.streaming.checkpointLocation", s"${OzoneConfig.checkpointPath}/streaming-ingestion")
      .getOrCreate()

    OzoneConfig.configureS3A(spark)

    import spark.implicits._

    // ─── Stream 1: Task Events from API ──────────────────────
    val taskEventsStream = readKafkaTopic(spark, "task-events")
    val parsedTaskEvents = parseTaskEvents(taskEventsStream)

    val taskEventsQuery = parsedTaskEvents
      .writeStream
      .format("avro")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("path", s"${OzoneConfig.rawEventsPath}/task-events")
      .option("checkpointLocation", s"${OzoneConfig.checkpointPath}/streaming-ingestion/task-events")
      .partitionBy("year", "month", "day", "hour")
      .start()

    // ─── Stream 2: CDC Events from Debezium ──────────────────
    val cdcStream = readKafkaTopic(spark, "task-cdc.public.tasks")
    val parsedCdcEvents = parseCdcEvents(cdcStream)

    val cdcQuery = parsedCdcEvents
      .writeStream
      .format("avro")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("path", s"${OzoneConfig.rawEventsPath}/task-cdc")
      .option("checkpointLocation", s"${OzoneConfig.checkpointPath}/streaming-ingestion/task-cdc")
      .partitionBy("year", "month", "day", "hour")
      .start()

    println("Streaming ingestion started. Consuming from Kafka, writing to Ozone...")
    println(s"  Task events → ${OzoneConfig.rawEventsPath}/task-events")
    println(s"  CDC events  → ${OzoneConfig.rawEventsPath}/task-cdc")

    // Wait for both streams
    spark.streams.awaitAnyTermination()
  }

  /**
   * Read from a Kafka topic as a streaming DataFrame.
   */
  private def readKafkaTopic(spark: SparkSession, topic: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", OzoneConfig.KAFKA_BROKERS)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * Parse task events from Kafka JSON messages.
   * Adds date partitioning columns.
   */
  private def parseTaskEvents(df: DataFrame): DataFrame = {
    df.select(
        col("key").cast(StringType).as("kafka_key"),
        from_json(col("value").cast(StringType), taskEventSchema).as("event"),
        col("topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp")
      )
      .select(
        col("kafka_key"),
        col("event.*"),
        col("topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp")
      )
      .withColumn("event_ts", to_timestamp(col("timestamp")))
      .withColumn("year", year(col("event_ts")).cast(StringType))
      .withColumn("month", format_string("%02d", month(col("event_ts"))))
      .withColumn("day", format_string("%02d", dayofmonth(col("event_ts"))))
      .withColumn("hour", format_string("%02d", hour(col("event_ts"))))
  }

  /**
   * Parse CDC events from Debezium.
   * Debezium events have a different structure — extract the payload.
   */
  private def parseCdcEvents(df: DataFrame): DataFrame = {
    df.select(
        col("key").cast(StringType).as("cdc_key"),
        col("value").cast(StringType).as("cdc_value"),
        col("topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp")
      )
      .withColumn("event_ts", col("kafka_timestamp"))
      .withColumn("year", year(col("event_ts")).cast(StringType))
      .withColumn("month", format_string("%02d", month(col("event_ts"))))
      .withColumn("day", format_string("%02d", dayofmonth(col("event_ts"))))
      .withColumn("hour", format_string("%02d", hour(col("event_ts"))))
  }
}
