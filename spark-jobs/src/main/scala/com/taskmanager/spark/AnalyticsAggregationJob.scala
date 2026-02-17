package com.taskmanager.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties

/**
 * Spark Analytics Aggregation Job
 *
 * Reads curated Parquet data from Ozone, computes aggregations:
 * - Task completion rates by day
 * - Average task lifetime (creation to completion)
 * - SLA compliance metrics
 * - Trend analysis
 *
 * Writes results to:
 * - Ozone analytics-output bucket (Parquet)
 * - PostgreSQL analytics tables (for API queries)
 *
 * Scheduled daily via Airflow.
 *
 * Usage:
 *   spark-submit --class com.taskmanager.spark.AnalyticsAggregationJob task-manager-spark.jar [date]
 */
object AnalyticsAggregationJob {

  // PostgreSQL connection properties
  val PG_URL: String = sys.env.getOrElse("PG_URL", "jdbc:postgresql://postgres.task-manager.svc.cluster.local:5432/taskmanager")
  val PG_USER: String = sys.env.getOrElse("PG_USER", "taskmanager")
  val PG_PASSWORD: String = sys.env.getOrElse("PG_PASSWORD", "taskmanager")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TaskManager-Analytics-Aggregation")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    OzoneConfig.configureS3A(spark)

    import spark.implicits._

    // Determine processing date
    val processingDate = if (args.length > 0) {
      args(0)
    } else {
      java.time.LocalDate.now().minusDays(1).toString
    }

    println(s"Analytics Aggregation Job starting for date: $processingDate")

    // ─── Step 1: Read Curated Task Events ───────────────────────────
    val taskEvents = readCuratedParquet(spark, s"${OzoneConfig.curatedDataPath}/task-events", processingDate)

    if (taskEvents.isEmpty) {
      println(s"No curated task events found for $processingDate. Exiting.")
      spark.stop()
      return
    }

    // ─── Step 2: Compute Daily Summary ──────────────────────────────
    val dailySummary = computeDailySummary(taskEvents, processingDate)

    // ─── Step 3: Compute Task Lifetime Metrics ──────────────────────
    val lifetimeMetrics = computeLifetimeMetrics(taskEvents, processingDate)

    // ─── Step 4: Combine Results ────────────────────────────────────
    val combinedMetrics = dailySummary.join(lifetimeMetrics, Seq("date"))

    // ─── Step 5: Write to Ozone ─────────────────────────────────────
    writeToOzone(combinedMetrics, processingDate)

    // ─── Step 6: Write to PostgreSQL ────────────────────────────────
    writeToPostgreSQL(combinedMetrics)

    println(s"Analytics Aggregation Job completed for date: $processingDate")
    spark.stop()
  }

  /**
   * Read curated Parquet files for a specific date.
   */
  private def readCuratedParquet(spark: SparkSession, basePath: String, date: String): DataFrame = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7)
    val day = date.substring(8, 10)

    val path = s"$basePath/year=$year/month=$month/day=$day"

    try {
      spark.read
        .format("parquet")
        .load(path)
    } catch {
      case e: org.apache.spark.sql.AnalysisException if e.getMessage.contains("Path does not exist") =>
        println(s"Warning: Path does not exist: $path")
        spark.emptyDataFrame
    }
  }

  /**
   * Compute daily summary metrics.
   */
  private def computeDailySummary(df: DataFrame, date: String): DataFrame = {
    // Get unique tasks and their states
    val latestTaskStates = df
      .filter(col("eventType").isin("CREATE", "UPDATE"))
      .withColumn("rn", row_number().over(
        Window.partitionBy("taskId").orderBy(col("eventTimestamp").desc)
      ))
      .filter(col("rn") === 1)
      .select("taskId", "taskDone", "taskCreatedAt")

    // Count total and completed tasks
    latestTaskStates.agg(
      lit(date).as("date"),
      count("*").as("total_tasks"),
      sum(when(col("taskDone") === true, 1).otherwise(0)).as("completed_tasks"),
      sum(when(col("taskDone") === false, 1).otherwise(0)).as("pending_tasks")
    )
    .withColumn("completion_rate",
      when(col("total_tasks") > 0,
        round(col("completed_tasks") / col("total_tasks") * 100, 2)
      ).otherwise(lit(0.0))
    )
  }

  /**
   * Compute task lifetime metrics (time from creation to completion).
   */
  private def computeLifetimeMetrics(df: DataFrame, date: String): DataFrame = {
    // Find CREATE and UPDATE (done=true) events for each task
    val createEvents = df
      .filter(col("eventType") === "CREATE")
      .select(
        col("taskId"),
        col("eventTimestamp").as("createdAt")
      )

    val completeEvents = df
      .filter(col("eventType") === "UPDATE" && col("taskDone") === true)
      .withColumn("rn", row_number().over(
        Window.partitionBy("taskId").orderBy(col("eventTimestamp").asc)
      ))
      .filter(col("rn") === 1)
      .select(
        col("taskId"),
        col("eventTimestamp").as("completedAt")
      )

    // Join to compute lifetime
    val taskLifetimes = createEvents.join(completeEvents, "taskId")
      .withColumn("lifetime_hours",
        round(
          (unix_timestamp(col("completedAt")) - unix_timestamp(col("createdAt"))) / 3600.0,
          2
        )
      )
      .filter(col("lifetime_hours") >= 0)

    // Aggregate average lifetime
    taskLifetimes.agg(
      lit(date).as("date"),
      round(avg("lifetime_hours"), 2).as("avg_lifetime_hours"),
      round(min("lifetime_hours"), 2).as("min_lifetime_hours"),
      round(max("lifetime_hours"), 2).as("max_lifetime_hours")
    )
  }

  /**
   * Write results to Ozone analytics-output bucket.
   */
  private def writeToOzone(df: DataFrame, date: String): Unit = {
    df.write
      .mode("overwrite")
      .format("parquet")
      .save(s"${OzoneConfig.analyticsOutputPath}/daily-summary/date=$date")

    println(s"Written analytics to Ozone: ${OzoneConfig.analyticsOutputPath}/daily-summary/date=$date")
  }

  /**
   * Write results to PostgreSQL analytics table.
   */
  private def writeToPostgreSQL(df: DataFrame): Unit = {
    val props = new Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASSWORD)
    props.setProperty("driver", "org.postgresql.Driver")

    df.write
      .mode("overwrite")
      .jdbc(PG_URL, "analytics_daily_summary", props)

    println(s"Written analytics to PostgreSQL: analytics_daily_summary")
  }
}
