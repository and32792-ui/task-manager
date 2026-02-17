package com.taskmanager.spark

import org.apache.spark.sql.SparkSession

/**
 * Shared configuration for connecting Spark to Apache Ozone via S3A.
 */
object OzoneConfig {

  val S3_ENDPOINT: String = sys.env.getOrElse("OZONE_S3_ENDPOINT", "http://ozone-s3g:9878")
  val ACCESS_KEY: String = sys.env.getOrElse("OZONE_ACCESS_KEY", "ozone-access-key")
  val SECRET_KEY: String = sys.env.getOrElse("OZONE_SECRET_KEY", "ozone-secret-key")
  val VOLUME: String = sys.env.getOrElse("OZONE_VOLUME", "taskmanager")

  val KAFKA_BROKERS: String = sys.env.getOrElse("KAFKA_BROKERS", "kafka:29092")

  // Ozone bucket paths (S3A URIs)
  def rawEventsPath: String = s"s3a://$VOLUME.raw-events"
  def curatedDataPath: String = s"s3a://$VOLUME.curated-data"
  def analyticsOutputPath: String = s"s3a://$VOLUME.analytics-output"
  def checkpointPath: String = s"s3a://$VOLUME.checkpoints"

  /**
   * Configure a SparkSession with S3A settings for Ozone.
   */
  def configureS3A(spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT)
    hadoopConf.set("fs.s3a.access.key", ACCESS_KEY)
    hadoopConf.set("fs.s3a.secret.key", SECRET_KEY)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set("fs.s3a.change.detection.mode", "none")
  }
}
