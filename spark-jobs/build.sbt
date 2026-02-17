name := "task-manager-spark"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  // Spark core (provided at runtime by Spark cluster)
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  // Spark Kafka integration
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Avro support
  "org.apache.spark" %% "spark-avro" % sparkVersion,

  // Hadoop AWS / S3A for Ozone S3 compatibility
  "org.apache.hadoop" % "hadoop-aws" % "3.3.6",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.640",

  // PostgreSQL JDBC driver (for analytics output)
  "org.postgresql" % "postgresql" % "42.7.3",

  // Logging
  "org.slf4j" % "slf4j-api" % "2.0.12" % "provided"
)

// Assembly plugin settings for fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "reference.conf"                     => MergeStrategy.concat
  case _                                    => MergeStrategy.first
}

assembly / assemblyJarName := "task-manager-spark.jar"
