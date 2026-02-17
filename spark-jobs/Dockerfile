# Multi-stage build: compile Scala, then package into Spark image
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.10_7_1.9.8_2.12.18 AS builder

WORKDIR /app
COPY project/ project/
COPY build.sbt .

# Download dependencies first (cached layer)
RUN sbt update

# Copy source and build fat JAR
COPY src/ src/
RUN sbt assembly

# Runtime: Spark base image
FROM apache/spark:3.5.1

# Copy the fat JAR
COPY --from=builder /app/target/scala-2.12/task-manager-spark.jar /opt/spark/jars/task-manager-spark.jar

USER root
RUN chmod 644 /opt/spark/jars/task-manager-spark.jar
USER spark
