require("dotenv").config();

module.exports = {
  port: parseInt(process.env.PORT, 10) || 3000,

  postgres: {
    host: process.env.PG_HOST || "localhost",
    port: parseInt(process.env.PG_PORT, 10) || 5432,
    database: process.env.PG_DATABASE || "taskmanager",
    user: process.env.PG_USER || "taskmanager",
    password: process.env.PG_PASSWORD || "taskmanager",
    max: parseInt(process.env.PG_POOL_MAX, 10) || 20,
  },

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    clientId: process.env.KAFKA_CLIENT_ID || "task-manager-api",
    topic: process.env.KAFKA_TOPIC || "task-events",
  },

  ozone: {
    endpoint: process.env.OZONE_S3_ENDPOINT || "http://localhost:9878",
    region: process.env.OZONE_REGION || "us-east-1",
    accessKeyId: process.env.OZONE_ACCESS_KEY || "ozone-access-key",
    secretAccessKey: process.env.OZONE_SECRET_KEY || "ozone-secret-key",
    buckets: {
      rawEvents: "raw-events",
      curatedData: "curated-data",
      analyticsOutput: "analytics-output",
      attachments: "attachments",
    },
  },
};
