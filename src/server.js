const express = require("express");
const path = require("path");
const config = require("./config");
const kafkaProducer = require("./kafka/producer");
const taskRoutes = require("./routes/tasks");
const analyticsRoutes = require("./routes/analytics");
const attachmentRoutes = require("./routes/attachments");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "public")));

// API routes
app.use("/api/tasks", taskRoutes);
app.use("/api/tasks", attachmentRoutes);
app.use("/api/analytics", analyticsRoutes);

// Health check
app.get("/api/health", async (req, res) => {
  const pool = require("./db/pool");
  try {
    await pool.query("SELECT 1");
    res.json({ status: "healthy", postgres: "connected" });
  } catch (err) {
    res.status(503).json({ status: "unhealthy", postgres: err.message });
  }
});

// Error handler
app.use((err, req, res, _next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ error: "Internal server error" });
});

async function start() {
  // Connect Kafka producer (non-blocking — app works without Kafka)
  await kafkaProducer.connect();

  app.listen(config.port, () => {
    console.log(`Task Manager API running at http://localhost:${config.port}`);
    console.log(`  PostgreSQL: ${config.postgres.host}:${config.postgres.port}/${config.postgres.database}`);
    console.log(`  Kafka: ${config.kafka.brokers.join(", ")}`);
  });
}

// Graceful shutdown
async function shutdown(signal) {
  console.log(`\n${signal} received. Shutting down gracefully...`);
  await kafkaProducer.disconnect();
  const pool = require("./db/pool");
  await pool.end();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
