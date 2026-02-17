const { Kafka, CompressionTypes } = require("kafkajs");
const config = require("../config");

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  idempotent: true,
});

let connected = false;

const kafkaProducer = {
  async connect() {
    if (connected) return;
    try {
      await producer.connect();
      connected = true;
      console.log("Kafka producer connected");
    } catch (err) {
      console.error("Failed to connect Kafka producer:", err.message);
      console.warn("Kafka events will be skipped until connection is restored");
    }
  },

  async disconnect() {
    if (!connected) return;
    await producer.disconnect();
    connected = false;
    console.log("Kafka producer disconnected");
  },

  /**
   * Emit a task event to Kafka.
   * @param {string} eventType - CREATE, UPDATE, DELETE
   * @param {object} task - The task data
   * @param {object} [previousState] - Previous task state for updates
   */
  async emit(eventType, task, previousState = null) {
    if (!connected) {
      console.warn(`Kafka not connected, skipping event: ${eventType}`);
      return;
    }

    const event = {
      eventId: crypto.randomUUID(),
      eventType,
      timestamp: new Date().toISOString(),
      source: "task-manager-api",
      data: {
        task,
        previousState,
      },
    };

    try {
      await producer.send({
        topic: config.kafka.topic,
        compression: CompressionTypes.GZIP,
        messages: [
          {
            key: task.id,
            value: JSON.stringify(event),
            headers: {
              eventType,
              source: "task-manager-api",
            },
          },
        ],
      });
    } catch (err) {
      // Log but don't fail the request — Kafka is async/best-effort from API perspective
      console.error(`Failed to emit Kafka event ${eventType}:`, err.message);
    }
  },
};

module.exports = kafkaProducer;
