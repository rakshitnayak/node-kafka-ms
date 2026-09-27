import { config } from "./config.js";
import app, { kafka } from "./app.js";

const server = app.listen(config.port, () => {
  console.log(`Kafka Lab running at http://localhost:${config.port}`);
  console.log(`Kafka broker: ${config.kafka.brokers.join(", ")}`);
});

const shutdown = async () => {
  server.close();
  await kafka.disconnect();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
