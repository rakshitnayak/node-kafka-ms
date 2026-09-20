import express from "express";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { config } from "./config.js";
import { KafkaService } from "./kafka-service.js";

const app = express();
const kafka = new KafkaService(config.kafka);
const publicDirectory = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public",
);

app.use(express.json({ limit: "32kb" }));
app.use(express.static(publicDirectory));

app.get("/api/health", (_request, response) => {
  response.json({ app: "ok", kafka: kafka.snapshot() });
});

app.get("/api/kafka/state", async (_request, response) => {
  try {
    response.json(await kafka.topicState());
  } catch (error) {
    response.status(503).json({ error: error.message, ...kafka.snapshot() });
  }
});

app.post("/api/kafka/connect", async (_request, response) => {
  try {
    response.json(await kafka.connect());
  } catch (error) {
    response.status(503).json({ error: error.message });
  }
});

app.post("/api/kafka/messages", async (request, response) => {
  const key = typeof request.body.key === "string" ? request.body.key : "";
  const value =
    typeof request.body.value === "string" ? request.body.value.trim() : "";

  if (!value) {
    return response.status(400).json({ error: "value is required" });
  }

  try {
    return response.status(201).json(await kafka.produce({ key, value }));
  } catch (error) {
    return response.status(503).json({ error: error.message });
  }
});

app.get("/api/events", (request, response) => {
  response.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  response.flushHeaders();

  const send = (event) => {
    response.write(`event: ${event.type}\n`);
    response.write(`data: ${JSON.stringify(event)}\n\n`);
  };
  send({ type: "status", payload: kafka.snapshot() });
  const unsubscribe = kafka.subscribe(send);
  const heartbeat = setInterval(() => response.write(": heartbeat\n\n"), 15_000);

  request.on("close", () => {
    clearInterval(heartbeat);
    unsubscribe();
  });
});

app.get("*", (_request, response) => {
  response.sendFile(path.join(publicDirectory, "index.html"));
});

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
