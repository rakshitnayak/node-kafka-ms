import express from "express";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { config } from "./config.js";
import { KafkaService } from "./kafka-service.js";

export const kafka = new KafkaService(config.kafka);
export const liveKafkaEnabled = !config.isVercel;

const app = express();
const publicDirectory = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public",
);

app.use(express.json({ limit: "32kb" }));

app.get("/api/health", (_request, response) => {
  response.json({
    app: "ok",
    deployment: config.isVercel ? "vercel" : "local",
    features: { liveKafka: liveKafkaEnabled },
    kafka: kafka.snapshot(),
  });
});

app.get("/api/kafka/state", async (_request, response) => {
  if (!liveKafkaEnabled) {
    return response.status(501).json({
      error: "Live Kafka requires the always-on worker planned for Phase 2.",
    });
  }

  try {
    return response.json(await kafka.topicState());
  } catch (error) {
    return response.status(503).json({ error: error.message, ...kafka.snapshot() });
  }
});

app.post("/api/kafka/connect", async (_request, response) => {
  if (!liveKafkaEnabled) {
    return response.status(501).json({
      error: "Live Kafka is disabled on the serverless Phase 1 deployment.",
    });
  }

  try {
    return response.json(await kafka.connect());
  } catch (error) {
    return response.status(503).json({ error: error.message });
  }
});

app.post("/api/kafka/messages", async (request, response) => {
  if (!liveKafkaEnabled) {
    return response.status(501).json({
      error: "Live Kafka is disabled on the serverless Phase 1 deployment.",
    });
  }

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
  if (!liveKafkaEnabled) {
    return response.status(501).json({
      error: "The Kafka event stream requires an always-on worker.",
    });
  }

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

if (!config.isVercel) {
  app.use(express.static(publicDirectory));
  app.get("*", (_request, response) => {
    response.sendFile(path.join(publicDirectory, "index.html"));
  });
}

export default app;
