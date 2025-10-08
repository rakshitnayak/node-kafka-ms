import express from "express";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import { startProducer, publishMessage } from "./producer.js";
import { startConsumer } from "./consumer.js";

dotenv.config();

const PORT = process.env.PORT || 3000;

const app = express();
app.use(bodyParser.json());

// health
app.get("/health", (req, res) => res.json({ status: "ok" }));

// publish endpoint
app.post("/publish", async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: "message is required" });

  try {
    await publishMessage(
      JSON.stringify({ message, ts: new Date().toISOString() })
    );
    return res.json({ status: "sent" });
  } catch (err) {
    console.error("publish error", err);
    return res.status(500).json({ error: "publish failed" });
  }
});

// start everything
(async () => {
  try {
    await startProducer();
    console.log("Producer connected");

    // consumer handler example — process messages
    await startConsumer(async ({ topic, partition, value }) => {
      console.log(`Consumed message on ${topic}[${partition}]: ${value}`);
      // add processing logic here (DB write, call another service, etc.)
    });

    app.listen(PORT, () => console.log(`Server listening on ${PORT}`));
  } catch (err) {
    console.error("Failed to start service", err);
    process.exit(1);
  }
})();
