import "dotenv/config";

const numberFromEnv = (name, fallback) => {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? value : fallback;
};

export const config = {
  port: numberFromEnv("PORT", 3000),
  isVercel: process.env.VERCEL === "1",
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || "kafka-lab",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092")
      .split(",")
      .map((broker) => broker.trim())
      .filter(Boolean),
    topic: process.env.TOPIC || "kafka-lab-events",
    groupId: process.env.GROUP_ID || "kafka-lab-learners",
    partitions: numberFromEnv("TOPIC_PARTITIONS", 3),
  },
};
