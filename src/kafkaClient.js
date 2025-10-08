import { Kafka } from "kafkajs";
import dotenv from "dotenv";
dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");

export const kafka = new Kafka({
  clientId: "nodejs-kafka-microservice",
  brokers,
});

export const topic = process.env.TOPIC || "test-topic";
export const groupId = process.env.GROUP_ID || "my-service-group";
