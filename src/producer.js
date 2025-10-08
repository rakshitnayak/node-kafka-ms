import { kafka, topic } from "./kafkaClient.js";

const producer = kafka.producer();

export async function startProducer() {
  await producer.connect();
}

export async function publishMessage(value) {
  // value must be string or Buffer
  await producer.send({
    topic,
    messages: [{ value }],
  });
}

export async function disconnectProducer() {
  await producer.disconnect();
}
