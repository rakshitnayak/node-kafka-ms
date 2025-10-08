import { kafka, topic, groupId } from "./kafkaClient.js";

const consumer = kafka.consumer({ groupId });

export async function startConsumer(onMessage) {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value ? message.value.toString() : null;
      // call the provided handler
      try {
        await onMessage({ topic, partition, value, headers: message.headers });
      } catch (err) {
        console.error("Error in onMessage handler", err);
      }
    },
  });
}

export async function disconnectConsumer() {
  await consumer.disconnect();
}
