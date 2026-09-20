import { Kafka, logLevel } from "kafkajs";

const MAX_RECENT_MESSAGES = 100;

export class KafkaService {
  constructor(options) {
    this.options = options;
    this.kafka = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: logLevel.NOTHING,
    });
    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: options.groupId });
    this.listeners = new Set();
    this.recentMessages = [];
    this.connected = false;
    this.connecting = false;
    this.consumerStarted = false;
    this.lastError = null;
  }

  snapshot() {
    return {
      connected: this.connected,
      connecting: this.connecting,
      brokers: this.options.brokers,
      topic: this.options.topic,
      groupId: this.options.groupId,
      partitions: this.options.partitions,
      lastError: this.lastError,
      recentMessages: this.recentMessages,
    };
  }

  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(type, payload) {
    for (const listener of this.listeners) {
      listener({ type, payload, at: new Date().toISOString() });
    }
  }

  async connect() {
    if (this.connected || this.connecting) return this.snapshot();

    this.connecting = true;
    this.lastError = null;
    this.emit("status", this.snapshot());

    try {
      await this.admin.connect();
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: this.options.topic,
            numPartitions: this.options.partitions,
            replicationFactor: 1,
          },
        ],
      });
      await this.producer.connect();
      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: this.options.topic,
        fromBeginning: true,
      });

      if (!this.consumerStarted) {
        this.consumerStarted = true;
        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const record = {
              topic,
              partition,
              offset: message.offset,
              key: message.key?.toString() ?? null,
              value: message.value?.toString() ?? "",
              timestamp: message.timestamp,
            };
            this.recentMessages = [record, ...this.recentMessages].slice(
              0,
              MAX_RECENT_MESSAGES,
            );
            this.emit("message", record);
          },
        });
      }

      this.connected = true;
    } catch (error) {
      this.connected = false;
      this.lastError = error.message;
      throw error;
    } finally {
      this.connecting = false;
      this.emit("status", this.snapshot());
    }

    return this.snapshot();
  }

  async produce({ key, value }) {
    if (!this.connected) throw new Error("Kafka is not connected");

    const result = await this.producer.send({
      topic: this.options.topic,
      messages: [{ key: key || null, value }],
    });

    return result.map(({ topicName, partition, baseOffset }) => ({
      topic: topicName,
      partition,
      offset: baseOffset,
    }));
  }

  async topicState() {
    if (!this.connected) return this.snapshot();

    const [metadata, offsets] = await Promise.all([
      this.admin.fetchTopicMetadata({ topics: [this.options.topic] }),
      this.admin.fetchTopicOffsets(this.options.topic),
    ]);

    return {
      ...this.snapshot(),
      metadata: metadata.topics[0] ?? null,
      offsets,
    };
  }

  async disconnect() {
    const tasks = [];
    if (this.consumerStarted) tasks.push(this.consumer.disconnect());
    tasks.push(this.producer.disconnect(), this.admin.disconnect());
    await Promise.allSettled(tasks);
    this.connected = false;
    this.consumerStarted = false;
    this.emit("status", this.snapshot());
  }
}
