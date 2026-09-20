# Kafka Lab

Kafka Lab is an interactive way to learn Kafka with Node.js. It contains:

- A browser simulator that works without a Kafka broker
- A real KafkaJS producer and consumer
- Visual partitions, offsets, consumer ownership, and lag
- Consumer crash and group-rebalance experiments
- Six short lessons connected to hands-on exercises

## What the simulator teaches

The simulator models the core Kafka rules:

1. A topic is split into ordered partitions.
2. Every record receives an offset within its partition.
3. A key consistently routes related records to one partition.
4. Consumers in one group divide partition ownership.
5. Consumer progress is stored as committed offsets.
6. Consumer lag is the difference between available and processed records.
7. When a consumer fails, the group rebalances its partitions.

The simulation deliberately focuses on these concepts. It is not a replacement
for a real broker and does not model replication, retention, or delivery failures.

## Run the simulator

```sh
npm install
npm start
```

Open <http://localhost:3000>. The **Simulator** and **Learn** sections work even
when Kafka is not running.

## Run with a real Kafka broker

Start the included single-node KRaft broker:

```sh
docker compose up -d
npm start
```

Open <http://localhost:3000>, choose **Live Kafka**, and click **Connect Kafka**.
The server creates `kafka-lab-events`, starts a KafkaJS producer, and starts a
consumer in the `kafka-lab-learners` group.

Stop Kafka without deleting its records:

```sh
docker compose stop
```

To remove the broker and its learning data:

```sh
docker compose down -v
```

## Configuration

Copy `.env.example` to `.env` and change values when necessary:

```dotenv
PORT=3000
KAFKA_CLIENT_ID=kafka-lab
KAFKA_BROKERS=localhost:9092
TOPIC=kafka-lab-events
GROUP_ID=kafka-lab-learners
TOPIC_PARTITIONS=3
```

## Project structure

```text
public/
  index.html          Browser UI
  styles.css          Responsive visual design
  app.js              Simulator, lessons, and live event rendering
src/
  config.js           Environment configuration
  kafka-service.js    KafkaJS admin, producer, consumer, and event stream
  index.js            Express server and HTTP API
docker-compose.yml    Local single-node Kafka broker
```

## Learning path

1. Use the simulator without keys and observe round-robin partitioning.
2. Reuse one key and verify that its records stay in one partition.
3. Produce faster than you consume and watch lag increase.
4. Add more consumers than partitions and observe idle consumers.
5. Crash a consumer and observe partition reassignment.
6. Repeat the experiment in Live Kafka and inspect actual offsets.

## Useful commands

```sh
npm run check
npm run dev
docker compose logs -f kafka
```

The Docker setup uses one broker and replication factor 1 for learning. A
production Kafka cluster needs multiple brokers, replication, security,
monitoring, and a deliberate retry and delivery-semantics strategy.
