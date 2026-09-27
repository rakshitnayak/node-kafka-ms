# Kafka Lab

Kafka Lab is an interactive Kafka learning project built with Node.js,
Express, and KafkaJS. It helps beginners understand Kafka visually before
trying the same concepts against a real local Kafka broker.

## What the project includes

- An interactive producer, topic, partition, and consumer-group simulator
- Visual message keys, offsets, committed positions, and consumer lag
- Configurable partition and consumer counts
- Consumer failure and group-rebalance experiments
- 18 detailed Kafka learning chapters with practical exercises
- A real KafkaJS producer and consumer for local experimentation
- A single-node Kafka 3.7.1 KRaft environment using Docker Compose

The simulator works without Kafka. The **Live Kafka** section connects to the
local Docker broker and lets you publish and consume real records.

## Prerequisites

- Node.js 20 or newer
- npm
- Docker Desktop or another Docker installation, only for Live Kafka

## Install the project

```sh
git clone https://github.com/rakshitnayak/node-kafka-ms.git
cd node-kafka-ms
npm install
cp .env.example .env
```

## Run the simulator and learning guide

Kafka is not required for the simulator or documentation:

```sh
npm start
```

Open <http://localhost:3000> and use the **Simulator** and **Learn** tabs.

For automatic restarts while editing:

```sh
npm run dev
```

## Run with real Kafka

Start the included broker:

```sh
docker compose up -d
```

Start Kafka Lab:

```sh
npm start
```

Open <http://localhost:3000>, choose **Live Kafka**, click **Connect Kafka**,
and send a record. The application creates the configured topic, starts a
KafkaJS producer, and starts a consumer in the configured consumer group.

Inspect the Kafka container:

```sh
docker compose ps
docker compose logs -f kafka
```

Stop Kafka while preserving its data:

```sh
docker compose stop
```

Remove Kafka and the local learning data volume:

```sh
docker compose down -v
```

## Configuration

The default `.env.example` contains:

```dotenv
PORT=3000
KAFKA_CLIENT_ID=kafka-lab
KAFKA_BROKERS=localhost:9092
TOPIC=kafka-lab-events
GROUP_ID=kafka-lab-learners
TOPIC_PARTITIONS=3
```

Copy it to `.env` before changing local values. `.env` is ignored by Git.

## Project structure

```text
public/
  index.html          Application screens and controls
  styles.css          Responsive visual design
  app.js              Simulator and browser behavior
  lessons.js          Kafka learning curriculum
src/
  app.js              Express routes and application export
  config.js           Environment configuration
  index.js            Local HTTP server and graceful shutdown
  kafka-service.js    KafkaJS admin, producer, and consumer
test/
  local-app.test.js   Local-mode behavior
  vercel-app.test.js  Serverless-mode behavior
docker-compose.yml    Local single-node Kafka broker
```

## Checks

```sh
npm run check
npm test
```

The Docker environment uses one broker and replication factor 1 for learning.
It is not a production Kafka cluster and cannot demonstrate broker failover or
replica durability by itself.
