# Node Kafka Server

This project is a **Node.js Kafka server** using **KafkaJS** and is designed to run smoothly inside **Docker**.

## 🚀 Features

- Node.js Kafka producer/consumer setup
- Easy to run using Docker Compose

---

## 📦 Prerequisites

Make sure you have installed:

- **Node.js**
- **Docker**

---

## ▶️ How to Run Using Docker

### 1. Start Kafka

```sh
docker run -p 9092:9092 apache/kafka:3.7.1
```

### 2. Run your app

```sh
npm i
npm run start
```

---

## 🔍 Useful Kafka Commands (Docker)

### Get shell access to container

```sh
docker ps
docker exec -it container_id /bin/bash
cd /opt/kafka/bin
```

### Create topic

```sh
./kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```

## 📝 Notes

- Make sure Kafka is running before starting the Node server.
