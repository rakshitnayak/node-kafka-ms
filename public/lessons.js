export const lessons = [
  {
    title: "Why Kafka exists",
    eyebrow: "01 · The problem",
    summary:
      "Kafka moves streams of events between systems without forcing every producer to know every consumer. It gives applications a durable buffer, replayable history, and a way to scale event processing horizontally.",
    diagram: ["Applications", "Kafka event log", "Many consumers"],
    sections: [
      {
        title: "From direct calls to event streams",
        body: "Without Kafka, an order service might call payments, inventory, email, analytics, and fraud detection directly. Each dependency makes the request slower and more fragile. With Kafka, the order service publishes one event. Independent consumers process it at their own speed.",
      },
      {
        title: "Kafka is not just a queue",
        body: "Traditional queues commonly remove work after it is acknowledged. Kafka retains records according to a topic policy, so another consumer group can read the same history later. Consumers track their own position instead of owning or deleting records.",
      },
      {
        title: "Where it fits",
        body: "Kafka is useful for event-driven services, activity tracking, log aggregation, change-data capture, stream processing, and moving data between databases. It is less suitable for tiny systems, request-response work, very large file storage, or tasks that need complex per-message routing.",
      },
    ],
    keyPoints: [
      "Producers and consumers are decoupled.",
      "Records remain replayable for a configured time.",
      "Kafka is designed for high-throughput ordered event streams.",
    ],
    task: "Imagine an online store. List three services that could independently react to an order-created event.",
  },
  {
    title: "Records, topics, and logs",
    eyebrow: "02 · Core vocabulary",
    summary:
      "A record is the unit of data. Records are appended to named topics, and every topic acts like a durable chronological log rather than a mutable database table.",
    diagram: ["Record", "Topic: orders", "Stored history"],
    sections: [
      {
        title: "Anatomy of a record",
        body: "A Kafka record has a value and may also have a key, timestamp, and headers. Kafka treats the key and value as bytes; your application decides whether those bytes represent JSON, Avro, Protobuf, text, or something else.",
      },
      {
        title: "Topics are categories",
        body: "A topic groups one kind of event, such as orders, payments, or inventory-updates. Topic names should describe stable business facts. Events are usually written in the past tense—order-created—because they report something that already happened.",
      },
      {
        title: "Append-only does not mean permanent",
        body: "New records are appended; existing records are not edited in place. Kafka deletes old segments according to retention rules, or compacts a topic by key. Applications build current state by interpreting the log.",
      },
    ],
    keyPoints: [
      "Kafka stores byte arrays, not JavaScript objects.",
      "Headers carry small metadata such as trace IDs.",
      "Topic design is part of your domain design.",
    ],
    task: "Produce a record and inspect its value, key, partition, and offset in the simulator.",
  },
  {
    title: "Partitions and ordering",
    eyebrow: "03 · Parallel logs",
    summary:
      "Kafka splits a topic into partitions. Each partition is an independent ordered log, which is the fundamental unit of storage, ordering, and consumer parallelism.",
    diagram: ["Topic", "P0: 0 → 1 → 2", "P1: 0 → 1 → 2"],
    sections: [
      {
        title: "Offsets are partition-local",
        body: "Every appended record receives a monotonically increasing offset inside its partition. P0 offset 5 and P1 offset 5 are unrelated records. Kafka does not provide one global topic-wide order.",
      },
      {
        title: "Partitions unlock scale",
        body: "Different brokers can host different partitions, and different consumers can process them concurrently. More partitions raise potential throughput, but also increase metadata, open files, replication traffic, and rebalance work.",
      },
      {
        title: "Partition count is a design choice",
        body: "You can increase a topic's partition count, but changing it alters key-to-partition mapping for future records. Reducing the count is not a normal Kafka operation. Choose enough capacity with room to grow, without creating thousands unnecessarily.",
      },
    ],
    keyPoints: [
      "Ordering exists only within one partition.",
      "A partition is processed by at most one consumer per group.",
      "Partition count limits useful consumer parallelism.",
    ],
    task: "Remove the message key and produce six records across three partitions. Observe that each partition has its own offset sequence.",
  },
  {
    title: "Keys and partitioning",
    eyebrow: "04 · Routing",
    summary:
      "A producer chooses the destination partition. A key lets related events consistently reach the same partition so their local ordering is preserved.",
    diagram: ["customer-42", "partitioner hash", "Always one partition"],
    sections: [
      {
        title: "Choose a business key",
        body: "Use the identifier whose events must stay ordered: orderId for an order workflow, customerId for a customer timeline, or accountId for balance changes. A random event ID usually does not provide useful grouping.",
      },
      {
        title: "No key means distribution",
        body: "When the key is absent, producers normally distribute records to improve batching and balance. The exact strategy depends on the client. Do not assume two unkeyed records will stay together.",
      },
      {
        title: "Watch for hot partitions",
        body: "If one key receives far more traffic than others, its partition becomes a bottleneck. Good keys balance load while keeping the ordering boundary your business actually requires.",
      },
    ],
    keyPoints: [
      "Same key plus stable partition count gives stable routing.",
      "Keys create ordering boundaries, not global ordering.",
      "Uneven keys can create uneven broker and consumer load.",
    ],
    task: "Produce several records with customer-42, then change to customer-99. Compare their partition destinations.",
  },
  {
    title: "Brokers, clusters, and KRaft",
    eyebrow: "05 · Cluster anatomy",
    summary:
      "A broker is one Kafka server. Several brokers form a cluster, share partitions, replicate data, and coordinate through Kafka's KRaft metadata quorum.",
    diagram: ["Broker 1", "KRaft metadata quorum", "Brokers 2 and 3"],
    sections: [
      {
        title: "What a broker does",
        body: "A broker accepts produce and fetch requests, stores partition log segments on disk, serves replicas, and reports cluster state. Clients use seed broker addresses only to bootstrap; metadata tells them which broker currently leads each partition.",
      },
      {
        title: "Controllers manage metadata",
        body: "Modern Kafka uses KRaft controllers to manage topic metadata, partition leadership, broker membership, and configuration. Older clusters used ZooKeeper, but new learning projects should focus on KRaft.",
      },
      {
        title: "Clients talk to leaders",
        body: "For each partition, one broker is the leader. Producers and consumers communicate with that leader. Followers copy the leader's log and may take over after a failure.",
      },
    ],
    keyPoints: [
      "A cluster is multiple cooperating brokers.",
      "KRaft stores and coordinates cluster metadata.",
      "Partition leadership can move between brokers.",
    ],
    task: "Inspect docker-compose.yml. This lab has one combined broker/controller, which is useful for learning but offers no failover.",
  },
  {
    title: "Replication and durability",
    eyebrow: "06 · Surviving failure",
    summary:
      "Replication copies each partition across brokers. The leader handles client traffic while follower replicas continuously copy its log.",
    diagram: ["Leader replica", "In-sync replicas", "Failover"],
    sections: [
      {
        title: "Replication factor",
        body: "Replication factor 3 means three brokers hold a copy of each partition. It cannot exceed the number of available brokers. Replicas should be distributed so one machine failure does not remove every copy.",
      },
      {
        title: "ISR and acknowledgements",
        body: "The in-sync replica set contains replicas sufficiently caught up with the leader. With producer acks=all, Kafka waits for every required in-sync replica. min.insync.replicas can reject writes when too few safe copies remain.",
      },
      {
        title: "Availability versus consistency",
        body: "Allowing an out-of-sync replica to become leader may restore availability but can lose acknowledged data. Production clusters normally prefer clean leader election and enough replicas to tolerate failures safely.",
      },
    ],
    keyPoints: [
      "Replication factor 1 cannot survive broker loss.",
      "acks=all and min.insync.replicas work together.",
      "Replication protects availability; backups still protect against human mistakes.",
    ],
    task: "Compare this lab's replication factor 1 with a three-broker design using replication factor 3 and min.insync.replicas 2.",
  },
  {
    title: "How producers work",
    eyebrow: "07 · Publishing",
    summary:
      "A producer serializes records, selects partitions, batches records per broker, optionally compresses them, and waits for the configured acknowledgement level.",
    diagram: ["Serialize", "Batch + compress", "Broker acknowledgement"],
    sections: [
      {
        title: "Batching creates throughput",
        body: "Kafka is fast because it sends groups of records rather than one network request per event. batch size and linger settings trade a small amount of latency for larger, more efficient requests. Compression often improves both network and storage efficiency.",
      },
      {
        title: "Acknowledgement choices",
        body: "acks=0 does not wait, acks=1 waits only for the leader, and acks=all waits for the required in-sync replicas. Stronger acknowledgement improves durability at the cost of some latency.",
      },
      {
        title: "Retries need idempotence",
        body: "A producer may retry after a timeout even when the broker actually stored the first attempt. Idempotent production lets Kafka recognize duplicate retry sequences and preserve ordering within the producer session.",
      },
    ],
    keyPoints: [
      "Throughput comes from batching and sequential I/O.",
      "Compression is applied to record batches.",
      "Handle send failures instead of silently dropping events.",
    ],
    code: {
      title: "KafkaJS producer",
      body: `await producer.send({
  topic: "orders",
  messages: [{
    key: order.customerId,
    value: JSON.stringify(order)
  }]
});`,
    },
    task: "Use Live Kafka locally and compare the returned partition and offset for repeated keys.",
  },
  {
    title: "Consumers and consumer groups",
    eyebrow: "08 · Reading",
    summary:
      "A consumer fetches records from partition leaders. Consumers sharing a group divide partitions so the group processes each partition through one active member at a time.",
    diagram: ["P0 + P1 + P2", "Consumer group", "C1 owns 0,2 · C2 owns 1"],
    sections: [
      {
        title: "Groups are independent subscribers",
        body: "The payments group and analytics group can both read the orders topic. Each group has its own assignments and committed offsets. Inside one group, members cooperate rather than each receiving every record.",
      },
      {
        title: "Partitions cap parallel work",
        body: "With three partitions, at most three consumers in one group can actively read. A fourth member is idle until ownership changes. One consumer can own multiple partitions when there are fewer consumers than partitions.",
      },
      {
        title: "The poll loop matters",
        body: "Consumers repeatedly fetch records, process them, and maintain group membership with heartbeats. Slow or blocked processing can miss timeouts and cause Kafka to remove the member, triggering a rebalance.",
      },
    ],
    keyPoints: [
      "Different groups each receive the topic's records.",
      "Members within one group share the work.",
      "Keep processing bounded and heartbeat behavior healthy.",
    ],
    code: {
      title: "KafkaJS consumer",
      body: `await consumer.subscribe({ topic: "orders" });

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    await handleOrder(JSON.parse(message.value.toString()));
  }
});`,
    },
    task: "Set two partitions and three consumers. Identify the idle member, then increase to three partitions.",
  },
  {
    title: "Offsets, commits, and lag",
    eyebrow: "09 · Progress",
    summary:
      "An offset identifies a record's position in one partition. A consumer group commits offsets to store its progress and resume after restarts.",
    diagram: ["Committed offset 3", "Read offsets 3,4,5", "Lag 3"],
    sections: [
      {
        title: "Committed offset means next position",
        body: "After successfully processing offset 7, a group normally commits 8—the next record it expects. On restart or reassignment, the new consumer asks Kafka to resume from that committed position.",
      },
      {
        title: "Auto-commit versus manual control",
        body: "Auto-commit is convenient but can commit before external work is safely finished if processing is structured poorly. Manual or carefully timed commits provide more control, but committing too late can cause duplicate processing after failure.",
      },
      {
        title: "Lag is a symptom",
        body: "Lag is the distance between a partition's latest offset and a group's committed position. Growing lag can mean slow processing, unavailable consumers, traffic spikes, hot partitions, downstream failures, or rebalances—not merely that Kafka is slow.",
      },
    ],
    keyPoints: [
      "Offsets belong to a topic-partition and consumer group.",
      "Committing records progress; it does not delete data.",
      "Monitor lag per partition, not only as one total.",
    ],
    task: "Produce five records and consume two. Observe the partition cursors and total lag, then finish the backlog.",
  },
  {
    title: "Rebalancing and failures",
    eyebrow: "10 · Group coordination",
    summary:
      "When group membership or topic partitions change, Kafka redistributes partition ownership. This coordination process is called a rebalance.",
    diagram: ["Member leaves", "Partitions pause + move", "Processing resumes"],
    sections: [
      {
        title: "Why rebalances happen",
        body: "A consumer starts, stops, crashes, misses its session timeout, changes subscription, or the topic gains partitions. The group coordinator then calculates or accepts a new assignment.",
      },
      {
        title: "Why they are disruptive",
        body: "Partition processing pauses while ownership changes. Frequent rebalances increase latency and can repeat work around the last commit. Graceful shutdown, appropriate timeouts, and stable consumer instances reduce disruption.",
      },
      {
        title: "Cooperative rebalancing",
        body: "Cooperative assignors move only the partitions that need to change instead of revoking everything at once. Static group membership can also reduce unnecessary reassignment during brief restarts.",
      },
    ],
    keyPoints: [
      "A rebalance changes ownership, not stored records.",
      "Processing resumes from committed offsets.",
      "Repeated rebalances usually indicate unhealthy consumers or configuration.",
    ],
    task: "Build lag, crash consumer 1, and watch its partitions move while their offsets remain unchanged.",
  },
  {
    title: "Delivery guarantees",
    eyebrow: "11 · Failure semantics",
    summary:
      "Delivery guarantees describe what can happen when a process fails between reading, performing work, and committing progress.",
    diagram: ["Read", "Process", "Commit offset"],
    sections: [
      {
        title: "At-most-once",
        body: "Commit before processing. A crash after the commit but before the work finishes loses that work. Records are processed zero or one time, which is acceptable only when occasional loss is harmless.",
      },
      {
        title: "At-least-once",
        body: "Process first, then commit. A crash after processing but before committing causes the record to be read again. Work is not lost, but duplicate side effects are possible. This is the most common practical model.",
      },
      {
        title: "Exactly-once needs a boundary",
        body: "Kafka transactions can atomically publish output records and consumer offsets for Kafka-to-Kafka processing. They cannot magically make an unrelated HTTP API or database transaction exactly-once. External effects still need idempotency or an outbox pattern.",
      },
    ],
    keyPoints: [
      "Failures make commit timing observable.",
      "At-least-once plus idempotent handlers is a strong default.",
      "Always define exactly which side effects a guarantee covers.",
    ],
    task: "Pretend processing succeeds but committing fails. Explain why the same record will be delivered again.",
  },
  {
    title: "Idempotency and transactions",
    eyebrow: "12 · Safe repetition",
    summary:
      "Idempotent operations produce the same final result when repeated. They are essential because distributed systems cannot eliminate every ambiguous retry.",
    diagram: ["Event ID", "Deduplication check", "One business effect"],
    sections: [
      {
        title: "Idempotent consumers",
        body: "Give every event a stable identifier. Before applying a side effect, record or atomically check that identifier. Database uniqueness constraints, upserts, and state-version checks are common tools.",
      },
      {
        title: "The transactional outbox",
        body: "When a database update and an event must stay consistent, write both the business change and an outbox row in one database transaction. A relay later publishes the outbox row to Kafka, avoiding the dual-write failure gap.",
      },
      {
        title: "Kafka transactions",
        body: "A transactional producer can atomically write to multiple Kafka partitions. A consume-transform-produce application can also include consumed offsets in that transaction so output and progress become visible together.",
      },
    ],
    keyPoints: [
      "Retries are normal; design effects to tolerate them.",
      "Producer idempotence and consumer idempotence solve different problems.",
      "Transactions have a cost and should protect a real atomicity requirement.",
    ],
    task: "Design an idempotency key for charging an order so receiving the payment event twice cannot charge twice.",
  },
  {
    title: "Retention and compaction",
    eyebrow: "13 · Data lifecycle",
    summary:
      "Kafka stores records in immutable segment files and removes old data using time, size, or key-compaction policies.",
    diagram: ["Active segment", "Closed segments", "Delete or compact"],
    sections: [
      {
        title: "Delete retention",
        body: "A delete-policy topic removes old log segments after configured time or size thresholds. Retention is independent of consumption: unread records can expire, and consumed records remain until the policy removes them.",
      },
      {
        title: "Log compaction",
        body: "A compacted topic eventually retains at least the latest value for each key, making it useful for rebuilding current state. Compaction is asynchronous and does not mean only one record per key is visible at every instant.",
      },
      {
        title: "Tombstones",
        body: "A record with a key and null value is a tombstone. In a compacted topic it represents deletion; after a configured period, Kafka can remove both the tombstone and older values for that key.",
      },
    ],
    keyPoints: [
      "Consumption never directly deletes a record.",
      "Retention must cover outage and replay requirements.",
      "Compaction preserves key history semantics, not exact snapshots at every moment.",
    ],
    task: "Choose delete retention for audit events and compaction for a latest-customer-profile stream. Explain the difference.",
  },
  {
    title: "Schemas and event evolution",
    eyebrow: "14 · Data contracts",
    summary:
      "Events are APIs stored over time. A schema describes their structure, and compatibility rules let producers and consumers evolve independently.",
    diagram: ["Producer schema", "Schema registry", "Compatible consumers"],
    sections: [
      {
        title: "Why plain JSON is not enough",
        body: "JSON is readable, but it does not by itself enforce required fields, types, defaults, or compatibility. Avro, Protobuf, and JSON Schema can formalize those rules, usually with a schema registry.",
      },
      {
        title: "Compatibility",
        body: "Backward compatibility means new consumers can read old data. Forward compatibility means old consumers can read newly produced data. Full compatibility supports both directions under the registry's rules.",
      },
      {
        title: "Evolve additively",
        body: "Prefer adding optional fields with sensible defaults. Avoid silently changing a field's meaning or type. Event names and payloads should express business facts rather than mirror temporary database tables.",
      },
    ],
    keyPoints: [
      "Consumers may replay events written years earlier.",
      "Validate schemas before publishing incompatible records.",
      "Treat topic payloads as long-lived public contracts.",
    ],
    task: "Add an optional currency field to an order event without breaking consumers that only understand amount.",
  },
  {
    title: "Retries and dead-letter topics",
    eyebrow: "15 · Error handling",
    summary:
      "Some failures are temporary and should be retried; others are permanent and need isolation, investigation, or correction.",
    diagram: ["Main topic", "Retry with backoff", "Dead-letter topic"],
    sections: [
      {
        title: "Classify failures",
        body: "A timeout or unavailable database may succeed later. An invalid schema, missing required field, or unknown account may never succeed unchanged. Retrying permanent failures forever blocks useful work and grows lag.",
      },
      {
        title: "Backoff without blocking",
        body: "Immediate retries can overload a failing dependency. Retry topics with increasing delays, scheduled retry storage, or bounded in-process retries spread attempts over time. Preserve the original key, payload, error, and attempt count.",
      },
      {
        title: "Dead-letter topics are not trash",
        body: "A dead-letter topic isolates records after retry policy is exhausted. It needs ownership, alerts, retention, inspection tools, and a safe replay process. Otherwise it simply hides data loss.",
      },
    ],
    keyPoints: [
      "Retry only errors that may actually recover.",
      "Use exponential backoff and bounded attempts.",
      "Make replay idempotent and observable.",
    ],
    task: "Classify a network timeout, malformed JSON, and a rate-limit response as retryable or permanent.",
  },
  {
    title: "Security",
    eyebrow: "16 · Protecting Kafka",
    summary:
      "Production Kafka needs encrypted connections, authenticated clients, authorization rules, protected credentials, and careful control over sensitive event data.",
    diagram: ["TLS", "SASL identity", "Topic ACLs"],
    sections: [
      {
        title: "Encryption and authentication",
        body: "TLS encrypts traffic and verifies broker identity. SASL mechanisms such as SCRAM authenticate applications. Credentials belong in a secret manager or deployment environment, never in source control or browser JavaScript.",
      },
      {
        title: "Authorization",
        body: "Access-control lists should give each application only the topic and group operations it requires. Producer services need write access; consumer services need read access plus permission for their group IDs.",
      },
      {
        title: "Data governance",
        body: "Events may be retained and copied widely. Minimize personal data, define retention intentionally, audit access, and consider field-level encryption or tokenization for especially sensitive values.",
      },
    ],
    keyPoints: [
      "Never expose Kafka credentials to the frontend.",
      "Use TLS whenever credentials or private data cross a network.",
      "Apply least privilege per service and environment.",
    ],
    task: "Review .env.example and identify which future managed-Kafka values would need to be deployment secrets.",
  },
  {
    title: "Operations and observability",
    eyebrow: "17 · Running Kafka",
    summary:
      "A healthy Kafka system is monitored at the broker, partition, producer, consumer-group, and application levels—not by one dashboard number.",
    diagram: ["Broker health", "Producer errors", "Consumer lag"],
    sections: [
      {
        title: "Cluster signals",
        body: "Watch offline partitions, under-replicated partitions, ISR shrink events, disk usage, request latency, controller health, network saturation, and partition distribution. Disk capacity must account for retention and replication.",
      },
      {
        title: "Client signals",
        body: "Producers need send error rate, retry rate, acknowledgement latency, and batch efficiency. Consumers need lag per partition, processing time, rebalance frequency, commit failures, and records processed per second.",
      },
      {
        title: "Operational discipline",
        body: "Use graceful shutdown, rolling deployments, capacity planning, tested disaster recovery, quotas, and documented ownership. Alert on user impact and sustained abnormal trends rather than every transient spike.",
      },
    ],
    keyPoints: [
      "Lag without processing latency lacks context.",
      "Under-replicated partitions reduce failure tolerance.",
      "Capacity planning must include replay and traffic spikes.",
    ],
    task: "Name one broker, producer, and consumer metric you would alert on for an order-processing system.",
  },
  {
    title: "Putting it together with KafkaJS",
    eyebrow: "18 · Practical workflow",
    summary:
      "A production-oriented Node service connects once, handles shutdown, validates events, publishes with stable keys, processes idempotently, and exposes health and metrics.",
    diagram: ["Node producer", "Kafka cluster", "Node consumer worker"],
    sections: [
      {
        title: "Producer responsibility",
        body: "Validate the event, choose a stable key, serialize it, attach useful headers such as schema version and trace ID, await Kafka's acknowledgement, and surface failures to the caller or an outbox relay.",
      },
      {
        title: "Consumer responsibility",
        body: "Deserialize and validate again, perform bounded idempotent work, classify errors, commit only at the chosen safety point, and stop gracefully so another member can take ownership cleanly.",
      },
      {
        title: "Your path through this lab",
        body: "First master the simulator's keys, partitions, offsets, groups, lag, and rebalance. Then repeat those experiments using Live Kafka locally. Finally add multiple services, retries, schemas, replication, and observability one concept at a time.",
      },
    ],
    keyPoints: [
      "Keep producers and consumers in dedicated modules.",
      "Do not run a permanent Kafka consumer in a short-lived serverless request.",
      "Prefer simple, observable processing over clever hidden behavior.",
    ],
    code: {
      title: "Graceful shutdown",
      body: `const shutdown = async () => {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);`,
    },
    task: "Run this project locally, complete every simulator experiment, then trace one real record from producer acknowledgement to consumer output.",
  },
];
