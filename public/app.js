const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const lessons = [
  {
    title: "The mental model",
    eyebrow: "01 · Record log",
    copy: "Kafka is a distributed, append-only log. Producers append records to topics; consumers independently remember how far they have read. Kafka keeps records for a configured retention period instead of deleting them after one consumer reads them.",
    diagram: ["Producer", "Topic log", "Consumer"],
    task: "Produce three records, consume only one, and watch lag become 2. The records remain visible after consumption.",
  },
  {
    title: "Topics and partitions",
    eyebrow: "02 · Parallel logs",
    copy: "A topic is split into partitions. Each partition is its own ordered log, with offsets starting at 0. Kafka guarantees order inside one partition—not across the whole topic. More partitions allow more parallel consumers.",
    diagram: ["Topic", "P0: 0 → 1 → 2", "P1: 0 → 1"],
    task: "Choose three partitions and produce records without a key. They spread across partitions in round-robin order.",
  },
  {
    title: "Keys preserve local order",
    eyebrow: "03 · Routing",
    copy: "When a record has a key, the producer hashes that key to choose a partition. Records with the same key therefore land in the same partition, preserving order for that entity—such as one customer or order.",
    diagram: ["customer-42", "hash(key)", "Partition 1"],
    task: "Produce several records with customer-42, then change the key. Same-key records stay together.",
  },
  {
    title: "Offsets and consumer lag",
    eyebrow: "04 · Progress",
    copy: "An offset is a record’s position in one partition. A consumer group commits offsets to remember its progress. Lag is the distance between the newest available offsets and the group’s committed offsets.",
    diagram: ["Committed: 2", "Records: 3, 4, 5", "Lag: 3"],
    task: "Produce five records and consume them one at a time. Notice the next offset and lag change independently per partition.",
  },
  {
    title: "Consumer groups scale work",
    eyebrow: "05 · Parallelism",
    copy: "Within one consumer group, a partition belongs to only one consumer at a time. That prevents duplicate processing inside the group. A group cannot usefully have more active consumers than partitions; extra consumers remain idle.",
    diagram: ["P0 → C1", "P1 → C2", "P2 → C1"],
    task: "Set two partitions and three consumers. The third consumer has no partition and becomes idle.",
  },
  {
    title: "Failures trigger rebalancing",
    eyebrow: "06 · Recovery",
    copy: "When a consumer joins or leaves a group, Kafka reassigns partitions among the remaining consumers. This is a rebalance. Processing briefly pauses, then resumes from the group’s committed offsets.",
    diagram: ["Consumer 1 fails", "Rebalance", "Consumer 2 owns all"],
    task: "Create lag, crash consumer 1, and inspect how its partitions move to active consumers without losing their offsets.",
  },
];

const simulation = {
  partitions: 3,
  consumers: 2,
  records: [],
  nextOffsets: [],
  cursors: [],
  produced: 0,
  consumed: 0,
  roundRobin: 0,
  consumerOneCrashed: false,
  autoTimer: null,
  explanationStep: 1,
};

function hashKey(key) {
  let hash = 0;
  for (const character of key) {
    hash = (hash * 31 + character.charCodeAt(0)) | 0;
  }
  return Math.abs(hash);
}

function assignments() {
  const activeConsumers = Array.from(
    { length: simulation.consumers },
    (_, index) => index,
  ).filter((index) => !(simulation.consumerOneCrashed && index === 0));
  const result = Array.from({ length: simulation.consumers }, () => []);

  for (let partition = 0; partition < simulation.partitions; partition += 1) {
    if (activeConsumers.length) {
      result[activeConsumers[partition % activeConsumers.length]].push(partition);
    }
  }
  return result;
}

function lag() {
  return simulation.records.reduce(
    (total, records, partition) =>
      total + records.length - simulation.cursors[partition],
    0,
  );
}

function explain(title, copy) {
  simulation.explanationStep += 1;
  $("#explanation-number").textContent = String(
    simulation.explanationStep,
  ).padStart(2, "0");
  $("#explanation-title").textContent = title;
  $("#explanation-copy").textContent = copy;
}

function renderSimulation() {
  $("#produced-count").textContent = simulation.produced;
  $("#consumed-count").textContent = simulation.consumed;
  $("#lag-count").textContent = lag();
  $("#partition-label").textContent = `${simulation.partitions} partition${simulation.partitions === 1 ? "" : "s"}`;

  const partitionList = $("#partition-list");
  partitionList.replaceChildren();
  simulation.records.forEach((records, partition) => {
    const row = document.createElement("div");
    row.className = "partition";

    const name = document.createElement("span");
    name.className = "partition-name";
    name.textContent = `P${partition}`;

    const lane = document.createElement("div");
    lane.className = "record-lane";
    if (!records.length) {
      const empty = document.createElement("span");
      empty.className = "empty-lane";
      empty.textContent = "empty log";
      lane.append(empty);
    }
    records.slice(-9).forEach((record) => {
      const block = document.createElement("span");
      block.className = `record${record.offset < simulation.cursors[partition] ? " consumed" : ""}`;
      block.textContent = record.offset;
      block.title = `${record.value} · key: ${record.key || "none"}`;
      lane.append(block);
    });

    const offset = document.createElement("span");
    offset.className = "partition-offset";
    offset.textContent = `next ${simulation.cursors[partition]}`;
    row.append(name, lane, offset);
    partitionList.append(row);
  });

  const consumerAssignments = assignments();
  const consumerList = $("#consumer-list");
  consumerList.replaceChildren();
  consumerAssignments.forEach((partitions, index) => {
    const consumer = document.createElement("div");
    const crashed = simulation.consumerOneCrashed && index === 0;
    consumer.className = `consumer${crashed ? " crashed" : ""}${!crashed && !partitions.length ? " idle" : ""}`;
    const title = document.createElement("strong");
    title.textContent = `Consumer ${index + 1}${crashed ? " — crashed" : ""}`;
    const detail = document.createElement("small");
    detail.textContent = crashed
      ? "not sending heartbeats"
      : partitions.length
        ? `owns P${partitions.join(", P")}`
        : "idle — no partition available";
    consumer.append(title, detail);
    consumerList.append(consumer);
  });

  $("#group-state").textContent = simulation.consumerOneCrashed
    ? "rebalanced"
    : "stable";
  $("#group-state").className = `tag ${simulation.consumerOneCrashed ? "error" : "success"}`;
  $("#toggle-crash").textContent = simulation.consumerOneCrashed
    ? "Recover consumer 1"
    : "Crash consumer 1";
}

function resetSimulation() {
  clearInterval(simulation.autoTimer);
  simulation.autoTimer = null;
  simulation.partitions = Number($("#partition-count").value);
  simulation.consumers = Number($("#consumer-count").value);
  simulation.records = Array.from(
    { length: simulation.partitions },
    () => [],
  );
  simulation.nextOffsets = Array(simulation.partitions).fill(0);
  simulation.cursors = Array(simulation.partitions).fill(0);
  simulation.produced = 0;
  simulation.consumed = 0;
  simulation.roundRobin = 0;
  simulation.consumerOneCrashed = false;
  simulation.explanationStep = 0;
  $("#toggle-auto").textContent = "Start auto-consume";
  explain(
    "Kafka is an append-only log",
    "Produce a record. Kafka will append it to one partition and assign an increasing offset.",
  );
  renderSimulation();
}

function produceSimulationRecord() {
  const value = $("#message-value").value.trim();
  const key = $("#message-key").value.trim();
  if (!value) {
    explain("A record needs a value", "Enter a message value before producing.");
    return;
  }

  const partition = key
    ? hashKey(key) % simulation.partitions
    : simulation.roundRobin++ % simulation.partitions;
  const offset = simulation.nextOffsets[partition]++;
  simulation.records[partition].push({ value, key, offset });
  simulation.produced += 1;
  explain(
    `Appended to partition ${partition} at offset ${offset}`,
    key
      ? `Kafka hashed the key “${key}”. Reusing this key keeps related records ordered in partition ${partition}.`
      : `Without a key, the producer used round-robin routing. Ordering is only guaranteed inside partition ${partition}.`,
  );
  renderSimulation();
}

function consumeNext() {
  const ownership = assignments();
  let partition = -1;
  for (let index = 0; index < simulation.partitions; index += 1) {
    if (simulation.cursors[index] < simulation.records[index].length) {
      partition = index;
      break;
    }
  }

  if (partition === -1) {
    explain("Consumer lag is zero", "There are no unprocessed records in this consumer group.");
    return false;
  }

  const consumer = ownership.findIndex((owned) => owned.includes(partition));
  if (consumer === -1) {
    explain(
      "No active consumer",
      "Records remain safely in Kafka and lag grows until a consumer rejoins the group.",
    );
    return false;
  }

  const offset = simulation.cursors[partition];
  simulation.cursors[partition] += 1;
  simulation.consumed += 1;
  explain(
    `Consumer ${consumer + 1} processed P${partition}:${offset}`,
    `The payments group can now commit offset ${simulation.cursors[partition]} for partition ${partition}. Another group would keep its own independent offset.`,
  );
  renderSimulation();
  return true;
}

function toggleAutoConsume() {
  if (simulation.autoTimer) {
    clearInterval(simulation.autoTimer);
    simulation.autoTimer = null;
    $("#toggle-auto").textContent = "Start auto-consume";
    return;
  }
  $("#toggle-auto").textContent = "Stop auto-consume";
  simulation.autoTimer = setInterval(consumeNext, 800);
}

function toggleConsumerCrash() {
  simulation.consumerOneCrashed = !simulation.consumerOneCrashed;
  explain(
    simulation.consumerOneCrashed ? "The group rebalanced" : "Consumer 1 rejoined",
    simulation.consumerOneCrashed
      ? "Kafka reassigned consumer 1’s partitions to the remaining active consumers. Their committed offsets did not change."
      : "Joining also triggers a rebalance, distributing partitions across the enlarged group.",
  );
  renderSimulation();
}

function renderLessons(activeIndex = 0) {
  const nav = $("#lesson-nav");
  nav.replaceChildren();
  lessons.forEach((lesson, index) => {
    const button = document.createElement("button");
    button.className = `lesson-button${index === activeIndex ? " active" : ""}`;
    button.innerHTML = `<span>${String(index + 1).padStart(2, "0")}</span><span>${lesson.title}</span>`;
    button.addEventListener("click", () => renderLessons(index));
    nav.append(button);
  });

  const lesson = lessons[activeIndex];
  const content = $("#lesson-content");
  content.replaceChildren();
  const eyebrow = document.createElement("p");
  eyebrow.className = "eyebrow";
  eyebrow.textContent = lesson.eyebrow;
  const title = document.createElement("h2");
  title.textContent = lesson.title;
  const copy = document.createElement("p");
  copy.textContent = lesson.copy;
  const diagram = document.createElement("div");
  diagram.className = "lesson-diagram";
  lesson.diagram.forEach((label, index) => {
    if (index) {
      const arrow = document.createElement("i");
      arrow.textContent = "→";
      diagram.append(arrow);
    }
    const item = document.createElement("span");
    item.textContent = label;
    diagram.append(item);
  });
  const tryIt = document.createElement("div");
  tryIt.className = "try-it";
  const tryTitle = document.createElement("strong");
  tryTitle.textContent = "Try it in the simulator";
  const task = document.createElement("p");
  task.textContent = lesson.task;
  tryIt.append(tryTitle, task);
  content.append(eyebrow, title, copy, diagram, tryIt);
}

function switchView(viewName) {
  $$(".nav-button").forEach((button) =>
    button.classList.toggle("active", button.dataset.view === viewName),
  );
  $$(".view").forEach((view) => {
    const active = view.id === `${viewName}-view`;
    view.hidden = !active;
    view.classList.toggle("active", active);
  });
}

function setLiveState(state) {
  const connected = Boolean(state.connected);
  const connecting = Boolean(state.connecting);
  $("#live-status").textContent = connecting
    ? "connecting"
    : connected
      ? "connected"
      : "offline";
  $("#live-status").className = `tag ${connected ? "success" : connecting ? "" : "error"}`;
  $("#connect-kafka").disabled = connected || connecting;
  $("#connect-kafka").textContent = connected
    ? "Kafka connected"
    : connecting
      ? "Connecting…"
      : "Connect Kafka";
  if (state.brokers) $("#live-broker").textContent = state.brokers.join(", ");
  if (state.topic) $("#live-topic").textContent = state.topic;
  if (state.groupId) $("#live-group").textContent = state.groupId;
  $("#connection-message").textContent = state.lastError
    ? state.lastError
    : connected
      ? "The producer and consumer are connected. Send a record and watch it return through the consumer group."
      : "Start Kafka with Docker, then connect from here.";
  $("#header-status").innerHTML = `<span class="status-dot"></span><span>${connected ? "Kafka connected" : "Simulator ready"}</span>`;
}

function addLiveRecord(record) {
  const list = $("#event-list");
  $(".empty-state", list)?.remove();
  const row = document.createElement("div");
  row.className = "event-row";
  const values = [
    `P${record.partition} / ${record.offset}`,
    record.key ?? "—",
    record.value,
    record.timestamp
      ? new Date(Number(record.timestamp)).toLocaleTimeString()
      : new Date().toLocaleTimeString(),
  ];
  values.forEach((value) => {
    const cell = document.createElement("span");
    cell.textContent = value;
    row.append(cell);
  });
  list.prepend(row);
}

async function connectKafka() {
  const button = $("#connect-kafka");
  button.disabled = true;
  button.textContent = "Connecting…";
  $("#connection-message").textContent = "Creating the topic and starting the producer and consumer…";
  try {
    const response = await fetch("/api/kafka/connect", { method: "POST" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Could not connect");
    setLiveState(payload);
  } catch (error) {
    setLiveState({ connected: false, lastError: error.message });
  }
}

async function produceLiveRecord(event) {
  event.preventDefault();
  const response = await fetch("/api/kafka/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      value: $("#live-value").value,
      key: $("#live-key").value,
    }),
  });
  const payload = await response.json();
  $("#connection-message").textContent = response.ok
    ? `Kafka acknowledged the record in P${payload[0].partition} at offset ${payload[0].offset}.`
    : payload.error;
}

function startEventStream() {
  const events = new EventSource("/api/events");
  events.addEventListener("status", (event) => {
    setLiveState(JSON.parse(event.data).payload);
  });
  events.addEventListener("message", (event) => {
    addLiveRecord(JSON.parse(event.data).payload);
  });
}

$$('.nav-button').forEach((button) =>
  button.addEventListener("click", () => switchView(button.dataset.view)),
);
$("#partition-count").addEventListener("change", resetSimulation);
$("#consumer-count").addEventListener("change", resetSimulation);
$("#reset-simulation").addEventListener("click", resetSimulation);
$("#produce-message").addEventListener("click", produceSimulationRecord);
$("#consume-next").addEventListener("click", consumeNext);
$("#toggle-auto").addEventListener("click", toggleAutoConsume);
$("#toggle-crash").addEventListener("click", toggleConsumerCrash);
$("#connect-kafka").addEventListener("click", connectKafka);
$("#live-producer-form").addEventListener("submit", produceLiveRecord);
$("#clear-events").addEventListener("click", () => {
  $("#event-list").innerHTML = '<p class="empty-state">No records consumed in this browser session.</p>';
});

resetSimulation();
renderLessons();
startEventStream();
fetch("/api/health")
  .then((response) => response.json())
  .then(({ kafka }) => setLiveState(kafka))
  .catch(() => {});
