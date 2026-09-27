import { lessons } from "./lessons.js";

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

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
    button.setAttribute("aria-current", index === activeIndex ? "step" : "false");
    const number = document.createElement("span");
    number.textContent = String(index + 1).padStart(2, "0");
    const label = document.createElement("span");
    label.textContent = lesson.title;
    button.append(number, label);
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
  const progress = document.createElement("p");
  progress.className = "lesson-progress";
  progress.textContent = `Lesson ${activeIndex + 1} of ${lessons.length}`;
  const summary = document.createElement("p");
  summary.className = "lesson-summary";
  summary.textContent = lesson.summary;
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

  const sectionList = document.createElement("div");
  sectionList.className = "lesson-sections";
  lesson.sections.forEach((section) => {
    const wrapper = document.createElement("section");
    const heading = document.createElement("h3");
    heading.textContent = section.title;
    const body = document.createElement("p");
    body.textContent = section.body;
    wrapper.append(heading, body);
    sectionList.append(wrapper);
  });

  const takeaway = document.createElement("section");
  takeaway.className = "lesson-takeaways";
  const takeawayTitle = document.createElement("h3");
  takeawayTitle.textContent = "Key takeaways";
  const takeawayList = document.createElement("ul");
  lesson.keyPoints.forEach((point) => {
    const item = document.createElement("li");
    item.textContent = point;
    takeawayList.append(item);
  });
  takeaway.append(takeawayTitle, takeawayList);

  let codeExample = null;
  if (lesson.code) {
    codeExample = document.createElement("section");
    codeExample.className = "lesson-code";
    const codeTitle = document.createElement("h3");
    codeTitle.textContent = lesson.code.title;
    const pre = document.createElement("pre");
    const code = document.createElement("code");
    code.textContent = lesson.code.body;
    pre.append(code);
    codeExample.append(codeTitle, pre);
  }

  const tryIt = document.createElement("div");
  tryIt.className = "try-it";
  const tryTitle = document.createElement("strong");
  tryTitle.textContent = "Try it in the simulator";
  const task = document.createElement("p");
  task.textContent = lesson.task;
  tryIt.append(tryTitle, task);

  const lessonActions = document.createElement("div");
  lessonActions.className = "lesson-actions";
  const previous = document.createElement("button");
  previous.className = "button secondary";
  previous.textContent = "Previous lesson";
  previous.disabled = activeIndex === 0;
  previous.addEventListener("click", () => renderLessons(activeIndex - 1));
  const next = document.createElement("button");
  next.className = "button primary";
  next.textContent =
    activeIndex === lessons.length - 1 ? "Back to lesson 1" : "Next lesson";
  next.addEventListener("click", () =>
    renderLessons(activeIndex === lessons.length - 1 ? 0 : activeIndex + 1),
  );
  lessonActions.append(previous, next);

  content.append(eyebrow, progress, title, summary, diagram, sectionList);
  if (codeExample) content.append(codeExample);
  content.append(takeaway, tryIt, lessonActions);
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

function setLiveUnavailable(message) {
  $("#live-status").textContent = "Phase 2";
  $("#live-status").className = "tag";
  $("#connect-kafka").disabled = true;
  $("#connect-kafka").textContent = "Requires always-on worker";
  $("#connection-message").textContent = message;
  $$("#live-producer-form input, #live-producer-form button").forEach(
    (control) => {
      control.disabled = true;
    },
  );
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
  try {
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
  } catch (error) {
    $("#connection-message").textContent = error.message;
  }
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

async function initializeLiveMode() {
  try {
    const response = await fetch("/api/health");
    const { kafka, features } = await response.json();
    setLiveState(kafka);

    if (features?.liveKafka) {
      startEventStream();
    } else {
      setLiveUnavailable(
        "The simulator and lessons are fully available. Live Kafka will return in Phase 2 with a persistent consumer worker.",
      );
    }
  } catch {
    setLiveUnavailable(
      "The simulator works independently, but the deployment API is currently unavailable.",
    );
  }
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
initializeLiveMode();
