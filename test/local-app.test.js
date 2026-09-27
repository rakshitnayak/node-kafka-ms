import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import app from "../src/app.js";
import { lessons } from "../public/lessons.js";

const routeHandler = (path, method) =>
  app._router.stack.find(
    (layer) => layer.route?.path === path && layer.route.methods[method],
  ).route.stack[0].handle;

const responseRecorder = () => ({
  statusCode: 200,
  body: null,
  status(code) {
    this.statusCode = code;
    return this;
  },
  json(body) {
    this.body = body;
    return this;
  },
});

test("local health enables live Kafka", async () => {
  const response = responseRecorder();
  await routeHandler("/api/health", "get")({}, response);

  assert.equal(response.statusCode, 200);
  assert.equal(response.body.app, "ok");
  assert.equal(response.body.deployment, "local");
  assert.equal(response.body.features.liveKafka, true);
});

test("public application contains the simulator and lessons", async () => {
  const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");

  assert.match(html, /Kafka Lab/);
  assert.match(html, /id="simulator-view"/);
  assert.match(html, /id="learn-view"/);
});

test("learning curriculum covers Kafka from fundamentals to operations", () => {
  assert.equal(lessons.length, 18);
  assert.equal(lessons[0].title, "Why Kafka exists");
  assert.equal(lessons.at(-1).title, "Putting it together with KafkaJS");

  for (const lesson of lessons) {
    assert.ok(lesson.summary.length > 80, `${lesson.title} needs a detailed summary`);
    assert.ok(lesson.sections.length >= 3, `${lesson.title} needs three sections`);
    assert.ok(lesson.keyPoints.length >= 3, `${lesson.title} needs key takeaways`);
    assert.ok(lesson.task, `${lesson.title} needs a practical exercise`);
  }
});
