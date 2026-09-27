import assert from "node:assert/strict";
import { test } from "node:test";

process.env.VERCEL = "1";
const { default: app } = await import("../src/app.js");

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

test("Vercel health reports simulator-only mode", async () => {
  const response = responseRecorder();
  await routeHandler("/api/health", "get")({}, response);

  assert.equal(response.statusCode, 200);
  assert.equal(response.body.deployment, "vercel");
  assert.equal(response.body.features.liveKafka, false);
});

test("Vercel rejects persistent Kafka connection endpoints", async () => {
  const response = responseRecorder();
  await routeHandler("/api/kafka/connect", "post")({}, response);

  assert.equal(response.statusCode, 501);
  assert.match(
    response.body.error,
    /disabled on the serverless Phase 1 deployment/,
  );
});
