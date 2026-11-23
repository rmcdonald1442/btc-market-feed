import assert from "node:assert/strict";

const base = process.env.BASIS_URL || "http://127.0.0.1:4330";
const transport = process.argv.includes("--kafka") ? "kafka" : "local";
const financeModule = await fetch(base + "/finance.mjs");
assert.equal(
  financeModule.status,
  200,
  "Shared financial module must be served to the browser",
);
assert.match(financeModule.headers.get("content-type") ?? "", /javascript/);
async function request(path, body, expected) {
  const response = await fetch(base + "/api" + path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  assert.equal(response.status, expected, text);
  return JSON.parse(text);
}
async function rawRequest(path, json, expected) {
  const response = await fetch(base + "/api" + path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: json,
  });
  const text = await response.text();
  assert.equal(response.status, expected, text);
  return JSON.parse(text);
}

const state = await request("/sessions", { seed: 73, transport }, 200);
const path = `/sessions/${state.id}/step`;
const command = {
  revision: state.revision,
  milliseconds: 100,
  events_only: true,
};
const [first, concurrentRetry] = await Promise.all([
  request(path, command, 200),
  request(path, command, 200),
]);
assert.deepEqual(
  concurrentRetry,
  first,
  "identical concurrent retries return the accepted response",
);

await request(path, { ...command, milliseconds: 101 }, 409);
await request(path, { ...command, revision: 0 }, 409);

const raced = await request("/sessions", { seed: 74, transport }, 200);
const racePath = `/sessions/${raced.id}/step`;
const raceResponses = await Promise.all(
  [111, 112].map((milliseconds) =>
    fetch(base + "/api" + racePath, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ revision: raced.revision, milliseconds }),
    }),
  ),
);
assert.deepEqual(
  raceResponses.map((response) => response.status).sort(),
  [200, 409],
);
const closeResponse = await fetch(base + "/api/sessions/" + raced.id, {
  method: "DELETE",
});
const closeText = await closeResponse.text();
assert.equal(closeResponse.status, 200, closeText);
assert.deepEqual(JSON.parse(closeText), {
  closed: true,
  id: raced.id,
  cleanup_error: null,
});
await request(
  `/sessions/${raced.id}/step`,
  { revision: raced.revision + 1, milliseconds: 1 },
  404,
);

const replay = await request("/replays", { transport }, 200);
const legacyReply = await request(
  `/replays/${replay.id}/batch`,
  {
    revision: 0,
    events: [
      {
        clock_ms: 0,
        type: "tick",
        rng: 1,
        fair_cents: 1,
        sample: false,
        elapsed_ms: 0,
        next_event_ms: 1,
      },
    ],
  },
  200,
);
assert.equal(
  legacyReply.events[0].basis_cents,
  0,
  "Legacy Tick records receive the current contract default",
);
await request(
  `/replays/${replay.id}/batch`,
  {
    revision: 0,
    events: [
      {
        clock_ms: 0,
        type: "tick",
        rng: 2,
        fair_cents: 1,
        sample: false,
        elapsed_ms: 0,
        next_event_ms: 1,
      },
    ],
  },
  409,
);
await rawRequest(
  `/replays/${replay.id}/batch`,
  '{"revision":18446744073709551615,"events":[{"clock_ms":0,"type":"tick","rng":1,"fair_cents":1,"sample":false,"elapsed_ms":0,"next_event_ms":1}]}',
  409,
);
const replayClose = await fetch(base + "/api/replays/" + replay.id, {
  method: "DELETE",
});
const replayCloseText = await replayClose.text();
assert.equal(replayClose.status, 200, replayCloseText);
assert.equal(JSON.parse(replayCloseText).closed, true);
await request(
  `/replays/${replay.id}/batch`,
  {
    revision: 1,
    events: [
      {
        clock_ms: 0,
        type: "tick",
        rng: 1,
        fair_cents: 1,
        sample: false,
        elapsed_ms: 0,
        next_event_ms: 1,
      },
    ],
  },
  404,
);

assert.equal(
  (await fetch(base + "/api/sessions/" + state.id, { method: "DELETE" }))
    .status,
  200,
);
console.log(
  "Command retry, competing revision, stale revision, replay payload, and oversized revision checks passed.",
);
