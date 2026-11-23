import assert from "node:assert/strict";
import { LiveShowcase } from "../web/live-showcase.mjs";
import { narratedBookChange } from "../web/event-activity.mjs";

const base = process.env.BASIS_URL ?? "http://127.0.0.1:4330";
const run = new LiveShowcase({
  base: base + "/api",
  mode: "l3",
  seed: 75,
  scenario: "queue_demo",
});
const moments = [];
try {
  await run.start();
  while (run.snapshot.clock_ms < 6500 && moments.length < 2) {
    const before = run.snapshot;
    const { snapshot, events } = await run.tick();
    if (snapshot.clock_ms < 1100) continue;
    const step = moments.length === 0 ? "amend" : "fill";
    const moment = narratedBookChange(before, snapshot, events, step);
    if (moment) moments.push(moment);
  }
  assert.equal(run.snapshot.transport, "kafka");
  assert.ok(run.brokerRecords > 0);
  assert.equal(moments.length, 2);
  const [amendment, fill] = moments;
  assert.equal(amendment.market, "spot");
  assert.ok(amendment.remaining_lots < amendment.previous_lots);
  assert.equal(fill.market, "future");
  assert.ok(fill.lots > 0 && fill.remaining_lots > 0);
  console.log(JSON.stringify({ brokerRecords: run.brokerRecords, amendment, fill }));
} finally {
  await run.close();
}
