import { test } from "node:test";
import assert from "node:assert/strict";

import { toResult } from "../src/index.ts";

test("results are typed without a switch over outcome kinds", () => {
  const rows = toResult({ kind: "result", completed: true, result: { kind: "rows", columns: ["region", "avg price"], rows: [{ region: "west", "avg price": 600.3 }] } }, "s1");
  assert.equal(rows.ok, true);
  assert.equal(rows.status, "done");
  assert.deepEqual(rows.rows, [{ region: "west", "avg price": 600.3 }]);
  assert.match(rows.text, /REGION\s+AVG PRICE\nwest\s+600\.3/);

  const count = toResult({ kind: "result", completed: true, result: { kind: "rows", note: "3", answer: { aggregate: "count", count: 3 } } }, "s1");
  assert.equal(count.value, 3);
  assert.equal(count.text, "3");

  const answer = toResult({ kind: "answer", completed: true, message: "125.66 lb" }, "s1");
  assert.equal(answer.status, "answered");
  assert.equal(answer.value, "125.66 lb");

  const choice = toResult({ kind: "choice", completed: false, question: "Which file?", options: ["a.pdf", "b.pdf"] }, "s1");
  assert.equal(choice.status, "needs_input");
  assert.deepEqual(choice.options, ["a.pdf", "b.pdf"]);
  assert.equal(choice.ok, false);

  const files = toResult({ kind: "result", completed: true, result: { kind: "files", files: [{ path: "a.pdf", dir: "", name: "a.pdf", ext: "pdf", size: 10, modified: "2026-11-08" }] } }, "s1");
  assert.deepEqual(files.rows, [{ path: "a.pdf", size: 10, modified: "2026-11-08" }]);

  const denied = toResult({ kind: "denied", completed: false, reason: "needs approval" }, "s1");
  assert.equal(denied.status, "denied");
  assert.equal(denied.text, "needs approval");
});

