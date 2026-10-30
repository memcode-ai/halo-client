import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";

import * as sdk from "../src/index.ts";
import { Halo, find, HaloError, INSTALL_LINE, Msg, PROTOCOL_VERSION, type Envelope, type Process } from "../src/index.ts";

// a minimal halo: it completes the handshake and opens sessions; other lines arrive on `sent`
function fakeHalo() {
  const stdin = new PassThrough();
  const stdout = new PassThrough();
  let exit!: () => void;
  const exited = new Promise<void>((r) => (exit = r));
  const sent: Envelope[] = [];
  let n = 0;
  const write = (e: Partial<Envelope>, data: unknown = {}) => stdout.write(JSON.stringify({ version: PROTOCOL_VERSION, ...e, data }) + "\n");
  createInterface({ input: stdin }).on("line", (l) => {
    const e = JSON.parse(l) as Envelope;
    if (e.type === Msg.Initialize) write({ type: Msg.Initialized }, { protocol: PROTOCOL_VERSION, halo_version: "test", service: "halo/1", signed_in: true, capabilities: ["sessions"] });
    else if (e.type === Msg.CreateSession) {
      const id = "S" + ++n;
      write({ type: Msg.SessionCreated, id: e.id, session_id: id }, { session_id: id });
    } else if (e.type === Msg.Shutdown) write({ type: Msg.ShutdownComplete, id: e.id });
    else sent.push(e);
  });
  stdin.on("finish", () => {
    stdout.end();
    exit();
  });
  const proc: Process = { stdin, stdout, exited, kill: () => (stdout.end(), exit()) };
  return { proc, sent, write };
}

test("no one-shot helper: a client must be created and reused", () => {
  assert.equal((sdk as Record<string, unknown>).run, undefined);
  assert.equal((sdk as Record<string, unknown>).default, undefined);
  assert.equal(typeof Halo.connect, "function");
  assert.equal((sdk as Record<string, unknown>).connect, undefined);
});

test("creating clients per request earns a reuse warning (advice, never a refusal)", async () => {
  const warnings: string[] = [];
  const on = (w: Error & { code?: string }) => w.code === "HALO_CLIENT_REUSE" && warnings.push(w.message);
  process.on("warning", on);
  const clients = [];
  for (let i = 0; i < 2; i++) clients.push(await Halo.connect({ launch: () => fakeHalo().proc }));
  await new Promise((r) => setImmediate(r));
  process.off("warning", on);
  assert.equal(warnings.length, 1);
  for (const c of clients) await c.close();
});

test("one turn at a time per session; another session is unaffected", async () => {
  const h = fakeHalo();
  const c = await Halo.connect({ launch: () => h.proc });
  const s = await c.session();
  const ex = s.begin("hang");
  assert.throws(() => s.begin("again"), (e: unknown) => e instanceof HaloError && e.code === "turn_in_progress");
  const other = await c.session();
  const ex2 = other.begin("in another session");
  await new Promise((r) => setTimeout(r, 10));
  assert.deepEqual(h.sent.map((e) => [e.type, e.session_id]), [[Msg.UserTurn, "S1"], [Msg.UserTurn, "S2"]]);
  h.write({ type: Msg.Outcome, session_id: "S2", turn_id: ex2.turnId }, { kind: "answer", completed: true });
  assert.equal((await ex2.outcome).kind, "answer");
  h.write({ type: Msg.Outcome, session_id: "S1", turn_id: ex.turnId }, { kind: "answer", completed: true, model: "halo-orbit-preview" });
  await ex.outcome;
  assert.equal(s.model, "halo-orbit-preview");
  await c.close();
});

test("a missing halo is reported with the install line", () => {
  const dir = mkdtempSync(join(tmpdir(), "halo-find-"));
  const old = { PATH: process.env.PATH, HALO_BIN: process.env.HALO_BIN, HALO_INSTALL_DIR: process.env.HALO_INSTALL_DIR, HOME: process.env.HOME };
  try {
    process.env.PATH = dir;
    delete process.env.HALO_BIN;
    process.env.HALO_INSTALL_DIR = dir;
    assert.throws(() => find(join(dir, "missing")), (e: unknown) => e instanceof HaloError && e.message.includes(INSTALL_LINE));
    writeFileSync(join(dir, "halo"), "#!/bin/sh\n", { mode: 0o755 });
    assert.equal(find(), join(dir, "halo"));
  } finally {
    Object.assign(process.env, old);
  }
});

// end to end with a real halo (HALO_E2E_BIN, and HALO_SERVER for a local Halo service); skipped otherwise
test("a real halo: sessions, a turn, shutdown", { skip: !process.env.HALO_E2E_BIN && "HALO_E2E_BIN not set" }, async () => {
  const c = await Halo.connect({ binPath: process.env.HALO_E2E_BIN });
  const s = await c.session({ cwd: mkdtempSync(join(tmpdir(), "halo-e2e-")) });
  const o = await s.run("list the files here");
  assert.ok(o.kind === "result" || o.kind === "answer", o.kind);
  assert.ok(s.model, "the first turn pinned a model");
  await s.close();
  await c.close();
});
