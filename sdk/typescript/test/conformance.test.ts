// The conformance transcripts (../conformance/*.jsonl) are scripted sessions of envelopes in (host → halo) and
// out (halo → host). This plays halo's side of every transcript that applies to bindings through an injected
// process, drives the binding from the host's side through its public API, and requires that the binding sends
// exactly the transcript's envelopes and reports the same events and outcomes.

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";

import {
  Halo,
  HaloError,
  Msg,
  type ApprovalResponseData,
  type ChoiceResponseData,
  type Envelope,
  type ErrorData,
  type Execution,
  
  type HaloSession,
  type Interactor,
  type OfferResponseData,
  type OutcomeData,
  type Process,
  type SessionCreatedData,
} from "../src/index.ts";

const DIR = join(import.meta.dirname, "..", "..", "conformance");

interface Line {
  in?: Envelope;
  out?: Envelope;
  in_raw?: string;
  out_raw?: string;
  eof?: boolean;
  halo_exit?: boolean;
}

function canonical(v: unknown): string {
  const sort = (x: unknown): unknown => {
    if (Array.isArray(x)) return x.map(sort);
    if (x && typeof x === "object") {
      return Object.fromEntries(Object.keys(x).sort().map((k) => [k, sort((x as Record<string, unknown>)[k])]));
    }
    return x;
  };
  return JSON.stringify(sort(v));
}

// halo's side of a transcript
class FakeHalo {
  readonly stdin = new PassThrough();
  readonly stdout = new PassThrough();
  readonly errs: string[] = [];
  read = 0;
  #moved: (() => void)[] = [];
  #exit!: () => void;
  readonly exited = new Promise<void>((r) => (this.#exit = r));
  done = false;

  process(): Process {
    return { stdin: this.stdin, stdout: this.stdout, exited: this.exited, kill: () => this.end() };
  }

  end(): void {
    if (this.done) return;
    this.done = true;
    this.stdout.end();
    this.#exit();
    for (const m of this.#moved.splice(0)) m();
  }

  async play(lines: Line[]): Promise<void> {
    const it = createInterface({ input: this.stdin })[Symbol.asyncIterator]();
    try {
      for (let i = 0; i < lines.length; i++) {
        const l = lines[i];
        if (l.out) this.stdout.write(canonical(l.out) + "\n");
        else if (l.out_raw !== undefined) this.stdout.write(l.out_raw + "\n");
        else if (l.in) {
          const next = await it.next();
          if (next.done) {
            this.errs.push(`line ${i + 2}: the binding sent nothing, want ${canonical(l.in)}`);
            return;
          }
          const got = canonical(JSON.parse(next.value));
          if (got !== canonical(l.in)) {
            this.errs.push(`line ${i + 2}: the binding sent\n  ${got}\nwant\n  ${canonical(l.in)}`);
            return;
          }
          this.read = i + 1;
          for (const m of this.#moved.splice(0)) m();
        } else if (l.eof) {
          const next = await it.next();
          if (!next.done) this.errs.push(`line ${i + 2}: want EOF from the binding, got ${next.value}`);
          return;
        } else if (l.halo_exit) return;
      }
    } finally {
      this.end();
    }
  }

  async consumed(i: number): Promise<void> {
    while (this.read <= i && !this.done) await new Promise<void>((r) => this.#moved.push(r));
  }
}

// answers each request only when the replay reaches its line
class Scripted implements Interactor {
  approvals: ApprovalResponseData[] = [];
  choices: ChoiceResponseData[] = [];
  offers: OfferResponseData[] = [];
  #wait: (() => void)[] = [];
  release(): void {
    for (const w of this.#wait.splice(0)) w();
  }
  async #take<T>(q: T[], signal: AbortSignal): Promise<T | undefined> {
    while (!q.length && !signal.aborted) await new Promise<void>((r) => this.#wait.push(r));
    return q.shift();
  }
  async approve(_r: unknown, signal: AbortSignal) {
    const a = await this.#take(this.approvals, signal);
    return a ? { decision: a.decision as "allow", message: a.message } : { decision: "deny" as const };
  }
  async choose(_r: unknown, signal: AbortSignal) {
    const c = await this.#take(this.choices, signal);
    return c && !c.cancel ? c.index : null;
  }
  async offer(_r: unknown, signal: AbortSignal) {
    return (await this.#take(this.offers, signal))?.yes ?? false;
  }
}

interface Run {
  id: string;
  session: string;
  ex: Execution;
  events: string[];
  result: Promise<{ o?: OutcomeData; e?: unknown }>;
}

function answerTo(rest: Line[], id: string): Envelope | undefined {
  return rest.find((l) => l.out?.id === id)?.out;
}

function expectedStart(rest: Line[]): string {
  for (const l of rest) {
    if (l.halo_exit) return "halo_exited";
    if (l.out?.type === Msg.Initialized) return "";
    if (l.out?.type === Msg.Error) return (l.out.data as ErrorData).code;
  }
  return "halo_exited";
}

async function replay(lines: Line[]): Promise<void> {
  const fake = new FakeHalo();
  const played = fake.play(lines);
  const script = new Scripted();
  let client: Halo | undefined;
  let closed = false;
  const sessions = new Map<string, HaloSession>();
  const runs: Run[] = [];
  const last = (s: string) => [...runs].reverse().find((r) => r.session === s);
  for (let i = 0; i < lines.length; i++) {
    const e = lines[i].in;
    if (!e) continue;
    const d = (e.data ?? {}) as Record<string, any>;
    switch (e.type) {
      case Msg.Initialize: {
        const want = expectedStart(lines.slice(i + 1));
        try {
          client = await Halo.connect({
            clientName: d.client?.name, clientVersion: d.client?.version, require: d.require, usage: d.usage,
            interactor: d.capabilities?.length ? script : undefined, launch: () => fake.process(),
          });
          assert.equal(want, "", "connect succeeded, want " + want);
        } catch (err) {
          assert.ok(err instanceof HaloError, String(err));
          assert.equal(err.code, want);
          await played;
          assert.deepEqual(fake.errs, []);
          return;
        }
        break;
      }
      case Msg.CreateSession: {
        const want = answerTo(lines.slice(i + 1), e.id!)!;
        try {
          const s = await client!.session({
            cwd: d.cwd, permissionMode: d.permission_mode, allowedOperations: d.allowed_operations,
            disallowedOperations: d.disallowed_operations, allowCatastrophic: d.allow_catastrophic,
            allowUnconfined: d.allow_unconfined, settingSources: d.setting_sources, model: d.model, idleTimeoutMs: d.idle_timeout_ms,
          });
          const created = want.data as SessionCreatedData;
          assert.equal(want.type, Msg.SessionCreated);
          assert.equal(s.id, created.session_id);
          assert.equal(s.model, created.model ?? "");
          sessions.set(s.id, s);
        } catch (err) {
          assert.equal(want.type, Msg.Error, String(err));
          assert.equal((err as HaloError).code, (want.data as ErrorData).code);
        }
        break;
      }
      case Msg.UserTurn: {
        const prev = last(e.session_id!);
        if (prev) await prev.result;
        const s = sessions.get(e.session_id!)!;
        const ex = s.begin(d.text);
        assert.equal(ex.turnId, e.turn_id);
        const r: Run = { id: e.turn_id!, session: e.session_id!, ex, events: [], result: undefined as never };
        const collect = (async () => {
          for await (const ev of ex.events()) r.events.push(ev.type);
        })();
        r.result = ex.outcome.then(
          async (o) => (await collect, { o }),
          async (err) => (await collect, { e: err }),
        );
        runs.push(r);
        break;
      }
      case Msg.ApprovalResponse:
        script.approvals.push(d as ApprovalResponseData);
        script.release();
        break;
      case Msg.ChoiceResponse:
        script.choices.push(d as ChoiceResponseData);
        script.release();
        break;
      case Msg.OfferResponse:
        script.offers.push(d as OfferResponseData);
        script.release();
        break;
      case Msg.Cancel: {
        const r = runs.find((x) => x.id === e.turn_id)!;
        while (!r.events.includes(Msg.TurnStarted)) await new Promise((res) => setTimeout(res, 2));
        r.ex.cancel();
        break;
      }
      case Msg.Reset: {
        const prev = last(e.session_id!);
        if (prev) await prev.result;
        await sessions.get(e.session_id!)!.reset();
        break;
      }
      case Msg.CloseSession:
        await sessions.get(e.session_id!)!.close();
        break;
      case Msg.Shutdown:
        await client!.close();
        closed = true;
        break;
    }
    await fake.consumed(i);
  }
  for (const r of runs) await r.result;
  for (const r of runs) checkTurn(r, lines, await r.result);
  if (client && !closed) await client.close();
  await played;
  assert.deepEqual(fake.errs, []);
}

function checkTurn(r: Run, lines: Line[], got: { o?: OutcomeData; e?: unknown }): void {
  const events: string[] = [];
  let terminal: Envelope | undefined;
  let exited = false;
  for (const l of lines) {
    if (l.halo_exit) exited = true;
    if (!l.out || l.out.turn_id !== r.id) continue;
    if ([Msg.ApprovalRequest, Msg.ChoiceRequest, Msg.OfferRequest].includes(l.out.type as never)) continue;
    if (l.out.type === Msg.Outcome || l.out.type === Msg.Error) terminal = l.out;
    else events.push(l.out.type);
  }
  assert.deepEqual(r.events, events, `${r.id}: events`);
  if (!terminal) {
    assert.ok(exited, `${r.id}: the transcript has no terminal envelope`);
    assert.equal((got.e as HaloError)?.code, "halo_exited", `${r.id}: want halo_exited`);
  } else if (terminal.type === Msg.Outcome) {
    const want = terminal.data as OutcomeData;
    assert.ok(got.o, `${r.id}: ${got.e}`);
    assert.equal(got.o.kind, want.kind);
    assert.equal(got.o.code ?? "", want.code ?? "");
    assert.equal(got.o.completed, want.completed);
  } else {
    const want = terminal.data as ErrorData;
    const err = got.e as HaloError;
    assert.ok(err instanceof HaloError, `${r.id}: ${got.o?.kind}`);
    assert.equal(err.code, want.code);
    assert.equal(err.retryable, want.retryable);
    assert.equal(err.retryAfterMs, want.retry_after_ms ?? 0);
  }
}

const files = existsSync(DIR) ? readdirSync(DIR).filter((f) => f.endsWith(".jsonl")).sort() : [];
let bindings = 0;
for (const f of files) {
  const [head, ...rest] = readFileSync(join(DIR, f), "utf8").trim().split("\n");
  const meta = JSON.parse(head).meta as { name: string; applies: string[] };
  if (!meta.applies.includes("binding")) continue;
  bindings++;
  test(meta.name, { timeout: 20_000 }, () => replay(rest.map((l) => JSON.parse(l) as Line)));
}
test("enough binding transcripts", { skip: files.length === 0 && "transcripts not present" }, () => {
  assert.ok(bindings >= 20, `only ${bindings} binding transcripts`);
});
