// The Halo Agent SDK for TypeScript: it drives the installed Halo binary (`halo --protocol stream-json`), speaks
// Halo's machine protocol (newline-delimited JSON on the process's stdin and stdout) and routes approvals, choices
// and offers to an Interactor. It holds only process lifecycle, protocol I/O and callbacks: interpretation,
// permissions, previews, confinement and execution all stay inside Halo.
//
// One client owns one long-lived halo process. Create it ONCE, when your application or module starts, and reuse
// it from every request handler; open a session per conversation:
//
//   const halo = await Halo.connect();                  // at startup
//   const session = await halo.session({ cwd });       // per conversation
//   const outcome = await session.run("what is using port 5432?");
//   await session.close();
//   await halo.close();                                  // at shutdown
//
// Connecting and opening sessions are local: only a turn that needs interpreting reaches the Halo service.
//
// Halo controls the machine and account it runs on. Never forward untrusted users' requests to it; a service
// that takes requests from other people must isolate each user's Halo in a container or VM.

import { spawn } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, join } from "node:path";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";

import {
  PROTOCOL_VERSION,
  Msg,
  Code,
  type ApprovalRequestData,
  type ChoiceRequestData,
  type CreateSessionData,
  type Envelope,
  type ErrorData,
  type InitializeData,
  type InitializedData,
  type OfferRequestData,
  type OutcomeData,
  type SessionCreatedData,
  type WarningData,
} from "./protocol.gen.ts";

export * from "./protocol.gen.ts";

export type Outcome = OutcomeData;
export type ApprovalRequest = ApprovalRequestData;
export type ChoiceRequest = ChoiceRequestData;
export type OfferRequest = OfferRequestData;
export type Warning = WarningData;

/** The official way to install Halo. The SDK never downloads, updates or bundles a binary. */
export const INSTALL_LINE = "curl -fsSL https://memcode.ai/halo/install.sh | sh";

/** Approval decisions. A host cannot change the operation it approves: deny, with a message, and ask again in words. */
export const Decision = { Allow: "allow", AllowSession: "allow_session", Deny: "deny" } as const;
export type Decision = (typeof Decision)[keyof typeof Decision];

export interface Approval {
  decision: Decision;
  message?: string;
}

/** Usage hints: guidance only; they never change what Halo may do. */
export type Usage = "local" | "hosted";

/** Local error codes (the binding's own; Halo's are in Code). */
export const LocalCode = { HaloExited: "halo_exited", Closed: "client_closed", NotInstalled: "not_installed" } as const;

/** Capabilities a Halo announces that the binding relies on. */
export const Cap = { ModelPinning: "model_pinning", Sessions: "sessions" } as const;

/** What Halo asks the host. Every method may be async; the signal aborts when the turn ends. */
export interface Interactor {
  approve?(req: ApprovalRequest, signal: AbortSignal): Approval | Promise<Approval>;
  /** The chosen index, or null to cancel. */
  choose?(req: ChoiceRequest, signal: AbortSignal): number | null | Promise<number | null>;
  offer?(req: OfferRequest, signal: AbortSignal): boolean | Promise<boolean>;
}

/** A started halo process (injectable for tests). */
export interface Process {
  stdin: Writable;
  stdout: Readable;
  /** Resolves when the process has exited. */
  exited: Promise<void>;
  kill(): void;
}

export type Launch = (bin: string, args: string[], stderr: (chunk: string) => void) => Process;

export interface ClientOptions {
  /** The halo binary; undefined discovers it (HALO_BIN, PATH, the installer's locations). */
  binPath?: string;
  clientName?: string;
  clientVersion?: string;
  usage?: Usage;
  /** Answers approvals, choices and offers for every session that does not set its own. Without one Halo never asks. */
  interactor?: Interactor;
  /** Capabilities the client needs; an older Halo fails connect with version_mismatch. */
  require?: string[];
  /** Refuses an installed Halo older than this ("0.2.0"); a development build ("dev") passes. */
  minHaloVersion?: string;
  /** Halo's diagnostics (stderr); undefined discards them. */
  stderr?: (chunk: string) => void;
  /** Starts the process (tests); undefined runs the binary. */
  launch?: Launch;
}

export interface SessionOptions {
  /** The session's trusted folder; undefined is the halo process's. */
  cwd?: string;
  /** default | dontAsk | acceptEdits | plan | bypassPermissions */
  permissionMode?: string;
  allowedOperations?: string[];
  disallowedOperations?: string[];
  allowCatastrophic?: boolean;
  allowUnconfined?: boolean;
  /** CLI settings to inherit: none unless named ("user", "project"). */
  settingSources?: string[];
  /** Pin an exact public Halo model ("halo-orbit-preview"); undefined pins the default at the first turn. */
  model?: string;
  /** Close the session after this long without a turn (0 or undefined: never). */
  idleTimeoutMs?: number;
  /** Overrides the client's Interactor (the client must have one, so that Halo asks). */
  interactor?: Interactor;
}

export class HaloError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  readonly retryAfterMs: number;
  constructor(code: string, message: string, retryable = false, retryAfterMs = 0) {
    super(`halo: ${code}: ${message}`);
    this.name = "HaloError";
    this.code = code;
    this.retryable = retryable;
    this.retryAfterMs = retryAfterMs;
  }
  static of(d: ErrorData): HaloError {
    return new HaloError(d.code, d.message, d.retryable, d.retry_after_ms ?? 0);
  }
}

/** One protocol event of a turn. Unknown event types pass through (a newer Halo's). */
export interface TurnEvent {
  type: string;
  turnId: string;
  id?: string;
  data: unknown;
}

const CLOSE_GRACE_MS = 5000;

// --- accidental per-request construction -------------------------------------------------------------------------
// Several clients alive at once, or clients created in quick succession, almost always mean a client per request.
// It is advice (a process warning, once), never a refusal.
let liveClients = 0;
const recentStarts: number[] = [];
let warnedReuse = false;
function noteClientStart(): void {
  liveClients++;
  const now = Date.now();
  recentStarts.push(now);
  while (recentStarts.length && now - recentStarts[0] > 60_000) recentStarts.shift();
  if (!warnedReuse && (liveClients > 1 || recentStarts.length >= 5)) {
    warnedReuse = true;
    process.emitWarning(
      "Several Halo clients were created in this process. Create one client when your application starts and reuse it: one client keeps one Halo process, with a session per conversation.",
      { code: "HALO_CLIENT_REUSE" },
    );
  }
}

// --- discovery -----------------------------------------------------------------------------------------------------

function executable(p: string): boolean {
  try {
    const st = statSync(p);
    return st.isFile() && (st.mode & 0o111) !== 0;
  } catch {
    return false;
  }
}

/** The installed halo binary: binPath, then $HALO_BIN, then `halo` on PATH, then the installer's locations. */
export function find(binPath?: string): string {
  const notInstalled = () =>
    new HaloError(LocalCode.NotInstalled, `Halo is not installed (install it with: ${INSTALL_LINE})`);
  if (binPath) {
    if (executable(binPath)) return binPath;
    throw notInstalled();
  }
  if (process.env.HALO_BIN) {
    if (executable(process.env.HALO_BIN)) return process.env.HALO_BIN;
    throw notInstalled();
  }
  for (const dir of (process.env.PATH ?? "").split(delimiter)) {
    if (dir && executable(join(dir, "halo"))) return join(dir, "halo");
  }
  for (const dir of [process.env.HALO_INSTALL_DIR, "/usr/local/bin", join(homedir(), ".local", "bin")]) {
    if (dir && existsSync(join(dir, "halo")) && executable(join(dir, "halo"))) return join(dir, "halo");
  }
  throw notInstalled();
}

const spawnLaunch: Launch = (bin, args, stderr) => {
  const child = spawn(bin, args, { stdio: ["pipe", "pipe", "pipe"] });
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (c: string) => stderr(c));
  const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
  child.once("error", () => {});
  return { stdin: child.stdin, stdout: child.stdout, exited, kill: () => child.kill("SIGKILL") };
};

function older(have: string, want: string): boolean {
  const h = have.replace(/^v/, "").split(".");
  const w = want.replace(/^v/, "").split(".");
  for (let i = 0; i < Math.max(h.length, w.length); i++) {
    const a = parseInt((h[i] ?? "0").split("-")[0], 10) || 0;
    const b = parseInt((w[i] ?? "0").split("-")[0], 10) || 0;
    if (a !== b) return a < b;
  }
  return false;
}

// --- the client ----------------------------------------------------------------------------------------------------

type Waiter = { resolve: (e: Envelope) => void; reject: (e: unknown) => void };

/** One running halo process. Create it once with Halo.connect and reuse it from every request handler. */
export class Halo {
  /**
   * Starts the halo process and completes the handshake. Call it once, at startup, and reuse the client. It talks
   * to nothing but the local process.
   */
  static async connect(opts: ClientOptions = {}): Promise<Halo> {
    const bin = opts.launch ? (opts.binPath ?? "halo") : find(opts.binPath);
    const launch = opts.launch ?? spawnLaunch;
    const proc = launch(bin, ["--protocol", "stream-json"], opts.stderr ?? (() => {}));
    const client = new Halo(opts, proc);
    try {
      await client._handshake();
    } catch (e) {
      await client._kill();
      throw e;
    }
    return client;
  }

  /** The handshake: Halo's version and capabilities, and whether it is signed in. */
  info!: InitializedData;
  readonly #opts: ClientOptions;
  readonly #proc: Process;
  readonly #sessions = new Map<string, HaloSession>();
  readonly #creating = new Map<string, HaloSession>();
  readonly #waiting = new Map<string, Waiter>();
  readonly #warnings: Warning[] = [];
  readonly #seq = new Map<string, number>();
  #ready?: { resolve: () => void; reject: (e: unknown) => void };
  #handshook = false;
  #exited = false;
  #closing?: Promise<void>;
  #counted = false;

  constructor(opts: ClientOptions, proc: Process) {
    this.#opts = opts;
    this.#proc = proc;
    const lines = createInterface({ input: proc.stdout, crlfDelay: Infinity });
    lines.on("line", (l) => this.#line(l));
    lines.once("close", () => this.#gone());
    proc.exited.then(() => this.#gone());
  }

  /** @internal */
  async _handshake(): Promise<void> {
    const ready = new Promise<void>((resolve, reject) => (this.#ready = { resolve, reject }));
    const init: InitializeData = {
      protocol: PROTOCOL_VERSION,
      client: { name: this.#opts.clientName ?? "halo-sdk-ts", ...(this.#opts.clientVersion ? { version: this.#opts.clientVersion } : {}) },
    };
    if (this.#opts.interactor) init.capabilities = ["approvals", "choices", "offers"];
    if (this.#opts.require?.length) init.require = this.#opts.require;
    if (this.#opts.usage) init.usage = this.#opts.usage;
    this._send({ type: Msg.Initialize }, init);
    await ready;
    if (this.info.protocol !== PROTOCOL_VERSION) {
      throw new HaloError(Code.VersionMismatch, `halo speaks machine protocol ${this.info.protocol}, this SDK ${PROTOCOL_VERSION}; run \`halo upgrade\` or update the SDK`);
    }
    const min = this.#opts.minHaloVersion;
    if (min && this.info.halo_version !== "dev" && older(this.info.halo_version, min)) {
      throw new HaloError(Code.VersionMismatch, `halo ${this.info.halo_version} is older than ${min}; run \`halo upgrade\``);
    }
    this.#counted = true;
    noteClientStart();
  }

  /** Halo's advice so far about how this client uses it. */
  get warnings(): readonly Warning[] {
    return [...this.#warnings];
  }

  /** @internal */
  _next(kind: string): string {
    const n = (this.#seq.get(kind) ?? 0) + 1;
    this.#seq.set(kind, n);
    return kind + n;
  }

  /** @internal */
  _send(env: Omit<Envelope, "version">, data?: unknown): void {
    if (this.#exited) throw new HaloError(LocalCode.HaloExited, "halo has exited");
    const out: Envelope = { version: PROTOCOL_VERSION, ...env };
    if (data !== undefined) (out as { data?: unknown }).data = data;
    this.#proc.stdin.write(JSON.stringify(out) + "\n");
  }

  /** @internal Sends a lifecycle request and waits for the envelope answering its id. */
  _request(env: Omit<Envelope, "version">, data?: unknown): Promise<Envelope> {
    return new Promise<Envelope>((resolve, reject) => {
      if (this.#exited) return reject(new HaloError(LocalCode.HaloExited, "halo has exited"));
      this.#waiting.set(env.id!, {
        resolve: (e) => (e.type === Msg.Error ? reject(HaloError.of(e.data as ErrorData)) : resolve(e)),
        reject,
      });
      try {
        this._send(env, data);
      } catch (e) {
        this.#waiting.delete(env.id!);
        reject(e);
      }
    });
  }

  /** Opens a logical session (one conversation). It is local: nothing reaches the Halo service. */
  async session(opts: SessionOptions = {}): Promise<HaloSession> {
    if (opts.model && !this.info.capabilities.includes(Cap.ModelPinning)) {
      throw new HaloError(Code.VersionMismatch, "this Halo cannot pin a model; run `halo upgrade`");
    }
    const data: CreateSessionData = {};
    if (opts.cwd) data.cwd = opts.cwd;
    if (opts.permissionMode) data.permission_mode = opts.permissionMode;
    if (opts.allowedOperations?.length) data.allowed_operations = opts.allowedOperations;
    if (opts.disallowedOperations?.length) data.disallowed_operations = opts.disallowedOperations;
    if (opts.allowCatastrophic) data.allow_catastrophic = true;
    if (opts.allowUnconfined) data.allow_unconfined = true;
    if (opts.settingSources?.length) data.setting_sources = opts.settingSources;
    if (opts.model) data.model = opts.model;
    if (opts.idleTimeoutMs) data.idle_timeout_ms = opts.idleTimeoutMs;
    const id = this._next("s");
    const s = new HaloSession(this, opts);
    this.#creating.set(id, s);
    try {
      await this._request({ type: Msg.CreateSession, id }, data);
    } finally {
      this.#creating.delete(id);
    }
    return s;
  }

  /** @internal The Interactor for a session. */
  _interactor(s: SessionOptions): Interactor | undefined {
    return s.interactor ?? this.#opts.interactor;
  }

  /** Shuts the process down: Halo cancels running turns and closes every session, then exits. Idempotent. */
  close(): Promise<void> {
    this.#closing ??= (async () => {
      if (!this.#exited) {
        const timer = new Promise<void>((r) => setTimeout(r, CLOSE_GRACE_MS).unref());
        await Promise.race([this._request({ type: Msg.Shutdown, id: this._next("q") }).catch(() => {}), timer]);
      }
      await this._kill();
    })();
    return this.#closing;
  }

  /** @internal Closes stdin and waits for the exit, killing the process after a grace period. */
  async _kill(): Promise<void> {
    try {
      this.#proc.stdin.end();
    } catch {}
    const grace = new Promise<"late">((r) => setTimeout(() => r("late"), CLOSE_GRACE_MS).unref());
    if ((await Promise.race([this.#proc.exited.then(() => "done" as const), grace])) === "late") {
      this.#proc.kill();
      await this.#proc.exited;
    }
  }

  #line(line: string): void {
    let env: Envelope;
    try {
      env = JSON.parse(line) as Envelope;
    } catch {
      return; // a malformed line is skipped
    }
    if (!env || typeof env.type !== "string" || !env.type) return;
    if (!this.#handshook) {
      if (env.type === Msg.Initialized) {
        this.#handshook = true;
        this.info = env.data as InitializedData;
        this.#ready?.resolve();
      } else if (env.type === Msg.Error) {
        this.#ready?.reject(HaloError.of(env.data as ErrorData));
      }
      return;
    }
    this.#route(env);
  }

  #route(env: Envelope): void {
    if (env.type === Msg.SessionCreated && env.id) {
      const s = this.#creating.get(env.id);
      if (s) {
        const d = env.data as SessionCreatedData;
        s._created(d.session_id, d.model ?? "");
        this.#sessions.set(d.session_id, s);
      }
    }
    const s = env.session_id ? this.#sessions.get(env.session_id) : undefined;
    if (env.type === Msg.Warning) {
      const w = env.data as Warning;
      this.#warnings.push(w);
      s?._warn(w);
      return;
    }
    if (env.type === Msg.SessionClosed && env.session_id) {
      s?._ended(new HaloError(LocalCode.Closed, "the session is closed"));
      this.#sessions.delete(env.session_id);
    }
    const waiter = env.id ? this.#waiting.get(env.id) : undefined;
    const request = env.type === Msg.ApprovalRequest || env.type === Msg.ChoiceRequest || env.type === Msg.OfferRequest;
    if (waiter && !request && (env.type !== Msg.Error || !env.turn_id)) {
      this.#waiting.delete(env.id!);
      waiter.resolve(env);
      return;
    }
    s?._route(env);
  }

  #gone(): void {
    if (this.#exited) return;
    this.#exited = true;
    if (this.#counted) liveClients--;
    const err = new HaloError(LocalCode.HaloExited, "halo exited");
    this.#ready?.reject(new HaloError(LocalCode.HaloExited, "halo exited before the handshake (is it a Halo with machine mode?)"));
    for (const w of this.#waiting.values()) w.reject(err);
    this.#waiting.clear();
    for (const s of this.#sessions.values()) s._ended(err);
    this.#sessions.clear();
  }
}

// --- sessions and turns --------------------------------------------------------------------------------------------

/** One conversation in the client's process: its own context, held results, permissions and model. */
export class HaloSession {
  readonly #c: Halo;
  readonly #opts: SessionOptions;
  #id = "";
  #model = "";
  #exec?: Execution;
  #ended?: HaloError;
  readonly #warnings: Warning[] = [];

  /** @internal */
  constructor(c: Halo, opts: SessionOptions) {
    this.#c = c;
    this.#opts = opts;
  }

  /** The session's id in the protocol. */
  get id(): string {
    return this.#id;
  }

  /** The public Halo model this session is pinned to ("" until the first turn pins the default). */
  get model(): string {
    return this.#model;
  }

  get warnings(): readonly Warning[] {
    return [...this.#warnings];
  }

  /** @internal */
  _created(id: string, model: string): void {
    this.#id = id;
    this.#model = model;
  }

  /** @internal */
  _warn(w: Warning): void {
    this.#warnings.push(w);
  }

  /** @internal */
  _ended(err: HaloError): void {
    if (this.#ended) return;
    this.#ended = err;
    this.#exec?._end(undefined, err);
    this.#exec = undefined;
  }

  /** Submits a request in words and waits for its outcome; rejects with a HaloError (throttled, not signed in …). */
  async run(prompt: string, opts: { signal?: AbortSignal } = {}): Promise<Outcome> {
    return this.begin(prompt, opts).outcome;
  }

  /** Submits a request and returns its Execution at once. Aborting the signal cancels the turn. */
  begin(prompt: string, opts: { signal?: AbortSignal } = {}): Execution {
    if (this.#exec) throw new HaloError(Code.TurnInProgress, "a turn is already running in this session");
    if (this.#ended) {
      throw this.#ended.code === LocalCode.Closed ? new HaloError(Code.UnknownSession, "the session is closed") : this.#ended;
    }
    const ex = new Execution(this.#c, this, this.#c._next("t"));
    this.#exec = ex;
    try {
      this.#c._send({ type: Msg.UserTurn, session_id: this.#id, turn_id: ex.turnId }, { text: prompt });
    } catch (e) {
      this.#exec = undefined;
      throw e;
    }
    if (opts.signal) {
      if (opts.signal.aborted) ex.cancel();
      else opts.signal.addEventListener("abort", () => ex.cancel(), { once: true });
    }
    ex.outcome.catch(() => {});
    return ex;
  }

  /** Clears the session's context, held results and remembered approvals; the id and model stay. */
  async reset(): Promise<void> {
    if (this.#exec) throw new HaloError(Code.TurnInProgress, "a turn is already running in this session");
    await this.#c._request({ type: Msg.Reset, session_id: this.#id, id: this.#c._next("r") });
  }

  /** Ends the session (a running turn is cancelled first); the client and its other sessions are untouched. */
  async close(): Promise<void> {
    if (this.#ended) return;
    try {
      await this.#c._request({ type: Msg.CloseSession, session_id: this.#id, id: this.#c._next("x") });
    } catch (e) {
      if (e instanceof HaloError && (e.code === Code.UnknownSession || e.code === LocalCode.HaloExited)) return;
      throw e;
    }
  }

  /** @internal */
  _route(env: Envelope): void {
    if (env.type === Msg.ApprovalRequest || env.type === Msg.ChoiceRequest || env.type === Msg.OfferRequest) {
      void this.#answer(env);
      return;
    }
    const ex = this.#exec;
    if (!ex || env.turn_id !== ex.turnId) return; // a late event after its outcome
    if (env.type === Msg.Outcome) {
      const o = env.data as Outcome;
      if (o.model) this.#model = o.model;
      this.#exec = undefined;
      ex._end(o);
    } else if (env.type === Msg.Error) {
      this.#exec = undefined;
      ex._end(undefined, HaloError.of(env.data as ErrorData));
    } else {
      ex._push({ type: env.type, turnId: env.turn_id ?? "", id: env.id, data: env.data });
    }
  }

  // answers with the Interactor; with none: deny, cancel, no
  async #answer(env: Envelope): Promise<void> {
    const it = this.#c._interactor(this.#opts);
    const signal = this.#exec && this.#exec.turnId === env.turn_id ? this.#exec._signal : new AbortController().signal;
    const reply = { session_id: this.#id, turn_id: env.turn_id, id: env.id };
    try {
      if (env.type === Msg.ApprovalRequest) {
        let a: Approval = { decision: Decision.Deny, message: "denied by the SDK default (no Interactor)" };
        if (it?.approve) a = await it.approve(env.data as ApprovalRequest, signal);
        const decision = a.decision === Decision.Allow || a.decision === Decision.AllowSession ? a.decision : Decision.Deny;
        this.#c._send({ type: Msg.ApprovalResponse, ...reply }, { decision, ...(a.message ? { message: a.message } : {}) });
      } else if (env.type === Msg.ChoiceRequest) {
        const n = it?.choose ? await it.choose(env.data as ChoiceRequest, signal) : null;
        this.#c._send({ type: Msg.ChoiceResponse, ...reply }, n === null || n === undefined ? { cancel: true, index: 0 } : { index: n });
      } else {
        const yes = it?.offer ? await it.offer(env.data as OfferRequest, signal) : false;
        this.#c._send({ type: Msg.OfferResponse, ...reply }, { yes: !!yes });
      }
    } catch {
      // halo has exited, or the Interactor threw: the turn ends on its own
    }
  }
}

/** One running turn. */
export class Execution {
  readonly turnId: string;
  /** Resolves with the turn's outcome; rejects with a HaloError. */
  readonly outcome: Promise<Outcome>;
  /** @internal */
  readonly _signal: AbortSignal;
  readonly #c: Halo;
  readonly #s: HaloSession;
  readonly #abort = new AbortController();
  readonly #queue: TurnEvent[] = [];
  #wake?: () => void;
  #done = false;
  #cancelled = false;
  #settle!: { resolve: (o: Outcome) => void; reject: (e: unknown) => void };

  /** @internal */
  constructor(c: Halo, s: HaloSession, turnId: string) {
    this.#c = c;
    this.#s = s;
    this.turnId = turnId;
    this._signal = this.#abort.signal;
    this.outcome = new Promise<Outcome>((resolve, reject) => (this.#settle = { resolve, reject }));
  }

  /** The turn's events in order (turn_started, proposal, step_result, and any newer type). */
  async *events(): AsyncGenerator<TurnEvent> {
    for (;;) {
      if (this.#queue.length) {
        yield this.#queue.shift()!;
        continue;
      }
      if (this.#done) return;
      await new Promise<void>((r) => (this.#wake = r));
    }
  }

  /** Interrupts the turn (idempotent; this session's turn only). */
  cancel(): void {
    if (this.#done || this.#cancelled) return;
    this.#cancelled = true;
    try {
      this.#c._send({ type: Msg.Cancel, session_id: this.#s.id, turn_id: this.turnId });
    } catch {}
  }

  /** @internal */
  _push(e: TurnEvent): void {
    this.#queue.push(e);
    this.#wake?.();
  }

  /** @internal */
  _end(o?: Outcome, err?: unknown): void {
    if (this.#done) return;
    this.#done = true;
    this.#abort.abort();
    if (err) this.#settle.reject(err);
    else this.#settle.resolve(o!);
    this.#wake?.();
  }
}
