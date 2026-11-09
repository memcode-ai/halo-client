// The Halo SDK's public surface: query() for one request, session() for a conversation. Everything here is a
// thin layer over the client in index.ts (one Halo process, many sessions).

import {
  Halo,
  HaloError,
  type ApprovalRequest,
  type ChoiceRequest,
  type ClientOptions,
  type HaloSession,
  type Outcome,
  type TurnEvent,
} from "./index.ts";

export type PermissionMode = "default" | "acceptEdits" | "bypassPermissions" | "plan" | "dontAsk";

/** What canUseOperation is told about an operation that needs approval. */
export interface OperationRequest {
  /** The operations it would run ("files.delete", "git.push"). */
  operations: string[];
  /** READ | WRITE | DESTRUCTIVE | COMMAND | EXECUTE */
  effect: string;
  /** Why it needs approval, in words. */
  reason: string;
  /** The exact command, when it runs one. */
  command?: string;
  /** Machine authorities it needs beyond the folder. */
  authorities: string[];
  /** Whether "remember" is offered for this kind of request. */
  rememberable: boolean;
}

/** Your answer: allow it (once, or for the rest of the session with remember) or deny it with a reason. */
export type OperationDecision = { behavior: "allow"; remember?: boolean } | { behavior: "deny"; message?: string };

export interface Options {
  /** The folder Halo works in (default: the current directory). */
  cwd?: string;
  /** Pin a public Halo model ("halo-solstice-preview"); default: the service's default. */
  model?: string;
  /** default: anything that needs approval asks canUseOperation (or is denied without one). acceptEdits: writes in the
   * folder run. plan: nothing runs, the result is the proposal. dontAsk: what needs approval is denied.
   * bypassPermissions: everything runs except disallowed operations and Halo's catastrophic guards. */
  permissionMode?: PermissionMode;
  /** Operations that run without asking ("git.*", "files.read"). */
  allowedOperations?: string[];
  /** Operations that never run, in every mode ("files.delete", "processes.signal"). */
  disallowedOperations?: string[];
  /** Decides operations that need approval (when allowedOperations and the mode don't). */
  canUseOperation?: (op: OperationRequest, ctx: { signal: AbortSignal }) => OperationDecision | Promise<OperationDecision>;
  /** Picks among readings when a request has several; without it the result asks (status "needs_input"). */
  onChoice?: (question: string, options: string[]) => number | null | Promise<number | null>;
  /** Lets Halo send a path's real name to the service when it needs it to understand a request (default: never). */
  allowSendingPaths?: boolean;
  /** Cancels the request. */
  signal?: AbortSignal;
  /** The halo binary (default: discovered). */
  binPath?: string;
}

export type Status = "done" | "answered" | "needs_input" | "proposed" | "unsupported" | "denied" | "cancelled" | "failed";

/** A request's result: the answer as Halo would say it, and the same thing typed. */
export interface QueryResult {
  /** done or answered */
  ok: boolean;
  status: Status;
  /** What Halo would say (an answer, a table, a question, why it didn't run). */
  text: string;
  /** Typed rows when the result is a table (files, processes, a query); otherwise empty. */
  rows: Record<string, unknown>[];
  columns: string[];
  /** A single value (a count, a total, a calculation, a yes/no), when the result is one. */
  value?: number | string | boolean;
  /** When status is needs_input: what Halo asks, and the options when it is a choice. Answer with another query. */
  question?: string;
  options?: string[];
  /** The operation that ran (or would run), and its command when it has one. */
  operation?: string;
  command?: string;
  sessionId: string;
  /** The full outcome, for anything not surfaced above. */
  outcome: Outcome;
}

/** A progress event while a request runs (its plan, each step of a job), then the result. */
export type QueryEvent = { type: "event"; event: TurnEvent } | { type: "result"; result: QueryResult };

/**
 * Ask Halo one thing. `await query(...)` gives the result; `for await (const e of query(...))` gives progress
 * events, then the result.
 *
 *   const r = await query("what is using port 5432?");
 *   console.log(r.text);
 */
export function query(prompt: string, options: Options = {}): Query {
  return new Query(async () => ({ s: await session(options), owned: true }), prompt, options.signal, options.binPath);
}

/** A conversation: follow-ups ("kill it", "only the PDFs") refer to earlier results. Close it when done. */
export async function session(options: Options = {}): Promise<Session> {
  const c = await shared(options.binPath);
  const hooks: Hooks = { options };
  const s = await held(c, () => c.session({
    cwd: options.cwd,
    permissionMode: options.permissionMode,
    allowedOperations: options.allowedOperations,
    disallowedOperations: options.disallowedOperations,
    model: options.model,
    interactor: interactorFor(hooks),
  }));
  return new Session(s, c);
}

// held: the application stays alive while Halo is working on something it waits for
async function held<T>(c: Halo, f: () => Promise<T>): Promise<T> {
  c._hold(true);
  try {
    return await f();
  } finally {
    c._hold(false);
  }
}

export class Session {
  readonly #s: HaloSession;
  readonly #c: Halo;
  /** @internal */
  constructor(s: HaloSession, c: Halo) {
    this.#s = s;
    this.#c = c;
  }
  get id(): string {
    return this.#s.id;
  }
  /** Ask within this conversation. */
  query(prompt: string, opts: { signal?: AbortSignal } = {}): Query {
    return new Query(async () => ({ s: this, owned: false }), prompt, opts.signal);
  }
  /** Forget the conversation's context and held results (the session stays open). */
  reset(): Promise<void> {
    return held(this.#c, () => this.#s.reset());
  }
  close(): Promise<void> {
    return held(this.#c, () => this.#s.close());
  }
  /** @internal */
  get _inner(): HaloSession {
    return this.#s;
  }
}

/** A running request: await it for the result, or iterate it for progress events then the result. */
export class Query implements PromiseLike<QueryResult>, AsyncIterable<QueryEvent> {
  readonly #events: TurnEvent[] = [];
  #wake?: () => void;
  #done = false;
  readonly #result: Promise<QueryResult>;

  /** @internal */
  constructor(open: () => Promise<{ s: Session; owned: boolean }>, prompt: string, signal?: AbortSignal, binPath?: string) {
    this.#result = (async () => {
      const c = await shared(binPath);
      c._hold(true); // from opening the session to its outcome
      let s: Session | undefined;
      let owned = false;
      try {
        ({ s, owned } = await open());
        const ex = s._inner.begin(prompt, { signal });
        const pump = (async () => {
          for await (const e of ex.events()) {
            this.#events.push(e);
            this.#wake?.();
          }
        })();
        const o = await ex.outcome;
        await pump;
        return toResult(o, s.id);
      } finally {
        if (owned && s) await s.close().catch(() => {});
        c._hold(false);
        this.#done = true;
        this.#wake?.();
      }
    })();
    this.#result.catch(() => {
      this.#done = true;
      this.#wake?.();
    });
  }

  then<A = QueryResult, B = never>(ok?: ((r: QueryResult) => A | PromiseLike<A>) | null, err?: ((e: unknown) => B | PromiseLike<B>) | null): Promise<A | B> {
    return this.#result.then(ok, err);
  }

  async *[Symbol.asyncIterator](): AsyncIterator<QueryEvent> {
    for (;;) {
      if (this.#events.length) {
        yield { type: "event", event: this.#events.shift()! };
        continue;
      }
      if (this.#done) break;
      await new Promise<void>((r) => (this.#wake = r));
    }
    yield { type: "result", result: await this.#result };
  }
}

// --- the shared client: one Halo process for the application, held only while a request runs -----------------------

let client: Promise<Halo> | undefined;
type Hooks = { options: Options };

async function shared(binPath?: string): Promise<Halo> {
  if (!client) {
    const opts: ClientOptions = {
      binPath,
      clientName: "halo-sdk-ts",
      // Halo asks the host for approvals, choices and offers; each session answers through its own options
      interactor: { approve: async () => ({ decision: "deny", message: "no session" }) },
    };
    client = Halo.connect(opts).then((c) => {
      c._hold(false); // an idle Halo never keeps the application alive
      return c;
    });
    client.catch(() => (client = undefined));
  }
  return client;
}

/** Shut the shared Halo process down (optional: it never keeps your application alive). */
export async function close(): Promise<void> {
  const c = client;
  client = undefined;
  if (c) await (await c).close();
}

function interactorFor(h: Hooks) {
  return {
    approve: async (req: ApprovalRequest, signal: AbortSignal) => {
      const o = h.options;
      if (!o.canUseOperation) {
        return { decision: "deny" as const, message: "needs approval: allow it with allowedOperations, permissionMode or canUseOperation" };
      }
      const d = await o.canUseOperation(
        {
          operations: req.operations ?? [],
          effect: req.effect ?? "",
          reason: req.reason ?? "",
          command: req.plan?.command,
          authorities: (req.authorities ?? []).map((a) => a.name),
          rememberable: !!req.rememberable,
        },
        { signal },
      );
      if (d.behavior === "allow") return { decision: (d.remember ? "allow_session" : "allow") as "allow" };
      return { decision: "deny" as const, message: d.message ?? "denied" };
    },
    choose: async (req: ChoiceRequest) => (h.options.onChoice ? await h.options.onChoice(req.question ?? "", req.options ?? []) : null),
    offer: async () => !!h.options.allowSendingPaths,
  };
}

// --- results ----------------------------------------------------------------------------------------------------

const statusOf: Record<string, Status> = {
  answer: "answered", result: "done", question: "needs_input", choice: "needs_input", proposed: "proposed",
  unsupported: "unsupported", unrecognized: "unsupported", denied: "denied", declined: "denied", cancelled: "cancelled",
  failed: "failed",
};

export function toResult(o: Outcome, sessionId: string): QueryResult {
  const status = statusOf[o.kind] ?? "failed";
  const res = o.result ?? lastStep(o);
  let rows: Record<string, unknown>[] = res?.rows ?? [];
  let columns: string[] = res?.columns ?? [];
  if (!rows.length && res?.files?.length) {
    rows = res.files.map((f) => ({ path: f.path, size: f.size, modified: f.modified }));
    columns = ["path", "size", "modified"];
  }
  if (rows.length && !columns.length) columns = Object.keys(rows[0]);
  const a = res?.answer;
  let value: number | string | boolean | undefined = a?.number ?? a?.count ?? a?.exists;
  if (value === undefined && o.kind === "answer" && o.message) value = o.message;
  const text =
    o.message ??
    o.question ??
    (res?.note && (rows.length === 0 || value !== undefined) ? res.note : undefined) ??
    res?.text ??
    (rows.length ? table(columns, rows) : "") ??
    o.reason ??
    "";
  return {
    ok: status === "done" || status === "answered",
    status,
    text: text || o.reason || "",
    rows,
    columns,
    value,
    question: o.question,
    options: o.options,
    operation: o.plan?.operation,
    command: o.command ?? o.plan?.command,
    sessionId,
    outcome: o,
  };
}

function lastStep(o: Outcome) {
  const steps = o.program?.steps ?? [];
  for (let i = steps.length - 1; i >= 0; i--) if (steps[i].result) return steps[i].result;
  return undefined;
}

function table(columns: string[], rows: Record<string, unknown>[]): string {
  const cell = (v: unknown) => (v === undefined || v === null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v));
  const w = columns.map((c) => Math.max(c.length, ...rows.map((r) => cell(r[c]).length)));
  const line = (vals: string[]) => vals.map((v, i) => v.padEnd(w[i])).join("  ").trimEnd();
  return [line(columns.map((c) => c.toUpperCase())), ...rows.map((r) => line(columns.map((c) => cell(r[c]))))].join("\n");
}

export { HaloError };
