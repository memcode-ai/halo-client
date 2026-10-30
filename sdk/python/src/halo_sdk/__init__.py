"""The Halo Agent SDK for Python.

It drives the installed Halo binary (``halo --protocol stream-json``), speaks Halo's machine protocol
(newline-delimited JSON on the process's stdin and stdout) and routes approvals, choices and offers to an
Interactor. It holds only process lifecycle, protocol I/O and callbacks: interpretation, permissions, previews,
confinement and execution all stay inside Halo.

One client owns one long-lived halo process. Create it ONCE, when your application starts, and reuse it; open a
session per conversation::

    client = await Halo.connect()                        # at startup
    session = await client.session(cwd="/path/to/project")
    outcome = await session.run("what is using port 5432?")
    await session.close()
    await client.close()                                 # at shutdown

Connecting and opening sessions are local: only a turn that needs interpreting reaches the Halo service.

Halo controls the machine and account it runs on. Never forward untrusted users' requests to it; a service that
takes requests from other people must isolate each user's Halo in a container or VM.

Defaults are safe: with no Interactor, Halo never asks, so whatever needs approval is denied (approval_required),
choices end the turn and offers are declined.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import shutil
import time
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from ._protocol import *  # noqa: F401,F403  (the generated wire types and constants)
from ._protocol import (
    PROTOCOL_VERSION,
    ApprovalRequestData,
    ChoiceRequestData,
    Code,
    Msg,
    OfferRequestData,
    OutcomeData,
    WarningData,
)

__all__ = [
    "Halo", "find", "HaloSession", "Execution", "HaloError", "HaloReuseWarning", "Approval",
    "Interactor", "ClientOptions", "SessionOptions", "Decision", "INSTALL_LINE", "PROTOCOL_VERSION", "Msg", "Code",
]

INSTALL_LINE = "curl -fsSL https://memcode.ai/halo/install.sh | sh"
"""The official way to install Halo. The SDK never downloads, updates or bundles a binary."""

Outcome = OutcomeData
ApprovalRequest = ApprovalRequestData
ChoiceRequest = ChoiceRequestData
OfferRequest = OfferRequestData
Warning_ = WarningData


class Decision:
    """Approval decisions. A host cannot change the operation it approves: deny, with a message, and ask again."""

    ALLOW = "allow"
    ALLOW_SESSION = "allow_session"
    DENY = "deny"


class LocalCode:
    """The binding's own error codes (Halo's are in Code)."""

    HALO_EXITED = "halo_exited"
    CLOSED = "client_closed"
    NOT_INSTALLED = "not_installed"


MODEL_PINNING = "model_pinning"
CLOSE_GRACE = 5.0


@dataclass
class Approval:
    decision: str
    message: str = ""


class Interactor(Protocol):
    """What Halo asks the host. Methods may be plain or async."""

    def approve(self, req: ApprovalRequest) -> Approval | Awaitable[Approval]: ...
    def choose(self, req: ChoiceRequest) -> int | None | Awaitable[int | None]: ...
    def offer(self, req: OfferRequest) -> bool | Awaitable[bool]: ...


class HaloError(Exception):
    """A typed failure: an error envelope from Halo, or a failure of the binding itself."""

    def __init__(self, code: str, message: str, retryable: bool = False, retry_after_ms: int = 0):
        super().__init__(f"halo: {code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms

    @classmethod
    def of(cls, d: dict[str, Any]) -> HaloError:
        return cls(d.get("code", ""), d.get("message", ""), bool(d.get("retryable")), int(d.get("retry_after_ms") or 0))


class HaloReuseWarning(UserWarning):
    """Several Halo clients were created in this process: create one at startup and reuse it."""


@dataclass
class ClientOptions:
    bin_path: str | None = None
    client_name: str = "halo-sdk-python"
    client_version: str = ""
    usage: str = ""  # "local" (default) or "hosted"
    interactor: Interactor | None = None
    require: list[str] = field(default_factory=list)
    min_halo_version: str = ""
    stderr: Callable[[str], None] | None = None
    launch: Callable[[str, list[str]], Awaitable[Any]] | None = None  # tests


@dataclass
class SessionOptions:
    cwd: str | None = None
    permission_mode: str | None = None  # default | dontAsk | acceptEdits | plan | bypassPermissions
    allowed_operations: list[str] = field(default_factory=list)
    disallowed_operations: list[str] = field(default_factory=list)
    allow_catastrophic: bool = False
    allow_unconfined: bool = False
    setting_sources: list[str] = field(default_factory=list)  # none unless named: "user", "project"
    model: str | None = None  # pin an exact public Halo model; None pins the default at the first turn
    idle_timeout: float | None = None  # seconds; None or 0: never
    interactor: Interactor | None = None  # overrides the client's (the client must have one, so that Halo asks)


# --- accidental per-request construction (advice, never a refusal) ----------------------------------------------

_live_clients = 0
_recent_starts: list[float] = []
_warned_reuse = False


def _note_client_start() -> None:
    global _live_clients, _warned_reuse
    _live_clients += 1
    now = time.monotonic()
    _recent_starts.append(now)
    while _recent_starts and now - _recent_starts[0] > 60:
        _recent_starts.pop(0)
    if not _warned_reuse and (_live_clients > 1 or len(_recent_starts) >= 5):
        _warned_reuse = True
        warnings.warn(
            "Several Halo clients were created in this process. Create one client when your application starts and "
            "reuse it: one client keeps one Halo process, with a session per conversation.",
            HaloReuseWarning,
            stacklevel=3,
        )


def _note_client_end() -> None:
    global _live_clients
    _live_clients -= 1


# --- discovery -----------------------------------------------------------------------------------------------------


def _executable(p: str | None) -> bool:
    return bool(p) and os.path.isfile(p) and os.access(p, os.X_OK)


def find(bin_path: str | None = None) -> str:
    """The installed halo binary: bin_path, then $HALO_BIN, then ``halo`` on PATH, then the installer's places."""
    missing = HaloError(LocalCode.NOT_INSTALLED, f"Halo is not installed (install it with: {INSTALL_LINE})")
    if bin_path:
        if _executable(bin_path):
            return bin_path
        raise missing
    if os.environ.get("HALO_BIN"):
        if _executable(os.environ["HALO_BIN"]):
            return os.environ["HALO_BIN"]
        raise missing
    on_path = shutil.which("halo")
    if on_path:
        return on_path
    for d in (os.environ.get("HALO_INSTALL_DIR"), "/usr/local/bin", os.path.expanduser("~/.local/bin")):
        if d and _executable(os.path.join(d, "halo")):
            return os.path.join(d, "halo")
    raise missing


def _older(have: str, want: str) -> bool:
    h, w = have.lstrip("v").split("."), want.lstrip("v").split(".")
    for i in range(max(len(h), len(w))):
        a = int((h[i] if i < len(h) else "0").split("-")[0] or 0)
        b = int((w[i] if i < len(w) else "0").split("-")[0] or 0)
        if a != b:
            return a < b
    return False


async def _spawn(bin_path: str, args: list[str], stderr: Callable[[str], None] | None) -> Any:
    proc = await asyncio.create_subprocess_exec(
        bin_path, *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE if stderr else asyncio.subprocess.DEVNULL, limit=2 << 20,
    )
    if stderr:
        async def pump() -> None:
            while line := await proc.stderr.readline():
                stderr(line.decode(errors="replace"))
        asyncio.get_running_loop().create_task(pump())
    return proc


async def _maybe(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


# --- the client ----------------------------------------------------------------------------------------------------


class Halo:
    """One running halo process. Create it once with ``await Halo.connect()`` and reuse it from every task."""

    @classmethod
    async def connect(cls, options: ClientOptions | None = None, **kw: Any) -> Halo:
        """Start the halo process and complete the handshake. Call it once, at startup, and reuse the client.

        It talks to nothing but the local process.
        """
        opts = options or ClientOptions(**kw)
        if opts.launch:
            proc = await opts.launch(opts.bin_path or "halo", ["--protocol", "stream-json"])
        else:
            proc = await _spawn(find(opts.bin_path), ["--protocol", "stream-json"], opts.stderr)
        client = cls(opts, proc)
        try:
            await client._handshake()
        except BaseException:
            await client._kill()
            raise
        return client

    def __init__(self, opts: ClientOptions, proc: Any):
        self._opts = opts
        self._proc = proc
        self.info: dict[str, Any] = {}
        self._sessions: dict[str, HaloSession] = {}
        self._creating: dict[str, HaloSession] = {}
        self._waiting: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._warnings: list[dict[str, Any]] = []
        self._seq: dict[str, int] = {}
        self._ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._handshook = False
        self._exited = False
        self._counted = False
        self._closing: asyncio.Task[None] | None = None
        self._reader = asyncio.get_running_loop().create_task(self._read())

    async def __aenter__(self) -> Halo:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def _handshake(self) -> None:
        init: dict[str, Any] = {"protocol": PROTOCOL_VERSION, "client": {"name": self._opts.client_name}}
        if self._opts.client_version:
            init["client"]["version"] = self._opts.client_version
        if self._opts.interactor is not None:
            init["capabilities"] = ["approvals", "choices", "offers"]
        if self._opts.require:
            init["require"] = list(self._opts.require)
        if self._opts.usage:
            init["usage"] = self._opts.usage
        await self._send({"type": Msg.INITIALIZE}, init)
        await self._ready
        if self.info.get("protocol") != PROTOCOL_VERSION:
            raise HaloError(Code.VERSION_MISMATCH, f"halo speaks machine protocol {self.info.get('protocol')}, this SDK "
                            f"{PROTOCOL_VERSION}; run `halo upgrade` or update the SDK")
        m = self._opts.min_halo_version
        if m and self.info.get("halo_version") != "dev" and _older(self.info.get("halo_version", ""), m):
            raise HaloError(Code.VERSION_MISMATCH, f"halo {self.info.get('halo_version')} is older than {m}; run `halo upgrade`")
        self._counted = True
        _note_client_start()

    @property
    def warnings(self) -> list[dict[str, Any]]:
        """Halo's advice so far about how this client uses it."""
        return list(self._warnings)

    def _next(self, kind: str) -> str:
        self._seq[kind] = self._seq.get(kind, 0) + 1
        return f"{kind}{self._seq[kind]}"

    async def _send(self, env: dict[str, Any], data: Any = None) -> None:
        if self._exited:
            raise HaloError(LocalCode.HALO_EXITED, "halo has exited")
        out = {"version": PROTOCOL_VERSION, **env}
        if data is not None:
            out["data"] = data
        self._proc.stdin.write((json.dumps(out, separators=(",", ":")) + "\n").encode())
        try:
            await self._proc.stdin.drain()
        except (ConnectionError, BrokenPipeError) as e:
            raise HaloError(LocalCode.HALO_EXITED, str(e)) from e

    async def _request(self, env: dict[str, Any], data: Any = None) -> dict[str, Any]:
        if self._exited:
            raise HaloError(LocalCode.HALO_EXITED, "halo has exited")
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._waiting[env["id"]] = fut
        try:
            await self._send(env, data)
            e = await fut
        finally:
            self._waiting.pop(env["id"], None)
        if e.get("type") == Msg.ERROR:
            raise HaloError.of(e.get("data") or {})
        return e

    async def session(self, options: SessionOptions | None = None, **kw: Any) -> HaloSession:
        """Open a logical session (one conversation). It is local: nothing reaches the Halo service."""
        o = options or SessionOptions(**kw)
        if o.model and MODEL_PINNING not in self.info.get("capabilities", []):
            raise HaloError(Code.VERSION_MISMATCH, "this Halo cannot pin a model; run `halo upgrade`")
        data: dict[str, Any] = {}
        if o.cwd:
            data["cwd"] = o.cwd
        if o.permission_mode:
            data["permission_mode"] = o.permission_mode
        if o.allowed_operations:
            data["allowed_operations"] = list(o.allowed_operations)
        if o.disallowed_operations:
            data["disallowed_operations"] = list(o.disallowed_operations)
        if o.allow_catastrophic:
            data["allow_catastrophic"] = True
        if o.allow_unconfined:
            data["allow_unconfined"] = True
        if o.setting_sources:
            data["setting_sources"] = list(o.setting_sources)
        if o.model:
            data["model"] = o.model
        if o.idle_timeout:
            data["idle_timeout_ms"] = int(o.idle_timeout * 1000)
        rid = self._next("s")
        s = HaloSession(self, o)
        self._creating[rid] = s
        try:
            await self._request({"type": Msg.CREATE_SESSION, "id": rid}, data)
        finally:
            self._creating.pop(rid, None)
        return s

    def _interactor(self, o: SessionOptions) -> Interactor | None:
        return o.interactor or self._opts.interactor

    async def close(self) -> None:
        """Shut the process down: Halo cancels running turns and closes every session, then exits. Idempotent."""
        if self._closing is None:
            self._closing = asyncio.get_running_loop().create_task(self._close())
        await asyncio.shield(self._closing)

    async def _close(self) -> None:
        if not self._exited:
            try:
                await asyncio.wait_for(self._request({"type": Msg.SHUTDOWN, "id": self._next("q")}), CLOSE_GRACE)
            except (HaloError, asyncio.TimeoutError):
                pass
        await self._kill()

    async def _kill(self) -> None:
        try:
            self._proc.stdin.close()
        except Exception:
            pass
        try:
            await asyncio.wait_for(self._proc.wait(), CLOSE_GRACE)
        except asyncio.TimeoutError:
            self._proc.kill()
            await self._proc.wait()
        await asyncio.gather(self._reader, return_exceptions=True)

    async def _read(self) -> None:
        try:
            while True:
                line = await self._proc.stdout.readline()
                if not line:
                    break
                try:
                    env = json.loads(line)
                except ValueError:
                    continue  # a malformed line is skipped
                if not isinstance(env, dict) or not env.get("type"):
                    continue
                if not self._handshook:
                    if env["type"] == Msg.INITIALIZED:
                        self._handshook = True
                        self.info = env.get("data") or {}
                        if not self._ready.done():
                            self._ready.set_result(None)
                    elif env["type"] == Msg.ERROR and not self._ready.done():
                        self._ready.set_exception(HaloError.of(env.get("data") or {}))
                    continue
                self._route(env)
        finally:
            self._gone()

    def _route(self, env: dict[str, Any]) -> None:
        typ, sid, rid = env["type"], env.get("session_id"), env.get("id")
        if typ == Msg.SESSION_CREATED and rid in self._creating:
            s = self._creating[rid]
            d = env.get("data") or {}
            s._created(d.get("session_id", ""), d.get("model", ""))
            self._sessions[s.id] = s
        s = self._sessions.get(sid) if sid else None
        if typ == Msg.WARNING:
            self._warnings.append(env.get("data") or {})
            if s:
                s._warnings.append(env.get("data") or {})
            return
        if typ == Msg.SESSION_CLOSED and sid:
            if s:
                s._ended(HaloError(LocalCode.CLOSED, "the session is closed"))
            self._sessions.pop(sid, None)
        fut = self._waiting.get(rid) if rid else None
        request = typ in (Msg.APPROVAL_REQUEST, Msg.CHOICE_REQUEST, Msg.OFFER_REQUEST)
        if fut is not None and not request and (typ != Msg.ERROR or not env.get("turn_id")):
            if not fut.done():
                fut.set_result(env)
            return
        if s:
            s._route(env)

    def _gone(self) -> None:
        if self._exited:
            return
        self._exited = True
        if self._counted:
            _note_client_end()
        err = HaloError(LocalCode.HALO_EXITED, "halo exited")
        if not self._ready.done():
            self._ready.set_exception(HaloError(LocalCode.HALO_EXITED, "halo exited before the handshake (is it a Halo with machine mode?)"))
        for fut in self._waiting.values():
            if not fut.done():
                fut.set_exception(err)
        for s in list(self._sessions.values()):
            s._ended(err)
        self._sessions.clear()


# --- sessions and turns --------------------------------------------------------------------------------------------


class HaloSession:
    """One conversation in the client's process: its own context, held results, permissions and model."""

    def __init__(self, client: Halo, opts: SessionOptions):
        self._c = client
        self._opts = opts
        self.id = ""
        self.model = ""
        self._exec: Execution | None = None
        self._end: HaloError | None = None
        self._warnings: list[dict[str, Any]] = []

    async def __aenter__(self) -> HaloSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    @property
    def warnings(self) -> list[dict[str, Any]]:
        return list(self._warnings)

    def _created(self, sid: str, model: str) -> None:
        self.id, self.model = sid, model

    def _ended(self, err: HaloError) -> None:
        if self._end:
            return
        self._end = err
        if self._exec:
            self._exec._finish(None, err)
            self._exec = None

    async def run(self, prompt: str) -> Outcome:
        """Submit a request in words and wait for its outcome; raises HaloError (throttled, not signed in …)."""
        ex = await self.begin(prompt)
        return await ex.outcome()

    async def begin(self, prompt: str) -> Execution:
        """Submit a request and return its Execution at once."""
        if self._exec:
            raise HaloError(Code.TURN_IN_PROGRESS, "a turn is already running in this session")
        if self._end:
            raise HaloError(Code.UNKNOWN_SESSION, "the session is closed") if self._end.code == LocalCode.CLOSED else self._end
        ex = Execution(self, self._c._next("t"))
        self._exec = ex
        try:
            await self._c._send({"type": Msg.USER_TURN, "session_id": self.id, "turn_id": ex.turn_id}, {"text": prompt})
        except BaseException:
            self._exec = None
            raise
        return ex

    async def reset(self) -> None:
        """Clear the session's context, held results and remembered approvals; the id and model stay."""
        if self._exec:
            raise HaloError(Code.TURN_IN_PROGRESS, "a turn is already running in this session")
        await self._c._request({"type": Msg.RESET, "session_id": self.id, "id": self._c._next("r")})

    async def close(self) -> None:
        """End the session (a running turn is cancelled first); the client and its other sessions are untouched."""
        if self._end:
            return
        try:
            await self._c._request({"type": Msg.CLOSE_SESSION, "session_id": self.id, "id": self._c._next("x")})
        except HaloError as e:
            if e.code not in (Code.UNKNOWN_SESSION, LocalCode.HALO_EXITED):
                raise

    def _route(self, env: dict[str, Any]) -> None:
        typ = env["type"]
        if typ in (Msg.APPROVAL_REQUEST, Msg.CHOICE_REQUEST, Msg.OFFER_REQUEST):
            asyncio.get_running_loop().create_task(self._answer(env))
            return
        ex = self._exec
        if ex is None or env.get("turn_id") != ex.turn_id:
            return  # a late event after its outcome
        if typ == Msg.OUTCOME:
            o = env.get("data") or {}
            if o.get("model"):
                self.model = o["model"]
            self._exec = None
            ex._finish(o, None)
        elif typ == Msg.ERROR:
            self._exec = None
            ex._finish(None, HaloError.of(env.get("data") or {}))
        else:
            ex._push({"type": typ, "turn_id": env.get("turn_id", ""), "id": env.get("id"), "data": env.get("data")})

    async def _answer(self, env: dict[str, Any]) -> None:
        it = self._c._interactor(self._opts)
        reply = {"session_id": self.id, "turn_id": env.get("turn_id"), "id": env.get("id")}
        data = env.get("data") or {}
        try:
            if env["type"] == Msg.APPROVAL_REQUEST:
                a = Approval(Decision.DENY, "denied by the SDK default (no Interactor)")
                if it is not None and hasattr(it, "approve"):
                    a = await _maybe(it.approve(data))
                decision = a.decision if a.decision in (Decision.ALLOW, Decision.ALLOW_SESSION) else Decision.DENY
                body: dict[str, Any] = {"decision": decision}
                if a.message:
                    body["message"] = a.message
                await self._c._send({"type": Msg.APPROVAL_RESPONSE, **reply}, body)
            elif env["type"] == Msg.CHOICE_REQUEST:
                n = await _maybe(it.choose(data)) if it is not None and hasattr(it, "choose") else None
                await self._c._send({"type": Msg.CHOICE_RESPONSE, **reply}, {"cancel": True, "index": 0} if n is None else {"index": n})
            else:
                yes = bool(await _maybe(it.offer(data))) if it is not None and hasattr(it, "offer") else False
                await self._c._send({"type": Msg.OFFER_RESPONSE, **reply}, {"yes": yes})
        except Exception:
            pass  # halo has exited, or the Interactor raised: the turn ends on its own


class Execution:
    """One running turn."""

    def __init__(self, session: HaloSession, turn_id: str):
        self._s = session
        self.turn_id = turn_id
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._result: asyncio.Future[Outcome] = asyncio.get_running_loop().create_future()
        self._cancelled = False

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """The turn's events in order (turn_started, proposal, step_result, and any newer type)."""
        while (e := await self._queue.get()) is not None:
            yield e

    async def outcome(self) -> Outcome:
        """The turn's outcome; raises HaloError."""
        return await asyncio.shield(self._result)

    async def cancel(self) -> None:
        """Interrupt the turn (idempotent; this session's turn only)."""
        if self._result.done() or self._cancelled:
            return
        self._cancelled = True
        try:
            await self._s._c._send({"type": Msg.CANCEL, "session_id": self._s.id, "turn_id": self.turn_id})
        except HaloError:
            pass

    def _push(self, e: dict[str, Any]) -> None:
        self._queue.put_nowait(e)

    def _finish(self, o: Outcome | None, err: HaloError | None) -> None:
        if self._result.done():
            return
        if err is not None:
            self._result.set_exception(err)
            self._result.exception()  # retrieved: no "never retrieved" noise when nobody awaits it
        else:
            self._result.set_result(o)  # type: ignore[arg-type]
        self._queue.put_nowait(None)
