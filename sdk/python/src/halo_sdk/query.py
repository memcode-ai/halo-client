"""The Halo SDK's public surface: query() for one request, session() for a conversation.

    import asyncio, halo_sdk

    async def main():
        r = await halo_sdk.query("what is using port 5432?")
        print(r.text)

    asyncio.run(main())

A thin layer over the client in __init__ (one Halo process for the application, many sessions).
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Union

PermissionMode = Literal["default", "acceptEdits", "bypassPermissions", "plan", "dontAsk"]
Status = Literal["done", "answered", "needs_input", "proposed", "unsupported", "denied", "cancelled", "failed"]


@dataclass
class OperationRequest:
    """What can_use_operation is told about an operation that needs approval."""

    operations: list[str]  # "files.delete", "git.push"
    effect: str  # READ | WRITE | DESTRUCTIVE | COMMAND | EXECUTE
    reason: str
    command: str | None
    authorities: list[str]
    rememberable: bool


@dataclass
class Allow:
    remember: bool = False  # for the rest of the session


@dataclass
class Deny:
    message: str = "denied"


Decision = Union[Allow, Deny]


@dataclass
class QueryResult:
    """A request's result: the answer as Halo would say it, and the same thing typed."""

    ok: bool  # done or answered
    status: Status
    text: str  # what Halo would say
    rows: list[dict[str, Any]]  # typed rows when the result is a table; otherwise empty
    columns: list[str]
    value: Any = None  # a single value (a count, a total, a calculation, yes/no)
    question: str | None = None  # needs_input: what Halo asks; answer with another query
    options: list[str] | None = None
    operation: str | None = None
    command: str | None = None
    session_id: str = ""
    outcome: dict[str, Any] = field(default_factory=dict)  # the full outcome


async def query(prompt: str, *, cwd: str | None = None, model: str | None = None, permission_mode: PermissionMode | None = None,
                allowed_operations: list[str] | None = None, disallowed_operations: list[str] | None = None,
                can_use_operation: Callable[[OperationRequest], Decision | Awaitable[Decision]] | None = None,
                on_choice: Callable[[str, list[str]], int | None | Awaitable[int | None]] | None = None,
                allow_sending_paths: bool = False, bin_path: str | None = None) -> QueryResult:
    """Ask Halo one thing and get its typed result."""
    s = await session(cwd=cwd, model=model, permission_mode=permission_mode, allowed_operations=allowed_operations,
                      disallowed_operations=disallowed_operations, can_use_operation=can_use_operation, on_choice=on_choice,
                      allow_sending_paths=allow_sending_paths, bin_path=bin_path)
    try:
        return await s.query(prompt)
    finally:
        await s.close()


async def session(*, cwd: str | None = None, model: str | None = None, permission_mode: PermissionMode | None = None,
                  allowed_operations: list[str] | None = None, disallowed_operations: list[str] | None = None,
                  can_use_operation: Callable[[OperationRequest], Decision | Awaitable[Decision]] | None = None,
                  on_choice: Callable[[str, list[str]], int | None | Awaitable[int | None]] | None = None,
                  allow_sending_paths: bool = False, bin_path: str | None = None) -> Session:
    """A conversation: follow-ups ("kill it", "only the PDFs") refer to earlier results. Use `async with`, or close()."""
    from . import SessionOptions  # the low-level client

    c = await _shared(bin_path)
    inner = await c.session(SessionOptions(
        cwd=cwd, permission_mode=permission_mode, allowed_operations=list(allowed_operations or []),
        disallowed_operations=list(disallowed_operations or []), model=model,
        interactor=_Interactor(can_use_operation, on_choice, allow_sending_paths)))
    return Session(inner)


class Session:
    def __init__(self, inner: Any):
        self._s = inner

    @property
    def id(self) -> str:
        return self._s.id

    async def query(self, prompt: str) -> QueryResult:
        """Ask within this conversation."""
        o = await self._s.run(prompt)
        return to_result(o, self._s.id)

    async def reset(self) -> None:
        await self._s.reset()

    async def close(self) -> None:
        await self._s.close()

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


# --- the shared client: one Halo process per event loop -------------------------------------------------------------

_clients: dict[int, Any] = {}


async def _shared(bin_path: str | None) -> Any:
    from . import ClientOptions, Halo

    loop = asyncio.get_running_loop()
    c = _clients.get(id(loop))
    if c is None or getattr(c, "_exited", False):
        c = await Halo.connect(ClientOptions(bin_path=bin_path, client_name="halo-sdk-python",
                                             interactor=_Interactor(None, None, False)))
        _clients[id(loop)] = c
    return c


async def close() -> None:
    """Shut the shared Halo process down (call it before your event loop ends, e.g. at the end of main)."""
    c = _clients.pop(id(asyncio.get_running_loop()), None)
    if c is not None:
        await c.close()


class _Interactor:
    def __init__(self, can_use, on_choice, allow_paths: bool):
        self._can_use, self._on_choice, self._paths = can_use, on_choice, allow_paths

    async def approve(self, req: dict[str, Any]) -> Any:
        from . import Approval, Decision as D

        if self._can_use is None:
            return Approval(D.DENY, "needs approval: allow it with allowed_operations, permission_mode or can_use_operation")
        plan = req.get("plan") or {}
        d = self._can_use(OperationRequest(
            operations=list(req.get("operations") or []), effect=req.get("effect", ""), reason=req.get("reason", ""),
            command=plan.get("command"), authorities=[a.get("name", "") for a in req.get("authorities") or []],
            rememberable=bool(req.get("rememberable"))))
        if inspect.isawaitable(d):
            d = await d
        if isinstance(d, Allow):
            return Approval(D.ALLOW_SESSION if d.remember else D.ALLOW)
        return Approval(D.DENY, getattr(d, "message", "denied"))

    async def choose(self, req: dict[str, Any]) -> int | None:
        if self._on_choice is None:
            return None
        n = self._on_choice(req.get("question", ""), list(req.get("options") or []))
        return await n if inspect.isawaitable(n) else n

    async def offer(self, req: dict[str, Any]) -> bool:
        return self._paths


# --- results ------------------------------------------------------------------------------------------------------

_STATUS = {"answer": "answered", "result": "done", "question": "needs_input", "choice": "needs_input", "proposed": "proposed",
           "unsupported": "unsupported", "unrecognized": "unsupported", "denied": "denied", "declined": "denied",
           "cancelled": "cancelled", "failed": "failed"}


def to_result(o: dict[str, Any], session_id: str) -> QueryResult:
    status = _STATUS.get(o.get("kind", ""), "failed")
    res = o.get("result") or _last_step(o) or {}
    rows = list(res.get("rows") or [])
    columns = list(res.get("columns") or [])
    if not rows and res.get("files"):
        rows = [{"path": f.get("path"), "size": f.get("size"), "modified": f.get("modified")} for f in res["files"]]
        columns = ["path", "size", "modified"]
    if rows and not columns:
        columns = list(rows[0].keys())
    a = res.get("answer") or {}
    value = next((a[k] for k in ("number", "count", "exists") if k in a), None)
    if value is None and o.get("kind") == "answer":
        value = o.get("message")
    note = res.get("note")
    text = (o.get("message") or o.get("question")
            or (note if note and (not rows or value is not None) else None)
            or res.get("text") or (_table(columns, rows) if rows else None) or o.get("reason") or "")
    plan = o.get("plan") or {}
    return QueryResult(ok=status in ("done", "answered"), status=status, text=text, rows=rows, columns=columns, value=value,
                       question=o.get("question"), options=o.get("options"), operation=plan.get("operation"),
                       command=o.get("command") or plan.get("command"), session_id=session_id,
                       outcome=o)


def _last_step(o: dict[str, Any]) -> dict[str, Any] | None:
    for st in reversed((o.get("program") or {}).get("steps") or []):
        if st.get("result"):
            return st["result"]
    return None


def _table(columns: list[str], rows: list[dict[str, Any]]) -> str:
    cell = lambda v: "" if v is None else str(v)  # noqa: E731
    w = [max(len(c), *(len(cell(r.get(c))) for r in rows)) for c in columns]
    line = lambda vals: "  ".join(v.ljust(w[i]) for i, v in enumerate(vals)).rstrip()  # noqa: E731
    return "\n".join([line([c.upper() for c in columns])] + [line([cell(r.get(c)) for c in columns]) for r in rows])
