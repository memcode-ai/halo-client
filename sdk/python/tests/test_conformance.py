"""Replays every binding conformance transcript (../../conformance/*.jsonl) through the public API: a fake process
plays halo's lines, the binding is driven from the host's lines, and the binding must send exactly the
transcript's envelopes and report the same events and outcomes."""

from __future__ import annotations

import asyncio
import json
import os
import unittest
from pathlib import Path
from typing import Any

import halo_sdk
from halo_sdk import Approval, HaloError, Msg

DIR = Path(__file__).resolve().parents[2] / "conformance"


def canonical(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class _Stdin:
    def __init__(self, fake: FakeHalo):
        self._fake = fake

    def write(self, b: bytes) -> None:
        self._fake._buf += b
        while b"\n" in self._fake._buf:
            line, self._fake._buf = self._fake._buf.split(b"\n", 1)
            self._fake.lines.put_nowait(line.decode())

    async def drain(self) -> None:
        if self._fake.done:
            raise BrokenPipeError("halo exited")

    def close(self) -> None:
        self._fake.lines.put_nowait(None)


class FakeHalo:
    """halo's side of a transcript, as a process."""

    def __init__(self) -> None:
        self.lines: asyncio.Queue[str | None] = asyncio.Queue()
        self._buf = b""
        self.stdin = _Stdin(self)
        self.stdout = asyncio.StreamReader(limit=2 << 20)
        self.errs: list[str] = []
        self.read = 0
        self.done = False
        self._moved = asyncio.Event()
        self._exited = asyncio.Event()

    def end(self) -> None:
        if not self.done:
            self.done = True
            self.stdout.feed_eof()
            self._exited.set()
            self._moved.set()

    async def wait(self) -> int:
        await self._exited.wait()
        return 0

    def kill(self) -> None:
        self.end()

    async def play(self, lines: list[dict[str, Any]]) -> None:
        try:
            for i, ln in enumerate(lines):
                if "out" in ln:
                    self.stdout.feed_data((canonical(ln["out"]) + "\n").encode())
                elif "out_raw" in ln:
                    self.stdout.feed_data((ln["out_raw"] + "\n").encode())
                elif "in" in ln:
                    got = await self.lines.get()
                    if got is None:
                        self.errs.append(f"line {i + 2}: the binding sent nothing, want {canonical(ln['in'])}")
                        return
                    if canonical(json.loads(got)) != canonical(ln["in"]):
                        self.errs.append(f"line {i + 2}: the binding sent\n  {canonical(json.loads(got))}\nwant\n  {canonical(ln['in'])}")
                        return
                    self.read = i + 1
                    self._moved.set()
                elif ln.get("eof"):
                    got = await self.lines.get()
                    if got is not None:
                        self.errs.append(f"line {i + 2}: want EOF from the binding, got {got}")
                    return
                elif ln.get("halo_exit"):
                    return
        finally:
            self.end()

    async def consumed(self, i: int) -> None:
        while self.read <= i and not self.done:
            self._moved.clear()
            await self._moved.wait()


class Scripted:
    """Answers each request only when the replay reaches its line."""

    def __init__(self) -> None:
        self.approvals: list[dict[str, Any]] = []
        self.choices: list[dict[str, Any]] = []
        self.offers: list[dict[str, Any]] = []
        self._ev = asyncio.Event()

    def release(self) -> None:
        self._ev.set()

    async def _take(self, q: list[dict[str, Any]]) -> dict[str, Any]:
        while not q:
            self._ev.clear()
            await self._ev.wait()
        return q.pop(0)

    async def approve(self, req: Any) -> Approval:
        a = await self._take(self.approvals)
        return Approval(a["decision"], a.get("message", ""))

    async def choose(self, req: Any) -> int | None:
        c = await self._take(self.choices)
        return None if c.get("cancel") else c.get("index", 0)

    async def offer(self, req: Any) -> bool:
        return bool((await self._take(self.offers)).get("yes"))


def answer_to(rest: list[dict[str, Any]], rid: str) -> dict[str, Any]:
    return next(ln["out"] for ln in rest if "out" in ln and ln["out"].get("id") == rid)


def expected_start(rest: list[dict[str, Any]]) -> str:
    for ln in rest:
        if ln.get("halo_exit"):
            return "halo_exited"
        if "out" in ln and ln["out"]["type"] == Msg.INITIALIZED:
            return ""
        if "out" in ln and ln["out"]["type"] == Msg.ERROR:
            return ln["out"]["data"]["code"]
    return "halo_exited"


class Run:
    def __init__(self, tid: str, session: str, ex: halo_sdk.Execution):
        self.id, self.session, self.ex = tid, session, ex
        self.events: list[str] = []

        async def collect() -> None:
            async for e in ex.events():
                self.events.append(e["type"])

        self.collector = asyncio.get_running_loop().create_task(collect())

    async def result(self) -> tuple[Any, Any]:
        try:
            o = await self.ex.outcome()
            r = (o, None)
        except HaloError as e:
            r = (None, e)
        await self.collector
        return r


async def replay(tc: unittest.TestCase, lines: list[dict[str, Any]]) -> None:
    fake = FakeHalo()
    played = asyncio.get_running_loop().create_task(fake.play(lines))
    script = Scripted()
    client: halo_sdk.Halo | None = None
    closed = False
    sessions: dict[str, halo_sdk.HaloSession] = {}
    runs: list[Run] = []

    async def launch(_bin: str, _args: list[str]) -> FakeHalo:
        return fake

    def last(sid: str) -> Run | None:
        return next((r for r in reversed(runs) if r.session == sid), None)

    for i, ln in enumerate(lines):
        e = ln.get("in")
        if e is None:
            continue
        d = e.get("data") or {}
        t = e["type"]
        if t == Msg.INITIALIZE:
            want = expected_start(lines[i + 1:])
            try:
                client = await halo_sdk.Halo.connect(halo_sdk.ClientOptions(
                    client_name=d["client"]["name"], client_version=d["client"].get("version", ""),
                    require=d.get("require", []), usage=d.get("usage", ""),
                    interactor=script if d.get("capabilities") else None, launch=launch))
                tc.assertEqual(want, "", "connect succeeded")
            except HaloError as err:
                tc.assertEqual(err.code, want)
                await played
                tc.assertEqual(fake.errs, [])
                return
        elif t == Msg.CREATE_SESSION:
            want = answer_to(lines[i + 1:], e["id"])
            try:
                s = await client.session(halo_sdk.SessionOptions(
                    cwd=d.get("cwd"), permission_mode=d.get("permission_mode"), allowed_operations=d.get("allowed_operations", []),
                    disallowed_operations=d.get("disallowed_operations", []), allow_catastrophic=d.get("allow_catastrophic", False),
                    allow_unconfined=d.get("allow_unconfined", False), setting_sources=d.get("setting_sources", []),
                    model=d.get("model"), idle_timeout=(d.get("idle_timeout_ms") or 0) / 1000 or None))
                tc.assertEqual(want["type"], Msg.SESSION_CREATED)
                tc.assertEqual(s.id, want["data"]["session_id"])
                tc.assertEqual(s.model, want["data"].get("model", ""))
                sessions[s.id] = s
            except HaloError as err:
                tc.assertEqual(want["type"], Msg.ERROR)
                tc.assertEqual(err.code, want["data"]["code"])
        elif t == Msg.USER_TURN:
            prev = last(e["session_id"])
            if prev:
                await prev.result()
            ex = await sessions[e["session_id"]].begin(d["text"])
            tc.assertEqual(ex.turn_id, e["turn_id"])
            runs.append(Run(e["turn_id"], e["session_id"], ex))
        elif t == Msg.APPROVAL_RESPONSE:
            script.approvals.append(d)
            script.release()
        elif t == Msg.CHOICE_RESPONSE:
            script.choices.append(d)
            script.release()
        elif t == Msg.OFFER_RESPONSE:
            script.offers.append(d)
            script.release()
        elif t == Msg.CANCEL:
            r = next(x for x in runs if x.id == e["turn_id"])
            while Msg.TURN_STARTED not in r.events:
                await asyncio.sleep(0.002)
            await r.ex.cancel()
        elif t == Msg.RESET:
            prev = last(e["session_id"])
            if prev:
                await prev.result()
            await sessions[e["session_id"]].reset()
        elif t == Msg.CLOSE_SESSION:
            await sessions[e["session_id"]].close()
        elif t == Msg.SHUTDOWN:
            await client.close()
            closed = True
        await fake.consumed(i)
    results = [await r.result() for r in runs]
    for r, got in zip(runs, results):
        check_turn(tc, r, lines, got)
    if client and not closed:
        await client.close()
    await played
    tc.assertEqual(fake.errs, [])


def check_turn(tc: unittest.TestCase, r: Run, lines: list[dict[str, Any]], got: tuple[Any, Any]) -> None:
    events, terminal, exited = [], None, False
    for ln in lines:
        if ln.get("halo_exit"):
            exited = True
        o = ln.get("out")
        if not o or o.get("turn_id") != r.id:
            continue
        if o["type"] in (Msg.APPROVAL_REQUEST, Msg.CHOICE_REQUEST, Msg.OFFER_REQUEST):
            continue
        if o["type"] in (Msg.OUTCOME, Msg.ERROR):
            terminal = o
        else:
            events.append(o["type"])
    tc.assertEqual(r.events, events, f"{r.id}: events")
    outcome, err = got
    if terminal is None:
        tc.assertTrue(exited, f"{r.id}: the transcript has no terminal envelope")
        tc.assertEqual(getattr(err, "code", None), "halo_exited")
    elif terminal["type"] == Msg.OUTCOME:
        want = terminal["data"]
        tc.assertIsNotNone(outcome, f"{r.id}: {err}")
        tc.assertEqual((outcome["kind"], outcome.get("code", ""), outcome["completed"]), (want["kind"], want.get("code", ""), want["completed"]))
    else:
        want = terminal["data"]
        tc.assertIsInstance(err, HaloError, f"{r.id}: {outcome}")
        tc.assertEqual((err.code, err.retryable, err.retry_after_ms), (want["code"], want["retryable"], want.get("retry_after_ms", 0)))


class TestConformance(unittest.IsolatedAsyncioTestCase):
    pass


def _add(path: Path) -> bool:
    head, *rest = path.read_text().strip().split("\n")
    meta = json.loads(head)["meta"]
    if "binding" not in meta["applies"]:
        return False
    lines = [json.loads(x) for x in rest]

    async def t(self: TestConformance) -> None:
        await asyncio.wait_for(replay(self, lines), 20)

    setattr(TestConformance, "test_" + meta["name"], t)
    return True


_COUNT = sum(_add(p) for p in sorted(DIR.glob("*.jsonl"))) if DIR.exists() else 0


class TestCoverage(unittest.TestCase):
    @unittest.skipUnless(DIR.exists(), "transcripts not present")
    def test_enough_binding_transcripts(self) -> None:
        self.assertGreaterEqual(_COUNT, 20)


if __name__ == "__main__":
    unittest.main()
