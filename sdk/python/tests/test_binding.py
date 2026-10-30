from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import unittest
import warnings

import halo_sdk
from halo_sdk import HaloError, HaloReuseWarning, Msg, PROTOCOL_VERSION


class MiniHalo:
    """A minimal halo: it completes the handshake and opens sessions; other lines land in sent."""

    def __init__(self) -> None:
        self.stdout = asyncio.StreamReader()
        self.sent: list[dict] = []
        self._buf = b""
        self._exited = asyncio.Event()
        self._n = 0
        halo = self

        class Stdin:
            def write(self, b: bytes) -> None:
                halo._buf += b
                while b"\n" in halo._buf:
                    line, halo._buf = halo._buf.split(b"\n", 1)
                    halo._handle(json.loads(line))

            async def drain(self) -> None:
                pass

            def close(self) -> None:
                halo.stdout.feed_eof()
                halo._exited.set()

        self.stdin = Stdin()

    def write(self, **env) -> None:
        data = env.pop("data", {})
        self.stdout.feed_data((json.dumps({"version": PROTOCOL_VERSION, **env, "data": data}) + "\n").encode())

    def _handle(self, e: dict) -> None:
        if e["type"] == Msg.INITIALIZE:
            self.write(type=Msg.INITIALIZED, data={"protocol": PROTOCOL_VERSION, "halo_version": "test", "service": "halo/1",
                                                     "signed_in": True, "capabilities": ["sessions"]})
        elif e["type"] == Msg.CREATE_SESSION:
            self._n += 1
            sid = f"S{self._n}"
            self.write(type=Msg.SESSION_CREATED, id=e["id"], session_id=sid, data={"session_id": sid})
        elif e["type"] == Msg.SHUTDOWN:
            self.write(type=Msg.SHUTDOWN_COMPLETE, id=e["id"])
        else:
            self.sent.append(e)

    async def wait(self) -> int:
        await self._exited.wait()
        return 0

    def kill(self) -> None:
        self.stdin.close()


def launcher(h: MiniHalo):
    async def launch(_b, _a):
        return h
    return launch


class TestBinding(unittest.IsolatedAsyncioTestCase):
    def test_no_one_shot_helper(self) -> None:
        self.assertFalse(hasattr(halo_sdk, "run"), "a top-level run would start a process per call")
        self.assertFalse(hasattr(halo_sdk, "connect"), "one entry point: Halo.connect")

    async def test_reuse_warning(self) -> None:
        halo_sdk._warned_reuse = False
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            clients = [await halo_sdk.Halo.connect(launch=launcher(MiniHalo())) for _ in range(2)]
            for c in clients:
                await c.close()
        self.assertEqual(sum(issubclass(x.category, HaloReuseWarning) for x in w), 1)

    async def test_one_turn_per_session(self) -> None:
        h = MiniHalo()
        c = await halo_sdk.Halo.connect(launch=launcher(h))
        s = await c.session()
        ex = await s.begin("hang")
        with self.assertRaises(HaloError) as cm:
            await s.begin("again")
        self.assertEqual(cm.exception.code, "turn_in_progress")
        other = await c.session()
        ex2 = await other.begin("elsewhere")
        self.assertEqual([(e["type"], e["session_id"]) for e in h.sent], [(Msg.USER_TURN, "S1"), (Msg.USER_TURN, "S2")])
        h.write(type=Msg.OUTCOME, session_id="S2", turn_id=ex2.turn_id, data={"kind": "answer", "completed": True})
        self.assertEqual((await ex2.outcome())["kind"], "answer")
        h.write(type=Msg.OUTCOME, session_id="S1", turn_id=ex.turn_id, data={"kind": "answer", "completed": True, "model": "halo-orbit-preview"})
        await ex.outcome()
        self.assertEqual(s.model, "halo-orbit-preview")
        await c.close()

    def test_find(self) -> None:
        d = tempfile.mkdtemp()
        old = {k: os.environ.get(k) for k in ("PATH", "HALO_BIN", "HALO_INSTALL_DIR")}
        try:
            os.environ["PATH"] = d
            os.environ.pop("HALO_BIN", None)
            os.environ["HALO_INSTALL_DIR"] = d
            with self.assertRaises(HaloError) as cm:
                halo_sdk.find(os.path.join(d, "missing"))
            self.assertIn(halo_sdk.INSTALL_LINE, str(cm.exception))
            p = os.path.join(d, "halo")
            open(p, "w").write("#!/bin/sh\n")
            os.chmod(p, os.stat(p).st_mode | stat.S_IEXEC)
            self.assertEqual(halo_sdk.find(), p)
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    @unittest.skipUnless(os.environ.get("HALO_E2E_BIN"), "HALO_E2E_BIN not set")
    async def test_real_halo(self) -> None:
        async with await halo_sdk.Halo.connect(bin_path=os.environ["HALO_E2E_BIN"]) as c:
            async with await c.session(cwd=tempfile.mkdtemp()) as s:
                o = await s.run("list the files here")
                self.assertIn(o["kind"], ("result", "answer"))
                self.assertTrue(s.model)


if __name__ == "__main__":
    unittest.main()
