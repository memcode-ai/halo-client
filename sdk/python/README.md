# Halo Agent SDK for Python

Drive the installed Halo from your programs and agents. **Requires the next Halo client release.**

```sh
pip install halo-sdk   # with the next Halo client release (Python 3.11+)
```

```python
from halo_sdk import Halo

halo = await Halo.connect()  # once, when your application starts
session = await halo.session(cwd="/path/to/project")
outcome = await session.run("what is using port 5432?")
await halo.close()
```

Create one Halo client for your application and reuse it; open a session per conversation. Halo controls the
machine and account it runs on: never pass untrusted users' requests to it.

Docs: https://memcode.ai/docs/halo/agent-sdk · Tests: `PYTHONPATH=src python -m unittest discover -s tests` (they replay `../conformance`).

Apache-2.0 (see `../LICENSE`).
