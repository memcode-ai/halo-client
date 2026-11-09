# Halo Agent SDK

Drive the installed Halo from your own programs and agents. Each request goes in as words and comes back as a
typed outcome, with the same previews, permission checks, sandboxing and approvals as the Halo shell.

**Requires Halo client v0.1.18 or later** (`curl -fsSL https://memcode.ai/halo/install.sh | sh`). The Go module can be
installed today; the npm, PyPI (`memcode-halo`) and crates.io packages follow.

```
Your application
      │  one Halo client (create it once and reuse it)
      ▼
one local Halo process
      ├── session A   (a conversation)
      ├── session B
      └── session C
```

Creating a session does not launch another Halo process. Connecting and opening sessions happen on your machine;
only a request Halo needs to interpret contacts the Halo service.

| Language | Source | One request | A conversation | The client |
| --- | --- | --- | --- | --- |
| Go | `go/` (`go get github.com/memcode-ai/halo-client/sdk/go`) | `halo.Query` | `halo.Open` | `halo.Connect` |
| TypeScript | `typescript/` (`@memcode-ai/halo`) | `query()` | `session()` | `Halo.connect()` |
| Python | `python/` (`memcode-halo`, import `halo_sdk`) | `await query()` | `await session()` | `await Halo.connect()` |
| Rust | `rust/` (`halo-sdk`) | `query()` | `open()` | `Halo::connect()` |

`query` asks one thing and returns a typed result (status, text, rows, value, question). `session`/`open` keeps a
conversation so follow-ups ("kill it", "only the PDFs") refer to earlier results. `connect` is the lower-level
client: one Halo process, many sessions, every event.

- `protocol/`: the machine protocol's JSON Schema, which every binding speaks.
- `conformance/`: the transcripts every binding is tested against.

**Halo controls the machine and account it runs on.** Never pass untrusted users' requests to a Halo session on
your own machine or server. A service that takes requests from other people must run each user's Halo in its own
container or VM.

Docs: https://memcode.ai/docs/halo/agent-sdk
