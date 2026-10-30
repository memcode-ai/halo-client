# Halo Agent SDK

Drive the installed Halo from your own programs and agents. Each request goes in as words and comes back as a
typed outcome, with the same previews, permission checks, sandboxing and approvals as the Halo shell.

**Requires the next Halo client release.** The currently released client (v0.1.5) does not yet include Agent SDK
support. Packages on npm, PyPI and crates.io will follow that release; the Go module can already be installed.

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

| Language | Source | Entry point |
| --- | --- | --- |
| Go | `go/` (`go get github.com/memcode-ai/halo-client/sdk/go`) | `halo.Connect` |
| TypeScript | `typescript/` (`@memcode-ai/halo`, with the client release) | `Halo.connect()` |
| Python | `python/` (`halo-sdk`, import `halo_sdk`, with the client release) | `await Halo.connect()` |
| Rust | `rust/` (`halo-sdk`, with the client release) | `Halo::connect()` |

- `protocol/`: the machine protocol's JSON Schema, which every binding speaks.
- `conformance/`: the transcripts every binding is tested against.

**Halo controls the machine and account it runs on.** Never pass untrusted users' requests to a Halo session on
your own machine or server. A service that takes requests from other people must run each user's Halo in its own
container or VM.

Docs: https://memcode.ai/docs/halo/agent-sdk
