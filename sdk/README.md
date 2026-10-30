# Halo Agent SDK

Drive the installed Halo from your own programs and agents. Each request goes in as words and comes back as a
typed outcome, with the same previews, permission checks, sandboxing and approvals as the Halo shell.

The SDK runs the `halo` you already installed (`curl -fsSL https://memcode.ai/halo/install.sh | sh`). It needs
no extra runtime, daemon or service, and uses your Halo sign-in (`halo login`).

- `go/`: the Go binding (`go get github.com/memcode-ai/halo-client/sdk/go`)
- `protocol/`: the machine protocol's JSON Schema, which every binding speaks
- `conformance/`: the transcripts every binding is tested against

Docs: https://memcode.ai/docs/halo/agent-sdk
