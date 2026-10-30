# Halo Agent SDK for Rust

Drive the installed Halo from your programs and agents. **Requires the next Halo client release.**

```sh
cargo add halo-sdk   # with the next Halo client release (uses tokio)
```

```rust
let halo = halo_sdk::Halo::connect(Default::default()).await?; // once, when your program starts
let session = halo.session(Default::default()).await?;
let outcome = session.run("what is using port 5432?").await?;
halo.close().await;
```

Create one Halo client for your application and reuse it; open a session per conversation. Halo controls the
machine and account it runs on: never pass untrusted users' requests to it.

Docs: https://memcode.ai/docs/halo/agent-sdk · Tests: `cargo test` (they replay `../conformance`).

Apache-2.0 (see `../LICENSE`).
