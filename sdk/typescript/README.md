# Halo Agent SDK for TypeScript

Drive the installed Halo from your programs and agents. **Requires the next Halo client release.**

```sh
npm install @memcode-ai/halo   # with the next Halo client release
```

```ts
import { Halo } from "@memcode-ai/halo";

const halo = await Halo.connect(); // once, when your application starts
const session = await halo.session({ cwd: "/path/to/project" });
const outcome = await session.run("what is using port 5432?");
console.log(outcome.kind, outcome.message);
await halo.close();
```

Create one Halo client for your application and reuse it; open a session per conversation. Halo controls the
machine and account it runs on: never pass untrusted users' requests to it.

Docs: https://memcode.ai/docs/halo/agent-sdk · Tests: `npm ci && npm test` (they replay `../conformance`).

Apache-2.0 (see `../LICENSE`).
