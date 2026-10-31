# qqflow-server TypeScript example client

**Example only — not published to npm.** Minimal runnable reference with no compatibility guarantee and no release channel.

## Install

```bash
npm install --include=dev    # NODE_ENV=production would otherwise skip devDependencies and tsc
```

## Minimal example

```bash
npm run typecheck
npm run build

# Against a real server:
QQFLOW_BASE_URL=http://127.0.0.1:5032 QQFLOW_API_TOKEN=<token> node dist/smoke.js

# Against the in-repo stub (no real server needed):
node stub-server.mjs &
QQFLOW_API_TOKEN=fictional node dist/smoke.js
```

Requires Node 18+.

## Generated and handwritten layers

`client.ts` is handwritten; `smoke.ts` asserts real HTTP results; `stub-server.mjs` is an in-repo fake server (port 5098).

Setting `STUB_STATUS=500` on the stub must make the smoke test exit non-zero.

## Scope

`client.ts` covers `health`, `accounts`, `wait_ready`, `ensure_ready` and `watch` (SSE with `Last-Event-ID` resume and exponential backoff).

## Links

- [Server repository](https://github.com/ren-yc/qqflow-server)
- [API reference](https://github.com/ren-yc/qqflow-server/blob/master/docs/qqflow-server-api.md)
- [CHANGELOG](https://github.com/ren-yc/qqflow-server/blob/master/CHANGELOG.md)
- [License](https://github.com/ren-yc/qqflow-server/blob/master/LICENSE) — MIT
