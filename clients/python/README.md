# qqflow-sdk

Typed Python client for [qqflow-server](https://github.com/ren-yc/qqflow-server), a read-only HTTP/SSE service over local QQ NT databases.

## Install

```
pip install qqflow-sdk
```

## Minimal example

```python
import asyncio

from qqflow_sdk import Client


async def main() -> None:
    client = Client(base_url="http://127.0.0.1:5032", token="<token>")
    await client.health()
    sessions = await client.list_all_sessions()
    await client.aclose()

asyncio.run(main())
```

`SyncClient` mirrors `Client` method-for-method and drives its own private event-loop thread.

## Generated and handwritten layers

Types are generated; the behavior layer is handwritten. Never edit `generated/` — run `python scripts/regen.py`.

## Scope

Read-only over the user's databases; `sync` is the one state-changing call. Covers all read surfaces plus media export.

This client mirrors that surface one-to-one.

## Links

- [Server repository](https://github.com/ren-yc/qqflow-server)
- [API reference](https://github.com/ren-yc/qqflow-server/blob/master/docs/qqflow-server-api.md)
- [CHANGELOG](https://github.com/ren-yc/qqflow-server/blob/master/CHANGELOG.md)
- [License](https://github.com/ren-yc/qqflow-server/blob/master/LICENSE) — MIT
