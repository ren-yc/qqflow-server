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

Source databases stay read-only. `sync` refreshes the in-memory index; media export writes local copies.

This client mirrors that surface one-to-one.

## Targeted media recovery

Restore one unique message with `export_media_by_message(talker, platform_message_id, sender, timestamp)`.

The result provides the actual `mediaId`, `fileName` and credential-free relative `mediaUrl`.

`media_bytes` tries the existing filename first. After a 404 it exports the target once and fetches the returned handle once.

- 404: target or original media missing.
- 409: ambiguous target or registration changed.
- 422: no supported local media.
- 405/501: server lacks targeted export; upgrade it before recovery.

I/O and decode errors propagate. No recent-message search is used.

## Links

- [Server repository](https://github.com/ren-yc/qqflow-server)
- [API reference](https://github.com/ren-yc/qqflow-server/blob/master/docs/qqflow-server-api.md)
- [CHANGELOG](https://github.com/ren-yc/qqflow-server/blob/master/CHANGELOG.md)
- [License](https://github.com/ren-yc/qqflow-server/blob/master/LICENSE) — MIT
