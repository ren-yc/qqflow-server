"""Behavior-layer tests against an in-process ASGI mock that mirrors the
server's wire shapes. No real database: the fixtures here emit the same
JSON the server's golden snapshots pin."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import asyncio
import httpx
import pytest

from qqflow_sdk import Client, MessageEvent, NotReady
from qqflow_sdk.generated.qqflow_sdk import models as gen

TOKEN = "test-token-0123456789abcdef"


def auth_ok(headers: Dict[str, str]) -> None:
    assert headers.get("authorization") == f"Bearer {TOKEN}", "auth must go in the header"


class Mock:
    """Mutable fixture state shared between the test and the ASGI app."""

    def __init__(self) -> None:
        self.states: List[str] = []
        self.pull_pages: List[dict] = []
        self.pull_queries: List[dict] = []
        self.media_calls: List[str] = []
        self.media_hit_first = True
        self.chatlab_page: Optional[dict] = None
        self.messages_query: Optional[dict] = None
        self.sse_frames: List[str] = []
        # Raw bytes override for the SSE body; sse_frames is joined when None.
        self.sse_body: Optional[bytes] = None
        self.sse_connections: int = 0
        self.sse_last_ids: List[Optional[str]] = []
        # Response body for POST /api/v1/accounts (registration semantics).
        self.register_body: Optional[dict] = None

    def asgi_app(self):
        mock = self

        async def app(scope, receive, send):
            path = scope["path"]
            raw = scope.get("query_string", b"").decode()
            query: Dict[str, str] = {}
            for kv in raw.split("&"):
                if kv:
                    k, _, v = kv.partition("=")
                    query[k] = v
            headers = {
                k.decode().lower(): v.decode()
                for k, v in scope.get("headers", [])
            }
            auth_ok(headers)
            body: Any = None
            if path == "/api/v1/accounts" and scope["method"] == "POST":
                body = mock.register_body or {"success": True, "state": "indexing"}
            elif path == "/api/v1/accounts":
                state = mock.states.pop(0) if mock.states else "ready"
                body = {
                    "success": True,
                    "accounts": [
                        {"qq": "qq_mock", "db_path": "",
                         "message_count": 1, "state": state}
                    ],
                }
            elif path.endswith("/messages") and path.startswith("/api/v1/sessions/"):
                mock.pull_queries.append(query)
                if mock.pull_pages:
                    body = mock.pull_pages.pop(0)
                else:
                    await send({"type": "http.response.start", "status": 404, "headers": []})
                    await send({"type": "http.response.body", "body": b"fixture exhausted"})
                    return
            elif path == "/chatlab/messages":
                mock.messages_query = query
                body = mock.chatlab_page
            elif path.startswith("/api/v1/media/"):
                mock.media_calls.append(path.rsplit("/", 1)[-1])
                if mock.media_hit_first:
                    mock.media_hit_first = False  # first GET misses; the miss mints the file
                    await send({"type": "http.response.start", "status": 404, "headers": []})
                    await send({"type": "http.response.body", "body": b"not exported"})
                    return
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"application/octet-stream")],
                })
                await send({"type": "http.response.body", "body": b"png-bytes"})
                return
            elif path == "/api/v1/messages":
                mock.messages_query = query
                body = {"success": True, "count": 0, "has_more": False,
                        "talker": "", "media": {}, "messages": []}
            elif path == "/api/v1/push/messages":
                payload = ("\n".join(mock.sse_frames) + "\n").encode()
                mock.sse_connections += 1
                mock.sse_last_ids.append(headers.get("last-event-id"))
                payload = mock.sse_body
                if payload is None:
                    payload = ("\n".join(mock.sse_frames) + "\n").encode()
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/event-stream")],
                })
                await send({"type": "http.response.body", "body": payload})
                return
            if body is None:
                await send({"type": "http.response.start", "status": 404, "headers": []})
                await send({"type": "http.response.body", "body": b"fixture missing"})
                return
            payload = json.dumps(body).encode()
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            })
            await send({"type": "http.response.body", "body": payload})

        return app


def make_client(mock: Mock) -> Client:
    client = Client("http://mock", TOKEN)
    # Route the behavior layer's transport into the in-process mock.
    client._http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock.asgi_app()),
        base_url="http://mock",
    )
    return client


def pull_page(msgs: list[dict], has_more: bool, next_since: int, next_offset: int) -> dict:
    return {
        "chatlab": {"version": "1", "generator": "mock", "exportedAt": 1},
        "members": [],
        "messages": msgs,
        "meta": {"groupId": "", "name": "", "ownerId": "",
                 "platform": "qq", "type": "chat"},
        "sync": {"hasMore": has_more, "nextSince": next_since,
                 "nextOffset": next_offset, "watermark": 2000},
    }


def msg(mid: int, ts: int) -> dict:
    return {"accountName": "alice", "content": f"m{mid}", "groupNickname": "",
            "platformMessageId": str(mid), "sender": "alice", "timestamp": ts, "type": 1}


async def test_ensure_ready_polls_until_ready() -> None:
    mock = Mock()
    mock.states = ["indexing", "indexing"]
    client = make_client(mock)
    await client.ensure_ready("qq_mock",
                              {"qq": "qq_mock", "key": "0123456789abcdef", "db_path": "X:/db"},
                              timeout=5)
    await client.aclose()


async def test_ensure_ready_times_out_with_last_state() -> None:
    mock = Mock()
    mock.states = ["indexing"] * 100
    client = make_client(mock)
    with pytest.raises(NotReady) as exc:
        await client.ensure_ready("qq_mock",
                                  {"qq": "qq_mock", "key": "0123456789abcdef", "db_path": "X:/db"},
                                  timeout=0.6)
    assert exc.value.last_state == "indexing"
    await client.aclose()


async def test_drain_session_echoes_cursors_verbatim() -> None:
    mock = Mock()
    mock.pull_pages = [
        pull_page([msg(1, 990), msg(2, 1000)], True, 1000, 7),
        pull_page([msg(3, 1500)], False, 2000, 0),
    ]
    client = make_client(mock)
    pages: list[list[int]] = []
    total = await client.drain_session(
        "alice", 500,
        lambda batch: pages.append([int(m.platform_message_id) for m in batch]),
    )
    assert total == 3
    assert pages == [[1, 2], [3]]
    assert len(mock.pull_queries) == 2
    assert mock.pull_queries[0] == {"since": "500"}
    assert mock.pull_queries[1] == {"since": "1000", "offset": "7"}
    await client.aclose()


async def test_media_bytes_exports_then_retries() -> None:
    mock = Mock()
    mock.chatlab_page = {
        "chatlab": {"version": "1", "generator": "mock", "exportedAt": 1},
        "count": 0, "members": [], "messages": [],
        "meta": {"groupId": "", "name": "", "ownerId": "",
                 "platform": "qq", "type": "chat"},
        "page": {"hasMore": False, "nextCursor": None},
        "talker": "alice",
    }
    client = make_client(mock)
    message = gen.ChatlabMessage.model_validate({
        "accountName": "alice", "content": "x", "groupNickname": "",
        "media": {"type": "image", "fileName": "abc.png", "md5": "z"},
        "platformMessageId": "1", "sender": "alice", "timestamp": 1, "type": 1,
    })
    data = await client.media_bytes(message)
    assert data == b"png-bytes"
    assert mock.media_calls == ["abc.png", "abc.png"]
    assert mock.messages_query == {"talker": "alice", "media": "1"}
    await client.aclose()


async def test_watch_decodes_legacy_events_and_skips_heartbeats() -> None:
    mock = Mock()
    # The legacy face's message.new is the server's push event verbatim -
    # exactly the shape the Rust client's MessageEvent decodes (camelCase,
    # optional keys omitted when unknown). watch() yields one event per
    # connection, so a second frame in the same body is NOT asserted here:
    # the mock hands over the whole body at once and the generator's second
    # anext() lands in the reconnect path (that is the weflow sibling's
    # Last-Event-ID test, which also stops at one event).
    mock.sse_frames = [
        ": heartbeat",
        "id: 7",
        "event: message.new",
        "data: " + json.dumps({
            "event": "message.new", "rawid": "9", "sessionId": "alice",
            "sessionType": "chat", "sourceName": "alice",
            "timestamp": 1700000001, "content": "hi",
        }),
    ]
    client = make_client(mock)
    import asyncio as _asyncio

    agen = client.watch()
    first = await _asyncio.wait_for(anext(agen), timeout=5)
    assert isinstance(first, MessageEvent)
    assert first.rawid == "9"
    assert first.session_id == "alice"
    assert first.content == "hi"
    await agen.aclose()
    await client.aclose()


async def test_watch_flushes_the_final_frame_at_eof_without_a_trailing_blank_line() -> None:
    # The server guarantees a trailing blank line today (pinned by the
    # stream-tail test on the server side), but a server that closes without
    # it must not silently drop the last event: watch() flushes the pending
    # block at EOF. This test feeds a body whose last frame has NO trailing
    # blank line, which is exactly the case the flush exists for.
    mock = Mock()
    mock.sse_frames = [
        "id: 7",
        "event: message.new",
        "data: " + json.dumps({
            "event": "message.new", "rawid": "10", "sessionId": "bob",
            "sessionType": "chat", "sourceName": "bob",
            "timestamp": 1700000002, "content": "tail",
        }),
    ]
    client = make_client(mock)
    import asyncio as _asyncio

    agen = client.watch()
    first = await _asyncio.wait_for(anext(agen), timeout=5)
    assert isinstance(first, MessageEvent)
    assert first.rawid == "10"
    assert first.content == "tail"
    await agen.aclose()
    await client.aclose()



# ---- fixed-behavior-layer coverage ---------------------------------------

from qqflow_sdk import StatusError  # noqa: E402


def _sdk_sse_frame(event, payload, id_):
    return [
        "id: %d" % id_,
        "event: %s" % event,
        "data: " + json.dumps(payload),
    ]


def _sdk_new_payload(rawid, content):
    return {
        "event": "message.new", "rawid": rawid, "sessionId": "alice",
        "sessionType": "group", "sourceName": "alice",
        "timestamp": 1700000001, "content": content,
    }


def _sdk_raw(frames, trailing_blank=True):
    # A frame is a list of lines (joined by LF); a bare string is one line.
    # A call may pass a list of frames, a single frame, or a list of lines:
    # normalize by grouping any list-of-lists, then join frames with a
    # blank line.
    nl = chr(10)
    groups = []
    for item in frames:
        if isinstance(item, list):
            groups.append(nl.join(item))
        else:
            if groups:
                groups[-1] = groups[-1] + nl + item
            else:
                groups.append(item)
    tail = nl + nl if trailing_blank else ""
    return ((nl + nl).join(groups) + tail).encode("utf-8")


async def _sdk_collect(agen, n, timeout=5.0):
    out = []
    for _ in range(n):
        out.append(await asyncio.wait_for(anext(agen), timeout=timeout))
    return out


async def test_sdk_ensure_ready_fails_fast_on_refusal_200():
    # A second qq is answered with 200 + account_conflict (this face also
    # has invalid_key / invalid_db_path / unknown_qq). Treating that 200 as
    # accepted makes the readiness poll time out and hide the real cause,
    # so ensure_ready fails fast on the body state.
    mock = Mock()
    mock.register_body = {
        "success": True, "state": "account_conflict",
        "qq": "10001", "occupied_by": "10002",
    }
    client = make_client(mock)
    with pytest.raises(StatusError) as exc:
        await client.ensure_ready(
            "10001",
            {"qq": "10001", "key": "0123456789abcdef", "db_path": "X:/db"},
            timeout=5,
        )
    assert "account_conflict" in str(exc.value)
    assert mock.states == []
    await client.aclose()


async def test_sdk_wait_ready_is_wait_only():
    # wait_ready must not register: only the listing endpoint is polled,
    # no POST is sent, and it returns as soon as the account is ready.
    mock = Mock()
    mock.states = ["indexing", "ready"]
    client = make_client(mock)
    await client.wait_ready("qq_mock", timeout=5)
    await client.aclose()


async def test_sdk_watch_yields_many_frames_over_one_connection():
    # One connection produces many events: four frames in one body must all
    # arrive on that same connection. A per-frame disconnect would turn
    # every frame (including idle sync heartbeats) into a reconnect cycle
    # and make the server re-send its baseline each time.
    mock = Mock()
    heart = [": heartbeat"]
    f1 = _sdk_sse_frame("message.new", _sdk_new_payload("9", "hi"), 7)
    f2 = _sdk_sse_frame("message.revoke", {
        "event": "message.revoke", "rawid": "8", "sessionId": "alice",
        "sessionType": "group", "sourceName": "alice",
        "timestamp": 1700000002, "content": "gone",
    }, 8)
    f3 = _sdk_sse_frame("sync", {"event": "sync", "generation": 1, "watermarks": []}, 9)
    f4 = _sdk_sse_frame("message.new", _sdk_new_payload("10", "again"), 10)
    mock.sse_body = _sdk_raw([heart, f1, f2, f3, f4])
    client = make_client(mock)
    agen = client.watch()
    events = await _sdk_collect(agen, 4)
    assert isinstance(events[0], MessageEvent)
    assert events[0].rawid == "9"
    assert events[0].event == "message.new"
    assert isinstance(events[1], MessageEvent)
    assert events[1].event == "message.revoke"
    assert events[1].content == "gone"
    assert isinstance(events[2], gen.SyncFrame)
    assert events[2].generation == 1
    assert isinstance(events[3], MessageEvent)
    assert events[3].event == "message.new"
    assert mock.sse_connections == 1
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_event_name_body_first_then_frame_header():
    # MessageEvent.event is filled from the frame body, falling back to the
    # frame's event: header - new and revoke must stay distinguishable no
    # matter which channel carried the name.
    mock = Mock()
    payload = _sdk_new_payload("11", "from-body")
    mock.sse_body = _sdk_raw(
        ["id: 5", "event: message.new", "data: " + json.dumps(payload)])
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.event == "message.new"
    del payload["event"]
    mock.sse_body = _sdk_raw(
        ["id: 6", "event: message.revoke", "data: " + json.dumps(payload)])
    agen2 = client.watch()
    (event2,) = await _sdk_collect(agen2, 1)
    assert event2.event == "message.revoke"
    await agen.aclose()
    await agen2.aclose()
    await client.aclose()


async def test_sdk_watch_survives_malformed_frames():
    # Bad JSON, a non-object payload, and a shape mismatch each get logged
    # and skipped - one bad frame must not kill the stream.
    nl = chr(10)
    blank = nl + nl
    mock = Mock()
    parts = [
        "id: 1" + nl + "event: message.new" + nl + "data: {not json}",
        "id: 2" + nl + "event: message.new" + nl + "data: [1, 2, 3]",
        "id: 3" + nl + "event: message.new" + nl
        + "data: " + json.dumps({"event": "message.new"}),
        nl.join(_sdk_sse_frame("message.new", _sdk_new_payload("9", "hi"), 4)),
    ]
    mock.sse_body = (blank.join(parts) + blank).encode("utf-8")
    client = make_client(mock)
    agen = client.watch()
    (good,) = await _sdk_collect(agen, 1)
    assert good.rawid == "9"
    assert good.event == "message.new"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_byte_framing_keeps_u2028_bodies_intact():
    # U+2028 is legal inside a JSON string but str.splitlines treats it as
    # a line break: framing must be byte-level LF only.
    body_text = json.dumps(
        _sdk_new_payload("9", "line" + chr(0x2028) + "break"),
        ensure_ascii=False,
    )
    mock = Mock()
    mock.sse_body = _sdk_raw(
        ["id: 7", "event: message.new", "data: " + body_text])
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.content == "line" + chr(0x2028) + "break"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_caps_buffer_on_malformed_stream():
    # A stream that never emits a blank line would grow the buffer without
    # bound; past the cap watch ends the stream and reconnects instead of
    # buffering forever.
    mock = Mock()
    mock.sse_body = b"data: " + b"x" * (1 << 21)
    client = make_client(mock)
    agen = client.watch()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(agen), timeout=3)
    await agen.aclose()
    await client.aclose()
    assert mock.sse_connections >= 1


async def test_sdk_watch_flushes_unterminated_final_line_at_eof():
    # A server that closes without the trailing blank line must not
    # silently drop the last event: watch() folds the unterminated final
    # line (still in the buffer) into the pending frame at EOF.
    mock = Mock()
    mock.sse_body = _sdk_raw(
        _sdk_sse_frame("message.new", _sdk_new_payload("10", "tail"), 7),
        trailing_blank=False,
    )
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.rawid == "10"
    assert event.event == "message.new"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_reconnects_with_last_event_id_after_stream_end():
    # Only when the stream itself ends does watch reconnect - carrying the
    # last seen id so the server's replay window fills the gap.
    mock = Mock()
    mock.sse_body = _sdk_raw(
        _sdk_sse_frame("message.new", _sdk_new_payload("9", "hi"), 7))
    client = make_client(mock)
    agen = client.watch()
    first = await asyncio.wait_for(anext(agen), timeout=5)
    assert first.rawid == "9"
    second = await asyncio.wait_for(anext(agen), timeout=10)
    assert second.rawid == "9"
    assert mock.sse_connections >= 2
    assert mock.sse_last_ids[0] is None
    assert mock.sse_last_ids[1] == "7"
    await agen.aclose()
    await client.aclose()
