"""Behavior-layer tests against an in-process ASGI mock that mirrors the
server's wire shapes. No real database: the fixtures here emit the same
JSON the server's golden snapshots pin."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
import pytest

from qqflow_sdk import Client, MessageEvent, NotReady
from qqflow_sdk import client as sdkmod
from qqflow_sdk.generated.qqflow_sdk import models as gen

TOKEN = "test-token-0123456789abcdef"


def auth_ok(headers: dict[str, str]) -> None:
    assert headers.get("authorization") == f"Bearer {TOKEN}", "auth must go in the header"


class Mock:
    """Mutable fixture state shared between the test and the ASGI app."""

    def __init__(self) -> None:
        self.states: list[str] = []
        self.pull_pages: list[dict] = []
        self.pull_queries: list[dict] = []
        self.media_calls: list[str] = []
        self.media_hit_first = True
        self.chatlab_page: dict | None = None
        self.messages_query: dict | None = None
        self.sse_frames: list[str] = []
        # Raw bytes override for the SSE body; sse_frames is joined when None.
        self.sse_body: bytes | None = None
        # Per-connection bodies, popped in order; the last one repeats.
        self.sse_seq: list[bytes] = []
        # Split delivery: when set, sent as separate body messages.
        self.sse_chunks: list[bytes] | None = None
        self.sse_connections: int = 0
        self.sse_last_ids: list[str | None] = []
        # Request counters: which endpoints a call actually touched.
        self.post_calls: int = 0
        self.get_accounts_calls: int = 0
        self.sync_calls: int = 0
        # Response body for POST /api/v1/accounts (registration semantics).
        self.register_body: dict | None = None
        # When set, GET /api/v1/accounts answers this page verbatim.
        self.accounts_page: dict | None = None
        # When set, GET /api/v1/messages answers this page verbatim.
        self.native_page: dict | None = None
        # When set, GET /api/v1/contacts answers this page verbatim.
        self.contacts_page: dict | None = None
        # When set, GET /api/v1/group-members answers this page verbatim.
        self.group_members_page: dict | None = None
        self.group_members_queries: list[dict] = []
        # FIFO pages for GET /api/v1/sessions.
        self.sessions_pages: list[dict] = []
        # Query params seen by the sessions route, in order.
        self.sessions_queries: list[dict] = []
        # Whether each /health hit carried credentials (it must not).
        self.health_auth: list[bool] = []
        self.health_body: dict | None = None

    def asgi_app(self):
        mock = self

        async def app(scope, receive, send):
            path = scope["path"]
            raw = scope.get("query_string", b"").decode()
            query: dict[str, str] = {}
            for kv in raw.split("&"):
                if kv:
                    k, _, v = kv.partition("=")
                    query[k] = v
            headers = {
                k.decode().lower(): v.decode()
                for k, v in scope.get("headers", [])
            }
            if path == "/health":
                # Unauthenticated by design: record whether credentials rode
                # along (the SDK must not send any) instead of asserting them.
                mock.health_auth.append("authorization" in headers)
            else:
                auth_ok(headers)
            body: Any = None
            if path == "/api/v1/accounts" and scope["method"] == "POST":
                mock.post_calls += 1
                body = mock.register_body or {"success": True, "state": "indexing"}
            elif path == "/api/v1/accounts":
                mock.get_accounts_calls += 1
                if mock.accounts_page is not None:
                    body = mock.accounts_page
                else:
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
            elif path == "/api/v1/group-members":
                mock.group_members_queries.append(query)
                body = mock.group_members_page
            elif path == "/api/v1/sync":
                mock.sync_calls += 1
                body = {"success": True, "newMessages": 7, "revokeMessages": 2}
            elif path == "/api/v1/messages":
                mock.messages_query = query
                # The native face is camelCase on the wire; a fallback with
                # snake_case keys decodes as a shape break for every caller.
                body = mock.native_page or {
                    "success": True, "count": 0, "hasMore": False, "talker": "",
                    "media": {"count": 0, "enabled": False},
                    "messages": [],
                }
            elif path == "/health":
                body = mock.health_body or {
                    "account": "ready", "status": "ok", "version": "0.0.0",
                }
            elif path == "/api/v1/contacts":
                body = mock.contacts_page
            elif path == "/api/v1/sessions":
                mock.sessions_queries.append(query)
                body = mock.sessions_pages.pop(0) if mock.sessions_pages else None
            elif path == "/api/v1/push/messages":
                mock.sse_connections += 1
                mock.sse_last_ids.append(headers.get("last-event-id"))
                if mock.sse_seq:
                    payload = mock.sse_seq.pop(0) if len(mock.sse_seq) > 1 else mock.sse_seq[0]
                elif mock.sse_body is None:
                    payload = ("\n".join(mock.sse_frames) + "\n").encode()
                else:
                    payload = mock.sse_body
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/event-stream")],
                })
                if mock.sse_chunks is not None:
                    last = len(mock.sse_chunks) - 1
                    for i, part in enumerate(mock.sse_chunks):
                        await send({
                            "type": "http.response.body",
                            "body": part,
                            "more_body": i < last,
                        })
                else:
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


async def test_pull_page_decodes_the_sync_block_and_sends_the_cursors() -> None:
    mock = Mock()
    mock.pull_pages = [pull_page([msg(1, 1000)], True, 1000, 4)]
    client = make_client(mock)
    page = await client.pull_page("alice", 500, offset=7, limit=3)
    assert [m.platform_message_id for m in page.messages] == ["1"]
    assert page.sync.has_more is True
    assert page.sync.next_since == 1000
    assert page.sync.next_offset == 4
    assert page.sync.watermark == 2000
    assert mock.pull_queries[0] == {"since": "500", "offset": "7", "limit": "3"}
    await client.aclose()


async def test_pull_page_omits_defaulted_cursors_instead_of_sending_zero() -> None:
    """Absence, not ``0``: the server defaults both cursors, so a client that
    sends ``since=0`` is claiming a cursor it never read."""
    mock = Mock()
    mock.pull_pages = [pull_page([], False, 0, 0)]
    client = make_client(mock)
    await client.pull_page("alice", None)
    assert mock.pull_queries[0] == {}
    await client.aclose()


async def test_chatlab_messages_decodes_the_chatlab_envelope_and_paging() -> None:
    mock = Mock()
    mock.chatlab_page = {
        "chatlab": {"version": "1", "generator": "mock", "exportedAt": 1},
        "count": 1,
        "members": [{"accountName": "张三", "avatar": "", "groupNickname": "",
                     "platformId": "alice"}],
        "messages": [{"accountName": "alice", "content": "hi",
                      "groupNickname": "", "platformMessageId": "42",
                      "sender": "alice", "timestamp": 1700000000, "type": 1}],
        "meta": {"groupId": "", "name": "", "ownerId": "",
                 "platform": "qq", "type": "private"},
        "page": {"hasMore": True, "nextCursor": "1000"},
        "talker": "alice",
    }
    client = make_client(mock)
    page = await client.chatlab_messages("alice", keyword="hi", limit=50)
    assert page.count == 1
    assert page.page.has_more is True
    assert page.page.next_cursor == "1000"
    assert page.messages[0].platform_message_id == "42"
    assert page.messages[0].type == 1, "ChatLab type codes, not the native localType"
    assert mock.messages_query == {"talker": "alice", "keyword": "hi", "limit": "50"}
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
    # exactly the shape the Rust client's MessageEvent decodes (camelCase,
    # optional keys omitted when unknown). watch() delivers every frame on
    # one connection; this fixture only asserts the legacy decode shape, so
    # it stops at the first event (multi-frame delivery is covered by
    # test_sdk_watch_yields_many_frames_over_one_connection).
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
        f"id: {id_}",
        f"event: {event}",
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
    # The readiness poll never ran: counted GETs, not an empty list.
    assert mock.get_accounts_calls == 0
    await client.aclose()


async def test_sdk_wait_ready_is_wait_only():
    # wait_ready must not register: only the listing endpoint is polled,
    # no POST is sent, and it returns as soon as the account is ready.
    # Counted, not inferred.
    mock = Mock()
    mock.states = ["indexing", "ready"]
    client = make_client(mock)
    await client.wait_ready("qq_mock", timeout=5)
    assert mock.post_calls == 0
    assert mock.get_accounts_calls == 2
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


# ---- strengthened SSE guards: over-cap frames, multi-data join, backoff ----
# (replaces the old single-assertion cap test; the in-process ASGITransport
# buffers whole response bodies, so the signals are the cap WARNING, the
# escalating reconnect schedule, and frame-level delivery - not timing.)

_NL = chr(10)  # LF spelled without escapes: fixtures below build raw frames


class _StopWatch(Exception):
    """Raised from a stubbed sleep to end watch() deterministically."""


async def test_sdk_watch_caps_buffer_on_malformed_stream(monkeypatch, caplog) -> None:
    # A stream that never closes a frame would grow the buffer without
    # bound; past the cap watch ends the stream and reconnects instead.
    mock = Mock()
    mock.sse_body = b"data: " + b"x" * (1 << 21)  # 2 MiB, no blank line ever
    client = make_client(mock)
    with caplog.at_level(logging.WARNING, logger="qqflow_sdk.client"):
        agen = client.watch()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(agen), timeout=3)
        await agen.aclose()
        warned = [r.getMessage() for r in caplog.records]
    await client.aclose()
    cap_number = str(sdkmod._SSE_BUFFER_CAP)
    # 1) loud: the guard names the cap in a WARNING
    assert any(cap_number in m and "over" in m for m in warned)
    # 2) the stream was abandoned, not buffered: the reconnect loop ran
    assert mock.sse_connections >= 2
    # 3) contrast: lift the cap - the same stream then goes through the
    #    EOF-flush path (undecodable final frame), NOT the cap path: no
    #    warning carrying the cap number.
    monkeypatch.setattr(sdkmod, "_SSE_BUFFER_CAP", 1 << 30)
    mock2 = Mock()
    mock2.sse_body = b"data: " + b"x" * (1 << 21)
    client2 = make_client(mock2)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="qqflow_sdk.client"):
        agen2 = client2.watch()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(agen2), timeout=2)
        await agen2.aclose()
        warned2 = [r.getMessage() for r in caplog.records]
    await client2.aclose()
    assert not any(cap_number in m for m in warned2)


async def test_sdk_watch_delivers_valid_frames_after_overflow_reconnect() -> None:
    # Ending the over-cap stream is only half the guard: the reconnect
    # must still deliver the legitimate frames that follow.
    good = _sdk_raw(_sdk_sse_frame("message.new", _sdk_new_payload("11", "after"), 12))
    mock = Mock()
    mock.sse_seq = [b"data: " + b"x" * (1 << 21), good]
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1, timeout=10)
    assert event.rawid == "11"
    assert mock.sse_connections >= 2
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_rejects_oversized_single_frame_whole_and_split() -> None:
    # The cap bounds one *complete* frame too: an over-cap frame is not
    # delivered and the stream reconnects - whether it lands as one chunk
    # or is split across two body messages.
    big = json.dumps(_sdk_new_payload("9", "x" * ((1 << 20) + 8192)))
    whole = ("id: 7" + _NL + "event: message.new" + _NL + "data: " + big
             + _NL + _NL).encode()
    mock = Mock()
    mock.sse_body = whole
    client = make_client(mock)
    agen = client.watch()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(agen), timeout=2)
    await agen.aclose()
    await client.aclose()
    assert mock.sse_connections >= 2, "over-cap whole frame must end the stream"

    mock2 = Mock()
    mock2.sse_chunks = [
        ("id: 7" + _NL + "event: message.new" + _NL + "data: " + big + _NL).encode(),
        _NL.encode(),
    ]
    client2 = make_client(mock2)
    agen2 = client2.watch()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(agen2), timeout=2)
    await agen2.aclose()
    await client2.aclose()
    assert mock2.sse_connections >= 2, "over-cap split frame must end the stream"


async def test_sdk_watch_delivers_frame_just_under_the_cap() -> None:
    # Contrast for the single-frame guard: a frame below the cap must still
    # be delivered, else "reject over-cap frames" would just mean "reject
    # everything" and the guard would have no observable upper bound.
    body = json.dumps(_sdk_new_payload("13", "fits"))
    assert len(body) < sdkmod._SSE_BUFFER_CAP
    mock = Mock()
    mock.sse_body = ("id: 1" + _NL + "event: message.new" + _NL + "data: " + body
                     + _NL + _NL).encode()
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.rawid == "13"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_joins_multiple_data_lines_per_frame() -> None:
    # SSE spec: several data lines in one frame join with LF into the one
    # payload that gets dispatched. The old per-line overwrite kept only the
    # last line and dropped the frame as undecodable - a shape the server
    # does not emit today, pinned here as a guard.
    text = json.dumps(_sdk_new_payload("9", "joined"))
    cut = text.index(",") + 1
    mock = Mock()
    mock.sse_body = ("id: 7" + _NL + "event: message.new" + _NL + "data: " + text[:cut]
                     + _NL + "data: " + text[cut:] + _NL + _NL).encode()
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.rawid == "9"
    assert event.content == "joined"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_joins_three_data_lines_boundary() -> None:
    text = json.dumps(_sdk_new_payload("12", "three lines"))
    c1 = text.index(",") + 1
    c2 = text.index(",", c1) + 1
    mock = Mock()
    mock.sse_body = ("id: 7" + _NL + "data: " + text[:c1] + _NL + "data: " + text[c1:c2]
                     + _NL + "data: " + text[c2:] + _NL + _NL).encode()
    client = make_client(mock)
    agen = client.watch()
    (event,) = await _sdk_collect(agen, 1)
    assert event.rawid == "12"
    assert event.content == "three lines"
    await agen.aclose()
    await client.aclose()


async def test_sdk_watch_backoff_escalates_on_persistent_overflow(monkeypatch) -> None:
    # A stream that overflows on every connection must escalate 0.5, 1, 2,
    # 4... The old shape reset the delay on every 200, so a malformed-but-
    # connectable server reconnected forever at a flat 0.5s (measured: 6
    # connects in 3s). Sleep durations are captured through a stub, so the
    # assertion is about the schedule, not wall-clock.
    recorded: list = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *a, **kw):
        recorded.append(delay)
        if len(recorded) >= 4:
            raise _StopWatch()
        await real_sleep(0)

    monkeypatch.setattr(sdkmod.asyncio, "sleep", fake_sleep)
    mock = Mock()
    mock.sse_seq = [b"data: " + b"x" * (1 << 21)]
    client = make_client(mock)
    agen = client.watch()
    try:
        with pytest.raises(_StopWatch):
            await anext(agen)
    finally:
        monkeypatch.undo()
        await agen.aclose()
        await client.aclose()
    assert recorded == [0.5, 1.0, 2.0, 4.0]


async def test_sdk_watch_backoff_returns_to_floor_after_clean_stream_end(monkeypatch) -> None:
    # The contrast assertion: a stream that ends cleanly (EOF, no overflow)
    # reconnects from the 0.5s floor instead of carrying a stale doubled
    # delay over. One frame per connection, three deliveries: without the
    # clean-end reset the schedule would be [0.5, 1.0].
    recorded: list = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *a, **kw):
        recorded.append(delay)
        if len(recorded) >= 2:
            raise _StopWatch()
        await real_sleep(0)

    monkeypatch.setattr(sdkmod.asyncio, "sleep", fake_sleep)
    mock = Mock()
    mock.sse_body = _sdk_raw(_sdk_sse_frame("message.new", _sdk_new_payload("9", "hi"), 7))
    client = make_client(mock)
    agen = client.watch()
    try:
        first = await anext(agen)
        assert first.rawid == "9"
        second = await anext(agen)
        assert second.rawid == "9"
        with pytest.raises(_StopWatch):
            await anext(agen)
    finally:
        monkeypatch.undo()
        await agen.aclose()
        await client.aclose()
    assert recorded == [0.5, 0.5]


async def test_sdk_watch_drops_oversized_final_frame_at_eof_without_blank() -> None:
    # A final frame that is over cap AND lacks the trailing blank line must
    # not be delivered: the accumulation check (pending lines + buffer after
    # each chunk) fires before the EOF flush can fold the tail, so the
    # stream ends un-trusted and reconnects. Deleting the accumulation cap
    # check would deliver this frame; that is covered by the cap test above
    # - this pins the no-blank-line edge specifically.
    big = json.dumps(_sdk_new_payload("14", "x" * ((1 << 20) + 8192)))
    body = ("id: 9" + _NL + "event: message.new" + _NL + "data: " + big).encode()
    mock = Mock()
    mock.sse_body = body
    client = make_client(mock)
    agen = client.watch()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(agen), timeout=2)
    await agen.aclose()
    await client.aclose()
    assert mock.sse_connections >= 2, "over-cap EOF frame must not be delivered"


# ---- health / accounts / register ---------------------------------------


async def test_health_reports_version_and_account_phase() -> None:
    mock = Mock()
    mock.health_body = {"account": "ready", "status": "ok", "version": "9.9.9"}
    client = make_client(mock)
    health = await client.health()
    assert health.version == "9.9.9"
    assert health.status == "ok"
    assert health.account is gen.AccountPhase.READY
    assert mock.health_auth == [False], "/health is unauthenticated: no credentials"


async def test_accounts_expose_state_error_and_message_count() -> None:
    mock = Mock()
    mock.accounts_page = {
        "success": True,
        "accounts": [
            {"qq": "10001", "db_path": "X:/a", "message_count": 12, "state": "ready"},
            {"qq": "10002", "db_path": "X:/b", "message_count": 0,
             "state": "error", "error": "bad key"},
        ],
    }
    client = make_client(mock)
    accounts = await client.accounts()
    assert accounts[0].message_count == 12
    assert accounts[1].error == "bad key", "the failure reason exists only on this face"


async def test_register_returns_raw_state_without_polling() -> None:
    mock = Mock()
    mock.register_body = {
        "success": False, "state": "account_conflict",
        "occupied_by": "10009", "occupied_status": "ready",
    }
    client = make_client(mock)
    outcome = await client.register({"qq": "10001", "key": "k"})
    assert outcome.state == "account_conflict"
    assert outcome.status is None, "this state carries no status"
    assert outcome.body["occupied_by"] == "10009", "refusal extras stay reachable"
    assert mock.post_calls == 1, "one POST, no retry"
    assert mock.get_accounts_calls == 0, "register must not poll: waiting is wait_ready's job"


# ---- list_messages / contacts / media_bytes_by_id ------------------------


async def test_list_messages_pages_by_offset_and_exposes_native_fields() -> None:
    mock = Mock()
    mock.native_page = {
        "success": True, "count": 1, "hasMore": True, "talker": "10001",
        "media": {"count": 0, "enabled": False, "exportPath": "X:/export"},
        "messages": [{
            "content": "[image]", "createTime": 26_481, "isSend": 1, "localId": 7,
            "localType": 3,
            "media": {"fileName": "aabb.png", "md5": "aabbcc", "size": 1234,
                      "uuid": "R020-test", "width": 640, "height": 480},
            "mediaFileName": "aabb.png", "mediaId": "aabbcc",
            "mediaLocalPath": "X:/export/aabb.png", "mediaUrl": "/api/v1/media/aabb.png",
            "parsedContent": "[image]", "rawContent": "[image]",
            "replyToMessageId": "41", "senderName": "李四", "senderUsername": "u_b",
            "serverId": "113737825910786", "type": "image",
        }],
    }
    client = make_client(mock)
    page = await client.list_messages("10001", limit=500, offset=1000, media=True)
    assert page.has_more is True, "paging continues until has_more is false"
    assert page.media.export_path == "X:/export"
    message = page.messages[0]
    assert message.raw_content == "[image]", "the ChatLab shape drops rawContent"
    assert message.is_send == 1
    assert message.local_type == 3
    assert message.media_id == "aabbcc", "the fetchable handle"
    assert mock.messages_query == {
        "talker": "10001", "limit": "500", "offset": "1000", "media": "1",
    }


async def test_list_messages_accepts_unix_seconds_and_rejects_garbage() -> None:
    mock = Mock()
    client = make_client(mock)
    await client.list_messages("10001", start="1700000000")
    assert mock.messages_query["start"] == "1700000000", "the server parses unix seconds too"
    with pytest.raises(sdkmod.BadDate):
        await client.list_messages("10001", end="2025-01-01")


async def test_contacts_page_decodes_rows_and_paging_fields() -> None:
    mock = Mock()
    mock.contacts_page = {
        "success": True, "count": 1, "total": 42, "hasMore": True,
        "contacts": [{"alias": "", "avatarUrl": "", "displayName": "李四",
                      "nickname": "四儿", "remark": "客户李四", "type": "friend",
                      "username": "u_b"}],
    }
    client = make_client(mock)
    page = await client.contacts(limit=100, offset=0)
    assert page.count == 1
    assert page.total == 42
    assert page.has_more is True
    assert page.contacts[0].display_name == "李四"


async def test_group_members_decodes_roster_page_and_sends_chatroom_param() -> None:
    mock = Mock()
    mock.group_members_page = {
        "success": True, "chatroomId": "123@chatroom", "count": 2,
        "fromCache": False, "updatedAt": 1700000000123,
        "members": [
            {"alias": "", "avatarUrl": "", "displayName": "潜水者",
             "groupNickname": "", "isFriend": False, "isOwner": False,
             "messageCount": 0, "nickname": "", "remark": "", "wxid": "quiet"},
            {"alias": "a", "avatarUrl": "", "displayName": "张三",
             "groupNickname": "张三", "isFriend": True, "isOwner": True,
             "messageCount": 9, "nickname": "三儿", "remark": "客户张三",
             "wxid": "alice"},
        ],
    }
    client = make_client(mock)
    page = await client.group_members("123@chatroom", include_message_counts=True)
    assert page.count == 2
    assert page.updated_at == 1700000000123, (
        "updatedAt is **milliseconds** — a seconds truncation silently halves "
        "freshness precision")
    # messageCount is a conditional key on this face: present only when counts
    # were asked for, absent otherwise (not a placeholder 0).
    assert page.members[0].message_count == 0
    assert page.members[1].message_count == 9
    assert page.members[1].is_owner
    assert len(mock.group_members_queries) == 1
    q = mock.group_members_queries[0]
    assert q["chatroomId"] == "123%40chatroom"
    assert q.get("includeMessageCounts") == "1", "counts asked for"
    await client.aclose()


async def test_group_members_omits_include_message_counts_when_false() -> None:
    mock = Mock()
    mock.group_members_page = {
        "success": True, "chatroomId": "123@chatroom", "count": 0,
        "fromCache": False, "updatedAt": 0, "members": [],
    }
    client = make_client(mock)
    page = await client.group_members("123@chatroom")
    assert page.members == []
    assert len(mock.group_members_queries) == 1
    assert "includeMessageCounts" not in mock.group_members_queries[0]
    await client.aclose()


async def test_sync_now_posts_with_the_bearer_token_and_decodes_counters() -> None:
    """``sync_now`` is a write: one POST, bearer auth, counters decoded."""
    mock = Mock()
    client = make_client(mock)
    result = await client.sync_now()
    assert result.success is True
    assert result.new_messages == 7
    assert result.revoke_messages == 2
    assert mock.sync_calls == 1
    await client.aclose()


async def test_no_read_path_triggers_a_sync() -> None:
    """Readiness probing must never sync on its own.

    A client that synced while merely asking whether the server is up would turn
    every probe into a disk scan of the live database.
    """
    mock = Mock()
    mock.states = ["indexing"] * 100
    client = make_client(mock)
    await client.health()
    with pytest.raises(NotReady):
        await client.ensure_ready("qq_mock",
                                  {"qq": "qq_mock", "db_path": "X:/db"},
                                  timeout=0.6)
    assert mock.sync_calls == 0
    await client.aclose()


async def test_list_all_sessions_pages_and_collapses_cross_page_duplicates() -> None:
    def session(username: str) -> dict:
        return {"displayName": username, "lastTimestamp": 1, "type": 1,
                "unreadCount": 0, "username": username}

    mock = Mock()
    # A live list can shift between pages: "b" shows up twice. The loop stops
    # on an empty page (this face has no has_more).
    mock.sessions_pages = [
        {"success": True, "count": 2, "sessions": [session("a"), session("b")]},
        {"success": True, "count": 2, "sessions": [session("b"), session("c")]},
        {"success": True, "count": 0, "sessions": []},
    ]
    client = make_client(mock)
    # The polling consumer asks for the server's maximum page size: the request
    # count is sessions / page_size, and the default page is two orders of
    # magnitude smaller than the cap.
    all_sessions = await client.list_all_sessions(page_size=10000)
    assert [s.username for s in all_sessions] == ["a", "b", "c"]
    assert len(mock.sessions_queries) == 3, "one request per page, plus the empty terminator"
    for i, q in enumerate(mock.sessions_queries):
        assert q.get("limit") == "10000", f"page {i} must carry the page size: {q}"
    assert mock.sessions_queries[0]["offset"] == "0"
    assert mock.sessions_queries[1]["offset"] == "2", "offset advances by rows returned"


async def test_media_bytes_by_id_fetches_a_single_segment_handle() -> None:
    mock = Mock()
    mock.media_hit_first = False  # no export side door: the handle already resolves
    client = make_client(mock)
    data = await client.media_bytes_by_id("aabbcc")
    assert data == b"png-bytes"
    assert mock.media_calls == ["aabbcc"], "one GET for the handle it was given"

# ---- three behaviours fixed by the fourth serial-review round ----------------


async def test_time_bounds_are_ascii_only() -> None:
    """Full-width digits are not a time bound.

    `str.isdigit()` accepts them, so such input used to slip through and be
    refused by the server with a 400 - while the Rust client rejects it locally.
    The same input yielding two error classes is the divergence being fixed.
    """
    client = make_client(Mock())
    with pytest.raises(sdkmod.BadDate):
        # Written as escapes so the source carries no ambiguous literal.
        await client.chatlab_messages("wxid_a", end="\uff11\uff12\uff13")


async def test_group_members_rejects_an_empty_chatroom() -> None:
    """An empty chatroomId asks the server a different question.

    A 200 with an empty roster would read as "this group has no members", so the
    client refuses locally - the same fail-fast `talker` gets everywhere else.
    """
    client = make_client(Mock())
    with pytest.raises(sdkmod.ShapeError):
        await client.group_members("")


async def test_transport_failures_are_client_errors() -> None:
    """A refused connection must land inside the ClientError tree.

    httpx raises its own family, so `except ClientError` used to catch every
    server refusal while missing every network failure.
    """
    client = Client("http://127.0.0.1:1", TOKEN, timeout=2.0)
    with pytest.raises(sdkmod.TransportError):
        await client.group_members("10001")
    with pytest.raises(sdkmod.ClientError):
        await client.group_members("10001")
