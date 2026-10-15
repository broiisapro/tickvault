from collections.abc import Sequence
from pathlib import Path

import zstandard as zstd

from tickvault.models import RawMessage
from tickvault.rawio import RawReadStats, decode_line, encode_line, iter_raw


def _raw(
    *,
    venue: str = "kraken",
    connection_id: str = "kraken-20261015T031901Z-3fa2c1d0",
    ingest_seq: int = 0,
    recv_ts_ns: int = 1,
    kind: str = "msg",
    payload: str = '{"x":1}',
) -> RawMessage:
    return RawMessage(
        venue=venue,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="msg" if kind == "msg" else "control",
        payload=payload,
    )


def _write_raw(path: Path, messages: Sequence[RawMessage], *, tail: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for message in messages:
            writer.write(encode_line(message))
        if tail:
            writer.write(tail)


def test_encode_line_exact_bytes() -> None:
    message = RawMessage(
        venue="kraken",
        connection_id="kraken-20261015T031901Z-3fa2c1d0",
        ingest_seq=17,
        recv_ts_ns=1792034341558201000,
        kind="msg",
        payload='{"channel":"book"}',
    )
    assert encode_line(message) == (
        b'{"v":1,"venue":"kraken","connection_id":"kraken-20261015T031901Z-3fa2c1d0",'
        b'"ingest_seq":17,"recv_ts_ns":1792034341558201000,"kind":"msg",'
        b'"payload":"{\\"channel\\":\\"book\\"}"}\n'
    )


def test_decode_line_accepts_any_key_order() -> None:
    shuffled = (
        b'{"payload":"{\\"a\\":1}","kind":"msg","recv_ts_ns":42,"ingest_seq":3,'
        b'"connection_id":"c-1","venue":"coinbase","v":1}'
    )
    message = decode_line(shuffled)
    assert message == RawMessage(
        venue="coinbase",
        connection_id="c-1",
        ingest_seq=3,
        recv_ts_ns=42,
        kind="msg",
        payload='{"a":1}',
    )


def test_payload_roundtrip_byte_exact() -> None:
    payload = '{"text":"héllo \\"quoted\\" \\\\backslash\\\\ 你好","nested":{"a":[1,2]}}'
    message = _raw(payload=payload)
    encoded = encode_line(message)
    decoded = decode_line(encoded[:-1])
    assert decoded.payload == payload
    assert encode_line(decoded) == encoded
    assert b"h\xc3\xa9llo" in encoded
    assert b"\\u" not in encoded


def test_iter_raw_canonical_order(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    connection_a = "kraken-20261015T031901Z-0000000a"
    connection_b = "coinbase-20261015T031902Z-0000000b"
    _write_raw(
        root / "venue=kraken" / "date=2026-10-15" / "hour=03" / "a-00001.jsonl.zst",
        [
            _raw(connection_id=connection_a, ingest_seq=0, recv_ts_ns=30),
            _raw(connection_id=connection_a, ingest_seq=1, recv_ts_ns=10),
        ],
    )
    _write_raw(
        root / "venue=coinbase" / "date=2026-10-15" / "hour=03" / "b-00001.jsonl.zst",
        [
            _raw(connection_id=connection_b, ingest_seq=1, recv_ts_ns=20),
            _raw(connection_id=connection_b, ingest_seq=0, recv_ts_ns=10),
        ],
    )
    messages = list(iter_raw(root))
    assert [(m.recv_ts_ns, m.connection_id, m.ingest_seq) for m in messages] == [
        (10, connection_b, 0),
        (10, connection_a, 1),
        (20, connection_b, 1),
        (30, connection_a, 0),
    ]


def test_iter_raw_single_file(tmp_path: Path) -> None:
    path = tmp_path / "part.jsonl.zst"
    messages = [_raw(ingest_seq=0, recv_ts_ns=100), _raw(ingest_seq=1, recv_ts_ns=200)]
    _write_raw(path, messages)
    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == messages
    assert stats.files == 1
    assert stats.messages == 2
    assert stats.truncated_lines == 0
    assert stats.truncated_files == 0
    assert stats.bad_lines == 0


def test_truncated_tail_is_skipped_and_counted(tmp_path: Path) -> None:
    path = tmp_path / "part.jsonl.zst"
    messages = [_raw(ingest_seq=0, recv_ts_ns=100), _raw(ingest_seq=1, recv_ts_ns=200)]
    _write_raw(path, messages, tail=b'{"v":1,"venue":"kraken","connection')
    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == messages
    assert stats.messages == 2
    assert stats.truncated_lines == 1
    assert stats.truncated_files == 0
    assert stats.bad_lines == 0


def test_truncated_frame_is_tolerated_without_raising(tmp_path: Path) -> None:
    # Cutting a zstd stream mid-frame leaves a decodable prefix at best; the
    # reader must yield only complete, valid messages and never raise.
    path = tmp_path / "part.jsonl.zst"
    messages = [
        _raw(ingest_seq=index, recv_ts_ns=100 + index, payload=f'{{"n":{index}}}')
        for index in range(40)
    ]
    _write_raw(path, messages)
    compressed = path.read_bytes()
    path.write_bytes(compressed[: len(compressed) // 2])
    stats = RawReadStats()
    got = list(iter_raw(path, stats=stats))
    assert got == messages[: len(got)]
    assert stats.messages == len(got)
    assert stats.bad_lines == 0


def test_bad_complete_line_is_counted(tmp_path: Path) -> None:
    path = tmp_path / "part.jsonl.zst"
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        writer.write(b"not json\n")
        writer.write(encode_line(_raw(ingest_seq=0, recv_ts_ns=100)))
    stats = RawReadStats()
    assert [m.ingest_seq for m in iter_raw(path, stats=stats)] == [0]
    assert stats.bad_lines == 1
