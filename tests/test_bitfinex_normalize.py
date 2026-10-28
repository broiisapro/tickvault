"""Bitfinex WS v2 normalizer unit tests.

The frame shapes come from ``waves/wave-08/bitfinex-facts.md`` (verified
against the live venue and the committed five-minute fixture); the state tests
cover the channel-id lookup that Bitfinex needs because array frames carry no
symbol.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import NormalizeError, bitfinex
from tickvault.normalize.bitfinex import normalize
from tickvault.rawio import iter_raw

_CONNECTION = "bitfinex-20261028T033934Z-8dad90eb"
_RECV_TS = 1_793_158_778_146_393_000
_BOOK_CHAN = 396954
_TRADES_CHAN = 258792


@pytest.fixture(autouse=True)
def _clean_connection_state() -> None:
    """Keep the module-level channel maps of one test out of the next."""
    bitfinex._CONNECTIONS.clear()
    yield
    bitfinex._CONNECTIONS.clear()


def _raw(
    payload: object,
    *,
    kind: str = "msg",
    ingest_seq: int = 0,
    connection_id: str = _CONNECTION,
) -> RawMessage:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return RawMessage(
        venue="bitfinex",
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=_RECV_TS,
        kind="msg" if kind == "msg" else "control",
        payload=text,
    )


def _connect(
    symbols: tuple[str, ...] = ("BTC-USD",), connection_id: str = _CONNECTION
) -> NormalizedMessage:
    payload = {
        "event": "connect",
        "url": "wss://api-pub.bitfinex.com/ws/2",
        "symbols": list(symbols),
        "subscriptions": [{"event": "conf", "flags": 229376}],
    }
    return normalize(_raw(payload, kind="control", connection_id=connection_id))


def _subscribe(
    channel: str = "book",
    chan_id: int = _BOOK_CHAN,
    venue_symbol: str = "tBTCUSD",
    connection_id: str = _CONNECTION,
) -> NormalizedMessage:
    payload = {
        "event": "subscribed",
        "channel": channel,
        "chanId": chan_id,
        "symbol": venue_symbol,
        "pair": "BTCUSD",
    }
    return normalize(_raw(payload, connection_id=connection_id))


def _ready() -> None:
    """Connect and acknowledge both channels the way the connector does."""
    _connect()
    _subscribe("trades", _TRADES_CHAN)
    _subscribe("book", _BOOK_CHAN)


def test_connect_and_disconnect_controls_pass_the_payload_through() -> None:
    connected = _connect()
    assert connected.control is not None
    assert connected.control["event"] == "connect"
    assert connected.control["symbols"] == ["BTC-USD"]
    assert connected.sequence is None

    disconnected = normalize(
        _raw({"event": "disconnect", "reason": "duration_elapsed"}, kind="control")
    )
    assert disconnected.control == {"event": "disconnect", "reason": "duration_elapsed"}


def test_info_and_conf_frames_carry_no_records_and_no_sequence() -> None:
    info = normalize(
        _raw(
            '{"event":"info","version":2,"serverId":"b176bd51-1ada-4465-b4e1-4dd441fc0b4d",'
            '"platform":{"status":1}}'
        )
    )
    assert (info.channel, info.sequence, info.trades, info.book_updates) == (None, None, (), ())

    conf = normalize(_raw('{"event":"conf","status":"OK","flags":229376}'))
    assert (conf.channel, conf.sequence, conf.book_updates) == (None, None, ())


def test_subscribe_ack_registers_the_channel() -> None:
    _connect()
    ack = _subscribe("book", _BOOK_CHAN)
    assert (ack.channel, ack.sequence, ack.book_updates) == ("book", None, ())
    assert ack.raw.kind == "msg"

    # Without the ack the channel id is unknown; the ack is what makes the
    # following data frames resolvable.
    assert bitfinex._CONNECTIONS[_CONNECTION].channels[_BOOK_CHAN].channel == "book"


def test_book_snapshot_splits_signed_levels_into_bids_and_asks() -> None:
    _ready()
    snapshot = normalize(
        _raw(
            '[396954,[[81164,1,0.00022],[81163,1,0.0002822],'
            "[81171,2,-0.00602037],[81172,1,-0.06159783]],2,1793158779209]"
        )
    )
    assert snapshot.channel == "book"
    assert snapshot.sequence == 2
    assert len(snapshot.book_updates) == 1
    update = snapshot.book_updates[0]
    assert update.is_snapshot is True
    assert update.symbol == "BTC-USD"
    assert update.bids == (
        (Decimal("81164"), Decimal("0.00022")),
        (Decimal("81163"), Decimal("0.0002822")),
    )
    assert update.asks == (
        (Decimal("81171"), Decimal("0.00602037")),
        (Decimal("81172"), Decimal("0.06159783")),
    )
    assert update.checksum is None
    assert update.exchange_ts_ns == 1_793_158_779_209_000_000


def test_book_update_carries_one_level_with_its_side_and_exact_digits() -> None:
    _ready()
    bid = normalize(_raw("[396954,[81138,2,0.0005022],4,1793158779620]")).book_updates[0]
    assert (bid.is_snapshot, bid.bids, bid.asks) == (
        False,
        ((Decimal("81138"), Decimal("0.0005022")),),
        (),
    )
    assert bid.exchange_ts_ns == 1_793_158_779_620_000_000

    ask = normalize(_raw("[396954,[81137,1,-0.06162394],5,1793158779817]")).book_updates[0]
    assert (ask.bids, ask.asks) == ((), ((Decimal("81137"), Decimal("0.06162394")),))


def test_count_zero_removes_the_level_with_size_zero() -> None:
    _ready()
    bid_removal = normalize(_raw("[396954,[81154,0,1],3,1793158779620]")).book_updates[0]
    assert bid_removal.bids == ((Decimal("81154"), Decimal("0")),)
    assert bid_removal.asks == ()

    ask_removal = normalize(_raw("[396954,[81221,0,-1],960,1793158781868]")).book_updates[0]
    assert ask_removal.bids == ()
    assert ask_removal.asks == ((Decimal("81221"), Decimal("0")),)


def test_trailing_zeros_survive_the_json_parse() -> None:
    _ready()
    # Raw JSON text: a Python float literal would already have dropped the zero.
    update = normalize(_raw("[396954,[81154,1,0.0002200],3,1793158779620]")).book_updates[0]
    _, size = update.bids[0]
    assert str(size) == "0.0002200"
    assert format(size, "f") == "0.0002200"


def test_checksum_frame_is_an_empty_update_carrying_the_checksum() -> None:
    _ready()
    message = normalize(_raw('[396954,"cs",-717955449,61,1793158781868]'))
    assert message.channel == "book"
    assert message.sequence == 61
    assert len(message.book_updates) == 1
    update = message.book_updates[0]
    assert (update.symbol, update.is_snapshot, update.bids, update.asks) == (
        "BTC-USD",
        False,
        (),
        (),
    )
    assert update.checksum == -717955449
    assert update.exchange_ts_ns == 1_793_158_781_868_000_000


def test_heartbeat_consumes_its_sequence_number() -> None:
    _ready()
    heartbeat = normalize(_raw("[396954,\"hb\",299,1793158793000]"))
    assert (heartbeat.channel, heartbeat.sequence, heartbeat.book_updates) == ("book", 299, ())


@pytest.mark.parametrize(
    ("amount", "side"), [("0.00016301", "buy"), ("-0.00016301", "sell")]
)
def test_executed_trade_uses_its_own_timestamp_and_the_taker_side(
    amount: str, side: str
) -> None:
    _ready()
    message = normalize(
        _raw(f'[258792,"te",[2000903773,1793158780785,{amount},81170],26,1793158780786]')
    )
    assert message.channel == "trades"
    assert message.sequence == 26
    assert len(message.trades) == 1
    trade = message.trades[0]
    assert (trade.venue, trade.symbol, trade.side) == ("bitfinex", "BTC-USD", side)
    assert trade.trade_id == "2000903773"
    assert trade.price == Decimal("81170")
    assert trade.size == Decimal("0.00016301")
    # The trade row's own millisecond timestamp, not the message-level one.
    assert trade.exchange_ts_ns == 1_793_158_780_785_000_000
    assert (trade.recv_ts_ns, trade.connection_id, trade.ingest_seq) == (_RECV_TS, _CONNECTION, 0)
    assert trade.sequence == 26


def test_updated_trade_carries_its_sequence_but_no_records() -> None:
    _ready()
    message = normalize(
        _raw('[258792,"tu",[2000903773,1793158780785,0.00016301,81170],32,1793158780846]')
    )
    assert (message.channel, message.sequence, message.trades) == ("trades", 32, ())


def test_trades_snapshot_carries_its_sequence_but_no_records() -> None:
    _ready()
    message = normalize(
        _raw(
            '[258792,[[2000903768,1793158765067,0.00016296,81173],'
            "[2000903764,1793158760278,-0.00091869,81173]],1,1793158778228]"
        )
    )
    assert (message.channel, message.sequence, message.trades) == ("trades", 1, ())


def test_unsubscribe_ack_has_no_records_and_keeps_in_flight_frames_resolvable() -> None:
    _ready()
    ack = normalize(_raw({"event": "unsubscribed", "status": "OK", "chanId": _BOOK_CHAN}))
    assert (ack.channel, ack.sequence, ack.book_updates) == (None, None, ())

    # Frames already in flight kept arriving for about 100 ms after the ack,
    # and a resubscribe reuses the same channel id, so the map stays.
    late = normalize(_raw("[396954,[81154,0,1],700,1793158779620]"))
    assert late.book_updates[0].bids == ((Decimal("81154"), Decimal("0")),)


def test_acknowledged_unknown_channel_type_yields_no_records_but_its_sequence() -> None:
    _connect()
    ack = _subscribe("ticker", 123456, "tBTCUSD")
    assert (ack.channel, ack.sequence) == ("ticker", None)
    message = normalize(_raw('[123456,[81164,1,1,0.01,0.02],8,1793158779620]'))
    assert (message.channel, message.sequence, message.book_updates, message.trades) == (
        "ticker",
        8,
        (),
        (),
    )


def test_data_frame_before_any_subscribe_ack_raises() -> None:
    _connect()
    with pytest.raises(NormalizeError, match="no 'subscribed' ack"):
        normalize(_raw("[396954,[81154,0,1],3,1793158779620]"))


def test_data_frame_without_a_connect_line_raises() -> None:
    with pytest.raises(NormalizeError, match="no connect line"):
        normalize(_raw("[396954,[81154,0,1],3,1793158779620]"))


def test_disconnect_drops_the_connection_state() -> None:
    _ready()
    normalize(_raw({"event": "disconnect", "reason": "reconnect"}, kind="control"))
    with pytest.raises(NormalizeError, match="no connect line"):
        normalize(_raw("[396954,[81154,0,1],3,1793158779620]"))


def test_a_new_connect_line_replaces_the_channel_state() -> None:
    _ready()
    _connect()
    with pytest.raises(NormalizeError, match="no 'subscribed' ack"):
        normalize(_raw("[396954,[81154,0,1],3,1793158779620]"))


def test_long_symbols_resolve_from_the_colon_form_without_a_declared_symbol() -> None:
    # Older recordings' connect payloads may predate the declared-symbols rule;
    # the unambiguous colon form still maps to BASE-QUOTE.
    normalize(_raw({"event": "connect", "subscriptions": []}, kind="control"))
    _subscribe("book", 457192, "tUST:USD")
    update = normalize(_raw("[457192,[1.0001,1,100],7,1793158779620]")).book_updates[0]
    assert update.symbol == "UST-USD"


def test_declared_symbols_win_over_the_venue_spelling() -> None:
    _connect(symbols=("ETH-USD",))
    _subscribe("book", _BOOK_CHAN, "tETHUSD")
    update = normalize(_raw("[396954,[2000,1,1],3,1793158779620]")).book_updates[0]
    assert update.symbol == "ETH-USD"


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ("not json", "not valid JSON"),
        ('"a string"', "JSON object or array"),
        ("[]", "JSON object or array"),
        ('[396954,"nope",3,1793158779620]', "not hb/cs/te/tu"),
        ("[396954,[81154,1],3,1793158779620]", r"\[price, count, amount\]"),
        ("[396954,[81154,0,0],3,1793158779620]", "amount must not be zero"),
        ("[396954,[81154,1,1]]", "no sequence number"),
        ('[396954,"cs",1,2]', "checksum timestamp"),
        ('[258792,"te",[1,2,0.5],3,1793158779620]', "te entry"),
    ],
)
def test_malformed_frames_raise(payload: str, match: str) -> None:
    _ready()
    with pytest.raises(NormalizeError, match=match):
        normalize(_raw(payload))


def test_checksum_frame_on_a_trades_channel_raises() -> None:
    _ready()
    with pytest.raises(NormalizeError, match="checksum frame on a 'trades' channel"):
        normalize(_raw('[258792,"cs",1,2,1793158779620]'))


def test_shapes_that_are_not_numbers_raise() -> None:
    _ready()
    with pytest.raises(NormalizeError, match="book count must be an integer"):
        normalize(_raw('[396954,[81154,"1",0.5],3,1793158779620]'))
    with pytest.raises(NormalizeError, match="book price must be a JSON number"):
        normalize(_raw('[396954,[true,1,0.5],3,1793158779620]'))


def test_two_replays_in_one_process_are_identical(fixtures_dir: Path) -> None:
    """State is rebuilt from the raw lines, so a second replay repeats the first."""
    path = fixtures_dir / "bitfinex_btc_5m.jsonl.zst"
    first = [normalize(raw) for raw in iter_raw(path)]
    second = [normalize(raw) for raw in iter_raw(path)]

    assert first == second
    assert len(first) == 8241
    assert [message.sequence for message in first if message.sequence is not None] == list(
        range(1, 8236)
    )
    assert first[0].control is not None
    assert first[0].control["event"] == "connect"
    assert first[-1].control == {"event": "disconnect", "reason": "duration_elapsed"}
