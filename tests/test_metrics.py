"""PipelineMetrics tests: every method, isolated registries, HTTP endpoint (issue #113)."""

from __future__ import annotations

import http.client

from prometheus_client import CollectorRegistry

from tickvault.metrics import PipelineMetrics


def test_every_method_updates_the_expected_metric() -> None:
    metrics = PipelineMetrics()

    metrics.observe_raw("coinbase", 128, now_s=100.0)
    metrics.observe_raw("coinbase", 64, now_s=101.0)
    metrics.observe_raw("kraken", 32, now_s=99.0)
    metrics.observe_rows("trades", "coinbase", 5)
    metrics.observe_rows("events", "kraken", 0)
    metrics.observe_event("coinbase", "gap")
    metrics.observe_normalize_error("kraken")
    metrics.set_book_live("coinbase", "BTC-USD", True)
    metrics.set_book_live("kraken", "BTC-USD", False)
    metrics.observe_consumed()
    metrics.observe_consumed(3)
    metrics.set_buffer_lag(7)
    metrics.tick(now_s=102.0)

    text = metrics.render().decode("utf-8")
    assert 'tickvault_messages_total{venue="coinbase"} 2.0' in text
    assert 'tickvault_messages_total{venue="kraken"} 1.0' in text
    assert 'tickvault_rows_total{table="trades",venue="coinbase"} 5.0' in text
    assert 'tickvault_events_total{event_type="gap",venue="coinbase"} 1.0' in text
    assert 'tickvault_normalize_errors_total{venue="kraken"} 1.0' in text
    assert 'tickvault_book_live{symbol="BTC-USD",venue="coinbase"} 1.0' in text
    assert 'tickvault_book_live{symbol="BTC-USD",venue="kraken"} 0.0' in text
    assert "tickvault_raw_bytes_total 224.0" in text
    assert "tickvault_buffer_consumed_total 4.0" in text
    assert "tickvault_buffer_lag_messages 7.0" in text
    assert 'tickvault_last_message_age_seconds{venue="coinbase"} 1.0' in text
    assert 'tickvault_last_message_age_seconds{venue="kraken"} 3.0' in text


def test_every_metric_name_is_prefixed() -> None:
    metrics = PipelineMetrics()

    families = {
        line.split("{")[0].split(" ")[0]
        for line in metrics.render().decode("utf-8").splitlines()
        if line and not line.startswith("#")
    }

    assert families
    assert all(name.startswith("tickvault_") for name in families)


def test_two_instances_in_one_process_do_not_collide() -> None:
    first = PipelineMetrics()
    second = PipelineMetrics()

    first.observe_raw("coinbase", 1, now_s=10.0)
    second.observe_raw("kraken", 2, now_s=20.0)

    assert first.registry is not second.registry
    assert 'tickvault_messages_total{venue="coinbase"} 1.0' in first.render().decode("utf-8")
    assert "kraken" not in first.render().decode("utf-8")
    assert 'tickvault_messages_total{venue="kraken"} 1.0' in second.render().decode("utf-8")


def test_an_injected_registry_is_used_as_is() -> None:
    registry = CollectorRegistry()

    metrics = PipelineMetrics(registry)

    assert metrics.registry is registry


def test_tick_without_messages_renders_no_age_series() -> None:
    metrics = PipelineMetrics()

    metrics.tick(now_s=5.0)

    assert "tickvault_last_message_age_seconds{" not in metrics.render().decode("utf-8")


def test_zero_row_counts_still_expose_the_table() -> None:
    metrics = PipelineMetrics()

    metrics.observe_rows("events", "coinbase", 0)

    assert 'tickvault_rows_total{table="events",venue="coinbase"} 0.0' in (
        metrics.render().decode("utf-8")
    )


def test_serve_answers_an_http_get_with_the_text_format() -> None:
    metrics = PipelineMetrics()
    metrics.observe_raw("coinbase", 10, now_s=1.0)
    server = metrics.serve(0)
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
        try:
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            body = response.read()
        finally:
            connection.close()
    finally:
        server.close()

    assert server.port > 0
    assert response.status == 200
    assert b'tickvault_messages_total{venue="coinbase"} 1.0' in body
