import unittest
from dataclasses import replace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException, Request
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

import main


class CapacityTests(unittest.TestCase):
    def test_credentials_in_query_string_are_rejected(self) -> None:
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/data/items",
            "query_string": b"DynamicToken=secret",
            "headers": [],
        })
        with self.assertRaises(HTTPException) as raised:
            main.validate_query_access(request)
        self.assertEqual(raised.exception.status_code, 400)

    def test_credentials_are_read_from_headers(self) -> None:
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/data/items",
            "query_string": b"",
            "headers": [
                (b"authorization", b"Bearer dynamic-secret"),
                (b"x-sap-session-id", b"sap-session"),
                (b"x-sap-route-id", b"route"),
            ],
        })
        with (
            patch.object(main, "validate_dynamic_token") as validate_token,
            patch.object(main, "validate_sap_session") as validate_session,
        ):
            main.validate_query_access(request)
        validate_token.assert_called_once_with("dynamic-secret")
        validate_session.assert_called_once_with("sap-session", route_id="route")

    def test_metadata_cache_loads_value_once(self) -> None:
        cache = main.MetadataCache(ttl_seconds=60)
        calls = []

        def loader():
            calls.append(1)
            return "table"

        self.assertEqual(cache.get_or_load("key", loader), "table")
        self.assertEqual(cache.get_or_load("key", loader), "table")
        self.assertEqual(len(calls), 1)

    def test_query_control_cancels_driver_connection(self) -> None:
        class Connection:
            def __init__(self) -> None:
                self.cancelled = False

            def cancel(self) -> None:
                self.cancelled = True

        connection = Connection()
        control = main.QueryExecutionControl()
        control.attach(connection)
        control.cancel()
        self.assertTrue(connection.cancelled)

    def test_sap_circuit_breaker_opens_and_recovers(self) -> None:
        breaker = main.CircuitBreaker(failure_threshold=2, recovery_seconds=30)
        breaker.failure()
        breaker.failure()
        with self.assertRaises(HTTPException) as raised:
            breaker.before_call()
        self.assertEqual(raised.exception.status_code, 503)
        breaker.success()
        breaker.before_call()

    def test_sap_session_validation_is_cached(self) -> None:
        response = MagicMock(status_code=200)
        session = MagicMock()
        session.get.return_value = response
        cache = main.TTLValueCache(ttl_seconds=30)
        breaker = main.CircuitBreaker(failure_threshold=2, recovery_seconds=30)
        with (
            patch.object(main, "app_settings", replace(main.app_settings, sap_session_validation_enabled=True)),
            patch.object(main, "sap_session_cache", cache),
            patch.object(main, "sap_circuit_breaker", breaker),
            patch.object(main, "sap_http_session", return_value=session),
        ):
            main.validate_sap_session("session", "route")
            main.validate_sap_session("session", "route")
        session.get.assert_called_once()
        self.assertEqual(session.get.call_args.kwargs["timeout"], (10, 30))

    def test_sap_session_retries_transient_response(self) -> None:
        session = MagicMock()
        session.get.side_effect = [MagicMock(status_code=502), MagicMock(status_code=200)]
        cache = main.TTLValueCache(ttl_seconds=30)
        breaker = main.CircuitBreaker(failure_threshold=10, recovery_seconds=15)
        with (
            patch.object(main, "app_settings", replace(main.app_settings, sap_session_validation_enabled=True)),
            patch.object(main, "sap_session_cache", cache),
            patch.object(main, "sap_circuit_breaker", breaker),
            patch.object(main, "sap_http_session", return_value=session),
            patch.object(main.time_module, "sleep"),
        ):
            main.validate_sap_session("session", "route")
        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(breaker.status()["state"], "closed")

    def test_sap_session_validation_can_be_bypassed(self) -> None:
        disabled = replace(main.app_settings, sap_session_validation_enabled=False)
        with (
            patch.object(main, "app_settings", disabled),
            patch.object(main, "sap_http_session") as session,
        ):
            main.validate_sap_session(None)
        session.assert_not_called()

    def test_query_queue_rejects_when_full(self) -> None:
        controller = main.QueryAdmissionController(concurrency=1, queue_size=0, timeout_seconds=1)
        with controller.acquire():
            with self.assertRaises(HTTPException) as raised:
                with controller.acquire():
                    pass
        self.assertEqual(raised.exception.status_code, 429)

    def test_rate_limiter_rejects_excess(self) -> None:
        limiter = main.FixedWindowRateLimiter(request_limit=2, window_seconds=60)
        limiter.check("client")
        limiter.check("client")
        with self.assertRaises(HTTPException) as raised:
            limiter.check("client")
        self.assertEqual(raised.exception.status_code, 429)

    def test_default_limit_is_applied(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        table = Table(
            "items",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        table.create(engine)
        with engine.begin() as connection:
            connection.execute(table.insert(), [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}])

        request = Request({"type": "http", "method": "GET", "path": "/data/items", "query_string": b"", "headers": []})
        database = main.registry.get_database(main.app_settings.default_database)
        with (
            patch.object(main, "validate_query_access"),
            patch.object(main, "load_table", return_value=table),
            patch.object(main.registry, "get_database", return_value=database),
            patch.object(main.registry, "get_engine", return_value=engine),
            patch.object(main, "log_execution"),
        ):
            result = main.execute_query(main.app_settings.default_database, "items", request, None, None, 0)

        self.assertEqual(result["limit"], main.app_settings.default_limit)
        self.assertEqual(result["count"], 2)


if __name__ == "__main__":
    unittest.main()
