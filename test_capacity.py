import unittest
from unittest.mock import patch

from fastapi import HTTPException, Request
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

import main


class CapacityTests(unittest.TestCase):
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
