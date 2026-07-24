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

    def test_erp_flow_documents_are_appended_for_analise_fluxo(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        table = Table(
            "VW_FIN_ANALISE_FLUXO",
            MetaData(),
            Column("DocNum", Integer),
            Column("DocTotal", Integer),
            Column("CardName", String),
            Column("Origem", String),
            Column("Status", String),
        )
        table.create(engine)

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/data/VW_FIN_ANALISE_FLUXO",
                "query_string": b"user_code=joao.silva&company_db=SBO_ANAGAMING",
                "headers": [],
            }
        )
        database = main.registry.get_database(main.app_settings.default_database)
        erp_flow_document = {
            "approval_request_id": 1287,
            "step": 1,
            "doc_object_type": "22",
            "doc_type_name": "Pedido de Compra",
            "doc_entry": 9912,
            "doc_num": 411420,
            "doc_total": 12500,
            "currency": "BRL",
            "card_code": "PJ000123",
            "card_name": "Fornecedor ACME LTDA",
            "remarks": "Compra urgente de insumos",
            "creation_date": "2026-05-22T14:33:00Z",
            "approver_user_code": "joao.silva",
        }

        with (
            patch.object(main, "validate_query_access"),
            patch.object(main, "load_table", return_value=table),
            patch.object(main.registry, "get_database", return_value=database),
            patch.object(main.registry, "get_engine", return_value=engine),
            patch.object(main, "log_execution"),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=[erp_flow_document]) as fetch_erp_flow,
        ):
            result = main.execute_query(main.app_settings.default_database, "VW_FIN_ANALISE_FLUXO", request, None, None, 0)

        fetch_erp_flow.assert_called_once_with("SBO_ANAGAMING", "joao.silva")
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["data"][0]["Status"], main.ERP_FLOW_PENDING_STATUS)
        self.assertEqual(result["data"][0]["DocNum"], 411420)
        self.assertEqual(result["data"][0]["DocTotal"], 12500)
        self.assertEqual(result["data"][0]["CardName"], "Fornecedor ACME LTDA")
        self.assertEqual(result["data"][0]["Origem"], "ERP Flow")
        self.assertEqual(result["data"][0]["ERPFlowApprovalRequestId"], 1287)

    def test_erp_flow_documents_respect_query_filters(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        table = Table(
            "VW_FIN_ANALISE_FLUXO",
            MetaData(),
            Column("DocNum", Integer),
            Column("Status", String),
        )
        table.create(engine)

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/data/VW_FIN_ANALISE_FLUXO",
                "query_string": b"user_code=joao.silva&company_db=SBO_ANAGAMING&Status=Aberto",
                "headers": [],
            }
        )
        database = main.registry.get_database(main.app_settings.default_database)

        with (
            patch.object(main, "validate_query_access"),
            patch.object(main, "load_table", return_value=table),
            patch.object(main.registry, "get_database", return_value=database),
            patch.object(main.registry, "get_engine", return_value=engine),
            patch.object(main, "log_execution"),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=[{"doc_num": 411420}]),
        ):
            result = main.execute_query(main.app_settings.default_database, "VW_FIN_ANALISE_FLUXO", request, None, None, 0)

        self.assertEqual(result["count"], 0)


if __name__ == "__main__":
    unittest.main()
