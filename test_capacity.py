import unittest
from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

from fastapi import HTTPException, Request
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, select

import main


class CapacityTests(unittest.TestCase):
    def test_empty_date_values_are_ignored(self) -> None:
        request = Request({
            "type": "http", "method": "GET", "path": "/data/flow",
            "query_string": b"CampoData=invalid&DataInicio=&DataFim=%20%20", "headers": [],
        })
        original = select(1)
        result = main.apply_date_range_filters(original, {}, request, {})
        self.assertIs(result, original)

    def test_empty_start_date_keeps_valid_end_filter(self) -> None:
        column = Column("Data Lançamento", DateTime)
        request = Request({
            "type": "http", "method": "GET", "path": "/data/flow",
            "query_string": b"DataInicio=&DataFim=2025-06-03", "headers": [],
        })
        filters = {}
        main.apply_date_range_filters(select(1), {column.name.upper(): column}, request, filters)
        self.assertNotIn("DataInicio", filters)
        self.assertEqual(filters["DataFim"], ["2025-06-03"])
        self.assertEqual(filters["CampoData"], ["DataCriacao"])

    def test_date_range_filters_selected_column_and_includes_end_day(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        table = Table(
            "flow",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("Data Pagamento", DateTime),
            Column("Data Vencimento", DateTime),
        )
        table.create(engine)
        with engine.begin() as connection:
            connection.execute(table.insert(), [
                {"id": 1, "Data Pagamento": datetime(2025, 6, 2, 23, 59), "Data Vencimento": datetime(2025, 1, 1)},
                {"id": 2, "Data Pagamento": datetime(2025, 6, 3, 18, 30), "Data Vencimento": datetime(2025, 1, 1)},
                {"id": 3, "Data Pagamento": datetime(2025, 6, 4, 0, 0), "Data Vencimento": datetime(2025, 1, 1)},
            ])
        query = urlencode({
            "CampoData": "DataPagamento",
            "DataInicio": "2025-06-02",
            "DataFim": "2025-06-03",
        }).encode()
        request = Request({"type": "http", "method": "GET", "path": "/data/flow", "query_string": query, "headers": []})
        filters = {}
        statement = main.apply_date_range_filters(
            select(table),
            {column.name.upper(): column for column in table.columns},
            request,
            filters,
        )
        with engine.connect() as connection:
            ids = [row.id for row in connection.execute(statement)]
        self.assertEqual(ids, [1, 2])
        self.assertEqual(filters["CampoData"], ["DataPagamento"])

    def test_date_range_defaults_to_creation_date_alias(self) -> None:
        column = Column("Data Lançamento", DateTime)
        request = Request({
            "type": "http", "method": "GET", "path": "/data/flow",
            "query_string": b"DataInicio=2025-06-01", "headers": [],
        })
        filters = {}
        statement = main.apply_date_range_filters(
            select(1), {column.name.upper(): column}, request, filters,
        )
        self.assertIsNotNone(statement)
        self.assertEqual(filters["CampoData"], ["DataCriacao"])

    def test_posting_date_selector_falls_back_to_creation_date(self) -> None:
        column = Column("Data de criação", DateTime)
        query = urlencode({"CampoData": "DataLançamento", "DataInicio": "2025-06-01"}).encode()
        request = Request({
            "type": "http", "method": "GET", "path": "/data/flow", "query_string": query, "headers": [],
        })
        filters = {}
        statement = main.apply_date_range_filters(
            select(1), {column.name.upper(): column}, request, filters,
        )
        self.assertIsNotNone(statement)
        self.assertEqual(filters["CampoData"], ["DataLançamento"])

    def test_date_range_rejects_invalid_date_order(self) -> None:
        column = Column("Data Pagamento", DateTime)
        query = urlencode({
            "CampoData": "DataPagamento", "DataInicio": "2025-06-04", "DataFim": "2025-06-03",
        }).encode()
        request = Request({"type": "http", "method": "GET", "path": "/data/flow", "query_string": query, "headers": []})
        with self.assertRaises(HTTPException) as raised:
            main.apply_date_range_filters(select(1), {column.name.upper(): column}, request, {})
        self.assertEqual(raised.exception.status_code, 400)

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
            patch.object(main, "app_settings", replace(main.app_settings, external_approvals_api_key="configured")),
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
            patch.object(main, "app_settings", replace(main.app_settings, external_approvals_api_key="configured")),
            patch.object(main, "validate_query_access"),
            patch.object(main, "load_table", return_value=table),
            patch.object(main.registry, "get_database", return_value=database),
            patch.object(main.registry, "get_engine", return_value=engine),
            patch.object(main, "log_execution"),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=[{"doc_num": 411420}]),
        ):
            result = main.execute_query(main.app_settings.default_database, "VW_FIN_ANALISE_FLUXO", request, None, None, 0)

        self.assertEqual(result["count"], 0)

    def test_erp_flow_is_skipped_without_api_key(self) -> None:
        request = Request({
            "type": "http", "method": "GET", "path": "/data/VW_FIN_ANALISE_FLUXO",
            "query_string": b"", "headers": [],
        })
        table = Table("VW_FIN_ANALISE_FLUXO", MetaData(), Column("Status", String))
        settings = replace(main.app_settings, external_approvals_api_key="")
        with (
            patch.object(main, "app_settings", settings),
            patch.object(main, "fetch_erp_flow_pending_documents") as fetch,
        ):
            added = main.append_erp_flow_pending_documents(
                [], table, request, "SBO_ANAGAMING", "VW_FIN_ANALISE_FLUXO", {"STATUS": table.c.Status},
            )
        self.assertEqual(added, 0)
        fetch.assert_not_called()

    def test_erp_flow_company_scope_is_used_without_user_code(self) -> None:
        request = Request({
            "type": "http", "method": "GET", "path": "/data/VW_FIN_ANALISE_FLUXO",
            "query_string": b"", "headers": [],
        })
        table = Table(
            "VW_FIN_ANALISE_FLUXO", MetaData(), Column("Status", String), Column("Aprovador", String),
        )
        settings = replace(main.app_settings, external_approvals_api_key="configured")
        document = {
            "approval_request_id": 10,
            "pending_approvers": [{"user_id": 112, "user_code": "joao.silva", "step": 1}],
        }
        data = []
        with (
            patch.object(main, "app_settings", settings),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=[document]) as fetch,
        ):
            added = main.append_erp_flow_pending_documents(
                data, table, request, "SBO_ANAGAMING", "VW_FIN_ANALISE_FLUXO",
                {column.name.upper(): column for column in table.columns},
            )
        fetch.assert_called_once_with("SBO_ANAGAMING", None)
        self.assertEqual(added, 1)
        self.assertEqual(data[0]["Aprovador"], "joao.silva")
        self.assertEqual(data[0]["ERPFlowPendingApprovers"][0]["step"], 1)

    def test_erp_flow_documents_respect_date_range(self) -> None:
        table = Table(
            "VW_FIN_ANALISE_FLUXO", MetaData(),
            Column("Status", String), Column("Data Lançamento", DateTime),
        )
        query = urlencode({
            "user_code": "joao.silva", "DataInicio": "2026-05-01", "DataFim": "2026-05-31",
        }).encode()
        request = Request({
            "type": "http", "method": "GET", "path": "/data/VW_FIN_ANALISE_FLUXO",
            "query_string": query, "headers": [],
        })
        documents = [
            {"approval_request_id": 1, "creation_date": "2026-05-22T14:33:00Z"},
            {"approval_request_id": 2, "creation_date": "2026-06-01T00:00:00Z"},
        ]
        data = []
        column_map = {column.name.upper(): column for column in table.columns}
        with (
            patch.object(main, "app_settings", replace(main.app_settings, external_approvals_api_key="configured")),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=documents),
        ):
            added = main.append_erp_flow_pending_documents(
                data, table, request, "SBO_ANAGAMING", "VW_FIN_ANALISE_FLUXO", column_map,
            )
        self.assertEqual(added, 1)
        self.assertEqual(data[0]["ERPFlowApprovalRequestId"], 1)

    def test_erp_flow_due_date_is_used_by_data_vencimento_filter(self) -> None:
        table = Table(
            "VW_FIN_ANALISE_FLUXO", MetaData(),
            Column("Status", String), Column("Data Vencimento", DateTime),
        )
        query = urlencode({
            "DataInicio": "2026-06-01", "DataFim": "2026-07-30",
            "CampoData": "DataVencimento",
        }).encode()
        request = Request({
            "type": "http", "method": "GET", "path": "/data/VW_FIN_ANALISE_FLUXO",
            "query_string": query, "headers": [],
        })
        document = {
            "approval_request_id": 5,
            "due_date": "2026-07-14T00:00:00Z",
        }
        data = []
        column_map = {column.name.upper(): column for column in table.columns}
        with (
            patch.object(main, "app_settings", replace(main.app_settings, external_approvals_api_key="configured")),
            patch.object(main, "fetch_erp_flow_pending_documents", return_value=[document]),
        ):
            added = main.append_erp_flow_pending_documents(
                data, table, request, "SBO_OPENGAMING", "VW_FIN_ANALISE_FLUXO", column_map,
            )
        self.assertEqual(added, 1)
        self.assertEqual(data[0]["Data Vencimento"], "2026-07-14T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
