import json
import hashlib
import hmac
import logging
import os
import re
import sqlite3
import threading
import time as time_module
import unicodedata
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

import uvicorn
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.exc import NoSuchModuleError, NoSuchTableError, SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hana-db-api")

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
EXECUTION_LOG_DB = Path(os.getenv("HANA_DB_API_EXECUTION_LOG_DB", str(LOG_DIR / "executions.db"))).expanduser()
CONFIG_CANDIDATES = [
    Path(os.getenv("HANA_DB_API_CONFIG", "")).expanduser() if os.getenv("HANA_DB_API_CONFIG") else None,
    BASE_DIR / "config.json",
    BASE_DIR / "config",
]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
SUPPORTED_OPERATORS = {"eq", "like", "ilike", "contains", "startswith", "endswith", "gt", "gte", "lt", "lte", "in"}
RESERVED_QUERY_PARAMS = {
    "schema",
    "limit",
    "offset",
    "DB",
    "db",
    "Table",
    "table",
    "DynamicToken",
    "dynamic_token",
    "SessionId",
    "session_id",
    "RouteId",
    "route_id",
}
ERP_FLOW_ANALISE_FLUXO_VIEW = "VW_FIN_ANALISE_FLUXO"
ERP_FLOW_PENDING_STATUS = "Esboço (Pendente - ERP Flow)"
DEFAULT_EXTERNAL_APPROVALS_API_URL = "https://ryxlofwbyhkqcvzavbwn.supabase.co/functions/v1/external-approvals-api"
ERP_FLOW_QUERY_PARAMS = {
    "company_db",
    "CompanyDB",
    "ERPFlowCompanyDB",
    "erp_flow_company_db",
    "UserCode",
    "user_code",
    "SAPUserCode",
    "sap_user_code",
    "ERPFlowUserCode",
    "erp_flow_user_code",
    "ApproverUserCode",
    "approver_user_code",
}


@dataclass(frozen=True)
class AppSettings:
    host: str
    port: int
    default_database: str
    default_schema: Optional[str]
    default_limit: int
    max_limit: int
    restart_delay_seconds: int
    dynamic_token_secret: str
    sap_service_layer_url: str
    sap_service_layer_verify_ssl: bool
    auth_timeout_seconds: int
    schema_aliases: dict[str, str]
    db_pool_size: int
    db_max_overflow: int
    db_pool_timeout_seconds: int
    query_concurrency: int
    query_queue_size: int
    query_queue_timeout_seconds: int
    rate_limit_requests: int
    rate_limit_window_seconds: int
    execution_log_retention_days: int
    execution_log_cleanup_interval_seconds: int
    workers: int
    external_approvals_api_url: str
    external_approvals_api_key: str
    external_approvals_timeout_seconds: int


@dataclass(frozen=True)
class DatabaseConfig:
    name: str
    url: str
    label: str
    default_schema: Optional[str]
    allowed_schemas: list[str]
    schema_aliases: dict[str, str]


def locate_config_file() -> Path:
    for candidate in CONFIG_CANDIDATES:
        if candidate and candidate.exists():
            return candidate
    searched = ", ".join(str(candidate) for candidate in CONFIG_CANDIDATES if candidate)
    raise RuntimeError(f"Nenhum arquivo de configuracao encontrado. Procurei em: {searched}")


def load_raw_config() -> dict[str, Any]:
    config_path = locate_config_file()
    with config_path.open("r", encoding="utf-8") as handle:
        return expand_env_placeholders(json.load(handle))


def expand_env_placeholders(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: expand_env_placeholders(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env_placeholders(item) for item in value]
    if isinstance(value, str):
        return re.sub(r"\$\{([^}]+)\}", lambda match: os.getenv(match.group(1), match.group(0)), value)
    return value


def config_or_env(config: dict[str, Any], key: str, env_name: str, default: Any = "") -> Any:
    env_value = os.getenv(env_name)
    if env_value is not None:
        return env_value
    return config.get(key, default)


def config_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "sim"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "nao"}:
        return False
    return default


def normalize_aliases(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(alias).strip().lower(): str(schema).strip() for alias, schema in value.items() if alias and schema}


def build_connection_url(config: dict[str, Any]) -> str:
    direct_url = config.get("url")
    if direct_url:
        return direct_url

    driver = config.get("dialect") or config.get("driver") or "hana+hdbcli"
    database = config.get("database", "")

    if driver.startswith("sqlite"):
        if database in ("", ":memory:"):
            return "sqlite:///:memory:"
        if database.startswith("/"):
            return f"sqlite:///{database}"
        return f"sqlite:///{database}"

    username = config.get("username") or config.get("user") or ""
    password = config.get("password") or ""
    host = config.get("host") or config.get("address") or config.get("server") or "localhost"
    port = config.get("port")
    options = config.get("options", {})

    auth = ""
    if username:
        auth = quote_plus(str(username))
        if password:
            auth += f":{quote_plus(str(password))}"
        auth += "@"

    target = host
    if port:
        target = f"{target}:{port}"

    query = urlencode(options, doseq=True)
    path = f"/{database}" if database else ""
    suffix = f"?{query}" if query else ""
    return f"{driver}://{auth}{target}{path}{suffix}"


def load_settings() -> tuple[AppSettings, dict[str, DatabaseConfig]]:
    raw = load_raw_config()
    api_config = raw.get("api", {})
    database_configs = raw.get("databases", {})

    if not database_configs:
        raise RuntimeError("A configuracao precisa ter pelo menos um banco em 'databases'.")

    default_database = api_config.get("default_database") or next(iter(database_configs))
    if default_database not in database_configs:
        raise RuntimeError(f"Banco padrao '{default_database}' nao existe na secao 'databases'.")

    settings = AppSettings(
        host=api_config.get("host", "0.0.0.0"),
        port=int(api_config.get("port", 8000)),
        default_database=default_database,
        default_schema=api_config.get("default_schema"),
        default_limit=int(api_config.get("default_limit", 10000)),
        max_limit=int(api_config.get("max_limit", 10000)),
        restart_delay_seconds=int(api_config.get("restart_delay_seconds", 5)),
        dynamic_token_secret=str(config_or_env(api_config, "dynamic_token_secret", "HANA_QUERY_DYNAMIC_TOKEN_SECRET", "")),
        sap_service_layer_url=str(config_or_env(api_config, "sap_service_layer_url", "SAP_B1_SERVICE_LAYER_URL", "")),
        sap_service_layer_verify_ssl=config_bool(
            config_or_env(api_config, "sap_service_layer_verify_ssl", "SAP_B1_SERVICE_LAYER_VERIFY_SSL", False)
        ),
        auth_timeout_seconds=int(config_or_env(api_config, "auth_timeout_seconds", "HANA_QUERY_AUTH_TIMEOUT_SECONDS", 10)),
        schema_aliases=normalize_aliases(api_config.get("schema_aliases", {})),
        db_pool_size=int(config_or_env(api_config, "db_pool_size", "HANA_DB_POOL_SIZE", 4)),
        db_max_overflow=int(config_or_env(api_config, "db_max_overflow", "HANA_DB_MAX_OVERFLOW", 2)),
        db_pool_timeout_seconds=int(config_or_env(api_config, "db_pool_timeout_seconds", "HANA_DB_POOL_TIMEOUT_SECONDS", 30)),
        query_concurrency=int(config_or_env(api_config, "query_concurrency", "HANA_QUERY_CONCURRENCY", 5)),
        query_queue_size=int(config_or_env(api_config, "query_queue_size", "HANA_QUERY_QUEUE_SIZE", 25)),
        query_queue_timeout_seconds=int(config_or_env(api_config, "query_queue_timeout_seconds", "HANA_QUERY_QUEUE_TIMEOUT_SECONDS", 30)),
        rate_limit_requests=int(config_or_env(api_config, "rate_limit_requests", "HANA_RATE_LIMIT_REQUESTS", 0)),
        rate_limit_window_seconds=int(config_or_env(api_config, "rate_limit_window_seconds", "HANA_RATE_LIMIT_WINDOW_SECONDS", 60)),
        execution_log_retention_days=int(config_or_env(api_config, "execution_log_retention_days", "HANA_EXECUTION_LOG_RETENTION_DAYS", 30)),
        execution_log_cleanup_interval_seconds=int(config_or_env(api_config, "execution_log_cleanup_interval_seconds", "HANA_EXECUTION_LOG_CLEANUP_INTERVAL_SECONDS", 3600)),
        workers=int(config_or_env(api_config, "workers", "HANA_API_WORKERS", 2)),
        external_approvals_api_url=str(
            config_or_env(
                api_config,
                "external_approvals_api_url",
                "EXTERNAL_APPROVALS_API_URL",
                DEFAULT_EXTERNAL_APPROVALS_API_URL,
            )
        ).strip(),
        external_approvals_api_key=str(config_or_env(api_config, "external_approvals_api_key", "EXTERNAL_APPROVALS_API_KEY", "")).strip(),
        external_approvals_timeout_seconds=int(
            config_or_env(api_config, "external_approvals_timeout_seconds", "EXTERNAL_APPROVALS_TIMEOUT_SECONDS", 30)
        ),
    )

    databases: dict[str, DatabaseConfig] = {}
    for name, config in database_configs.items():
        databases[name] = DatabaseConfig(
            name=name,
            url=build_connection_url(config),
            label=config.get("label", name),
            default_schema=config.get("default_schema"),
            allowed_schemas=[schema.upper() for schema in config.get("allowed_schemas", [])],
            schema_aliases=normalize_aliases(config.get("schema_aliases", {})),
        )

    return settings, databases


class DatabaseRegistry:
    def __init__(self, databases: dict[str, DatabaseConfig]) -> None:
        self._databases = databases
        self._engines: dict[str, Any] = {}

    def get_database(self, name: str) -> DatabaseConfig:
        database = self._databases.get(name)
        if not database:
            raise HTTPException(status_code=404, detail=f"Banco '{name}' nao encontrado.")
        return database

    def get_engine(self, name: str):
        if name in self._engines:
            return self._engines[name]

        database = self.get_database(name)
        connect_args = {}
        if database.url.startswith("sqlite"):
            connect_args["check_same_thread"] = False

        try:
            engine_options: dict[str, Any] = {
                "pool_pre_ping": True,
                "pool_recycle": 1800,
                "future": True,
                "connect_args": connect_args,
            }
            if not database.url.startswith("sqlite"):
                engine_options.update(
                    pool_size=app_settings.db_pool_size,
                    max_overflow=app_settings.db_max_overflow,
                    pool_timeout=app_settings.db_pool_timeout_seconds,
                )
            engine = create_engine(database.url, **engine_options)
        except NoSuchModuleError as exc:
            raise RuntimeError(
                f"O driver SQLAlchemy do banco '{name}' nao esta instalado. URL configurada: {database.url}"
            ) from exc

        self._engines[name] = engine
        return engine

    def ping(self, name: str) -> None:
        database = self.get_database(name)
        engine = self.get_engine(name)
        ping_statement = "SELECT 1 FROM DUMMY" if database.url.startswith("hana") else "SELECT 1"
        with engine.connect() as connection:
            connection.execute(text(ping_statement))

    def list_databases(self) -> list[dict[str, Any]]:
        result = []
        for database in self._databases.values():
            result.append(
                {
                    "name": database.name,
                    "label": database.label,
                    "default_schema": database.default_schema,
                    "allowed_schemas": database.allowed_schemas,
                    "url_masked": mask_url_password(database.url),
                }
            )
        return result


def mask_url_password(url: str) -> str:
    return re.sub(r":[^:@/]+@", ":***@", url, count=1)


def validate_identifier(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise HTTPException(status_code=400, detail=f"{field_name} invalido: '{value}'")
    return value


def resolve_schema_alias(schema_name: Optional[str], database: DatabaseConfig) -> Optional[str]:
    if not schema_name:
        return schema_name
    key = schema_name.strip().lower()
    return database.schema_aliases.get(key) or app_settings.schema_aliases.get(key) or schema_name


def resolve_schema_and_object_name(raw_object_name: str, schema: Optional[str], database: DatabaseConfig) -> tuple[Optional[str], str]:
    explicit_schema = validate_identifier(schema, "schema")
    if "." in raw_object_name:
        raw_schema, raw_object_name = raw_object_name.split(".", 1)
        # If the client sent a leading dot like ".OBJECT_NAME" then raw_schema will be
        # an empty string. Treat an empty raw_schema as "no explicit schema provided"
        # instead of validating it (which would raise a 400). Only validate when
        # raw_schema is non-empty.
        if raw_schema != "":
            explicit_schema = validate_identifier(raw_schema, "schema")

    object_name = validate_identifier(raw_object_name, "object")
    schema_name = explicit_schema or database.default_schema or app_settings.default_schema
    schema_name = resolve_schema_alias(schema_name, database)

    if database.allowed_schemas and schema_name and schema_name.upper() not in database.allowed_schemas:
        raise HTTPException(status_code=403, detail=f"Schema '{schema_name}' nao autorizado para o banco '{database.name}'.")

    return schema_name, object_name


def load_table(database_name: str, schema_name: Optional[str], object_name: str) -> Table:
    engine = registry.get_engine(database_name)
    metadata = MetaData()
    inspector = inspect(engine)

    try:
        available_tables = {name.upper() for name in inspector.get_table_names(schema=schema_name)}
    except NotImplementedError:
        available_tables = set()

    try:
        available_views = {name.upper() for name in inspector.get_view_names(schema=schema_name)}
    except NotImplementedError:
        available_views = set()

    target_name = object_name.upper()

    if available_tables or available_views:
        if target_name not in available_tables and target_name not in available_views:
            raise HTTPException(
                status_code=404,
                detail=f"Objeto '{object_name}' nao encontrado no schema '{schema_name or '<default>'}'.",
            )

    try:
        return Table(object_name, metadata, schema=schema_name, autoload_with=engine)
    except NoSuchTableError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Objeto '{object_name}' nao encontrado no schema '{schema_name or '<default>'}.",
        ) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar metadados de '{object_name}': {exc}") from exc


def parse_filter_key(raw_key: str) -> tuple[str, str]:
    if "__" not in raw_key:
        return raw_key, "eq"

    column_name, operator = raw_key.rsplit("__", 1)
    if operator not in SUPPORTED_OPERATORS:
        raise HTTPException(status_code=400, detail=f"Operador '{operator}' nao suportado para o filtro '{raw_key}'.")
    return column_name, operator


def get_request_value(request: Request, *names: str) -> Optional[str]:
    for name in names:
        value = request.headers.get(name)
        if value:
            return value
        value = request.query_params.get(name)
        if value:
            return value
    return None


def service_layer_base_url() -> str:
    base_url = app_settings.sap_service_layer_url.rstrip("/")
    if not base_url:
        raise HTTPException(status_code=503, detail="SAP_B1_SERVICE_LAYER_URL nao configurado.")
    if not base_url.lower().endswith("/b1s/v1"):
        base_url = f"{base_url}/b1s/v1"
    return base_url


def generate_dynamic_token(hour_block: int) -> str:
    if not app_settings.dynamic_token_secret:
        raise HTTPException(status_code=503, detail="HANA_QUERY_DYNAMIC_TOKEN_SECRET nao configurado.")
    return hmac.new(
        app_settings.dynamic_token_secret.encode("utf-8"),
        str(hour_block).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def validate_dynamic_token(dynamic_token: Optional[str]) -> None:
    if not dynamic_token:
        raise HTTPException(status_code=401, detail="DynamicToken nao informado.")

    current_block = int(time_module.time()) // 3600
    valid_tokens = (
        generate_dynamic_token(current_block),
        generate_dynamic_token(current_block - 1),
    )
    received = dynamic_token.strip()
    if not any(hmac.compare_digest(received, expected) for expected in valid_tokens):
        raise HTTPException(status_code=401, detail="DynamicToken invalido.")


def validate_sap_session(session_id: Optional[str], route_id: Optional[str] = None) -> None:
    if not session_id:
        raise HTTPException(status_code=401, detail="SessionId nao informado.")

    cookie_parts = [f"B1SESSION={session_id.strip()}"]
    if route_id:
        cookie_parts.append(f"ROUTEID={route_id.strip()}")

    try:
        response = requests.get(
            f"{service_layer_base_url()}/Users(10)",
            headers={"Cookie": "; ".join(cookie_parts)},
            timeout=app_settings.auth_timeout_seconds,
            verify=app_settings.sap_service_layer_verify_ssl,
        )
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Falha ao validar SessionId no SAP.") from exc

    if response.status_code >= 400:
        raise HTTPException(status_code=401, detail="SessionId invalido ou expirado.")


def validate_query_access(request: Request) -> None:
    dynamic_token = get_request_value(request, "DynamicToken", "dynamic_token", "dynamictoken")
    session_id = get_request_value(request, "SessionId", "session_id", "sessionid")
    route_id = get_request_value(request, "RouteId", "route_id", "routeid")

    validate_dynamic_token(dynamic_token)
    validate_sap_session(session_id, route_id=route_id)


def is_erp_flow_augmented_object(object_name: str) -> bool:
    return object_name.upper() == ERP_FLOW_ANALISE_FLUXO_VIEW


def is_reserved_query_param(raw_key: str, object_name: Optional[str] = None) -> bool:
    if raw_key in RESERVED_QUERY_PARAMS or raw_key.lower() in {key.lower() for key in RESERVED_QUERY_PARAMS}:
        return True
    if object_name and is_erp_flow_augmented_object(object_name):
        return raw_key in ERP_FLOW_QUERY_PARAMS or raw_key.lower() in {key.lower() for key in ERP_FLOW_QUERY_PARAMS}
    return False


def get_erp_flow_company_db(request: Request, schema_name: Optional[str]) -> str:
    company_db = get_request_value(
        request,
        "ERPFlowCompanyDB",
        "erp_flow_company_db",
        "CompanyDB",
        "company_db",
        "DB",
        "db",
    )
    return (company_db or schema_name or app_settings.default_schema or "").strip()


def get_erp_flow_user_code(request: Request) -> str:
    user_code = get_request_value(
        request,
        "ERPFlowUserCode",
        "erp_flow_user_code",
        "SAPUserCode",
        "sap_user_code",
        "UserCode",
        "user_code",
        "ApproverUserCode",
        "approver_user_code",
    )
    return (user_code or "").strip()


def fetch_erp_flow_pending_documents(company_db: str, user_code: str) -> list[dict[str, Any]]:
    if not app_settings.external_approvals_api_url:
        raise HTTPException(status_code=503, detail="EXTERNAL_APPROVALS_API_URL nao configurada.")
    if not app_settings.external_approvals_api_key:
        raise HTTPException(status_code=503, detail="EXTERNAL_APPROVALS_API_KEY nao configurada.")
    if not company_db:
        raise HTTPException(status_code=400, detail="company_db nao informado para consultar aprovacoes do ERP Flow.")
    if not user_code:
        raise HTTPException(status_code=400, detail="user_code nao informado para consultar aprovacoes do ERP Flow.")

    try:
        response = requests.post(
            app_settings.external_approvals_api_url,
            headers={
                "X-API-Key": app_settings.external_approvals_api_key,
                "Content-Type": "application/json",
            },
            json={"op": "list", "company_db": company_db, "user_code": user_code},
            timeout=app_settings.external_approvals_timeout_seconds,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Falha ao consultar aprovacoes pendentes no ERP Flow.") from exc

    try:
        payload = response.json()
    except ValueError:
        payload = {}

    if response.status_code >= 400:
        error_message = payload.get("error") if isinstance(payload, dict) else None
        if not error_message:
            error_message = response.text[:500] or f"HTTP {response.status_code}"
        raise HTTPException(
            status_code=502,
            detail=f"Falha ao consultar aprovacoes pendentes no ERP Flow: {error_message}",
        )

    documents = payload.get("documents", []) if isinstance(payload, dict) else []
    if not isinstance(documents, list):
        return []
    return [document for document in documents if isinstance(document, dict)]


def normalize_field_key(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value))
    ascii_value = "".join(char for char in ascii_value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]", "", ascii_value.lower())


def put_document_value(
    row: dict[str, Any],
    columns_by_key: dict[str, str],
    candidate_names: tuple[str, ...],
    value: Any,
) -> None:
    if value is None:
        return
    for candidate in candidate_names:
        column_name = columns_by_key.get(normalize_field_key(candidate))
        if column_name:
            row[column_name] = value


def build_erp_flow_approval_row(document: dict[str, Any], table: Table) -> dict[str, Any]:
    row = {column.name: None for column in table.columns}
    columns_by_key = {normalize_field_key(column.name): column.name for column in table.columns}

    status_column = columns_by_key.get(normalize_field_key("Status"))
    if status_column:
        row[status_column] = ERP_FLOW_PENDING_STATUS
    row["Status"] = ERP_FLOW_PENDING_STATUS

    put_document_value(row, columns_by_key, ("Origem", "Source", "Fonte", "SistemaOrigem"), "ERP Flow")
    put_document_value(row, columns_by_key, ("ApprovalRequestId", "ERPFlowApprovalRequestId", "IdAprovacao", "AprovacaoId"), document.get("approval_request_id"))
    put_document_value(row, columns_by_key, ("Step", "ApprovalStep", "ERPFlowStep", "Etapa", "Passo"), document.get("step"))
    put_document_value(row, columns_by_key, ("ObjType", "ObjectType", "DocObjectType", "TipoObjeto"), document.get("doc_object_type"))
    put_document_value(row, columns_by_key, ("TipoDocumento", "DocTypeName", "DocumentoTipo", "Documento", "Tipo"), document.get("doc_type_name"))
    put_document_value(row, columns_by_key, ("DocEntry", "DocumentoEntry", "EntradaDocumento"), document.get("doc_entry"))
    put_document_value(row, columns_by_key, ("DocNum", "DocumentoNumero", "NumeroDocumento", "NumDocumento", "Numero"), document.get("doc_num"))
    put_document_value(row, columns_by_key, ("DocTotal", "TotalDocumento", "ValorDocumento", "ValorTotal", "Total", "Valor"), document.get("doc_total"))
    put_document_value(row, columns_by_key, ("Currency", "Moeda", "DocCurrency", "DocCurr"), document.get("currency"))
    put_document_value(row, columns_by_key, ("CardCode", "ParceiroCodigo", "CodigoParceiro", "FornecedorCodigo", "CodigoFornecedor"), document.get("card_code"))
    put_document_value(row, columns_by_key, ("CardName", "ParceiroNome", "NomeParceiro", "Fornecedor", "FornecedorNome", "NomeFornecedor"), document.get("card_name"))
    put_document_value(row, columns_by_key, ("Remarks", "Comentarios", "Observacoes", "Observacao", "Obs"), document.get("remarks"))
    put_document_value(row, columns_by_key, ("CreationDate", "DataCriacao", "DataLancamento", "DocDate", "DataDocumento"), document.get("creation_date"))
    put_document_value(row, columns_by_key, ("UpdateDate", "DataAtualizacao", "DataAlteracao"), document.get("update_date"))
    put_document_value(row, columns_by_key, ("OriginatorId", "SolicitanteId", "CriadorId"), document.get("originator_id"))
    put_document_value(row, columns_by_key, ("ApproverUserCode", "AprovadorUserCode", "UserCode", "UsuarioAprovador"), document.get("approver_user_code"))

    row["ERPFlowApprovalRequestId"] = document.get("approval_request_id")
    row["ERPFlowStep"] = document.get("step")
    row["ERPFlowDocObjectType"] = document.get("doc_object_type")
    row["ERPFlowApproverUserCode"] = document.get("approver_user_code")
    return row


def coerce_row_value(column: Any, value: Any) -> Any:
    if value is None:
        return None
    try:
        python_type = column.type.python_type
    except (AttributeError, NotImplementedError):
        return value

    if isinstance(value, python_type):
        return value
    try:
        if python_type is str:
            return str(value)
        if python_type is bool:
            return parse_bool(str(value))
        if python_type is int:
            return int(value)
        if python_type is float:
            return float(value)
        if python_type is Decimal:
            return Decimal(str(value))
        if python_type is datetime:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if python_type is date:
            return date.fromisoformat(str(value)[:10])
        if python_type is time:
            return time.fromisoformat(str(value))
    except (TypeError, ValueError):
        return value
    return value


def sql_like_matches(value: Any, pattern: str, *, case_sensitive: bool) -> bool:
    regex_parts = []
    for char in pattern:
        if char == "%":
            regex_parts.append(".*")
        elif char == "_":
            regex_parts.append(".")
        else:
            regex_parts.append(re.escape(char))
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.fullmatch("".join(regex_parts), str(value), flags=flags) is not None


def row_value_matches_filter(row_value: Any, column: Any, operator: str, raw_value: str) -> bool:
    if row_value is None:
        return False
    if operator == "like":
        return sql_like_matches(row_value, raw_value, case_sensitive=True)
    if operator == "ilike":
        return sql_like_matches(row_value, raw_value, case_sensitive=False)
    if operator == "contains":
        return raw_value.lower() in str(row_value).lower()
    if operator == "startswith":
        return str(row_value).lower().startswith(raw_value.lower())
    if operator == "endswith":
        return str(row_value).lower().endswith(raw_value.lower())

    converted_row_value = coerce_row_value(column, row_value)
    if operator == "in":
        values = [convert_value(column, item.strip()) for item in raw_value.split(",") if item.strip()]
        return converted_row_value in values

    converted_filter_value = convert_value(column, raw_value)
    if operator == "eq":
        return converted_row_value == converted_filter_value
    try:
        if operator == "gt":
            return converted_row_value > converted_filter_value
        if operator == "gte":
            return converted_row_value >= converted_filter_value
        if operator == "lt":
            return converted_row_value < converted_filter_value
        if operator == "lte":
            return converted_row_value <= converted_filter_value
    except TypeError:
        return False
    return False


def row_matches_request_filters(row: dict[str, Any], request: Request, column_map: dict[str, Any], object_name: str) -> bool:
    for raw_key, raw_value in request.query_params.multi_items():
        if not raw_key or is_reserved_query_param(raw_key, object_name):
            continue
        column_name, operator = parse_filter_key(raw_key)
        column = column_map.get(column_name.upper())
        if column is None:
            continue
        if not row_value_matches_filter(row.get(column.name), column, operator, raw_value):
            return False
    return True


def append_erp_flow_pending_documents(
    data: list[dict[str, Any]],
    table: Table,
    request: Request,
    schema_name: Optional[str],
    object_name: str,
    column_map: dict[str, Any],
) -> int:
    if not is_erp_flow_augmented_object(object_name):
        return 0

    company_db = get_erp_flow_company_db(request, schema_name)
    user_code = get_erp_flow_user_code(request)
    documents = fetch_erp_flow_pending_documents(company_db, user_code)
    added = 0
    for document in documents:
        row = build_erp_flow_approval_row(document, table)
        if row_matches_request_filters(row, request, column_map, object_name):
            data.append(row)
            added += 1
    logger.info(
        "ERP Flow retornou %s documentos pendentes para company_db=%s user_code=%s; adicionados=%s",
        len(documents),
        company_db,
        user_code,
        added,
    )
    return added


class QueryAdmissionController:
    """Fila FIFO limitada para proteger o banco e a memoria do processo."""

    def __init__(self, concurrency: int, queue_size: int, timeout_seconds: int) -> None:
        self.concurrency = max(1, concurrency)
        self.queue_size = max(0, queue_size)
        self.timeout_seconds = max(1, timeout_seconds)
        self._condition = threading.Condition()
        self._active = 0
        self._queue: deque[object] = deque()

    @contextmanager
    def acquire(self):
        with self._condition:
            if self._active < self.concurrency and not self._queue:
                self._active += 1
            else:
                if len(self._queue) >= self.queue_size:
                    raise HTTPException(status_code=429, detail="Fila de consultas cheia. Tente novamente mais tarde.")
                ticket = object()
                self._queue.append(ticket)
                deadline = time_module.monotonic() + self.timeout_seconds
                while self._queue[0] is not ticket or self._active >= self.concurrency:
                    remaining = deadline - time_module.monotonic()
                    if remaining <= 0:
                        self._queue.remove(ticket)
                        self._condition.notify_all()
                        raise HTTPException(status_code=429, detail="Tempo de espera na fila de consultas excedido.")
                    self._condition.wait(remaining)
                self._queue.popleft()
                self._active += 1

        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def status(self) -> dict[str, int]:
        with self._condition:
            return {
                "active": self._active,
                "queued": len(self._queue),
                "concurrency_limit": self.concurrency,
                "queue_limit": self.queue_size,
            }


class FixedWindowRateLimiter:
    def __init__(self, request_limit: int, window_seconds: int) -> None:
        self.request_limit = max(0, request_limit)
        self.window_seconds = max(1, window_seconds)
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, client: str) -> None:
        if self.request_limit == 0:
            return
        now = time_module.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            history = self._requests[client]
            while history and history[0] <= cutoff:
                history.popleft()
            if len(history) >= self.request_limit:
                retry_after = max(1, int(history[0] + self.window_seconds - now))
                raise HTTPException(
                    status_code=429,
                    detail=f"Limite de requisicoes excedido. Tente novamente em {retry_after}s.",
                    headers={"Retry-After": str(retry_after)},
                )
            history.append(now)


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "sim"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "nao"}:
        return False
    raise ValueError(f"Valor booleano invalido: {value}")


def convert_value(column: Any, raw_value: str) -> Any:
    try:
        python_type = column.type.python_type
    except (AttributeError, NotImplementedError):
        return raw_value

    if python_type is str:
        return raw_value
    if python_type is bool:
        return parse_bool(raw_value)
    if python_type is int:
        return int(raw_value)
    if python_type is float:
        return float(raw_value)
    if python_type is Decimal:
        return Decimal(raw_value)
    if python_type is datetime:
        return datetime.fromisoformat(raw_value)
    if python_type is date:
        return date.fromisoformat(raw_value)
    if python_type is time:
        return time.fromisoformat(raw_value)
    return raw_value


def build_predicate(column: Any, operator: str, raw_value: str):
    if operator == "eq":
        return column == convert_value(column, raw_value)
    if operator == "gt":
        return column > convert_value(column, raw_value)
    if operator == "gte":
        return column >= convert_value(column, raw_value)
    if operator == "lt":
        return column < convert_value(column, raw_value)
    if operator == "lte":
        return column <= convert_value(column, raw_value)
    if operator == "like":
        return column.like(raw_value)
    if operator == "ilike":
        return column.ilike(raw_value)
    if operator == "contains":
        return column.ilike(f"%{raw_value}%")
    if operator == "startswith":
        return column.ilike(f"{raw_value}%")
    if operator == "endswith":
        return column.ilike(f"%{raw_value}")
    if operator == "in":
        values = [convert_value(column, item.strip()) for item in raw_value.split(",") if item.strip()]
        return column.in_(values)
    raise HTTPException(status_code=400, detail=f"Operador '{operator}' nao suportado.")


def execute_query(
    database_name: str,
    raw_object_name: str,
    request: Request,
    schema: Optional[str],
    limit: Optional[int],
    offset: int,
) -> dict[str, Any]:
    execution_id = str(uuid.uuid4())
    started_at = _utc_now_iso()
    start_time = time_module.perf_counter()
    schema_name: Optional[str] = None
    object_name = raw_object_name
    applied_limit: Optional[int] = None
    filters: dict[str, list[str]] = {}
    statement_preview: str | None = None

    try:
        validate_query_access(request)
        database = registry.get_database(database_name)
        schema_name, object_name = resolve_schema_and_object_name(raw_object_name, schema, database)
        applied_limit = app_settings.default_limit if limit is None else int(limit)

        if applied_limit is not None and applied_limit > app_settings.max_limit:
            raise HTTPException(
                status_code=400,
                detail=f"O limite maximo por consulta e {app_settings.max_limit}.",
            )

        table = load_table(database_name, schema_name, object_name)
        column_map = {column.name.upper(): column for column in table.columns}
        statement = select(table).offset(offset)
        if applied_limit is not None:
            statement = statement.limit(applied_limit)

        for raw_key, raw_value in request.query_params.multi_items():
            if not raw_key:
                continue
            if is_reserved_query_param(raw_key, object_name):
                continue
            filters.setdefault(raw_key, []).append(raw_value)
            column_name, operator = parse_filter_key(raw_key)
            column = column_map.get(column_name.upper())
            if column is None:
                raise HTTPException(status_code=400, detail=f"Coluna '{column_name}' nao existe em '{object_name}'.")
            statement = statement.where(build_predicate(column, operator, raw_value))

        statement_preview = str(statement)[:4000]
        engine = registry.get_engine(database_name)
        logger.info(
            "Consultando banco=%s schema=%s objeto=%s limit=%s offset=%s",
            database_name,
            schema_name or "<default>",
            object_name,
            applied_limit,
            offset,
        )

        with engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()

        data = [dict(row) for row in rows]
        append_erp_flow_pending_documents(data, table, request, schema_name, object_name, column_map)

        duration_ms = int((time_module.perf_counter() - start_time) * 1000)
        log_execution(
            id=execution_id,
            started_at=started_at,
            finished_at=_utc_now_iso(),
            duration_ms=duration_ms,
            status="success",
            database_name=database_name,
            schema_name=schema_name,
            object_name=object_name,
            route=str(request.url.path),
            method=request.method,
            client_host=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            limit_value=applied_limit,
            offset_value=offset,
            row_count=len(data),
            filters_json=json.dumps(filters, ensure_ascii=False),
            statement_preview=statement_preview,
            error_type=None,
            error_message=None,
        )

        return {
            "success": True,
            "execution_id": execution_id,
            "database": database_name,
            "schema": schema_name,
            "object": object_name,
            "count": len(data),
            "limit": applied_limit,
            "offset": offset,
            "data": jsonable_encoder(data),
        }
    except HTTPException as exc:
        duration_ms = int((time_module.perf_counter() - start_time) * 1000)
        log_execution(
            id=execution_id,
            started_at=started_at,
            finished_at=_utc_now_iso(),
            duration_ms=duration_ms,
            status="error",
            database_name=database_name,
            schema_name=schema_name,
            object_name=object_name,
            route=str(request.url.path),
            method=request.method,
            client_host=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            limit_value=applied_limit,
            offset_value=offset,
            row_count=0,
            filters_json=json.dumps(filters, ensure_ascii=False),
            statement_preview=statement_preview,
            error_type="HTTPException",
            error_message=str(exc.detail),
        )
        raise
    except SQLAlchemyError as exc:
        duration_ms = int((time_module.perf_counter() - start_time) * 1000)
        logger.exception("Erro ao executar consulta")
        log_execution(
            id=execution_id,
            started_at=started_at,
            finished_at=_utc_now_iso(),
            duration_ms=duration_ms,
            status="error",
            database_name=database_name,
            schema_name=schema_name,
            object_name=object_name,
            route=str(request.url.path),
            method=request.method,
            client_host=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            limit_value=applied_limit,
            offset_value=offset,
            row_count=0,
            filters_json=json.dumps(filters, ensure_ascii=False),
            statement_preview=statement_preview,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise HTTPException(status_code=500, detail=f"Falha ao consultar o banco '{database_name}': {exc}") from exc


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


_execution_log_init_lock = threading.Lock()
_execution_log_initialized = False
_cleanup_stop_event = threading.Event()


def connect_execution_log() -> sqlite3.Connection:
    connection = sqlite3.connect(EXECUTION_LOG_DB, timeout=10)
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def init_execution_log() -> None:
    global _execution_log_initialized
    if _execution_log_initialized:
        return
    EXECUTION_LOG_DB.parent.mkdir(parents=True, exist_ok=True)
    with _execution_log_init_lock:
        if _execution_log_initialized:
            return
        with connect_execution_log() as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                logger.warning("Outro worker esta inicializando o WAL; continuando apos aguardar o banco.")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_log (
                    id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    database_name TEXT,
                    schema_name TEXT,
                    object_name TEXT,
                    route TEXT,
                    method TEXT,
                    client_host TEXT,
                    user_agent TEXT,
                    limit_value INTEGER,
                    offset_value INTEGER,
                    row_count INTEGER,
                    filters_json TEXT NOT NULL DEFAULT '{}',
                    statement_preview TEXT,
                    error_type TEXT,
                    error_message TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_log_started_at ON execution_log(started_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_log_status ON execution_log(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_log_database ON execution_log(database_name)")
        _execution_log_initialized = True


def log_execution(**payload: Any) -> str:
    execution_id = payload.get("id") or str(uuid.uuid4())
    init_execution_log()
    columns = [
        "id", "started_at", "finished_at", "duration_ms", "status", "database_name", "schema_name",
        "object_name", "route", "method", "client_host", "user_agent", "limit_value", "offset_value",
        "row_count", "filters_json", "statement_preview", "error_type", "error_message",
    ]
    values = [payload.get(column) for column in columns]
    with connect_execution_log() as conn:
        conn.execute(
            f"INSERT INTO execution_log ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            values,
        )
    return str(execution_id)


def list_execution_logs(
    *,
    limit: int = 100,
    offset: int = 0,
    database_name: str | None = None,
    object_name: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    init_execution_log()
    clauses: list[str] = []
    params: list[Any] = []
    if database_name:
        clauses.append("database_name = ?")
        params.append(database_name)
    if object_name:
        clauses.append("UPPER(object_name) = UPPER(?)")
        params.append(object_name)
    if status:
        clauses.append("status = ?")
        params.append(status)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    query = f"SELECT * FROM execution_log {where} ORDER BY started_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with connect_execution_log() as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(query, params).fetchall()]


def get_execution_log(execution_id: str) -> dict[str, Any] | None:
    init_execution_log()
    with connect_execution_log() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM execution_log WHERE id = ?", (execution_id,)).fetchone()
        return dict(row) if row else None


def purge_execution_logs() -> int:
    init_execution_log()
    with connect_execution_log() as conn:
        cursor = conn.execute("DELETE FROM execution_log")
        return int(cursor.rowcount or 0)


def cleanup_old_execution_logs() -> int:
    init_execution_log()
    cutoff = (datetime.utcnow() - timedelta(days=app_settings.execution_log_retention_days)).isoformat(timespec="milliseconds") + "Z"
    with connect_execution_log() as conn:
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute("DELETE FROM execution_log WHERE started_at < ?", (cutoff,))
            deleted = int(cursor.rowcount or 0)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if deleted:
        logger.info("Limpeza de auditoria removeu %s registros anteriores a %s.", deleted, cutoff)
    return deleted


def execution_log_cleanup_loop() -> None:
    while not _cleanup_stop_event.is_set():
        try:
            cleanup_old_execution_logs()
        except Exception:
            logger.exception("Falha na limpeza periodica do log de auditoria.")
        _cleanup_stop_event.wait(app_settings.execution_log_cleanup_interval_seconds)


app_settings, configured_databases = load_settings()
registry = DatabaseRegistry(configured_databases)
query_admission = QueryAdmissionController(
    app_settings.query_concurrency,
    app_settings.query_queue_size,
    app_settings.query_queue_timeout_seconds,
)
query_rate_limiter = FixedWindowRateLimiter(
    app_settings.rate_limit_requests,
    app_settings.rate_limit_window_seconds,
)
app = FastAPI(title="Universal Database Gateway", version="2.0.0")


@app.on_event("startup")
def startup_tasks() -> None:
    init_execution_log()
    _cleanup_stop_event.clear()
    threading.Thread(target=execution_log_cleanup_loop, name="execution-log-cleanup", daemon=True).start()


@app.on_event("shutdown")
def shutdown_tasks() -> None:
    _cleanup_stop_event.set()


@app.get("/health")
def healthcheck(deep: bool = Query(default=False)) -> Any:
    statuses = []
    overall_status = "ok"

    for database in registry.list_databases():
        status = {"name": database["name"], "status": "configured"}
        if deep:
            try:
                registry.ping(database["name"])
                status["status"] = "online"
            except Exception as exc:  # pragma: no cover - defensive
                status["status"] = "offline"
                status["detail"] = str(exc)
                overall_status = "degraded"
        statuses.append(status)

    payload = {
        "status": overall_status,
        "default_database": app_settings.default_database,
        "databases": statuses,
        "query_capacity": query_admission.status(),
    }
    if overall_status != "ok":
        return JSONResponse(status_code=503, content=payload)
    return payload


def execute_admitted_query(
    database_name: str,
    object_name: str,
    request: Request,
    schema: Optional[str],
    limit: Optional[int],
    offset: int,
) -> dict[str, Any]:
    client = request.client.host if request.client else "unknown"
    query_rate_limiter.check(client)
    with query_admission.acquire():
        return execute_query(database_name, object_name, request, schema, limit, offset)


@app.get("/databases")
def list_databases() -> dict[str, Any]:
    return {
        "default_database": app_settings.default_database,
        "items": registry.list_databases(),
    }


@app.get("/data/{object_name}")
def get_default_database_data(
    object_name: str,
    request: Request,
    schema: Optional[str] = None,
    limit: Optional[int] = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return execute_admitted_query(app_settings.default_database, object_name, request, schema, limit, offset)


@app.get("/data/{database_name}/{object_name}")
def get_database_data(
    database_name: str,
    object_name: str,
    request: Request,
    schema: Optional[str] = None,
    limit: Optional[int] = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    validate_identifier(database_name, "database")
    return execute_admitted_query(database_name, object_name, request, schema, limit, offset)


@app.get("/execution-logs")
def get_execution_logs(
    request: Request,
    limit: int = Query(default=100, ge=1),
    offset: int = Query(default=0, ge=0),
    database_name: Optional[str] = None,
    object_name: Optional[str] = None,
    status: Optional[str] = None,
) -> dict[str, Any]:
    logs = list_execution_logs(limit=limit, offset=offset, database_name=database_name, object_name=object_name, status=status)
    return {"success": True, "count": len(logs), "limit": limit, "offset": offset, "data": logs}


@app.get("/execution-logs/{execution_id}")
def read_execution_log(execution_id: str) -> dict[str, Any]:
    log = get_execution_log(execution_id)
    if not log:
        raise HTTPException(status_code=404, detail="Log de execucao nao encontrado.")
    return {"success": True, "data": log}


@app.post("/execution-logs/purge")
def purge_logs() -> dict[str, Any]:
    count = purge_execution_logs()
    return {"success": True, "deleted_count": count}


@app.get("/executions")
def get_executions(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    database_name: Optional[str] = Query(default=None),
    object_name: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    rows = list_execution_logs(
        limit=limit,
        offset=offset,
        database_name=database_name,
        object_name=object_name,
        status=status,
    )
    return {"success": True, "count": len(rows), "limit": limit, "offset": offset, "items": rows}


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str) -> dict[str, Any]:
    row = get_execution_log(execution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Execucao nao encontrada.")
    return {"success": True, "item": row}


@app.delete("/executions")
def delete_executions() -> dict[str, Any]:
    deleted = purge_execution_logs()
    return {"success": True, "deleted": deleted}


if __name__ == "__main__":
    uvicorn.run(app, host=app_settings.host, port=app_settings.port)
