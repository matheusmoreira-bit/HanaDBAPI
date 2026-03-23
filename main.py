import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.exc import NoSuchModuleError, SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hana-db-api")

BASE_DIR = Path(__file__).resolve().parent
CONFIG_CANDIDATES = [
    Path(os.getenv("HANA_DB_API_CONFIG", "")).expanduser() if os.getenv("HANA_DB_API_CONFIG") else None,
    BASE_DIR / "config.json",
    BASE_DIR / "config",
]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
SUPPORTED_OPERATORS = {"eq", "like", "ilike", "contains", "startswith", "endswith", "gt", "gte", "lt", "lte", "in"}
RESERVED_QUERY_PARAMS = {"schema", "limit", "offset"}


@dataclass(frozen=True)
class AppSettings:
    host: str
    port: int
    default_database: str
    default_schema: Optional[str]
    default_limit: int
    max_limit: int
    restart_delay_seconds: int


@dataclass(frozen=True)
class DatabaseConfig:
    name: str
    url: str
    label: str
    default_schema: Optional[str]
    allowed_schemas: list[str]


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
        default_limit=int(api_config.get("default_limit", 100)),
        max_limit=int(api_config.get("max_limit", 1000)),
        restart_delay_seconds=int(api_config.get("restart_delay_seconds", 5)),
    )

    databases: dict[str, DatabaseConfig] = {}
    for name, config in database_configs.items():
        databases[name] = DatabaseConfig(
            name=name,
            url=build_connection_url(config),
            label=config.get("label", name),
            default_schema=config.get("default_schema"),
            allowed_schemas=[schema.upper() for schema in config.get("allowed_schemas", [])],
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
            engine = create_engine(
                database.url,
                pool_pre_ping=True,
                pool_recycle=1800,
                future=True,
                connect_args=connect_args,
            )
        except NoSuchModuleError as exc:
            raise RuntimeError(
                f"O driver SQLAlchemy do banco '{name}' nao esta instalado. URL configurada: {database.url}"
            ) from exc

        self._engines[name] = engine
        return engine

    def ping(self, name: str) -> None:
        engine = self.get_engine(name)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

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
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar metadados de '{object_name}': {exc}") from exc


def parse_filter_key(raw_key: str) -> tuple[str, str]:
    if "__" not in raw_key:
        return raw_key, "eq"

    column_name, operator = raw_key.rsplit("__", 1)
    if operator not in SUPPORTED_OPERATORS:
        raise HTTPException(status_code=400, detail=f"Operador '{operator}' nao suportado para o filtro '{raw_key}'.")
    return column_name, operator


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
    database = registry.get_database(database_name)
    schema_name, object_name = resolve_schema_and_object_name(raw_object_name, schema, database)
    applied_limit = limit or app_settings.default_limit

    if applied_limit > app_settings.max_limit:
        raise HTTPException(
            status_code=400,
            detail=f"O limite maximo por consulta e {app_settings.max_limit}.",
        )

    table = load_table(database_name, schema_name, object_name)
    column_map = {column.name.upper(): column for column in table.columns}
    statement = select(table).limit(applied_limit).offset(offset)

    for raw_key, raw_value in request.query_params.multi_items():
        # Ignore empty query parameter names (e.g. when the URL ends with "?=")
        # which would otherwise lead to a confusing 400 error later.
        if not raw_key:
            continue

        if raw_key in RESERVED_QUERY_PARAMS:
            continue

        column_name, operator = parse_filter_key(raw_key)
        column = column_map.get(column_name.upper())
        if not column:
            raise HTTPException(status_code=400, detail=f"Coluna '{column_name}' nao existe em '{object_name}'.")
        statement = statement.where(build_predicate(column, operator, raw_value))

    engine = registry.get_engine(database_name)
    logger.info(
        "Consultando banco=%s schema=%s objeto=%s limit=%s offset=%s",
        database_name,
        schema_name or "<default>",
        object_name,
        applied_limit,
        offset,
    )

    try:
        with engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
    except SQLAlchemyError as exc:
        logger.exception("Erro ao executar consulta")
        raise HTTPException(status_code=500, detail=f"Falha ao consultar o banco '{database_name}': {exc}") from exc

    return {
        "success": True,
        "database": database_name,
        "schema": schema_name,
        "object": object_name,
        "count": len(rows),
        "limit": applied_limit,
        "offset": offset,
        "data": jsonable_encoder(rows),
    }


app_settings, configured_databases = load_settings()
registry = DatabaseRegistry(configured_databases)
app = FastAPI(title="Universal Database Gateway", version="2.0.0")


@app.get("/health")
def healthcheck(deep: bool = Query(default=False)) -> dict[str, Any]:
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

    return {
        "status": overall_status,
        "default_database": app_settings.default_database,
        "databases": statuses,
    }


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
    return execute_query(app_settings.default_database, object_name, request, schema, limit, offset)


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
    return execute_query(database_name, object_name, request, schema, limit, offset)


if __name__ == "__main__":
    uvicorn.run(app, host=app_settings.host, port=app_settings.port)
