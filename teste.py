import argparse

from sqlalchemy import text

from main import app_settings, registry


def main() -> int:
    parser = argparse.ArgumentParser(description="Testa a conectividade com um dos bancos configurados.")
    parser.add_argument(
        "--database",
        default=app_settings.default_database,
        help="Nome do banco configurado em 'databases'.",
    )
    args = parser.parse_args()

    print(f"Iniciando teste de conexao com '{args.database}'...")
    try:
        engine = registry.get_engine(args.database)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Conectado com sucesso!")
        return 0
    except Exception as exc:
        print(f"Erro na conexao: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
