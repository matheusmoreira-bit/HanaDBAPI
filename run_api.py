import logging
import signal
import subprocess
import sys
import time
from pathlib import Path

from main import app_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hana-db-api-runner")

BASE_DIR = Path(__file__).resolve().parent
shutdown_requested = False
child_process: subprocess.Popen | None = None


def handle_shutdown(signum, _frame) -> None:
    global shutdown_requested
    shutdown_requested = True
    logger.info("Sinal %s recebido. Encerrando supervisor...", signum)

    if child_process and child_process.poll() is None:
        child_process.terminate()


def build_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        app_settings.host,
        "--port",
        str(app_settings.port),
        "--workers",
        str(app_settings.workers),
    ]


def supervise() -> int:
    global child_process

    while not shutdown_requested:
        command = build_command()
        logger.info("Subindo API: %s", " ".join(command))
        child_process = subprocess.Popen(command, cwd=BASE_DIR)
        exit_code = child_process.wait()

        if shutdown_requested:
            return exit_code

        if exit_code == 0:
            logger.info("API finalizada normalmente.")
            return 0

        logger.warning(
            "API caiu com codigo %s. Reiniciando em %s segundos...",
            exit_code,
            app_settings.restart_delay_seconds,
        )
        time.sleep(app_settings.restart_delay_seconds)

    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    raise SystemExit(supervise())
