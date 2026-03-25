"""Centralized logging manager for the data pipeline.

Provides structured, consistent logging across all pipeline components
with support for console output (Rich-formatted), file output, and
database persistence via PostgreSQL.

Usage:
    from logging_manager import get_logger

    logger = get_logger(__name__)
    logger.info("Processing started", extra={"workflow": "uscis-forms"})
    logger.warning("Slow response", extra={"step": "acquire", "duration_s": 12.5})
    logger.error("Step failed", extra={"step": "parse", "asset": "census-pop"})
"""

import logging
import os
import queue
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class PipelineFormatter(logging.Formatter):
    """Formatter that appends structured extra fields as key=value pairs."""

    _BUILTIN_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)

    def format(self, record: logging.LogRecord) -> str:
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in self._BUILTIN_ATTRS and not k.startswith("_")
        }
        base = super().format(record)
        if extras:
            pairs = " ".join(f"{k}={v}" for k, v in extras.items())
            return f"{base} | {pairs}"
        return base


# ---------------------------------------------------------------------------
# Console handler with color
# ---------------------------------------------------------------------------

class _ColorHandler(logging.StreamHandler):
    """Stream handler that applies ANSI colors by log level."""

    _COLORS = {
        logging.DEBUG: "\033[90m",       # gray
        logging.INFO: "\033[36m",        # cyan
        logging.WARNING: "\033[33m",     # yellow
        logging.ERROR: "\033[31m",       # red
        logging.CRITICAL: "\033[1;31m",  # bold red
    }
    _RESET = "\033[0m"

    def emit(self, record: logging.LogRecord) -> None:
        color = self._COLORS.get(record.levelno, "")
        record.msg = f"{color}{record.msg}{self._RESET}"
        super().emit(record)


# ---------------------------------------------------------------------------
# Database handler — writes log records to the pipeline_logs table
# ---------------------------------------------------------------------------

# Extra fields that get promoted to dedicated columns
_PROMOTED_FIELDS = {"workflow", "step", "asset", "run_id"}


class DatabaseHandler(logging.Handler):
    """Async logging handler that writes records to PostgreSQL.

    Uses a background thread with a bounded queue so logging calls
    never block on database I/O.
    """

    def __init__(self, db_url: str, batch_size: int = 20, flush_interval: float = 2.0) -> None:
        super().__init__()
        self._db_url = db_url
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: queue.Queue[logging.LogRecord | None] = queue.Queue(maxsize=5000)
        self._engine: Any = None
        self._session_factory: Any = None

        self._worker = threading.Thread(target=self._run, daemon=True, name="log-db-writer")
        self._worker.start()

    def _get_engine(self) -> Any:
        """Lazy-create the SQLAlchemy engine and ensure the table exists."""
        if self._engine is None:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from db.models import Base

            self._engine = create_engine(self._db_url, echo=False, pool_pre_ping=True)
            # Create only the pipeline_logs table if it doesn't exist
            from db.models import PipelineLogModel
            PipelineLogModel.__table__.create(self._engine, checkfirst=True)
            self._session_factory = sessionmaker(bind=self._engine)
        return self._engine

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            pass  # Drop record rather than blocking the caller

    def _run(self) -> None:
        """Background worker that drains the queue in batches."""
        batch: list[logging.LogRecord] = []

        while True:
            try:
                record = self._queue.get(timeout=self._flush_interval)
                if record is None:
                    # Shutdown sentinel
                    if batch:
                        self._flush(batch)
                    return
                batch.append(record)
                if len(batch) >= self._batch_size:
                    self._flush(batch)
                    batch = []
            except queue.Empty:
                if batch:
                    self._flush(batch)
                    batch = []

    def _flush(self, batch: list[logging.LogRecord]) -> None:
        """Write a batch of records to the database."""
        try:
            self._get_engine()
            from db.models import PipelineLogModel

            session = self._session_factory()
            try:
                for record in batch:
                    extras = {
                        k: v for k, v in record.__dict__.items()
                        if k not in PipelineFormatter._BUILTIN_ATTRS
                        and not k.startswith("_")
                        and k not in _PROMOTED_FIELDS
                    }

                    # Strip ANSI color codes before storing in DB
                    import re
                    clean_message = re.sub(r"\x1b\[[0-9;]*m", "", record.getMessage())

                    log_entry = PipelineLogModel(
                        timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                        level=record.levelname,
                        logger_name=record.name,
                        message=clean_message,
                        run_id=getattr(record, "run_id", None),
                        workflow=getattr(record, "workflow", None),
                        step=getattr(record, "step", None),
                        asset=getattr(record, "asset", None),
                        extra=extras if extras else None,
                    )
                    session.add(log_entry)
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()
        except Exception:
            pass  # DB unavailable — don't crash the pipeline

    def close(self) -> None:
        self._queue.put(None)  # Send shutdown sentinel
        self._worker.join(timeout=5)
        super().close()


# ---------------------------------------------------------------------------
# Manager singleton
# ---------------------------------------------------------------------------

_configured = False
_console_configured = False


def configure(
    level: str = "INFO",
    log_file: str | None = None,
    log_dir: str | None = None,
    enable_db: bool | None = None,
    db_url: str | None = None,
) -> None:
    """Configure the root pipeline logger.

    Can be called multiple times — the console handler is set up once,
    and the DB/file handlers are added on the first call that enables them.
    This allows early ``get_logger()`` calls (at import time) to work with
    console-only output, and a later ``configure()`` call (after
    ``load_dotenv()``) to attach the database handler.

    Args:
        level: Minimum log level (INFO, WARNING, ERROR, DEBUG).
        log_file: Explicit log file path. Takes precedence over log_dir.
        log_dir: Directory for auto-named log files (pipeline_YYYYMMDD.log).
                 Defaults to PIPELINE_LOG_DIR env var.
        enable_db: Enable database logging. Defaults to PIPELINE_LOG_DB env
                   var ("true"/"1") or auto-detected from DATABASE_URL.
        db_url: Database URL for log storage. Defaults to DATABASE_URL env var.
    """
    global _configured, _console_configured

    root = logging.getLogger("pipeline")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.propagate = False

    formatter = PipelineFormatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    # Console handler — added only once
    if not _console_configured:
        _console_configured = True
        console_handler = _ColorHandler(sys.stderr)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    # Skip file/DB setup if already fully configured
    if _configured:
        return
    _configured = True

    # File handler (optional)
    file_path = _resolve_log_file(log_file, log_dir)
    if file_path:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(file_path), encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Database handler (optional)
    resolved_db_url = db_url or os.getenv("DATABASE_URL")
    should_enable_db = enable_db
    if should_enable_db is None:
        env_flag = os.getenv("PIPELINE_LOG_DB", "").lower()
        should_enable_db = env_flag in ("true", "1", "yes")

    if should_enable_db and resolved_db_url:
        db_handler = DatabaseHandler(resolved_db_url)
        db_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        root.addHandler(db_handler)


def _setup_console_only() -> None:
    """Attach a console handler without marking full configuration as done."""
    global _console_configured
    _console_configured = True

    root = logging.getLogger("pipeline")
    root.setLevel(logging.INFO)
    root.propagate = False

    formatter = PipelineFormatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    console_handler = _ColorHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)


def _resolve_log_file(
    log_file: str | None,
    log_dir: str | None,
) -> Path | None:
    """Determine the log file path."""
    if log_file:
        return Path(log_file)

    dir_path = log_dir or os.getenv("PIPELINE_LOG_DIR")
    if dir_path:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return Path(dir_path) / f"pipeline_{today}.log"

    return None


def get_logger(name: str) -> logging.Logger:
    """Get a logger scoped under the 'pipeline' namespace.

    Args:
        name: Module name, typically ``__name__``.

    Returns:
        A logger instance. If ``configure()`` has not been called yet,
        a minimal console-only configuration is applied so logs are
        never lost. The full configuration (DB, file) is deferred
        until the CLI calls ``configure()`` after ``load_dotenv()``.
    """
    if not _console_configured:
        # Minimal setup — console only, no DB/file.
        # The CLI will call configure() properly later.
        _setup_console_only()

    # Scope all loggers under 'pipeline.' so root config applies
    if not name.startswith("pipeline."):
        scoped = f"pipeline.{name}"
    else:
        scoped = name

    return logging.getLogger(scoped)
