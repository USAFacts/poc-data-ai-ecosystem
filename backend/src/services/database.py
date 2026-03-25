"""Database service for PostgreSQL/SQLite access."""

import os
from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def get_database_url() -> str:
    """Get the database URL based on configuration.

    Priority:
        1. DATABASE_URL env var (full connection string, e.g. postgresql://...)
        2. DATABASE_PATH env var (legacy SQLite fallback)
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # Legacy fallback: SQLite from DATABASE_PATH
    db_path = Path(os.getenv("DATABASE_PATH", "../ingestion-processing/pipeline.db"))
    if not db_path.is_absolute():
        db_path = Path(__file__).parent.parent.parent / db_path
    return f"sqlite:///{db_path}"


def _engine_kwargs(url: str) -> dict:
    """Get engine kwargs appropriate for the database dialect."""
    kwargs: dict = {"echo": False}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return kwargs


_url = get_database_url()
engine = create_engine(_url, **_engine_kwargs(_url))

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Get database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
