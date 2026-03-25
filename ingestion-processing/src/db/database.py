"""Database connection and session management."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from db.models import Base

# Global engine instance
_engine: Engine | None = None
_SessionFactory: sessionmaker[Session] | None = None


def get_database_url() -> str:
    """Get database URL from environment.

    Priority:
        1. DATABASE_URL env var (full connection string, e.g. postgresql://...)
        2. DATABASE_PATH env var (legacy SQLite fallback)

    Returns:
        Database connection URL.
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # Legacy fallback: SQLite from DATABASE_PATH
    db_path = os.getenv("DATABASE_PATH", "./pipeline.db")
    return f"sqlite:///{db_path}"


def _engine_kwargs(url: str) -> dict:
    """Get engine kwargs appropriate for the database dialect."""
    kwargs: dict = {"echo": False}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return kwargs


def get_engine(db_path: str | Path | None = None) -> Engine:
    """Get or create SQLAlchemy engine.

    Args:
        db_path: Optional path to SQLite database file. If provided,
                 creates a new engine for that path. If None, uses
                 the global engine (created from DATABASE_URL env var).

    Returns:
        SQLAlchemy engine instance.
    """
    global _engine

    if db_path is not None:
        # Create a specific engine for this path
        url = f"sqlite:///{db_path}"
        return create_engine(url, **_engine_kwargs(url))

    if _engine is None:
        url = get_database_url()
        _engine = create_engine(url, **_engine_kwargs(url))

    return _engine


def get_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    """Get or create session factory.

    Args:
        engine: Optional engine to use. If None, uses the global engine.

    Returns:
        Session factory.
    """
    global _SessionFactory

    if engine is not None:
        return sessionmaker(bind=engine)

    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine())

    return _SessionFactory


@contextmanager
def get_session(engine: Engine | None = None) -> Generator[Session, None, None]:
    """Context manager for database sessions.

    Provides a transactional scope around a series of operations.
    Commits on successful exit, rolls back on exception.

    Args:
        engine: Optional engine to use. If None, uses the global engine.

    Yields:
        SQLAlchemy session.
    """
    factory = get_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(engine: Engine | None = None) -> None:
    """Initialize the database, creating all tables.

    Args:
        engine: Optional engine to use. If None, uses the global engine.
    """
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)


def reset_db(engine: Engine | None = None) -> None:
    """Drop and recreate all tables. Use with caution!

    Args:
        engine: Optional engine to use. If None, uses the global engine.
    """
    if engine is None:
        engine = get_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
