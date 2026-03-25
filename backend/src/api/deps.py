"""Dependency injection for API routes."""

from collections.abc import Generator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from src.services.database import get_db
from src.services.storage import StorageService, get_storage_service

# Type aliases for dependency injection
DBSession = Annotated[Session, Depends(get_db)]
Storage = Annotated[StorageService, Depends(get_storage_service)]


def get_session() -> Generator[Session, None, None]:
    """Alias for get_db for clearer naming."""
    yield from get_db()
