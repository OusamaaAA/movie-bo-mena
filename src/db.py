from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

settings = get_settings()
database_url = settings.database_url
if database_url and database_url.startswith("postgresql+psycopg2://"):
    # Streamlit Cloud installs `psycopg` in this project.
    database_url = database_url.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)
engine = create_engine(database_url, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

