"""Database engine"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker


def to_sqlalchemy_url(dsn: str) -> str:
    """Force the psycopg (v3) driver onto a bare ``postgresql://`` DSN."""
    if dsn.startswith("postgresql+"):
        return dsn
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://") :]
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn[len("postgres://") :]
    return dsn


def make_engine(dsn: str, *, echo: bool = False) -> Engine:
    return create_engine(
        to_sqlalchemy_url(dsn),
        echo=echo,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        future=True,
    )


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def ping(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
