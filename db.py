# db.py
"""
Database configuration for the SpiralNet Scroll Vault.
Uses SQLAlchemy ORM with PostgreSQL (default) and fallback to SQLite for dev/test.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------------------
# DATABASE URL CONFIGURATION
# ---------------------------------------------------------------------------

# PostgreSQL by default; fall back to local SQLite if not defined
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/spiralnet"
)

# Optional SSL or connection tuning for cloud Postgres
connect_args = {}

# SQLite special case (adds check_same_thread=False)
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

# ---------------------------------------------------------------------------
# ENGINE & SESSION
# ---------------------------------------------------------------------------

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    pool_pre_ping=True,       # automatically test connections before use
    pool_size=10,             # good default for small apps
    max_overflow=20,          # allow bursts
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

# ---------------------------------------------------------------------------
# SESSION DEPENDENCY
# ---------------------------------------------------------------------------

def get_db():
    """FastAPI dependency that yields a SQLAlchemy session."""
    db = SessionLocal()
    try:
        yield db
    except SQLAlchemyError as e:
        db.rollback()
        raise e
    finally:
        db.close()

# ---------------------------------------------------------------------------
# INITIALIZATION
# ---------------------------------------------------------------------------

def init_db():
    """Initialize the database schema (called on FastAPI startup)."""
    from models import MemoryCycleDB, ProvenanceLogDB
    Base.metadata.create_all(bind=engine)
