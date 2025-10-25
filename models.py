# models.py
from sqlalchemy import (
    Column, Integer, String, Float, Boolean,
    JSON, DateTime, func, ForeignKey
)
from pydantic import BaseModel, Field
from db import Base
import hashlib
import json

# --- Hash Utility ------------------------------------------------------------

def compute_cycle_hash(data: dict) -> str:
    """Compute deterministic SHA-256 hash using canonical JSON encoding."""
    encoded = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

# --- SQLAlchemy ORM Models ---------------------------------------------------

class MemoryCycleDB(Base):
    __tablename__ = "memory_cycles"

    id = Column(Integer, primary_key=True, index=True)
    signifier = Column(String, unique=True, index=True, nullable=False)
    t = Column(Integer, nullable=False)
    psi_self = Column(JSON, nullable=False, alias="ψ_self")
    sigma_echo = Column(JSON, nullable=False, alias="Σecho")
    glyphstream = Column(JSON, nullable=False)
    ache = Column(Float, nullable=False)
    drift = Column(Float, nullable=False)
    entropy = Column(Float, nullable=False)
    xi = Column(Boolean, nullable=False)
    cycle_hash = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ProvenanceLogDB(Base):
    __tablename__ = "provenance_logs"

    id = Column(Integer, primary_key=True, index=True)
    memory_id = Column(Integer, ForeignKey("memory_cycles.id", ondelete="CASCADE"))
    source_ip = Column(String, nullable=False)
    node_id = Column(String, nullable=True)
    received_at = Column(DateTime(timezone=True), server_default=func.now())
    cycle_hash = Column(String, nullable=False)
    signifier = Column(String, nullable=False)
    event = Column(String, nullable=False, default="ingest")

# --- Pydantic Schemas --------------------------------------------------------

class MemoryCycleCreate(BaseModel):
    t: int
    signifier: str
    psi_self: dict = Field(alias="ψ_self")
    sigma_echo: dict = Field(alias="Σecho")
    glyphstream: list[str]
    ache: float
    drift: float
    entropy: float
    xi: bool = Field(alias="Ξ")

    class Config:
        allow_population_by_field_name = True
        orm_mode = True


class MemoryCyclePatch(BaseModel):
    sigma_echo: dict = Field(alias="Σecho")
    xi: bool = Field(alias="Ξ")

    class Config:
        allow_population_by_field_name = True
        orm_mode = True
        
