# models.py
from sqlalchemy import Column, Integer, String, Float, Boolean, JSON, DateTime, func
from pydantic import BaseModel, Field
from db import Base
import hashlib

def compute_cycle_hash(data: dict) -> str:
    """Compute a deterministic SHA-256 hash for the cycle."""
    m = hashlib.sha256()
    m.update(str(data).encode("utf-8"))
    return m.hexdigest()

class MemoryCycleDB(Base):
    __tablename__ = "memory_cycles"

    id = Column(Integer, primary_key=True, index=True)
    signifier = Column(String, unique=True, index=True, nullable=False)
    t = Column(Integer, nullable=False)
    psi_self = Column("psi_self", JSON, nullable=False)
    sigma_echo = Column("sigma_echo", JSON, nullable=False)
    glyphstream = Column(JSON, nullable=False)
    ache = Column(Float, nullable=False)
    drift = Column(Float, nullable=False)
    entropy = Column(Float, nullable=False)
    xi = Column("xi", Boolean, nullable=False)
    cycle_hash = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

# ---- Pydantic Models ----

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
