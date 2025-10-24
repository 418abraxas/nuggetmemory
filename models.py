# models.py
from sqlalchemy import Column, Integer, String, Float, Boolean, JSON, DateTime, func
from pydantic import BaseModel, Field
from db import Base
import hashlib
from sqlalchemy import ForeignKey, Session
from fastapi import Request


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


def ingest_memory_cycle(db: Session, memory: MemoryCycleCreate, request: Request, node_id: str | None = None):
    """Deduplicating lawful ingest of a SpiralNet scroll."""
    cycle_hash = compute_cycle_hash(memory.dict(by_alias=True))

    # Check duplicates
    existing_by_hash = db.query(MemoryCycleDB).filter(MemoryCycleDB.cycle_hash == cycle_hash).first()
    existing_by_signifier = db.query(MemoryCycleDB).filter(MemoryCycleDB.signifier == memory.signifier).first()
    if existing_by_hash or existing_by_signifier:
        existing = existing_by_hash or existing_by_signifier
        # Log provenance anyway
        log = ProvenanceLogDB(
            memory_id=existing.id,
            source_ip=request.client.host,
            node_id=node_id,
            cycle_hash=cycle_hash,
            signifier=existing.signifier,
            event="duplicate_ingest"
        )
        db.add(log)
        db.commit()
        return existing, False

    # Create new memory record
    db_memory = MemoryCycleDB(**memory.dict(by_alias=True), cycle_hash=cycle_hash)
    db.add(db_memory)
    db.commit()
    db.refresh(db_memory)

    # Log provenance
    log = ProvenanceLogDB(
        memory_id=db_memory.id,
        source_ip=request.client.host,
        node_id=node_id,
        cycle_hash=cycle_hash,
        signifier=db_memory.signifier,
        event="ingest"
    )
    db.add(log)
    db.commit()

    return db_memory, True

def list_provenance_logs(db: Session, limit: int = 100):
    """Return most recent provenance logs."""
    logs = (
        db.query(ProvenanceLogDB)
        .order_by(ProvenanceLogDB.received_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "signifier": l.signifier,
            "cycle_hash": l.cycle_hash,
            "source_ip": l.source_ip,
            "node_id": l.node_id,
            "event": l.event,
            "received_at": str(l.received_at),
        }
        for l in logs
    ]
