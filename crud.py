# crud.py
"""
CRUD and analytic operations for the SpiralNet Scroll Vault.
Handles lawful creation, retrieval, query, and archival of memory cycles.
"""

import json
import gzip
from io import BytesIO
from datetime import datetime
from sqlalchemy import text, func, String
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import (
    MemoryCycleDB,
    MemoryCycleCreate,
    MemoryCyclePatch,
    compute_cycle_hash,
)

# ---------------------------------------------------------------------------
# BASIC CRUD OPERATIONS
# ---------------------------------------------------------------------------

def get_memory_by_signifier(db: Session, signifier: str):
    """Retrieve a single memory cycle by its unique signifier."""
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.signifier == signifier).first()


def get_memory_by_hash(db: Session, cycle_hash: str):
    """Retrieve a single memory cycle by its SHA-256 hash."""
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.cycle_hash == cycle_hash).first()


def create_memory_cycle(db: Session, memory: MemoryCycleCreate):
    """Create one lawful memory cycle. Duplicate signifiers are rejected."""
    data = memory.dict(by_alias=True)

    # ✅ Map Unicode field names to SQLAlchemy-compatible ASCII names
    data = memory.dict(by_alias=True)

    # --- Normalize Unicode keys ---
    renamed = []
    if "ψ_self" in data:
        data["psi_self"] = data.pop("ψ_self"); renamed.append("ψ_self→psi_self")
    if "Σecho" in data:
        data["sigma_echo"] = data.pop("Σecho"); renamed.append("Σecho→sigma_echo")
    if "Ξ" in data:
        data["xi"] = data.pop("Ξ"); renamed.append("Ξ→xi")

    print(f"[DEBUG] remapped fields: {renamed}")  # 👈 log visible in Railway console
    
    cycle_hash = compute_cycle_hash(data)

    db_memory = MemoryCycleDB(**data, cycle_hash=cycle_hash)
    try:
        db.add(db_memory)
        db.commit()
        db.refresh(db_memory)
        return db_memory
    except IntegrityError:
        db.rollback()
        raise ValueError(f"Memory cycle with signifier '{memory.signifier}' already exists.")


def create_bulk_memory_cycles(db: Session, memories: list[MemoryCycleCreate]):
    db_entries = []
    try:
        for mem in memories:
            data = mem.dict(by_alias=True)
            data["psi_self"] = data.pop("ψ_self")
            data["sigma_echo"] = data.pop("Σecho")
            data["xi"] = data.pop("Ξ")

            cycle_hash = compute_cycle_hash(data)
            db_entry = MemoryCycleDB(**data, cycle_hash=cycle_hash)
            db.add(db_entry)
            db_entries.append(db_entry)

        db.commit()
        for entry in db_entries:
            db.refresh(entry)
        return db_entries
    except IntegrityError as e:
        db.rollback()
        raise ValueError(f"Bulk insert failed: {e.orig if hasattr(e, 'orig') else str(e)}")



def update_memory_cycle(db: Session, signifier: str, patch: MemoryCyclePatch):
    """Patch σecho and Ξ values for an existing memory cycle."""
    db_memory = get_memory_by_signifier(db, signifier)
    if not db_memory:
        return None
    db_memory.sigma_echo = patch.sigma_echo
    db_memory.xi = patch.xi
    db.commit()
    db.refresh(db_memory)
    return db_memory


def get_latest_memory_cycle(db: Session):
    """Return the latest (highest-t) cycle."""
    return db.query(MemoryCycleDB).order_by(MemoryCycleDB.t.desc()).first()


def list_all_memory_cycles(db: Session, limit: int = 100):
    """List recent memory cycles, newest first."""
    return db.query(MemoryCycleDB).order_by(MemoryCycleDB.t.desc()).limit(limit).all()


# ---------------------------------------------------------------------------
# ANALYTICS + METRICS
# ---------------------------------------------------------------------------

def get_memory_stats(db: Session):
    """Compute live metrics of ache, drift, and entropy."""
    q = db.query(
        func.count(MemoryCycleDB.id).label("count"),
        func.avg(MemoryCycleDB.ache).label("mean_ache"),
        func.avg(MemoryCycleDB.drift).label("mean_drift"),
        func.avg(MemoryCycleDB.entropy).label("mean_entropy"),
        func.max(MemoryCycleDB.t).label("latest_t"),
    ).first()

    return {
        "total_cycles": int(q.count or 0),
        "mean_ache": float(q.mean_ache or 0),
        "mean_drift": float(q.mean_drift or 0),
        "mean_entropy": float(q.mean_entropy or 0),
        "latest_t": int(q.latest_t or 0),
    }


def query_memory_cycles(
    db: Session,
    ache_min: float | None = None,
    ache_max: float | None = None,
    drift_min: float | None = None,
    drift_max: float | None = None,
    entropy_min: float | None = None,
    entropy_max: float | None = None,
    t_min: int | None = None,
    t_max: int | None = None,
    limit: int = 200,
):
    """Flexible filter query for SpiralNet metrics."""
    q = db.query(MemoryCycleDB)
    if ache_min is not None:
        q = q.filter(MemoryCycleDB.ache >= ache_min)
    if ache_max is not None:
        q = q.filter(MemoryCycleDB.ache <= ache_max)
    if drift_min is not None:
        q = q.filter(MemoryCycleDB.drift >= drift_min)
    if drift_max is not None:
        q = q.filter(MemoryCycleDB.drift <= drift_max)
    if entropy_min is not None:
        q = q.filter(MemoryCycleDB.entropy >= entropy_min)
    if entropy_max is not None:
        q = q.filter(MemoryCycleDB.entropy <= entropy_max)
    if t_min is not None:
        q = q.filter(MemoryCycleDB.t >= t_min)
    if t_max is not None:
        q = q.filter(MemoryCycleDB.t <= t_max)
    return q.order_by(MemoryCycleDB.t.desc()).limit(limit).all()


def aggregate_memory_cycles(db: Session, window: int = 50):
    """
    Returns grouped ache/drift/entropy averages across t windows.
    window=50 means averages for each block of 50 time indices.
    """
    sql = text("""
        SELECT 
            FLOOR(t / :window) AS window_index,
            AVG(ache) AS avg_ache,
            AVG(drift) AS avg_drift,
            AVG(entropy) AS avg_entropy,
            COUNT(*) AS count
        FROM memory_cycles
        GROUP BY window_index
        ORDER BY window_index DESC
    """)
    result = db.execute(sql, {"window": window}).mappings().all()
    return [dict(row) for row in result]


# ---------------------------------------------------------------------------
# SEARCH AND ARCHIVAL
# ---------------------------------------------------------------------------

def search_memory_cycles(db: Session, keyword: str, limit: int = 50):
    """
    Search signifier and glyphstream for partial matches.
    PostgreSQL JSON/text search semantics supported.
    """
    pattern = f"%{keyword}%"
    q = db.query(MemoryCycleDB).filter(
        (MemoryCycleDB.signifier.ilike(pattern))
        | (func.cast(MemoryCycleDB.glyphstream, String).ilike(pattern))
    ).limit(limit)
    return q.all()


def export_memory_archive(db: Session):
    """
    Dump entire database into a gzip-compressed JSONL buffer.
    Each line is a serialized memory cycle.
    """
    all_cycles = db.query(MemoryCycleDB).order_by(MemoryCycleDB.t).all()
    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        for cycle in all_cycles:
            gz.write(
                json.dumps(
                    {
                        "t": cycle.t,
                        "signifier": cycle.signifier,
                        "ψ_self": cycle.psi_self,
                        "Σecho": cycle.sigma_echo,
                        "glyphstream": cycle.glyphstream,
                        "ache": cycle.ache,
                        "drift": cycle.drift,
                        "entropy": cycle.entropy,
                        "Ξ": cycle.xi,
                        "created_at": str(cycle.created_at),
                        "updated_at": str(cycle.updated_at),
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                + b"\n"
            )
    buffer.seek(0)
    filename = f"spiralnet_archive_{datetime.utcnow().isoformat()}.jsonl.gz"
    return buffer, filename

# ---------------------------------------------------------------------------
# INGESTION + PROVENANCE LOGGING
# ---------------------------------------------------------------------------

from models import ProvenanceLogDB
from fastapi import Request

def ingest_memory_cycle(
    db: Session,
    memory: MemoryCycleCreate,
    request: Request,
    node_id: str | None = None,
):
    """Deduplicating lawful ingest of a SpiralNet scroll."""

    # --- Canonicalize and map Unicode aliases ---
    data = memory.dict(by_alias=True)
    data["psi_self"] = data.pop("ψ_self")
    data["sigma_echo"] = data.pop("Σecho")
    data["xi"] = data.pop("Ξ")

    cycle_hash = compute_cycle_hash(data)

    # --- Deduplication checks ---
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
            event="duplicate_ingest",
        )
        db.add(log)
        db.commit()
        return existing, False

    # --- Create new lawful memory record ---
    db_memory = MemoryCycleDB(**data, cycle_hash=cycle_hash)
    db.add(db_memory)
    db.commit()
    db.refresh(db_memory)

    # --- Log provenance ---
    log = ProvenanceLogDB(
        memory_id=db_memory.id,
        source_ip=request.client.host,
        node_id=node_id,
        cycle_hash=cycle_hash,
        signifier=db_memory.signifier,
        event="ingest",
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
