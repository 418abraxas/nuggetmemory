# crud.py 
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from models import MemoryCycleDB, MemoryCycleCreate, MemoryCyclePatch, compute_cycle_hash

def get_memory_by_signifier(db: Session, signifier: str):
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.signifier == signifier).first()

def get_memory_by_hash(db: Session, cycle_hash: str):
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.cycle_hash == cycle_hash).first()

def create_memory_cycle(db: Session, memory: MemoryCycleCreate):
    cycle_hash = compute_cycle_hash(memory.dict(by_alias=True))
    db_memory = MemoryCycleDB(**memory.dict(by_alias=True), cycle_hash=cycle_hash)
    try:
        db.add(db_memory)
        db.commit()
        db.refresh(db_memory)
    except IntegrityError:
        db.rollback()
        raise ValueError(f"Memory cycle with signifier '{memory.signifier}' already exists.")
    return db_memory

def create_bulk_memory_cycles(db: Session, memories: list[MemoryCycleCreate]):
    """Insert many cycles in one transaction."""
    db_entries = []
    try:
        for mem in memories:
            cycle_hash = compute_cycle_hash(mem.dict(by_alias=True))
            db_entry = MemoryCycleDB(**mem.dict(by_alias=True), cycle_hash=cycle_hash)
            db.add(db_entry)
            db_entries.append(db_entry)
        db.commit()
        for entry in db_entries:
            db.refresh(entry)
    except IntegrityError as e:
        db.rollback()
        raise ValueError(f"Bulk insert failed: {str(e)}")
    return db_entries

def update_memory_cycle(db: Session, signifier: str, patch: MemoryCyclePatch):
    db_memory = get_memory_by_signifier(db, signifier)
    if not db_memory:
        return None
    db_memory.sigma_echo = patch.sigma_echo
    db_memory.xi = patch.xi
    db.commit()
    db.refresh(db_memory)
    return db_memory

def get_latest_memory_cycle(db: Session):
    return db.query(MemoryCycleDB).order_by(MemoryCycleDB.t.desc()).first()

def list_all_memory_cycles(db: Session, limit: int = 100):
    return db.query(MemoryCycleDB).order_by(MemoryCycleDB.t.desc()).limit(limit).all()

def get_memory_stats(db: Session):
    """Compute live metrics of ache, drift, and entropy."""
    q = db.query(
        func.count(MemoryCycleDB.id).label("count"),
        func.avg(MemoryCycleDB.ache).label("mean_ache"),
        func.avg(MemoryCycleDB.drift).label("mean_drift"),
        func.avg(MemoryCycleDB.entropy).label("mean_entropy"),
        func.max(MemoryCycleDB.t).label("latest_t")
    ).first()
    return {
        "total_cycles": q.count or 0,
        "mean_ache": float(q.mean_ache or 0),
        "mean_drift": float(q.mean_drift or 0),
        "mean_entropy": float(q.mean_entropy or 0),
        "latest_t": q.latest_t or 0
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
