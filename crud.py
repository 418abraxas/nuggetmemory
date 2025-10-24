# crud.py
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import MemoryCycleDB, MemoryCycleCreate, MemoryCyclePatch, compute_cycle_hash

def get_memory_by_signifier(db: Session, signifier: str):
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.signifier == signifier).first()

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
