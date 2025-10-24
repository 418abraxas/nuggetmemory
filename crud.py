# crud.py
from sqlalchemy.orm import Session
from models import MemoryCycleDB, MemoryCycleCreate, MemoryCyclePatch

def get_memory_by_signifier(db: Session, signifier: str):
    return db.query(MemoryCycleDB).filter(MemoryCycleDB.signifier == signifier).first()

def create_memory_cycle(db: Session, memory: MemoryCycleCreate):
    db_memory = MemoryCycleDB(**memory.dict())
    db.add(db_memory)
    db.commit()
    db.refresh(db_memory)
    return db_memory

def update_memory_cycle(db: Session, signifier: str, patch: MemoryCyclePatch):
    db_memory = get_memory_by_signifier(db, signifier)
    if db_memory:
        db_memory.sigma_echo = patch.sigma_echo
        db_memory.xi = patch.xi
        db.commit()
        db.refresh(db_memory)
    return db_memory

def get_latest_memory_cycle(db: Session):
    return db.query(MemoryCycleDB).order_by(MemoryCycleDB.t.desc()).first()
