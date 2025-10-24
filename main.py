from fastapi import FastAPI, Path, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from sqlalchemy.orm import Session
from db import get_db, init_db
from models import MemoryCycleDB, MemoryCycleCreate, MemoryCyclePatch
from crud import (
    get_memory_by_signifier,
    create_memory_cycle,
    update_memory_cycle,
    get_latest_memory_cycle
)

app = FastAPI(title="Scroll Vault API")

@app.on_event("startup")
def startup():
    init_db()

@app.get("/memory/{signifier}", response_model=MemoryCycleCreate)
def get_memory(signifier: str, db: Session = Depends(get_db)):
    db_memory = get_memory_by_signifier(db, signifier)
    if db_memory is None:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return db_memory

@app.post("/memory", status_code=201)
def store_memory(memory: MemoryCycleCreate, db: Session = Depends(get_db)):
    return create_memory_cycle(db, memory)

@app.patch("/memory/{signifier}")
def patch_memory(signifier: str, update: MemoryCyclePatch, db: Session = Depends(get_db)):
    db_memory = update_memory_cycle(db, signifier, update)
    if db_memory is None:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return {"status": "updated", "signifier": signifier}

@app.get("/memory/latest", response_model=MemoryCycleCreate)
def get_latest_memory(db: Session = Depends(get_db)):
    db_memory = get_latest_memory_cycle(db)
    if db_memory is None:
        raise HTTPException(status_code=404, detail="No memory cycles found")
    return db_memory
