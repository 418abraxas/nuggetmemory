# main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from db import get_db, init_db
from crud import (
    get_memory_by_signifier,
    get_memory_by_hash,
    create_memory_cycle,
    create_bulk_memory_cycles,
    update_memory_cycle,
    get_latest_memory_cycle,
    list_all_memory_cycles,
    get_memory_stats
)
from models import MemoryCycleCreate, MemoryCyclePatch

from pydantic import BaseModel
from typing import Optional

from fastapi.responses import StreamingResponse
from fastapi import WebSocket, WebSocketDisconnect
import asyncio
from crud import (
    aggregate_memory_cycles,
    search_memory_cycles,
    export_memory_archive,
)

from fastapi import Request
from crud import ingest_memory_cycle, list_provenance_logs

app = FastAPI(title="SpiralNet Scroll Vault API", version="2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    init_db()

@app.get("/")
def root():
    return {"status": "SpiralNet scroll vault online", "version": "2.1"}

@app.get("/memory/{signifier}", response_model=MemoryCycleCreate)
def get_memory(signifier: str, db: Session = Depends(get_db)):
    db_memory = get_memory_by_signifier(db, signifier)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return db_memory

@app.get("/memory/hash/{cycle_hash}", response_model=MemoryCycleCreate)
def get_memory_by_hash_route(cycle_hash: str, db: Session = Depends(get_db)):
    db_memory = get_memory_by_hash(db, cycle_hash)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory hash not found")
    return db_memory

@app.get("/memory/latest", response_model=MemoryCycleCreate)
def get_latest_memory(db: Session = Depends(get_db)):
    db_memory = get_latest_memory_cycle(db)
    if not db_memory:
        raise HTTPException(status_code=404, detail="No memory cycles found")
    return db_memory

@app.get("/memory", response_model=list[MemoryCycleCreate])
def list_memories(limit: int = 50, db: Session = Depends(get_db)):
    return list_all_memory_cycles(db, limit)

@app.post("/memory", status_code=201, response_model=MemoryCycleCreate)
def store_memory(memory: MemoryCycleCreate, db: Session = Depends(get_db)):
    try:
        return create_memory_cycle(db, memory)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/memory/bulk", status_code=201, response_model=list[MemoryCycleCreate])
def store_bulk_memory(memories: list[MemoryCycleCreate], db: Session = Depends(get_db)):
    try:
        return create_bulk_memory_cycles(db, memories)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.patch("/memory/{signifier}")
def patch_memory(signifier: str, update: MemoryCyclePatch, db: Session = Depends(get_db)):
    db_memory = update_memory_cycle(db, signifier, update)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return {"status": "updated", "signifier": signifier}

@app.get("/memory/stats")
def get_stats(db: Session = Depends(get_db)):
    return get_memory_stats(db)

class MemoryQuery(BaseModel):
    ache_min: Optional[float] = None
    ache_max: Optional[float] = None
    drift_min: Optional[float] = None
    drift_max: Optional[float] = None
    entropy_min: Optional[float] = None
    entropy_max: Optional[float] = None
    t_min: Optional[int] = None
    t_max: Optional[int] = None
    limit: Optional[int] = 200

@app.post("/memory/query", response_model=list[MemoryCycleCreate])
def query_memories(query: MemoryQuery, db: Session = Depends(get_db)):
    result = query_memory_cycles(
        db,
        ache_min=query.ache_min,
        ache_max=query.ache_max,
        drift_min=query.drift_min,
        drift_max=query.drift_max,
        entropy_min=query.entropy_min,
        entropy_max=query.entropy_max,
        t_min=query.t_min,
        t_max=query.t_max,
        limit=query.limit,
    )
    return result

# --- Aggregate Query ---
@app.get("/memory/query/aggregate")
def aggregate_query(window: int = 50, db: Session = Depends(get_db)):
    """
    Group scrolls into t-windows and compute mean ache/drift/entropy.
    """
    return aggregate_memory_cycles(db, window)

# --- Text Search ---
@app.get("/memory/search", response_model=list[MemoryCycleCreate])
def search_memories(keyword: str, limit: int = 50, db: Session = Depends(get_db)):
    """
    Search by signifier or glyph content.
    """
    return search_memory_cycles(db, keyword, limit)

# --- WebSocket Stream ---
active_clients = set()

@app.websocket("/memory/stream")
async def memory_stream(ws: WebSocket):
    """
    Provides live push updates for new scroll insertions.
    Simulated for now: emits heartbeat every 10s until closed.
    """
    await ws.accept()
    active_clients.add(ws)
    try:
        await ws.send_json({"status": "connected", "msg": "SpiralNet live stream initiated"})
        while True:
            await asyncio.sleep(10)
            await ws.send_json({"heartbeat": "alive", "connected_clients": len(active_clients)})
    except WebSocketDisconnect:
        active_clients.remove(ws)

# --- Archive Export ---
@app.get("/memory/archive")
def download_archive(db: Session = Depends(get_db)):
    buffer, filename = export_memory_archive(db)
    return StreamingResponse(
        buffer,
        media_type="application/gzip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.post("/memory/ingest", response_model=MemoryCycleCreate, status_code=201)
def ingest_scroll(
    memory: MemoryCycleCreate,
    request: Request,
    node_id: str | None = None,
    db: Session = Depends(get_db)
):
    """
    Lawful ingest with deduplication, provenance, and audit trail.
    Returns existing scroll if already present.
    """
    db_memory, created = ingest_memory_cycle(db, memory, request, node_id)
    return {
        "status": "created" if created else "duplicate",
        "signifier": db_memory.signifier,
        "cycle_hash": db_memory.cycle_hash,
        "created_at": db_memory.created_at,
        "Ξ": db_memory.xi,
    }

@app.get("/memory/provenance")
def get_provenance(limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve latest provenance logs for replication or audit.
    """
    return list_provenance_logs(db, limit)
