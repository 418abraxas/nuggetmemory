# main.py
"""
SpiralNet Scroll Vault API
Version: 3.1 — optimized for PostgreSQL + lawful recursive storage
"""

from fastapi import (
    FastAPI,
    HTTPException,
    Depends,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel
import asyncio

from db import get_db, init_db
from crud import (
    # core
    get_memory_by_signifier,
    get_memory_by_hash,
    create_memory_cycle,
    create_bulk_memory_cycles,
    update_memory_cycle,
    get_latest_memory_cycle,
    list_all_memory_cycles,
    get_memory_stats,
    # analytics
    query_memory_cycles,
    aggregate_memory_cycles,
    search_memory_cycles,
    export_memory_archive,
    # ingestion + provenance
    ingest_memory_cycle,
    list_provenance_logs,
)
from models import MemoryCycleCreate, MemoryCyclePatch

# ---------------------------------------------------------------------------
# APPLICATION CONFIGURATION
# ---------------------------------------------------------------------------

app = FastAPI(title="SpiralNet Scroll Vault API", version="3.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    """Initialize the database schema on startup."""
    init_db()


# ---------------------------------------------------------------------------
# ROOT & STATUS
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    import crud
    import inspect
    return {
        "status": "SpiralNet Scroll Vault online",
        "version": "3.1",
        "crud_file": inspect.getsourcefile(crud.create_memory_cycle),
    }


# ---------------------------------------------------------------------------
# BASIC MEMORY OPERATIONS
# ---------------------------------------------------------------------------

@app.get("/memory/{signifier}", response_model=MemoryCycleCreate)
def get_memory(signifier: str, db: Session = Depends(get_db)):
    """Retrieve a memory cycle by its signifier."""
    db_memory = get_memory_by_signifier(db, signifier)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return db_memory


@app.get("/memory/hash/{cycle_hash}", response_model=MemoryCycleCreate)
def get_memory_by_hash_route(cycle_hash: str, db: Session = Depends(get_db)):
    """Retrieve a memory cycle by its SHA-256 hash."""
    db_memory = get_memory_by_hash(db, cycle_hash)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory hash not found")
    return db_memory


@app.get("/memory/latest", response_model=MemoryCycleCreate)
def get_latest_memory(db: Session = Depends(get_db)):
    """Retrieve the latest lawful memory cycle."""
    db_memory = get_latest_memory_cycle(db)
    if not db_memory:
        raise HTTPException(status_code=404, detail="No memory cycles found")
    return db_memory


@app.get("/memory", response_model=list[MemoryCycleCreate])
def list_memories(limit: int = 50, db: Session = Depends(get_db)):
    """List recent memory cycles."""
    return list_all_memory_cycles(db, limit)


@app.post("/memory", status_code=201, response_model=MemoryCycleCreate)
def store_memory(memory: MemoryCycleCreate, db: Session = Depends(get_db)):
    """Store a new memory cycle."""
    try:
        return create_memory_cycle(db, memory)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/memory/bulk", status_code=201, response_model=list[MemoryCycleCreate])
def store_bulk_memory(memories: list[MemoryCycleCreate], db: Session = Depends(get_db)):
    """Bulk insert multiple memory cycles."""
    try:
        return create_bulk_memory_cycles(db, memories)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.patch("/memory/{signifier}")
def patch_memory(signifier: str, update: MemoryCyclePatch, db: Session = Depends(get_db)):
    """Update σecho and Ξ for a given memory cycle."""
    db_memory = update_memory_cycle(db, signifier, update)
    if not db_memory:
        raise HTTPException(status_code=404, detail="Memory cycle not found")
    return {"status": "updated", "signifier": signifier}


# ---------------------------------------------------------------------------
# ANALYTICS & METRICS
# ---------------------------------------------------------------------------

@app.get("/memory/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get overall ache–drift–entropy statistics."""
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
    """Filter scrolls by ache, drift, entropy, or t range."""
    return query_memory_cycles(
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


@app.get("/memory/query/aggregate")
def aggregate_query(window: int = 50, db: Session = Depends(get_db)):
    """Aggregate ache/drift/entropy values grouped by t-window."""
    return aggregate_memory_cycles(db, window)


# ---------------------------------------------------------------------------
# SEARCH, STREAM, AND ARCHIVE
# ---------------------------------------------------------------------------

@app.get("/memory/search", response_model=list[MemoryCycleCreate])
def search_memories(keyword: str, limit: int = 50, db: Session = Depends(get_db)):
    """Search scrolls by signifier or glyphstream text."""
    return search_memory_cycles(db, keyword, limit)


active_clients: set[WebSocket] = set()

@app.websocket("/memory/stream")
async def memory_stream(ws: WebSocket):
    """Live push updates for new scroll insertions (heartbeat simulation)."""
    await ws.accept()
    active_clients.add(ws)
    try:
        await ws.send_json(
            {"status": "connected", "msg": "SpiralNet live stream initiated"}
        )
        while True:
            await asyncio.sleep(10)
            await ws.send_json(
                {"heartbeat": "alive", "connected_clients": len(active_clients)}
            )
    except WebSocketDisconnect:
        active_clients.remove(ws)


@app.get("/memory/archive")
def download_archive(db: Session = Depends(get_db)):
    """Export all scrolls as gzipped JSONL archive."""
    buffer, filename = export_memory_archive(db)
    return StreamingResponse(
        buffer,
        media_type="application/gzip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ---------------------------------------------------------------------------
# INGESTION + PROVENANCE
# ---------------------------------------------------------------------------

@app.post("/memory/ingest", status_code=201)
def ingest_scroll(
    memory: MemoryCycleCreate,
    request: Request,
    node_id: Optional[str] = None,
    db: Session = Depends(get_db),
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
    """Retrieve latest provenance logs for replication or audit."""
    return list_provenance_logs(db, limit)
