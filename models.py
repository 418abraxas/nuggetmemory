# models.py
from sqlalchemy import Column, Integer, String, Float, Boolean, JSON
from pydantic import BaseModel
from db import Base

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

class MemoryCycleCreate(BaseModel):
    t: int
    signifier: str
    psi_self: dict
    sigma_echo: dict
    glyphstream: list[str]
    ache: float
    drift: float
    entropy: float
    xi: bool

class MemoryCyclePatch(BaseModel):
    sigma_echo: dict
    xi: bool
