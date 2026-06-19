"""
TITAN PRIME — Journal API
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, JournalEntry, Trade

router = APIRouter(prefix="/api/journal", tags=["journal"])


class JournalEntryCreate(BaseModel):
    trade_id: Optional[int] = None
    sym: str
    direction: str
    entry_reason: Optional[str] = None
    exit_reason: Optional[str] = None
    lessons: Optional[str] = None
    chart_path: Optional[str] = None


def _entry_to_dict(e: JournalEntry) -> dict:
    return {
        "id": e.id,
        "trade_id": e.trade_id,
        "sym": e.sym,
        "direction": e.direction,
        "entry_reason": e.entry_reason,
        "exit_reason": e.exit_reason,
        "lessons": e.lessons,
        "chart_path": e.chart_path,
        "created_at": e.created_at.isoformat() if e.created_at else None,
    }


@router.get("")
async def list_journal(
    sym: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    session: AsyncSession = Depends(get_session),
):
    """List journal entries."""
    q = select(JournalEntry).order_by(desc(JournalEntry.created_at))
    if sym:
        q = q.where(JournalEntry.sym == sym)
    q = q.offset(offset).limit(limit)
    result = await session.execute(q)
    entries = result.scalars().all()
    return {"entries": [_entry_to_dict(e) for e in entries]}


@router.get("/{entry_id}")
async def get_journal_entry(entry_id: int, session: AsyncSession = Depends(get_session)):
    """Get a single journal entry."""
    result = await session.execute(select(JournalEntry).where(JournalEntry.id == entry_id))
    e = result.scalar_one_or_none()
    if not e:
        raise HTTPException(status_code=404, detail="Journal entry not found")
    return _entry_to_dict(e)


@router.post("")
async def create_journal_entry(
    data: JournalEntryCreate,
    session: AsyncSession = Depends(get_session),
):
    """Create a new journal entry."""
    entry = JournalEntry(
        trade_id=data.trade_id,
        sym=data.sym,
        direction=data.direction,
        entry_reason=data.entry_reason,
        exit_reason=data.exit_reason,
        lessons=data.lessons,
        chart_path=data.chart_path,
        created_at=datetime.utcnow(),
    )
    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return _entry_to_dict(entry)


@router.put("/{entry_id}")
async def update_journal_entry(
    entry_id: int,
    data: JournalEntryCreate,
    session: AsyncSession = Depends(get_session),
):
    """Update a journal entry."""
    result = await session.execute(select(JournalEntry).where(JournalEntry.id == entry_id))
    e = result.scalar_one_or_none()
    if not e:
        raise HTTPException(status_code=404, detail="Journal entry not found")
    if data.entry_reason is not None:
        e.entry_reason = data.entry_reason
    if data.exit_reason is not None:
        e.exit_reason = data.exit_reason
    if data.lessons is not None:
        e.lessons = data.lessons
    if data.chart_path is not None:
        e.chart_path = data.chart_path
    await session.commit()
    await session.refresh(e)
    return _entry_to_dict(e)


@router.delete("/{entry_id}")
async def delete_journal_entry(entry_id: int, session: AsyncSession = Depends(get_session)):
    """Delete a journal entry."""
    result = await session.execute(select(JournalEntry).where(JournalEntry.id == entry_id))
    e = result.scalar_one_or_none()
    if not e:
        raise HTTPException(status_code=404, detail="Journal entry not found")
    await session.delete(e)
    await session.commit()
    return {"deleted": True}
