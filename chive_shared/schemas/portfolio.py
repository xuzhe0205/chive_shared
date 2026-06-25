"""Pydantic schemas for /api/portfolios/* endpoints."""
from datetime import date, datetime

from pydantic import BaseModel


class HoldingMover(BaseModel):
    ticker: str
    shares: float
    last_close: float
    pct_change_today: float
    pct_of_nav: float


class LargestPosition(BaseModel):
    ticker: str
    pct_of_nav: float


class SleeveSummary(BaseModel):
    """Per-sleeve allocation summary. Populated for v3 portfolios only."""
    sleeve: str  # 'core' | 'speculative_growth' | 'tactical'
    target_pct: float
    actual_pct: float
    total_value_usd: float
    position_count: int


class HoldingsSummary(BaseModel):
    portfolio: str
    total_value_usd: float
    cash_usd: float
    position_count: int
    largest_position: LargestPosition | None
    top_movers: list[HoldingMover]
    data_as_of: datetime
    # M41-T13 — v3-only fields (None for v2 portfolios)
    schema_version: int = 2
    sleeve_summary: list[SleeveSummary] | None = None


class JournalTrade(BaseModel):
    ticker: str
    realized_gain_pct: float
    closed_date: date


class LastClosedTrade(BaseModel):
    ticker: str
    closed_date: date
    realized_gain_usd: float
    realized_gain_pct: float


class JournalSummary(BaseModel):
    portfolio: str
    closed_trade_count: int
    win_rate: float
    avg_realized_gain_pct: float
    best_trade: JournalTrade | None
    worst_trade: JournalTrade | None
    last_closed_trade: LastClosedTrade | None
    total_realized_pnl_ytd: float
