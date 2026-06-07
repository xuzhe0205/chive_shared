"""Pydantic schemas for /api/dashboard/* endpoints."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

MetricKey = Literal[
    "risk", "day_type", "nav_change", "health",
    "conviction", "cash_pct", "largest_position",
]
MetricColor = Literal["success", "danger", "warning", "info", "neutral"]


class DashboardMetric(BaseModel):
    key: MetricKey
    label: str
    value: str
    value_color: MetricColor
    subtitle: str | None = None


class DashboardSnapshot(BaseModel):
    portfolio: str
    metrics: list[DashboardMetric]
    source_run_id: str | None
    source_run_at: datetime | None
    is_stale: bool


ActionVerb = Literal["TRIM", "ADD", "HOLD", "BUY", "SELL"]
Severity = Literal["high", "medium", "low"]


class ActionItem(BaseModel):
    ticker: str
    action: ActionVerb
    headline: str
    rationale_short: str
    conviction: float = Field(ge=0, le=10)
    severity: Severity


class ActionItemsResponse(BaseModel):
    portfolio: str
    source_run_id: str | None
    actions: list[ActionItem]
    holdcount_others: int
