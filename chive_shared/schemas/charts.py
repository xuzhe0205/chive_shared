"""Pydantic schemas for /api/charts/* endpoints."""
from typing import Literal

from pydantic import BaseModel

ChartVerdict = Literal["BUY", "HOLD", "SELL", "TRIM", "ADD"]


class Candle(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class MASeries(BaseModel):
    time: str
    value: float


class ChartData(BaseModel):
    candles: list[Candle]
    sma_50: list[MASeries]
    sma_200: list[MASeries]


class ChartPreview(BaseModel):
    ticker: str
    verdict: ChartVerdict
    chart_data: ChartData


class ChartPreviewResponse(BaseModel):
    run_id: str
    charts: list[ChartPreview]
