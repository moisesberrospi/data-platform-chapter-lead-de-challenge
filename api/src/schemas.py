from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


TableName = Literal["departments", "jobs", "hired_employees"]
ModeName = Literal["strict", "partial"]


class TransactionRequest(BaseModel):
    table: TableName
    mode: ModeName = "strict"
    rows: List[Dict[str, Any]] = Field(..., min_length=1, max_length=1000)


class TransactionResponse(BaseModel):
    run_id: str
    table: str
    mode: str
    received: int
    inserted: int
    rejected: int
    reasons: Dict[str, int]
