from sqlalchemy import Column, Integer, String, DateTime, JSON, func, Index
from src.db import Base

class DQRejection(Base):
    __tablename__ = "dq_rejections"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)      # p.ej. hired_employees.csv
    table_name = Column(String, nullable=False)  # p.ej. hired_employees
    reason = Column(String, nullable=False)      # motivo del rechazo
    row_data = Column(JSON, nullable=False)      # fila original
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    run_id = Column(String, nullable=False)
    row_hash = Column(String, nullable=False)


Index("idx_dq_rejections_created_at", DQRejection.created_at)
