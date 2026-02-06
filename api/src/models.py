from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship

from src.db import Base


class Department(Base):
    __tablename__ = "departments"

    id = Column(Integer, primary_key=True)
    department = Column(String, nullable=False)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    job = Column(String, nullable=False)


class HiredEmployee(Base):
    __tablename__ = "hired_employees"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    datetime = Column(DateTime(timezone=True), nullable=False)

    # Nullable para soportar CSV con vacíos; DQ/rechazos se implementa en el siguiente hito
    department_id = Column(Integer, ForeignKey("departments.id"), nullable=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=True)

    department = relationship("Department")
    job = relationship("Job")


Index("idx_hired_datetime", HiredEmployee.datetime)
Index("idx_hired_dept_job_datetime", HiredEmployee.department_id, HiredEmployee.job_id, HiredEmployee.datetime)
