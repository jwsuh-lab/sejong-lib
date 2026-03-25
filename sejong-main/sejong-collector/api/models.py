"""SQLAlchemy ORM 모델"""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, Text, Float, Boolean, DateTime, Index, UniqueConstraint
)
from api.database import Base


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_code = Column(Text, nullable=False, index=True)
    title = Column(Text, nullable=False)
    link = Column(Text)
    description = Column(Text)
    published_date = Column(Text)
    document_type = Column(Text)
    doc_type_kr = Column(Text)
    pdf_url = Column(Text)
    pdf_size_kb = Column(Integer)
    authors = Column(Text)
    country = Column(Text, nullable=False, index=True)

    # 선별/점수
    relevance_score = Column(Float, default=0)
    matched_keywords = Column(Text)  # JSON array
    category = Column(Text)
    status = Column(Text, default="collected", index=True)
    # collected / selected / verified / excluded

    # 검증
    date_verified = Column(Boolean, default=False)
    date_verified_by = Column(Text)
    pdf_verified = Column(Boolean, default=False)
    lang_verified = Column(Boolean, default=False)
    excluded = Column(Boolean, default=False)
    exclude_reasons = Column(Text)  # JSON array

    # 메타데이터
    keywords = Column(Text)
    journal = Column(Text)
    volume_info = Column(Text)
    isbn = Column(Text)
    issn = Column(Text)
    license = Column(Text)
    brm_code1 = Column(Text)
    brm_code2 = Column(Text)
    brm_code1_2 = Column(Text)
    brm_code2_2 = Column(Text)

    # 추적
    source_file = Column(Text)
    content_hash = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("site_code", "link", name="uq_documents_site_link"),
        Index("idx_documents_score", relevance_score.desc()),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Site(Base):
    __tablename__ = "sites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(Text, unique=True, nullable=False)
    name = Column(Text)
    name_kr = Column(Text)
    country_code = Column(Text, index=True)
    country = Column(Text)
    org_type = Column(Text)
    acronym = Column(Text)
    url = Column(Text)
    brm_category = Column(Text)
    current_use = Column(Text)
    expected_count = Column(Integer)
    excluded = Column(Boolean, default=False)

    # 스케줄
    schedule_type = Column(Text, default="manual")  # daily/weekly/biweekly/monthly/manual
    schedule_days = Column(Text)  # JSON: [1,3,5]
    schedule_time = Column(Text, default="03:00")  # HH:MM
    last_crawled_at = Column(DateTime)
    next_crawl_at = Column(DateTime)
    crawl_status = Column(Text, default="idle")  # idle/running/error
    last_error = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.now)
    finished_at = Column(DateTime)
    status = Column(Text, default="running")
    # running / completed / partial / failed / stopped
    current_step = Column(Integer)
    progress = Column(Float, default=0)
    total_collected = Column(Integer, default=0)
    total_selected = Column(Integer, default=0)
    total_verified = Column(Integer, default=0)
    total_excluded = Column(Integer, default=0)
    error_message = Column(Text)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class PipelineLog(Base):
    __tablename__ = "pipeline_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, index=True)
    timestamp = Column(Text)
    level = Column(Text)
    step = Column(Text)
    message = Column(Text)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Setting(Base):
    __tablename__ = "settings"

    key = Column(Text, primary_key=True)
    value = Column(Text)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
