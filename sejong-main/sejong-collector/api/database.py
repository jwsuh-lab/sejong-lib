"""SQLAlchemy 엔진 + 세션 설정 (WAL 모드)"""

from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase

DB_PATH = Path(__file__).resolve().parent.parent / "sejong.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)


# WAL 모드 + 성능 프라그마
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA busy_timeout=5000;")
    cursor.close()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI Depends용 세션 제너레이터"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_background_session():
    """백그라운드 태스크(PipelineRunner 등)용 독립 DB 세션"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """테이블 생성 (최초 실행 시)"""
    from api.models import Document, Site, PipelineRun, PipelineLog, Setting  # noqa: F401
    Base.metadata.create_all(bind=engine)
