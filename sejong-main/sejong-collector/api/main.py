"""FastAPI 앱 + CORS 설정 + 라우터 등록"""

import os
import traceback

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db, init_db
from api.routers import dashboard, documents, pipeline, export, settings, sites

app = FastAPI(title="세종도서관 수집 API", version="1.0.0")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "traceback": traceback.format_exc()},
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(documents.router, prefix="/api/documents", tags=["documents"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])
app.include_router(sites.router, prefix="/api/sites", tags=["sites"])


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/api/health")
async def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "db": db_ok,
        "version": "1.0.0",
    }
