"""파이프라인 API — POST /run, /run-step, /stop, GET /status, /logs, /history"""

import asyncio
import json
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from api.database import get_db, get_background_session
from api.models import PipelineRun, PipelineLog
from api.schemas import (
    PipelineRunRequest, PipelineRunResponse,
    PipelineStatusResponse, PipelineHistoryItem,
)
from api.services.pipeline_runner import runner
from api.services.log_broadcaster import broadcaster

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent

router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
async def run_pipeline(req: PipelineRunRequest = None):
    """전체 또는 지정 단계 실행"""
    if runner.is_running:
        return PipelineRunResponse(runId=-1)

    steps = req.steps if req and req.steps else None

    # 백그라운드에서 실행
    async def _run():
        await runner.run_all(steps)

    asyncio.create_task(_run())

    # run_id를 위해 잠시 대기
    await asyncio.sleep(0.2)
    run_id = runner._current_run_id or 0
    return PipelineRunResponse(runId=run_id)


@router.post("/run-step")
async def run_single_step(step: int):
    """개별 단계 실행"""
    if runner.is_running:
        return {"error": "파이프라인이 이미 실행 중입니다"}

    async def _run():
        await runner.run_all([step])

    asyncio.create_task(_run())
    await asyncio.sleep(0.2)
    return {"ok": True, "step": step}


@router.post("/stop")
async def stop_pipeline():
    """실행 중인 파이프라인 중지"""
    await runner.stop()
    return {"stopped": True}


@router.get("/status", response_model=PipelineStatusResponse)
async def get_status(db: Session = Depends(get_db)):
    """현재 파이프라인 상태 + 최근 실행 결과"""
    # 기본 상태
    resp = PipelineStatusResponse(
        currentStep=runner.current_step,
        steps={str(k): v for k, v in runner.step_statuses.items()},
        progress=runner.progress,
        isRunning=runner.is_running,
    )

    # 최근 실행 결과 조회
    run_id = runner._current_run_id
    if run_id:
        run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
    else:
        run = db.query(PipelineRun).order_by(desc(PipelineRun.id)).first()

    if run:
        resp.runId = run.id
        resp.startedAt = run.started_at.isoformat() if run.started_at else None
        resp.finishedAt = run.finished_at.isoformat() if run.finished_at else None
        resp.finalStatus = run.status
        resp.totalCollected = run.total_collected or 0
        resp.totalSelected = run.total_selected or 0
        resp.totalVerified = run.total_verified or 0
        resp.totalExcluded = run.total_excluded or 0
        resp.errorMessage = run.error_message

    # Excel 파일 존재 여부
    resp.hasExcelFile = bool(list(BACKEND_DIR.glob("*.xlsx")) or list((BACKEND_DIR / "data").glob("*.xlsx")))

    return resp


@router.get("/history", response_model=list[PipelineHistoryItem])
async def get_history(db: Session = Depends(get_db)):
    """파이프라인 실행 이력"""
    runs = db.query(PipelineRun).order_by(desc(PipelineRun.id)).limit(20).all()
    return [
        PipelineHistoryItem(
            id=r.id,
            startedAt=r.started_at.isoformat() if r.started_at else None,
            finishedAt=r.finished_at.isoformat() if r.finished_at else None,
            status=r.status or "unknown",
            totalCollected=r.total_collected or 0,
            totalSelected=r.total_selected or 0,
            totalVerified=r.total_verified or 0,
            totalExcluded=r.total_excluded or 0,
            errorMessage=r.error_message,
        )
        for r in runs
    ]


@router.get("/logs")
async def stream_logs(request: Request):
    """SSE 로그 스트리밍"""
    last_id = int(request.headers.get("Last-Event-Id", "0"))
    queue = broadcaster.subscribe()

    async def event_generator():
        try:
            # Phase 1: 재연결 시 DB에서 누락분 전송
            if last_id > 0:
                with get_background_session() as session:
                    missed = session.query(PipelineLog).filter(
                        PipelineLog.id > last_id
                    ).order_by(PipelineLog.id).all()
                    for log in missed:
                        yield f"id: {log.id}\ndata: {json.dumps(log.to_dict())}\n\n"

            # Phase 2: 큐 기반 실시간 스트리밍
            while True:
                if await request.is_disconnected():
                    break
                try:
                    log = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"id: {log.get('id', '')}\ndata: {json.dumps(log, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            broadcaster.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
