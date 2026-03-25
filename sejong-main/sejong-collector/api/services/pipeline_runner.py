"""기존 스크립트를 subprocess로 실행하고 로그를 스트리밍"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from api.database import get_background_session
from api.models import PipelineRun, PipelineLog
from api.services.log_broadcaster import broadcaster
from api.services.sync_service import sync_service

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent


class PipelineStepError(Exception):
    def __init__(self, step: int, script: str):
        self.step = step
        self.script = script
        super().__init__(f"Step {step}: {script} 실패")


class PipelineRunner:
    """기존 스크립트를 subprocess로 실행하고 로그를 스트리밍"""

    DEFAULT_CRAWL_SCRIPTS = [
        {"script": "main.py", "args": ["crawl-us", "--all"], "label": "미국"},
        {"script": "main.py", "args": ["crawl-se", "--all"], "label": "스웨덴"},
        {"script": "main.py", "args": ["crawl-sg", "--all"], "label": "싱가포르"},
        {"script": "crawl_additional.py", "args": [], "label": "추가소스"},
        {"script": "crawl_eprs.py", "args": [], "label": "EPRS"},
        {"script": "crawl_more.py", "args": [], "label": "추가소스2"},
        {"script": "crawl_new_sites.py", "args": [], "label": "신규사이트"},
    ]

    def __init__(self):
        self._current_proc: asyncio.subprocess.Process | None = None
        self._is_running = False
        self._current_step: int | None = None
        self._current_run_id: int | None = None
        self._step_statuses: dict[int, str] = {1: "pending", 2: "pending", 3: "pending", 4: "pending", 5: "pending"}
        self._progress: float = 0
        self._load_steps()

    def _load_steps(self):
        """config.json에서 크롤링 스크립트 목록 동적 로드"""
        try:
            config = json.loads((BACKEND_DIR / "config.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            config = {}

        crawl_scripts = [
            (s["script"], s["args"])
            for s in config.get("crawl_scripts", self.DEFAULT_CRAWL_SCRIPTS)
            if s["script"] not in config.get("crawl_exclude", [])
        ]

        self.STEPS = {
            1: {"name": "크롤링", "scripts": crawl_scripts, "fail_policy": "continue"},
            2: {"name": "선별", "scripts": [("select_300.py", [])], "fail_policy": "abort"},
            3: {"name": "보강", "scripts": [
                ("resolve_dates_v2.py", []),
                ("resolve_pdfs.py", []),
                ("resolve_authors.py", []),
                ("resolve_excerpts.py", []),
                ("enrich_selection.py", []),
                ("metadata_resolver.py", []),
            ], "fail_policy": "continue"},
            4: {"name": "검증", "scripts": [("validate_selection.py", [])], "fail_policy": "abort"},
            5: {"name": "출력", "scripts": [("export_selection.py", [])], "fail_policy": "abort"},
        }

    async def run_all(self, steps: list[int] | None = None):
        """전체 또는 지정 단계 실행"""
        if self._is_running:
            raise RuntimeError("파이프라인이 이미 실행 중입니다")

        self._load_steps()  # 매 실행 시 config 리로드
        self._is_running = True
        steps = steps or [1, 2, 3, 4, 5]

        # pipeline_runs 레코드 생성
        with get_background_session() as session:
            run = PipelineRun(status="running")
            session.add(run)
            session.flush()
            run_id = run.id

        self._current_run_id = run_id
        self._step_statuses = {s: "pending" for s in range(1, 6)}

        final_status = "completed"
        error_msg = None

        try:
            for i, step in enumerate(steps):
                self._current_step = step
                self._step_statuses[step] = "running"
                self._progress = round((i / len(steps)) * 100)

                await self._emit_log(run_id, step, f"Step {step} ({self.STEPS[step]['name']}) 시작")

                try:
                    result = await self.run_step(step, run_id)
                    self._step_statuses[step] = result  # "completed" or "partial"
                    await self._emit_log(run_id, step, f"Step {step} 완료 ({result})")
                except PipelineStepError as e:
                    self._step_statuses[step] = "failed"
                    final_status = "failed"
                    error_msg = str(e)
                    await self._emit_log(run_id, step, f"Step {step} 실패: {e}")
                    break

                # 단계 완료 후 진행률 업데이트
                with get_background_session() as session:
                    run = session.query(PipelineRun).filter(PipelineRun.id == run_id).first()
                    if run:
                        run.current_step = step
                        run.progress = round(((i + 1) / len(steps)) * 100)

            # partial 체크
            if final_status == "completed" and any(
                v == "partial" for v in self._step_statuses.values()
            ):
                final_status = "partial"

        except asyncio.CancelledError:
            final_status = "stopped"
        except Exception as e:
            final_status = "failed"
            error_msg = str(e)
        finally:
            self._is_running = False
            self._current_step = None
            self._progress = 100 if final_status in ("completed", "partial") else self._progress

            # 최종 통계 + 상태 업데이트
            with get_background_session() as session:
                run = session.query(PipelineRun).filter(PipelineRun.id == run_id).first()
                if run:
                    run.finished_at = datetime.now()
                    run.status = final_status
                    run.error_message = error_msg
                    run.progress = self._progress

                    from api.models import Document
                    from sqlalchemy import func
                    run.total_collected = session.query(func.count(Document.id)).scalar() or 0
                    run.total_selected = session.query(func.count(Document.id)).filter(
                        Document.status.in_(["selected", "verified"])
                    ).scalar() or 0
                    run.total_verified = session.query(func.count(Document.id)).filter(
                        Document.status == "verified"
                    ).scalar() or 0
                    run.total_excluded = session.query(func.count(Document.id)).filter(
                        Document.status == "excluded"
                    ).scalar() or 0

            # 완료 요약 로그 emit
            duration = ""
            if run and run.started_at and run.finished_at:
                secs = int((run.finished_at - run.started_at).total_seconds())
                mins, s = divmod(secs, 60)
                duration = f"{mins}분 {s}초" if mins else f"{s}초"

            await self._emit_log(
                run_id, 0,
                f"=== 실행 {final_status} | "
                f"수집 {run.total_collected if run else 0} / "
                f"선별 {run.total_selected if run else 0} / "
                f"검증 {run.total_verified if run else 0} / "
                f"제외 {run.total_excluded if run else 0} | "
                f"소요시간 {duration} ==="
            )

            self._current_run_id = None

        return run_id

    async def run_step(self, step: int, run_id: int) -> str:
        """단일 단계 실행"""
        step_info = self.STEPS[step]
        has_failure = False

        for script, args in step_info["scripts"]:
            script_path = BACKEND_DIR / script
            if not script_path.exists():
                await self._emit_log(run_id, step, f"WARNING: {script} 파일 없음 — 스킵")
                if step_info["fail_policy"] == "abort":
                    raise PipelineStepError(step, script)
                has_failure = True
                continue

            full_args = [sys.executable, str(script_path)] + args

            await self._emit_log(run_id, step, f"실행: {script} {' '.join(args)}")

            self._current_proc = await asyncio.create_subprocess_exec(
                *full_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(BACKEND_DIR),
            )

            async for line in self._current_proc.stdout:
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    await self._emit_log(run_id, step, text)

            await self._current_proc.wait()

            if self._current_proc.returncode != 0:
                await self._emit_log(
                    run_id, step,
                    f"ERROR: {script} 실패 (exit code={self._current_proc.returncode})"
                )
                if step_info["fail_policy"] == "abort":
                    self._current_proc = None
                    raise PipelineStepError(step, script)
                else:
                    has_failure = True

            self._current_proc = None

        # 단계 완료 후 JSON → DB 동기화
        await sync_service.sync_after_step(step)
        return "partial" if has_failure else "completed"

    async def stop(self):
        """실행 중인 프로세스를 안전하게 종료"""
        proc = self._current_proc
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
            self._current_proc = None

        if self._is_running and self._current_run_id:
            with get_background_session() as session:
                run = session.query(PipelineRun).filter(
                    PipelineRun.id == self._current_run_id
                ).first()
                if run:
                    run.status = "stopped"
                    run.finished_at = datetime.now()

        self._is_running = False
        if self._current_step:
            self._step_statuses[self._current_step] = "failed"
        self._current_step = None

    async def _emit_log(self, run_id: int, step: int, text: str):
        level = self._parse_level(text)
        step_name = self.STEPS.get(step, {}).get("name", "요약" if step == 0 else str(step))
        log_entry = {
            "run_id": run_id,
            "step": step_name,
            "message": text,
            "level": level,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }

        with get_background_session() as session:
            db_log = PipelineLog(**log_entry)
            session.add(db_log)
            session.flush()
            log_entry["id"] = db_log.id

        await broadcaster.broadcast(log_entry)

    @staticmethod
    def _parse_level(text: str) -> str:
        text_lower = text.lower()
        if "error" in text_lower or "실패" in text_lower:
            return "error"
        if "warning" in text_lower or "경고" in text_lower or "차단" in text_lower:
            return "warning"
        if "완료" in text_lower or "success" in text_lower or "✓" in text:
            return "success"
        return "info"

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def current_step(self) -> int | None:
        return self._current_step

    @property
    def step_statuses(self) -> dict[int, str]:
        return self._step_statuses

    @property
    def progress(self) -> float:
        return self._progress


# 싱글턴
runner = PipelineRunner()
