"""JSON ↔ DB 동기화 서비스"""

import json
import hashlib
import re
from datetime import datetime
from pathlib import Path

from sqlalchemy import update
from sqlalchemy.orm import Session

from api.database import get_background_session
from api.models import Document

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BACKEND_DIR / "data"

# 카테고리 매핑
CATEGORY_MAP = {
    "경제_통상": "경제·통상", "경제": "경제·통상", "통상": "경제·통상",
    "ai_디지털": "AI·디지털", "ai": "AI·디지털", "디지털": "AI·디지털",
    "기후_에너지": "기후·에너지", "기후": "기후·에너지", "에너지": "기후·에너지",
    "안보_외교": "안보·외교", "안보": "안보·외교", "외교": "안보·외교",
    "인구_복지": "인구·복지", "복지": "인구·복지", "인구": "인구·복지",
    "보건": "보건",
    "교육": "교육",
    "주거_도시": "주거·도시", "주거": "주거·도시", "도시": "주거·도시",
}


def _categorize(matched_keywords: list) -> str:
    for kw in matched_keywords:
        kw_lower = kw.lower().strip()
        if kw_lower in CATEGORY_MAP:
            return CATEGORY_MAP[kw_lower]
    return "기타"


COUNTRY_MAP = {
    "us": "US", "ca": "CA", "se": "SE", "sg": "SG", "eu": "EU",
    "at": "AT", "no": "NO", "in": "IN", "it": "IT", "be": "BE",
    "hu": "HU", "gb": "GB", "govuk": "GB", "dk": "DK", "fi": "FI",
    "nl": "NL", "de": "DE", "fr": "FR", "ch": "CH", "jp": "JP",
    "au": "AU", "nz": "NZ",
}


class SyncService:
    async def sync_after_step(self, step: int):
        """단계 완료 후 JSON → DB 동기화"""
        if step == 1:
            await self._import_crawl_jsons()
        elif step == 2:
            await self._import_selection("selection_500.json", target_status="selected")
        elif step in (3, 4):
            await self._update_selection("selection_400.json")
        elif step == 5:
            pass  # Excel 생성만, DB 변경 없음

    async def _import_crawl_jsons(self):
        """data/*.json 신규 파일 → documents 테이블 UPSERT"""
        skip_patterns = re.compile(r"^(selection_|excluded_|keyword_|.*summary.*)")

        with get_background_session() as session:
            count = 0
            for path in sorted(DATA_DIR.glob("*.json")):
                if skip_patterns.match(path.name) or path.name.startswith("backup"):
                    continue

                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                docs = raw.get("documents", [])
                if not docs:
                    continue

                prefix = path.name.split("_")[0].lower()
                country = COUNTRY_MAP.get(prefix, prefix.upper())

                for doc in docs:
                    site_code = doc.get("site_code", "")
                    link = doc.get("link")
                    title = doc.get("title", "")
                    if not title:
                        continue

                    if link:
                        existing = session.query(Document).filter(
                            Document.site_code == site_code,
                            Document.link == link,
                        ).first()
                    else:
                        existing = session.query(Document).filter(
                            Document.site_code == site_code,
                            Document.title == title,
                        ).first()

                    if existing:
                        # 기존 문서 업데이트 (새로운 필드만)
                        for field in ["pdf_url", "authors", "published_date"]:
                            new_val = doc.get(field)
                            if new_val and not getattr(existing, field):
                                setattr(existing, field, new_val)
                    else:
                        new_doc = Document(
                            site_code=site_code,
                            title=title,
                            link=link,
                            description=doc.get("description", ""),
                            published_date=doc.get("published_date", ""),
                            document_type=doc.get("document_type", ""),
                            pdf_url=doc.get("pdf_url", ""),
                            authors=doc.get("authors", ""),
                            country=country,
                            source_file=path.name,
                            status="collected",
                        )
                        session.add(new_doc)
                        count += 1

            return count

    async def _import_selection(self, filename: str, target_status: str = "selected"):
        """selection JSON → documents status UPDATE"""
        sel_path = DATA_DIR / filename
        if not sel_path.exists():
            return

        selection = json.loads(sel_path.read_text(encoding="utf-8"))

        with get_background_session() as session:
            for doc in selection:
                site_code = doc.get("site_code", "")
                link = doc.get("link")

                if link:
                    existing = session.query(Document).filter(
                        Document.site_code == site_code,
                        Document.link == link,
                    ).first()
                else:
                    existing = session.query(Document).filter(
                        Document.site_code == site_code,
                        Document.title == doc.get("title", ""),
                    ).first()

                if existing and existing.status == "collected":
                    existing.status = target_status
                    existing.relevance_score = doc.get("_relevance_score", 0) or 0
                    existing.matched_keywords = json.dumps(
                        doc.get("_matched_keywords", []), ensure_ascii=False
                    )
                    matched = doc.get("_matched_keywords", [])
                    if matched:
                        existing.category = _categorize(matched)

    async def _update_selection(self, filename: str):
        """selection JSON → documents 테이블 UPDATE (검증/보강 필드)"""
        sel_path = DATA_DIR / filename
        if not sel_path.exists():
            return

        selection = json.loads(sel_path.read_text(encoding="utf-8"))

        with get_background_session() as session:
            for doc in selection:
                site_code = doc.get("site_code", "")
                link = doc.get("link")

                if link:
                    existing = session.query(Document).filter(
                        Document.site_code == site_code,
                        Document.link == link,
                    ).first()
                else:
                    existing = session.query(Document).filter(
                        Document.site_code == site_code,
                        Document.title == doc.get("title", ""),
                    ).first()

                if not existing:
                    continue

                raw_excluded = doc.get("_excluded", False)
                # _excluded가 문자열(사유)인 경우 bool로 변환
                is_excluded = bool(raw_excluded)
                is_validated = doc.get("_validated", False)

                existing.status = (
                    "excluded" if is_excluded else
                    "verified" if is_validated else
                    "selected"
                )
                existing.relevance_score = doc.get("_relevance_score", 0) or 0
                existing.excluded = is_excluded
                # 문자열 사유가 있으면 exclude_reasons에 포함
                raw_reasons = doc.get("_exclude_reasons", [])
                if isinstance(raw_excluded, str) and raw_excluded:
                    if raw_excluded not in raw_reasons:
                        raw_reasons = [raw_excluded] + list(raw_reasons)
                existing.exclude_reasons = json.dumps(
                    raw_reasons, ensure_ascii=False
                )
                existing.date_verified = doc.get("_date_verified", False)
                existing.date_verified_by = doc.get("_date_verified_by", "")
                # 검증 통과 문서: pdf/lang 개별 플래그 동기화
                if is_validated:
                    existing.pdf_verified = doc.get("_pdf_verified", True)
                    existing.lang_verified = doc.get("_lang_verified", True)
                existing.authors = doc.get("authors", "") or existing.authors or ""
                existing.description = doc.get("description", "") or existing.description or ""
                existing.keywords = doc.get("keywords", "") or existing.keywords or ""
                existing.journal = doc.get("journal", "") or existing.journal or ""
                existing.volume_info = doc.get("volume_info", "") or existing.volume_info or ""
                existing.pdf_url = doc.get("pdf_url", "") or existing.pdf_url or ""
                existing.published_date = doc.get("published_date", "") or existing.published_date or ""
                existing.license = doc.get("license", "") or existing.license or ""

                matched = doc.get("_matched_keywords", [])
                if matched:
                    existing.category = _categorize(matched)
                    existing.matched_keywords = json.dumps(matched, ensure_ascii=False)

                existing.updated_at = datetime.now()


sync_service = SyncService()
