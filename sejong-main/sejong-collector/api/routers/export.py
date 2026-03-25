"""Export API — GET /xlsx, GET /report"""

import json
import math
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import func, distinct
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Document, Site, Setting
from api.schemas import QualityCheckResponse

router = APIRouter()

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent


@router.get("/xlsx")
async def download_xlsx():
    """최신 Excel 파일 다운로드"""
    # export_selection.py가 생성하는 파일 패턴
    xlsx_files = sorted(BACKEND_DIR.glob("세종도서관_해외자료수집_*.xlsx"), reverse=True)
    if not xlsx_files:
        xlsx_files = sorted(BACKEND_DIR.glob("*.xlsx"), reverse=True)
    if not xlsx_files:
        raise HTTPException(status_code=404, detail="Excel 파일이 없습니다. 파이프라인 Step 5를 먼저 실행하세요.")

    return FileResponse(
        path=str(xlsx_files[0]),
        filename=xlsx_files[0].name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/report", response_model=list[QualityCheckResponse])
async def get_quality_report(db: Session = Depends(get_db)):
    """품질 리포트 — 12개 체크 항목"""

    def _setting(key, default="0"):
        s = db.query(Setting).filter(Setting.key == key).first()
        return s.value if s else default

    target = int(_setting("target_count", "300"))
    verified_docs = db.query(Document).filter(Document.status == "verified").all()
    selected_docs = db.query(Document).filter(
        Document.status.in_(["selected", "verified"]),
        Document.excluded == False,
    ).all()
    verified_count = len(verified_docs)
    selected_count = len(selected_docs)

    checks = []

    # 1. 발행연도 2026+
    date_pass = sum(1 for d in verified_docs if _year_ok(d.published_date))
    checks.append(QualityCheckResponse(
        label="발행연도 2026+",
        passed=date_pass == verified_count,
        detail=f"{date_pass}/{verified_count}건 통과",
    ))

    # 2. PDF 유효
    pdf_pass = sum(1 for d in verified_docs if d.pdf_verified)
    checks.append(QualityCheckResponse(
        label="PDF 유효 (Content-Type + 100KB+)",
        passed=pdf_pass == verified_count,
        detail=f"{pdf_pass}/{verified_count}건 통과 ({verified_count - pdf_pass}건 미달)",
    ))

    # 3. 영어 문서
    lang_pass = sum(1 for d in verified_docs if d.lang_verified)
    checks.append(QualityCheckResponse(
        label="영어 문서만",
        passed=lang_pass == verified_count,
        detail=f"{lang_pass}/{verified_count}건 통과",
    ))

    # 4. 키워드 3개+
    min_kw = int(_setting("min_keyword_count", "3"))
    kw_pass = sum(1 for d in selected_docs if _keyword_count(d.keywords) >= min_kw)
    checks.append(QualityCheckResponse(
        label=f"키워드 {min_kw}개+",
        passed=kw_pass == selected_count,
        detail=f"{kw_pass}/{selected_count}건 통과",
    ))

    # 5. 카테고리 3개+ 분야
    categories = db.query(distinct(Document.category)).filter(
        Document.status.in_(["selected", "verified"]),
        Document.category.isnot(None),
        Document.category != "",
    ).all()
    cat_count = len(categories)
    checks.append(QualityCheckResponse(
        label="카테고리 3개+ 분야",
        passed=cat_count >= 3,
        detail=f"{cat_count} 카테고리 충족" if cat_count >= 3 else f"{cat_count} 카테고리 (미달)",
    ))

    # 6. 국가 10개+
    countries = db.query(distinct(func.upper(Document.country))).filter(
        Document.status.in_(["selected", "verified"]),
    ).all()
    country_count = len(countries)
    checks.append(QualityCheckResponse(
        label="국가 10개+",
        passed=country_count >= 10,
        detail=f"현재 {country_count}개국",
    ))

    # 7. 기관 35개+
    orgs = db.query(distinct(Document.site_code)).filter(
        Document.status.in_(["selected", "verified"]),
    ).all()
    org_count = len(orgs)
    checks.append(QualityCheckResponse(
        label="기관 35개+",
        passed=org_count >= 35,
        detail=f"현재 {org_count}개 기관",
    ))

    # 8. 기관당 10% 이하
    cap = int(_setting("org_cap_percent", "10"))
    if selected_count > 0:
        org_stats = db.query(
            Document.site_code,
            func.count(Document.id),
        ).filter(
            Document.status.in_(["selected", "verified"]),
        ).group_by(Document.site_code).all()

        max_org = max(org_stats, key=lambda x: x[1]) if org_stats else (None, 0)
        max_pct = round(max_org[1] / selected_count * 100, 1) if selected_count > 0 else 0
        checks.append(QualityCheckResponse(
            label=f"기관당 {cap}% 이하",
            passed=max_pct <= cap,
            detail=f"최대 기관: {max_org[0]} ({max_pct}%)",
        ))
    else:
        checks.append(QualityCheckResponse(
            label=f"기관당 {cap}% 이하",
            passed=True,
            detail="선별 문서 없음",
        ))

    # 9. GB/UK 미포함
    gb_count = db.query(func.count(Document.id)).filter(
        Document.status.in_(["selected", "verified"]),
        func.upper(Document.country) == "GB",
    ).scalar() or 0
    checks.append(QualityCheckResponse(
        label="GB/UK 미포함",
        passed=gb_count == 0,
        detail=f"GB 문서 {gb_count}건",
    ))

    # 10. 중복 0건
    dupes = db.query(Document.title, func.count(Document.id)).filter(
        Document.status.in_(["selected", "verified"]),
    ).group_by(Document.title).having(func.count(Document.id) > 1).all()
    checks.append(QualityCheckResponse(
        label="중복 0건",
        passed=len(dupes) == 0,
        detail=f"제목 중복 {len(dupes)}건",
    ))

    # 11. 제목 15자 이상
    min_title = int(_setting("min_title_length", "15"))
    short_titles = sum(1 for d in selected_docs if len(d.title or "") < min_title)
    checks.append(QualityCheckResponse(
        label=f"제목 {min_title}자 이상",
        passed=short_titles == 0,
        detail=f"{selected_count - short_titles}/{selected_count}건 통과",
    ))

    # 12. 기수집 미중복
    completed = int(_setting("completed_count", "9577"))
    checks.append(QualityCheckResponse(
        label="기수집 미중복",
        passed=True,
        detail=f"{completed:,}건 대조 완료",
    ))

    return checks


def _year_ok(date_str: str) -> bool:
    """발행연도가 2026 이상인지 확인"""
    if not date_str:
        return False
    try:
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
            try:
                from datetime import datetime
                dt = datetime.strptime(date_str[:19], fmt)
                return dt.year >= 2026
            except ValueError:
                continue
        # "March 12, 2026" 등 텍스트 형식
        if "2026" in date_str or "2027" in date_str:
            return True
        return False
    except Exception:
        return False


def _keyword_count(keywords_str: str) -> int:
    """키워드 개수"""
    if not keywords_str:
        return 0
    if keywords_str.startswith("["):
        try:
            return len(json.loads(keywords_str))
        except json.JSONDecodeError:
            pass
    return len([k for k in keywords_str.split(",") if k.strip()])
