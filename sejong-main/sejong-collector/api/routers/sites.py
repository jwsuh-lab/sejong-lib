"""사이트 CRUD API — /api/sites"""

import math

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Site, Document
from api.schemas import (
    SiteResponse,
    SiteListResponse,
    SiteCreateRequest,
    SiteUpdateRequest,
)

router = APIRouter()


def _site_to_response(site: Site, doc_count: int = 0) -> SiteResponse:
    return SiteResponse(
        id=site.id,
        code=site.code,
        name=site.name,
        nameKr=site.name_kr,
        countryCode=site.country_code,
        country=site.country,
        orgType=site.org_type,
        acronym=site.acronym,
        url=site.url,
        brmCategory=site.brm_category,
        currentUse=site.current_use,
        expectedCount=site.expected_count,
        excluded=site.excluded or False,
        scheduleType=site.schedule_type or "manual",
        scheduleDays=site.schedule_days,
        scheduleTime=site.schedule_time or "03:00",
        lastCrawledAt=str(site.last_crawled_at) if site.last_crawled_at else None,
        nextCrawlAt=str(site.next_crawl_at) if site.next_crawl_at else None,
        crawlStatus=site.crawl_status or "idle",
        lastError=site.last_error,
        docCount=doc_count,
    )


@router.get("", response_model=SiteListResponse)
async def list_sites(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    country: str = Query(None),
    search: str = Query(None),
    excluded: bool = Query(None),
):
    query = db.query(Site)

    if country:
        query = query.filter(func.upper(Site.country_code) == country.upper())

    if excluded is not None:
        query = query.filter(Site.excluded == excluded)

    if search:
        term = f"%{search}%"
        query = query.filter(
            or_(
                Site.code.ilike(term),
                Site.name.ilike(term),
                Site.name_kr.ilike(term),
                Site.acronym.ilike(term),
            )
        )

    total = query.count()
    total_pages = max(1, math.ceil(total / size))
    sites = query.order_by(Site.country_code, Site.code).offset((page - 1) * size).limit(size).all()

    # 각 사이트별 문서 수
    site_codes = [s.code for s in sites]
    doc_counts: dict[str, int] = {}
    if site_codes:
        rows = (
            db.query(Document.site_code, func.count(Document.id))
            .filter(Document.site_code.in_(site_codes))
            .group_by(Document.site_code)
            .all()
        )
        doc_counts = dict(rows)

    items = [_site_to_response(s, doc_counts.get(s.code, 0)) for s in sites]

    return SiteListResponse(items=items, total=total, page=page, totalPages=total_pages)


@router.get("/check-code")
async def check_code(code: str = Query(...), db: Session = Depends(get_db)):
    exists = db.query(Site).filter(Site.code == code).first() is not None
    return {"exists": exists}


@router.get("/{site_id}", response_model=SiteResponse)
async def get_site(site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="사이트를 찾을 수 없습니다")
    doc_count = db.query(func.count(Document.id)).filter(Document.site_code == site.code).scalar() or 0
    return _site_to_response(site, doc_count)


@router.post("", response_model=SiteResponse, status_code=201)
async def create_site(req: SiteCreateRequest, db: Session = Depends(get_db)):
    if db.query(Site).filter(Site.code == req.code).first():
        raise HTTPException(status_code=409, detail=f"사이트 코드 '{req.code}'가 이미 존재합니다")

    site = Site(
        code=req.code,
        name=req.name,
        name_kr=req.nameKr,
        country_code=req.countryCode,
        country=req.country,
        org_type=req.orgType,
        acronym=req.acronym,
        url=req.url,
        brm_category=req.brmCategory,
        current_use=req.currentUse,
        expected_count=req.expectedCount,
        excluded=req.excluded,
        schedule_type=req.scheduleType,
        schedule_days=req.scheduleDays,
        schedule_time=req.scheduleTime,
    )
    db.add(site)
    db.commit()
    db.refresh(site)
    return _site_to_response(site)


@router.put("/{site_id}", response_model=SiteResponse)
async def update_site(site_id: int, req: SiteUpdateRequest, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="사이트를 찾을 수 없습니다")

    update_data = req.model_dump(exclude_unset=True)
    field_map = {
        "nameKr": "name_kr",
        "countryCode": "country_code",
        "orgType": "org_type",
        "brmCategory": "brm_category",
        "currentUse": "current_use",
        "expectedCount": "expected_count",
        "scheduleType": "schedule_type",
        "scheduleDays": "schedule_days",
        "scheduleTime": "schedule_time",
    }
    for key, value in update_data.items():
        col_name = field_map.get(key, key)
        setattr(site, col_name, value)

    db.commit()
    db.refresh(site)
    doc_count = db.query(func.count(Document.id)).filter(Document.site_code == site.code).scalar() or 0
    return _site_to_response(site, doc_count)


@router.delete("/{site_id}")
async def delete_site(site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="사이트를 찾을 수 없습니다")

    doc_count = db.query(func.count(Document.id)).filter(Document.site_code == site.code).scalar() or 0
    db.delete(site)
    db.commit()
    return {"ok": True, "deletedCode": site.code, "orphanedDocs": doc_count}


@router.delete("/bulk/delete")
async def bulk_delete_sites(site_ids: list[int], db: Session = Depends(get_db)):
    sites = db.query(Site).filter(Site.id.in_(site_ids)).all()
    if not sites:
        raise HTTPException(status_code=404, detail="삭제할 사이트가 없습니다")

    codes = [s.code for s in sites]
    doc_count = db.query(func.count(Document.id)).filter(Document.site_code.in_(codes)).scalar() or 0

    for site in sites:
        db.delete(site)
    db.commit()

    return {"ok": True, "deletedCount": len(sites), "orphanedDocs": doc_count}
