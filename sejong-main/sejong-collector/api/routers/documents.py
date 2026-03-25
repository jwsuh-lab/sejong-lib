"""문서 목록/상세 API"""

import json
import math

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Document, Site
from api.schemas import DocumentResponse, DocumentListResponse, FRONT_STATUS_MAP

router = APIRouter()

# dashboard.py 공유 플래그 매핑
from api.routers.dashboard import COUNTRY_FLAGS

# 문서유형 → 한글 매핑
DOC_TYPE_MAP = {
    "report": "보고서",
    "REPORT": "보고서",
    "policy_paper": "정책자료",
    "publication": "발간자료",
    "research": "보고서",
    "statistics": "통계자료",
    "briefing": "정책자료",
    "working_paper": "보고서",
    "testimony": "회의자료",
}


def _doc_to_response(doc: Document, site_name: str = "") -> dict:
    """Document ORM → DocumentResponse용 dict 변환"""
    keywords_raw = doc.keywords or ""
    if keywords_raw.startswith("["):
        try:
            keywords = json.loads(keywords_raw)
        except json.JSONDecodeError:
            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
    else:
        keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]

    exclude_raw = doc.exclude_reasons or "[]"
    try:
        exclude_reasons = json.loads(exclude_raw)
    except json.JSONDecodeError:
        exclude_reasons = []

    country = (doc.country or "").upper()

    return {
        "id": doc.id,
        "country": country,
        "countryFlag": COUNTRY_FLAGS.get(country, "🏳️"),
        "orgName": site_name or doc.site_code or "",
        "orgCode": doc.site_code or "",
        "title": doc.title or "",
        "docType": doc.doc_type_kr or DOC_TYPE_MAP.get(doc.document_type or "", "기타"),
        "publishedDate": doc.published_date or "",
        "relevanceScore": doc.relevance_score or 0,
        "status": doc.status or "collected",
        "pdfUrl": doc.pdf_url or None,
        "pdfSizeKb": doc.pdf_size_kb,
        "authors": doc.authors or "",
        "description": doc.description or "",
        "keywords": keywords,
        "category": doc.category or "",
        "brmCode1": doc.brm_code1 or "",
        "brmCode2": doc.brm_code2 or "",
        "journal": doc.journal or "",
        "volumeInfo": doc.volume_info or "",
        "isbn": doc.isbn or "",
        "issn": doc.issn or "",
        "license": doc.license or "",
        "dateVerified": doc.date_verified or False,
        "dateVerifiedBy": doc.date_verified_by or "",
        "pdfVerified": doc.pdf_verified or False,
        "langVerified": doc.lang_verified or False,
        "excludeReasons": exclude_reasons,
        "link": doc.link or "",
    }


@router.get("", response_model=DocumentListResponse)
async def list_documents(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    country: str = Query(None),
    status: str = Query(None),
    docType: str = Query(None),
    search: str = Query(None),
    sort: str = Query("score"),
    category: str = Query(None),
):
    query = db.query(Document)

    # status 필터: 프론트 status → 백엔드 status 역매핑
    if status:
        reverse_map = {
            "unverified": ["collected", "selected"],
            "verified": ["verified"],
            "excluded": ["excluded"],
        }
        backend_statuses = reverse_map.get(status, [status])
        query = query.filter(Document.status.in_(backend_statuses))

    if country:
        query = query.filter(func.upper(Document.country) == country.upper())

    if category:
        query = query.filter(Document.category == category)

    if docType:
        query = query.filter(
            or_(
                Document.doc_type_kr == docType,
                Document.document_type == docType,
            )
        )

    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                Document.title.ilike(search_term),
                Document.authors.ilike(search_term),
                Document.description.ilike(search_term),
                Document.site_code.ilike(search_term),
            )
        )

    # 정렬
    if sort == "date":
        query = query.order_by(Document.published_date.desc())
    else:
        query = query.order_by(Document.relevance_score.desc())

    total = query.count()
    total_pages = max(1, math.ceil(total / size))
    docs = query.offset((page - 1) * size).limit(size).all()

    # site_code → site_name 매핑
    site_codes = list(set(d.site_code for d in docs if d.site_code))
    site_map = {}
    if site_codes:
        sites = db.query(Site).filter(Site.code.in_(site_codes)).all()
        site_map = {s.code: s.name for s in sites}

    items = []
    for doc in docs:
        d = _doc_to_response(doc, site_map.get(doc.site_code, ""))
        items.append(DocumentResponse(**d))

    return DocumentListResponse(
        items=items,
        total=total,
        page=page,
        totalPages=total_pages,
    )


@router.get("/{doc_id}", response_model=DocumentResponse)
async def get_document(doc_id: int, db: Session = Depends(get_db)):
    doc = db.query(Document).filter(Document.id == doc_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다")

    site = db.query(Site).filter(Site.code == doc.site_code).first()
    site_name = site.name if site else ""

    d = _doc_to_response(doc, site_name)
    return DocumentResponse(**d)
