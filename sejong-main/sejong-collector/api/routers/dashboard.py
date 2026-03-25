"""대시보드 API — GET /summary, /countries, /categories"""

import json

from fastapi import APIRouter, Depends
from sqlalchemy import func, distinct, case, literal
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Document, Site, Setting
from api.schemas import SummaryResponse, CountryResponse, CategoryResponse

router = APIRouter()

# 국가코드 → 플래그
COUNTRY_FLAGS = {
    "US": "🇺🇸", "CA": "🇨🇦", "SE": "🇸🇪", "SG": "🇸🇬", "EU": "🇪🇺",
    "AT": "🇦🇹", "NO": "🇳🇴", "IN": "🇮🇳", "IT": "🇮🇹", "BE": "🇧🇪",
    "HU": "🇭🇺", "GB": "🇬🇧", "DK": "🇩🇰", "FI": "🇫🇮", "NL": "🇳🇱",
    "DE": "🇩🇪", "FR": "🇫🇷", "CH": "🇨🇭", "JP": "🇯🇵", "AU": "🇦🇺",
    "NZ": "🇳🇿", "KR": "🇰🇷", "IE": "🇮🇪", "ES": "🇪🇸", "PT": "🇵🇹",
    "PL": "🇵🇱", "CZ": "🇨🇿", "IL": "🇮🇱", "TW": "🇹🇼", "MX": "🇲🇽",
    "BR": "🇧🇷", "ZA": "🇿🇦", "KE": "🇰🇪", "NG": "🇳🇬",
    "SA": "🇸🇦", "GE": "🇬🇪", "GR": "🇬🇷", "JO": "🇯🇴", "PH": "🇵🇭",
    "TH": "🇹🇭", "VN": "🇻🇳", "MY": "🇲🇾", "ID": "🇮🇩", "CL": "🇨🇱",
    "CO": "🇨🇴", "PE": "🇵🇪", "AR": "🇦🇷", "EG": "🇪🇬", "TR": "🇹🇷",
    "RO": "🇷🇴", "HR": "🇭🇷", "SK": "🇸🇰", "SI": "🇸🇮", "LT": "🇱🇹",
    "LV": "🇱🇻", "EE": "🇪🇪", "BG": "🇧🇬", "CY": "🇨🇾", "LU": "🇱🇺",
    "MT": "🇲🇹", "IS": "🇮🇸", "INTL": "🌐", "ADDITIONAL": "🌐",
}

# 국가코드 → 국가명(한글)
COUNTRY_NAMES = {
    "US": "미국", "CA": "캐나다", "SE": "스웨덴", "SG": "싱가포르", "EU": "EU",
    "AT": "오스트리아", "NO": "노르웨이", "IN": "인도", "IT": "이탈리아",
    "BE": "벨기에", "HU": "헝가리", "GB": "영국", "DK": "덴마크", "FI": "핀란드",
    "NL": "네덜란드", "DE": "독일", "FR": "프랑스", "CH": "스위스", "JP": "일본",
    "AU": "호주", "NZ": "뉴질랜드", "KR": "한국", "IE": "아일랜드", "ES": "스페인",
    "PT": "포르투갈", "PL": "폴란드", "CZ": "체코", "IL": "이스라엘",
    "TW": "대만", "MX": "멕시코", "BR": "브라질", "ZA": "남아프리카",
    "KE": "케냐", "NG": "나이지리아",
    "SA": "사우디아라비아", "GE": "조지아", "GR": "그리스", "JO": "요르단",
    "PH": "필리핀", "TH": "태국", "VN": "베트남", "MY": "말레이시아",
    "ID": "인도네시아", "CL": "칠레", "CO": "콜롬비아", "PE": "페루",
    "AR": "아르헨티나", "EG": "이집트", "TR": "튀르키예",
    "RO": "루마니아", "HR": "크로아티아", "SK": "슬로바키아", "SI": "슬로베니아",
    "LT": "리투아니아", "LV": "라트비아", "EE": "에스토니아",
    "INTL": "국제기구", "ADDITIONAL": "추가소스",
}

# 국가별 쿼터 기본값
COUNTRY_QUOTA = {"US": 170, "CA": 45, "SE": 25, "SG": 25}


def _get_setting(db: Session, key: str, default: str = "") -> str:
    s = db.query(Setting).filter(Setting.key == key).first()
    return s.value if s else default


@router.get("/summary", response_model=SummaryResponse)
async def get_summary(db: Session = Depends(get_db)):
    total_collected = db.query(func.count(Document.id)).scalar() or 0
    json_file_count = db.query(func.count(distinct(Document.source_file))).filter(
        Document.source_file.isnot(None)
    ).scalar() or 0
    selected_active = db.query(func.count(Document.id)).filter(
        Document.status.in_(["selected", "verified"]),
        Document.excluded == False,
    ).scalar() or 0
    verified_count = db.query(func.count(Document.id)).filter(
        Document.status == "verified"
    ).scalar() or 0
    excluded_by_date = db.query(func.count(Document.id)).filter(
        Document.excluded == True,
        Document.exclude_reasons.like("%date%"),
    ).scalar() or 0
    excluded_by_pdf = db.query(func.count(Document.id)).filter(
        Document.excluded == True,
        Document.exclude_reasons.like("%pdf%"),
    ).scalar() or 0
    excluded_by_lang = db.query(func.count(Document.id)).filter(
        Document.excluded == True,
        Document.exclude_reasons.like("%lang%"),
    ).scalar() or 0
    total_countries = db.query(func.count(distinct(Site.country_code))).scalar() or 0
    total_orgs = db.query(func.count(Site.id)).scalar() or 0

    target = int(_get_setting(db, "target_count", "300"))
    completed = int(_get_setting(db, "completed_count", "9577"))
    gb_excluded = _get_setting(db, "gb_excluded", "true") == "true"
    gao_excluded = _get_setting(db, "gao_excluded", "true") == "true"

    verified_rate = round((verified_count / selected_active * 100), 1) if selected_active > 0 else 0

    return SummaryResponse(
        totalCollected=total_collected,
        jsonFileCount=json_file_count,
        selectedActive=selected_active,
        selectedTarget=target,
        verifiedCount=verified_count,
        verifiedRate=verified_rate,
        excludedByDate=excluded_by_date,
        excludedByPdf=excluded_by_pdf,
        excludedByLang=excluded_by_lang,
        totalCountries=total_countries,
        totalOrgs=total_orgs,
        completedDocs=completed,
        gbExcluded=gb_excluded,
        gaoExcluded=gao_excluded,
    )


@router.get("/countries", response_model=list[CountryResponse])
async def get_countries(db: Session = Depends(get_db)):
    # 국가별 문서 통계
    stats = db.query(
        Document.country,
        func.count(Document.id).label("collected"),
        func.sum(case(
            (Document.status.in_(["selected", "verified"]), 1),
            else_=0,
        )).label("selected"),
    ).group_by(Document.country).all()

    # 국가별 기관 수
    org_counts = dict(
        db.query(
            Site.country_code,
            func.count(Site.id),
        ).group_by(Site.country_code).all()
    )

    gb_excluded = _get_setting(db, "gb_excluded", "true") == "true"

    result = []
    for country, collected, selected in stats:
        code = country.upper() if country else "UNKNOWN"
        quota = COUNTRY_QUOTA.get(code, 0)
        is_excluded = (code == "GB" and gb_excluded)

        result.append(CountryResponse(
            code=code,
            name=COUNTRY_NAMES.get(code, code),
            flag=COUNTRY_FLAGS.get(code, "🏳️"),
            collected=collected or 0,
            selected=selected or 0,
            quota=quota,
            orgCount=org_counts.get(code, 0),
            excluded=is_excluded,
        ))

    result.sort(key=lambda x: x.collected, reverse=True)
    return result


# 카테고리 정의
CATEGORY_DEFS = {
    "경제·통상": {"name": "economy", "color": "#3b82f6", "icon": "💰"},
    "AI·디지털": {"name": "ai_digital", "color": "#8b5cf6", "icon": "🤖"},
    "기후·에너지": {"name": "climate", "color": "#22c55e", "icon": "🌿"},
    "안보·외교": {"name": "security", "color": "#ef4444", "icon": "🛡️"},
    "인구·복지": {"name": "welfare", "color": "#f97316", "icon": "👥"},
    "보건": {"name": "health", "color": "#ec4899", "icon": "🏥"},
    "교육": {"name": "education", "color": "#06b6d4", "icon": "📚"},
    "주거·도시": {"name": "housing", "color": "#f59e0b", "icon": "🏙️"},
}


@router.get("/categories", response_model=list[CategoryResponse])
async def get_categories(db: Session = Depends(get_db)):
    stats = db.query(
        Document.category,
        func.count(Document.id),
    ).filter(
        Document.status.in_(["selected", "verified"]),
        Document.category.isnot(None),
    ).group_by(Document.category).all()

    result = []
    for cat_kr, count in stats:
        if cat_kr in CATEGORY_DEFS:
            d = CATEGORY_DEFS[cat_kr]
            result.append(CategoryResponse(
                name=d["name"],
                nameKr=cat_kr,
                count=count,
                color=d["color"],
                icon=d["icon"],
            ))
        elif cat_kr and cat_kr != "기타":
            result.append(CategoryResponse(
                name=cat_kr,
                nameKr=cat_kr,
                count=count,
                color="#9ca3af",
                icon="📋",
            ))

    result.sort(key=lambda x: x.count, reverse=True)
    return result
