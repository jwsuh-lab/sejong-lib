"""JSON → DB 1회성 마이그레이션 스크립트

data/*.json + selection_400.json → SQLite (sejong.db)
sites.csv → sites 테이블
기본 설정값 → settings 테이블
"""

import csv
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 PYTHONPATH에 추가
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from api.database import engine, SessionLocal, Base, init_db
from api.models import Document, Site, PipelineRun, PipelineLog, Setting

DATA_DIR = ROOT / "data"

# 국가코드 → 국가명/플래그 매핑 (sites.csv 기반으로 보완)
COUNTRY_MAP = {
    "us": "US", "ca": "CA", "se": "SE", "sg": "SG", "eu": "EU",
    "at": "AT", "no": "NO", "in": "IN", "it": "IT", "be": "BE",
    "hu": "HU", "gb": "GB", "govuk": "GB", "dk": "DK", "fi": "FI",
    "nl": "NL", "de": "DE", "fr": "FR", "ch": "CH", "jp": "JP",
    "au": "AU", "nz": "NZ", "kr": "KR", "ie": "IE", "es": "ES",
    "pt": "PT", "pl": "PL", "cz": "CZ", "il": "IL", "tw": "TW",
    "mx": "MX", "br": "BR", "za": "ZA", "ke": "KE", "ng": "NG",
    "intl": "INTL",
}


def _guess_country_from_filename(filename: str) -> str:
    """파일명에서 국가코드 추출: us_Z00057_BEA_20260219.json → US"""
    prefix = filename.split("_")[0].lower()
    return COUNTRY_MAP.get(prefix, prefix.upper())


def _compute_content_hash(doc: dict) -> str:
    """URL 기반 해시 (link 없으면 title+site_code 폴백)"""
    key = doc.get("link") or f"{doc.get('site_code', '')}:{doc.get('title', '')}"
    return hashlib.md5(key.encode()).hexdigest()


def import_crawl_jsons(session):
    """data/*.json → documents 테이블"""
    count = 0
    seen_keys = set()  # (site_code, link_or_title) 중복 방지
    skip_patterns = re.compile(r"^(selection_|excluded_|keyword_|.*summary.*)")

    for path in sorted(DATA_DIR.glob("*.json")):
        if skip_patterns.match(path.name):
            continue
        if path.name.startswith("backup"):
            continue

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            print(f"  SKIP (파싱 실패): {path.name}")
            continue

        docs = raw.get("documents", [])
        if not docs:
            continue

        country = _guess_country_from_filename(path.name)

        for doc in docs:
            site_code = doc.get("site_code", "")
            link = doc.get("link") or ""
            title = doc.get("title", "")

            if not title:
                continue

            # link가 비어있으면 None으로 (UNIQUE 제약 회피)
            link_val = link.strip() if link else None

            # 메모리 내 중복 체크
            dedup_key = (site_code, link_val or title)
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            new_doc = Document(
                site_code=site_code,
                title=title,
                link=link_val,
                description=doc.get("description", ""),
                published_date=doc.get("published_date", ""),
                document_type=doc.get("document_type", ""),
                pdf_url=doc.get("pdf_url", ""),
                authors=doc.get("authors", ""),
                country=country,
                source_file=path.name,
                content_hash=_compute_content_hash(doc),
                status="collected",
            )
            session.add(new_doc)
            count += 1

    session.commit()
    print(f"  크롤링 데이터 임포트: {count}건")
    return count


def import_selection(session):
    """selection_400.json → documents 테이블 UPDATE (선별/검증 필드)"""
    sel_path = DATA_DIR / "selection_400.json"
    if not sel_path.exists():
        print("  selection_400.json 없음 — 스킵")
        return 0

    selection = json.loads(sel_path.read_text(encoding="utf-8"))
    updated = 0

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

        # selection에 있지만 크롤링 데이터에 없는 경우 → INSERT
        if not existing:
            country = doc.get("_country", "").upper()
            existing = Document(
                site_code=site_code,
                title=doc.get("title", ""),
                link=link,
                country=country if country else "UNKNOWN",
                source_file=doc.get("_source_file", ""),
                content_hash=_compute_content_hash(doc),
            )
            session.add(existing)
            session.flush()

        # 상태 결정 — _excluded가 문자열일 수도 있음 (truthy 체크)
        is_excluded = bool(doc.get("_excluded", False))
        is_validated = bool(doc.get("_validated", False))
        if is_excluded:
            status = "excluded"
        elif is_validated:
            status = "verified"
        else:
            status = "selected"

        # 필드 업데이트
        existing.status = status
        existing.relevance_score = doc.get("_relevance_score", 0) or 0
        existing.matched_keywords = json.dumps(doc.get("_matched_keywords", []), ensure_ascii=False)
        existing.excluded = is_excluded
        existing.exclude_reasons = json.dumps(doc.get("_exclude_reasons", []), ensure_ascii=False)
        existing.date_verified = bool(doc.get("_date_verified", False))
        existing.date_verified_by = doc.get("_date_verified_by", "")
        existing.authors = doc.get("authors", "") or existing.authors or ""
        existing.description = doc.get("description", "") or existing.description or ""
        existing.keywords = doc.get("keywords", "") or existing.keywords or ""
        existing.journal = doc.get("journal", "") or existing.journal or ""
        existing.volume_info = doc.get("volume_info", "") or existing.volume_info or ""
        existing.pdf_url = doc.get("pdf_url", "") or existing.pdf_url or ""
        existing.published_date = doc.get("published_date", "") or existing.published_date or ""
        existing.document_type = doc.get("document_type", "") or existing.document_type or ""
        existing.license = doc.get("license", "") or existing.license or ""

        # 카테고리 분류 (matched_keywords 기반)
        matched = doc.get("_matched_keywords", [])
        if matched:
            existing.category = _categorize(matched)

        existing.updated_at = datetime.now()
        updated += 1

    session.commit()
    print(f"  선별 데이터 업데이트: {updated}건")
    return updated


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
    """matched_keywords에서 카테고리 결정"""
    for kw in matched_keywords:
        kw_lower = kw.lower().strip()
        if kw_lower in CATEGORY_MAP:
            return CATEGORY_MAP[kw_lower]
    return "기타"


def import_sites(session):
    """sites.csv → sites 테이블"""
    sites_path = ROOT / "sites.csv"
    if not sites_path.exists():
        print("  sites.csv 없음 — 스킵")
        return 0

    count = 0
    seen_codes = set()
    with open(sites_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("기관코드", "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)

            site = Site(
                code=code,
                name=row.get("기관명", "").strip(),
                name_kr=row.get("기관명(한글명)", "").strip(),
                country_code=row.get("국가코드", "").strip(),
                country=row.get("국가", "").strip(),
                org_type=row.get("유형", "").strip(),
                acronym=row.get("기관명(약어)", "").strip(),
                url=row.get("수집url", "").strip(),
                brm_category=row.get("BRM대분류", "").strip(),
                current_use=row.get("현재사용", "").strip(),
                expected_count=int(row.get("연간 수집 예상 수량", "0") or "0"),
                excluded=(row.get("수집제외", "").strip() != ""),
            )
            session.add(site)
            count += 1

    session.commit()
    print(f"  사이트 임포트: {count}건")
    return count


def insert_default_settings(session):
    """기본 설정값 INSERT"""
    defaults = {
        "target_count": "300",
        "cutoff_date": "2026-01-01",
        "min_pdf_size_kb": "100",
        "min_title_length": "15",
        "min_keyword_count": "3",
        "org_cap_percent": "10",
        "gb_excluded": "true",
        "gao_excluded": "true",
        "completed_count": "9577",
    }

    for key, value in defaults.items():
        existing = session.query(Setting).filter(Setting.key == key).first()
        if not existing:
            session.add(Setting(key=key, value=value))

    session.commit()
    print(f"  기본 설정 {len(defaults)}건 입력")


def create_default_config():
    """config.json 기본값 생성 (없는 경우만)"""
    config_path = ROOT / "config.json"
    if config_path.exists():
        print("  config.json 이미 존재 — 스킵")
        return

    config = {
        "target_count": 300,
        "cutoff_date": "2026-01-01",
        "min_pdf_size_kb": 100,
        "min_title_length": 15,
        "min_keyword_count": 3,
        "org_cap_percent": 10,
        "gb_excluded": True,
        "gao_excluded": True,
        "country_quota": {"US": 170, "CA": 45, "SE": 25, "SG": 25},
        "crawl_scripts": [
            {"script": "main.py", "args": ["crawl-us", "--all"], "label": "미국"},
            {"script": "main.py", "args": ["crawl-se", "--all"], "label": "스웨덴"},
            {"script": "main.py", "args": ["crawl-sg", "--all"], "label": "싱가포르"},
            {"script": "main.py", "args": ["crawl", "AT", "--all"], "label": "오스트리아"},
            {"script": "main.py", "args": ["crawl", "CA", "--all"], "label": "캐나다"},
            {"script": "main.py", "args": ["crawl", "NO", "--all"], "label": "노르웨이"},
            {"script": "main.py", "args": ["crawl", "IT", "--all"], "label": "이탈리아"},
            {"script": "crawl_additional.py", "args": [], "label": "추가소스 (Fed, ECB, WHO 등)"},
            {"script": "crawl_eprs.py", "args": [], "label": "EPRS"},
            {"script": "crawl_more.py", "args": [], "label": "추가소스2 (BIS, ILO 등)"},
            {"script": "crawl_new_sites.py", "args": [], "label": "신규사이트 (Oxfam 등)"},
        ],
        "crawl_exclude": [],
    }

    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  config.json 생성 완료")


def main():
    print("=" * 60)
    print("세종도서관 JSON → DB 마이그레이션")
    print("=" * 60)

    # DB 초기화
    print("\n[1/5] 데이터베이스 초기화...")
    init_db()
    print("  sejong.db 테이블 생성 완료")

    session = SessionLocal()
    try:
        # 크롤링 데이터 임포트
        print("\n[2/5] 크롤링 데이터 임포트...")
        total_crawled = import_crawl_jsons(session)

        # 선별 데이터 업데이트
        print("\n[3/5] 선별 데이터 업데이트...")
        total_selected = import_selection(session)

        # 사이트 임포트
        print("\n[4/5] 사이트 데이터 임포트...")
        total_sites = import_sites(session)

        # 기본 설정
        print("\n[5/5] 기본 설정 + config.json...")
        insert_default_settings(session)
        create_default_config()

        # 검증
        print("\n" + "=" * 60)
        print("마이그레이션 완료 — 검증")
        print("=" * 60)

        doc_count = session.query(Document).count()
        selected_count = session.query(Document).filter(
            Document.status.in_(["selected", "verified"])
        ).count()
        verified_count = session.query(Document).filter(
            Document.status == "verified"
        ).count()
        excluded_count = session.query(Document).filter(
            Document.status == "excluded"
        ).count()
        site_count = session.query(Site).count()

        print(f"  documents 테이블: {doc_count}건")
        print(f"    - selected + verified: {selected_count}건")
        print(f"    - verified: {verified_count}건")
        print(f"    - excluded: {excluded_count}건")
        print(f"  sites 테이블: {site_count}건")
        print(f"\n  DB 파일: {ROOT / 'sejong.db'}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
