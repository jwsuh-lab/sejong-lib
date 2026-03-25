"""
수록잡지·권호정보·키워드 메타데이터 수집기
- 수집된 문서의 상세 페이지에서 수록잡지, 권호정보, 키워드 추출
- pdf_url_resolver.py와 동일한 패턴: 베이스 클래스 → 사이트별 특화 → 레지스트리 → 메인 실행
"""
import argparse
import csv
import json
import glob
import logging
import os
import re
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
COMPLETED_CSV = os.path.join(os.path.dirname(__file__), "completed sites.csv")
REQUEST_DELAY = 1.5
MAX_KEYWORDS_LEN = 300  # 키워드 문자열 최대 길이
MIN_DATE = datetime(2026, 1, 1)  # 수집 대상 최소 날짜


def _parse_date(date_str: str) -> datetime | None:
    """발행일 문자열을 datetime으로 파싱. 실패 시 None."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            pass
    try:
        return datetime.strptime(date_str.strip(), "%b %d, %Y")
    except ValueError:
        pass
    return None


def _load_completed() -> set[tuple[str, str]]:
    """completed sites.csv에서 (발행처, 자료명) 세트 로드"""
    completed = set()
    if not os.path.exists(COMPLETED_CSV):
        return completed
    with open(COMPLETED_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            publisher = (row.get("발행처") or "").strip()
            title = (row.get("자료명") or "").strip()
            if publisher and title:
                completed.add((publisher, title))
    logger.info(f"completed sites 로드: {len(completed)}건")
    return completed

# ═══════════════════════════════════════════════
#  사이트별 기본 시리즈명 매핑
# ═══════════════════════════════════════════════
SITE_SERIES_MAP = {
    # ── US ──
    "Z00057": "BEA Working Paper Series",                # BEA
    "Z00063": {                                           # FRB
        "feds_note": "FEDS Notes",
        "feds": "FEDS Working Paper",
        "ifdp": "IFDP",
        "_default": "Federal Reserve Board Publication",
    },
    "Z00065": "CBO Report",                              # CBO
    "Z00038": "ERS Report",                               # ERS
    "Z00054": "NIST Technical Publication",               # NIST
    "Z00014": "GAO Report",                               # GAO
    "Z00412": "CRS Report",                               # CRS
    "Z00071": "Census Bureau Report",                     # Census
    "Z00323": "RAND Research",                            # RAND
    "Z00048": "BJS Report",                               # BJS
    # ── AT (호주) ──
    "Z00175": "ABS Statistical Publication",              # Australian Bureau of Statistics
    "Z00261": "Austrade Report",                          # Austrade
    # ── BE (벨기에) ──
    "Z00309": "CEPS Publication",                         # Centre for European Policy Studies
    "Z00322": "Bruegel Publication",                      # Bruegel
    "Z00358": "Egmont Paper",                             # Royal Inst. for International Relations
    # ── CA (캐나다) ──
    "Z00139": "Public Safety Canada Publication",
    "Z00140": "Transport Canada Publication",
    "Z00141": "OSFI Publication",
    "Z00144": "Health Canada Publication",
    "Z00145": "Fisheries and Oceans Canada Publication",
    "Z00147": "PHAC Publication",
    "Z00148": "CIRNAC Publication",
    "Z00150": "NRCan Publication",
    "Z00151": "AAFC Publication",
    "Z00152": "Bank of Canada Publication",
    "Z00239": "Industry Canada Publication",
    "Z00242": "CIRNAC Publication",
    "Z00250": "Government of Canada Publication",
    "Z00313": "CIGI Paper",
    # ── GE (독일) ──
    "Z00447": "Kiel Working Paper",
    # ── HU (헝가리) ──
    "Z00399": "IFAT Publication",
    # ── IT (이탈리아) ──
    "Z00408": "CMCC Research Paper",
    # ── LT (리투아니아) ──
    "Z00337": "LFMI Policy Paper",
    "Z00338": "CGS Analysis",
    # ── MY (말레이시아) ──
    "Z00341": "ISIS Malaysia Publication",
    # ── NO (노르웨이) ──
    "Z00287": "NUPI Publication",
    # ── NZ (뉴질랜드) ──
    "Z00330": "CSS Publication",
    # ── QA (카타르) ──
    "Z00393": "Al Jazeera Center for Studies Report",
    # ── SA (사우디) ──
    "Z00360": "GRC Publication",
    # ── SP (스페인) ──
    "Z00312": "Elcano Royal Institute Analysis",
    "Z00367": "FAES Publication",
}

# Breadcrumb에서 시리즈명으로 인식할 키워드
_SERIES_KEYWORDS = [
    'paper', 'report', 'brief', 'note', 'analysis', 'commentary',
    'publication', 'working', 'discussion', 'research', 'study',
    'bulletin', 'review', 'journal', 'series', 'occasional',
]

# 페이지 본문에서 문서번호 추출용 패턴 (순서대로 시도)
_VOLUME_PATTERNS = [
    # "Working Paper No. 2026/08" 등
    (r'(Working Paper\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    (r'(Policy Brief\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    (r'(Technical Note\s*(?:No\.?\s*)?[A-Z]*[\d][\w./-]*)', 0),
    (r'(Discussion Paper\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    (r'(Staff (?:Paper|Report)\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    (r'(Research (?:Report|Paper)\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    (r'(Occasional Paper\s*(?:No\.?\s*)?[\d][\w./-]*)', 0),
    # 번호만: "No. 2026/08", "Nr. 123"
    (r'(?:No\.?|Nr\.?)\s*([\d]{2,}[/-][\d]+)', 0),
]


# ═══════════════════════════════════════════════
#  베이스 클래스
# ═══════════════════════════════════════════════
class BaseMetadataResolver(ABC):
    """메타데이터 추출 베이스 클래스"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SejongLibrary-MetadataResolver/1.0 (Research Purpose)',
            'Accept': 'text/html,application/json',
        })

    @abstractmethod
    def resolve(self, doc: dict) -> dict:
        """문서 상세 페이지에서 메타데이터 추출
        Returns: {"journal": str, "volume_info": str, "keywords": str}
        """
        ...

    def _get(self, url: str, timeout: int = 30) -> requests.Response | None:
        try:
            resp = self.session.get(url, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.debug(f"  요청 실패: {url} — {e}")
            return None

    def _soup(self, url: str) -> BeautifulSoup | None:
        resp = self._get(url)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, 'lxml')


# ═══════════════════════════════════════════════
#  키워드 정규화 유틸리티
# ═══════════════════════════════════════════════
def _normalize_keywords(raw: str) -> str:
    """키워드 문자열 정규화: 구분자 통일, 노이즈 제거, 길이 제한"""
    if not raw:
        return ""
    # 쉼표/세미콜론으로 분할
    parts = re.split(r'[,;]\s*', raw)
    cleaned = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # 10단어 이상이면 문장(노이즈) → 스킵
        if len(p.split()) > 10:
            continue
        cleaned.append(p)
    result = ', '.join(cleaned)
    if len(result) > MAX_KEYWORDS_LEN:
        # 마지막 온전한 키워드에서 자르기
        cut = result[:MAX_KEYWORDS_LEN]
        last_comma = cut.rfind(', ')
        if last_comma > 0:
            result = cut[:last_comma]
        else:
            result = cut
    return result


# ═══════════════════════════════════════════════
#  범용 HTML 메타데이터 추출기
# ═══════════════════════════════════════════════
class GenericMetaResolver(BaseMetadataResolver):
    """
    범용 HTML에서 수록잡지·권호정보·키워드 추출.
    추출 우선순위:
      수록잡지: meta citation → breadcrumb → JSON-LD → SITE_SERIES_MAP
      권호정보: meta citation → page text patterns → URL/제목 패턴
      키워드:   meta keywords → article:tag → JSON-LD → CSS 셀렉터
    """

    def resolve(self, doc: dict) -> dict:
        link = doc.get('link', '')
        site_code = doc.get('site_code', '')
        result = {"journal": "", "volume_info": "", "keywords": "", "license": "",
                  "isbn": "", "issn": "", "doc_type": ""}

        # SITE_SERIES_MAP 기본값 (최후 폴백)
        series_default = self._get_series_default(site_code, doc)

        # URL/제목에서 권호정보 패턴 추출 (HTTP 없이 가능)
        vol_from_url = self._extract_volume_from_url_or_title(
            link, doc.get('title', ''))

        # 상세 페이지 HTML 가져오기
        soup = None
        if link and link.startswith('http'):
            soup = self._soup(link)

        if soup is not None:
            # 1. meta 태그에서 추출
            meta_result = self._extract_from_meta_tags(soup)

            # 2. JSON-LD 구조화 데이터에서 추출
            jsonld_result = self._extract_from_jsonld(soup)

            # 3. Breadcrumb에서 시리즈명 추출
            breadcrumb_series = self._extract_series_from_breadcrumb(soup)

            # 4. 페이지 본문에서 문서번호 패턴 추출
            page_volume = self._extract_volume_from_page(soup)

            # ── 수록잡지 우선순위 ──
            result["journal"] = (
                meta_result.get("journal")
                or breadcrumb_series
                or jsonld_result.get("journal")
                or series_default
            )

            # ── 권호정보 우선순위 ──
            result["volume_info"] = (
                meta_result.get("volume_info")
                or jsonld_result.get("volume_info")
                or page_volume
                or vol_from_url
            )

            # ── 키워드 우선순위 ──
            raw_keywords = (
                meta_result.get("keywords")
                or jsonld_result.get("keywords")
                or self._extract_keywords_from_selectors(soup)
            )
            result["keywords"] = _normalize_keywords(raw_keywords)

            # ── 라이선스 (CC BY / Copyright 감지) ──
            result["license"] = (
                self._extract_license_from_meta(soup)
                or self._extract_license_from_jsonld(soup)
                or self._extract_license_from_link_rel(soup)
                or self._extract_license_from_text(soup)
                or self._extract_copyright_from_page(soup)
            )

            # ── ISBN/ISSN 추출 ──
            isbn, issn = self._extract_isbn_issn(soup)
            result["isbn"] = isbn
            result["issn"] = issn

            # ── 문서 유형 분류 ──
            result["doc_type"] = self._classify_doc_type(soup, doc)
        else:
            # HTML 접근 불가 시 기본값만
            result["journal"] = series_default
            result["volume_info"] = vol_from_url
            result["doc_type"] = self._classify_doc_type(None, doc)

        return result

    # ──────────────────────────────────────────
    #  meta 태그 추출
    # ──────────────────────────────────────────
    def _extract_from_meta_tags(self, soup: BeautifulSoup) -> dict:
        result = {"journal": "", "volume_info": "", "keywords": ""}

        # ── 키워드 ──
        meta_kw = soup.find('meta', attrs={'name': 'keywords'})
        if meta_kw and meta_kw.get('content', '').strip():
            result["keywords"] = meta_kw['content'].strip()

        if not result["keywords"]:
            tags = soup.find_all('meta', attrs={'property': 'article:tag'})
            tag_values = [t.get('content', '').strip() for t in tags
                          if t.get('content', '').strip()]
            if tag_values:
                result["keywords"] = ', '.join(tag_values)

        # ── 수록잡지 ──
        for attr_name in ('citation_journal_title', 'citation_series_title'):
            meta = soup.find('meta', attrs={'name': attr_name})
            if meta and meta.get('content', '').strip():
                result["journal"] = meta['content'].strip()
                break

        # ── 권호정보 ──
        meta_vol = soup.find('meta', attrs={'name': 'citation_volume'})
        meta_issue = soup.find('meta', attrs={'name': 'citation_issue'})
        if meta_vol and meta_vol.get('content', '').strip():
            vol = meta_vol['content'].strip()
            issue = ''
            if meta_issue and meta_issue.get('content', '').strip():
                issue = meta_issue['content'].strip()
            result["volume_info"] = f"Vol. {vol}, No. {issue}" if issue else f"Vol. {vol}"

        if not result["volume_info"]:
            meta_report = soup.find('meta', attrs={'name': 'citation_technical_report_number'})
            if meta_report and meta_report.get('content', '').strip():
                result["volume_info"] = meta_report['content'].strip()

        return result

    # ──────────────────────────────────────────
    #  JSON-LD (schema.org) 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_from_jsonld(soup: BeautifulSoup) -> dict:
        result = {"journal": "", "volume_info": "", "keywords": ""}
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)
                # 배열인 경우 첫 번째 또는 적합한 항목 선택
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # isPartOf → 시리즈/잡지명
                    part_of = item.get('isPartOf')
                    if isinstance(part_of, dict) and not result["journal"]:
                        result["journal"] = (
                            part_of.get('name', '')
                            or part_of.get('alternateName', '')
                        )
                    elif isinstance(part_of, str) and not result["journal"]:
                        result["journal"] = part_of

                    # volumeNumber, issueNumber
                    vol = item.get('volumeNumber', '')
                    issue = item.get('issueNumber', '')
                    if vol and not result["volume_info"]:
                        result["volume_info"] = f"Vol. {vol}, No. {issue}" if issue else f"Vol. {vol}"

                    # reportNumber
                    rn = item.get('reportNumber', '')
                    if rn and not result["volume_info"]:
                        result["volume_info"] = rn

                    # keywords (문자열 또는 리스트)
                    kw = item.get('keywords', '')
                    if kw and not result["keywords"]:
                        if isinstance(kw, list):
                            result["keywords"] = ', '.join(str(k) for k in kw)
                        else:
                            result["keywords"] = str(kw)
            except (json.JSONDecodeError, TypeError):
                continue
        return result

    # ──────────────────────────────────────────
    #  Breadcrumb에서 시리즈명 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_series_from_breadcrumb(soup: BeautifulSoup) -> str:
        selectors = [
            'nav[aria-label*="breadcrumb"] a',
            'nav[aria-label*="Breadcrumb"] a',
            '.breadcrumb a',
            '.breadcrumbs a',
            'ol.breadcrumb li a',
            '[class*="breadcrumb"] a',
        ]
        for selector in selectors:
            elements = soup.select(selector)
            if not elements:
                continue
            # 마지막에서 두 번째 항목이 보통 카테고리/시리즈
            for el in reversed(elements):
                text = el.get_text(strip=True)
                text_lower = text.lower()
                if any(kw in text_lower for kw in _SERIES_KEYWORDS):
                    return text
        return ""

    # ──────────────────────────────────────────
    #  페이지 본문에서 문서번호 패턴 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_volume_from_page(soup: BeautifulSoup) -> str:
        """h1 주변, 서브타이틀, .meta 영역에서 문서번호 패턴 검색"""
        # 검색 대상: 페이지 상단 텍스트 (h1, subtitle, meta 영역)
        search_texts = []
        for sel in ['h1', '.subtitle', '.meta-info', '.publication-meta',
                     '.article-meta', '.doc-info', '.paper-info',
                     '.report-number', '.publication-number',
                     '[class*="subtitle"]', '[class*="series"]']:
            for el in soup.select(sel):
                search_texts.append(el.get_text(strip=True))

        # h1 바로 다음 형제 요소도 확인
        h1 = soup.find('h1')
        if h1:
            for sib in h1.find_next_siblings()[:3]:
                search_texts.append(sib.get_text(strip=True))

        combined = ' '.join(search_texts)
        if not combined:
            return ""

        for pattern, _group in _VOLUME_PATTERNS:
            m = re.search(pattern, combined, re.IGNORECASE)
            if m:
                return m.group(1).strip() if m.lastindex else m.group(0).strip()

        return ""

    # ──────────────────────────────────────────
    #  CSS 셀렉터로 키워드 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_keywords_from_selectors(soup: BeautifulSoup) -> str:
        for selector in ['.tag', '.keyword', '.topic',
                         '.tags a', '.keywords a', '.topics a',
                         '.categories a', '[class*="tag"] a',
                         '[class*="keyword"]', '[class*="topic"] a']:
            elements = soup.select(selector)
            if elements:
                vals = []
                for el in elements:
                    t = el.get_text(strip=True)
                    # 10단어 이상이면 키워드가 아닌 본문
                    if t and len(t.split()) <= 10:
                        vals.append(t)
                if vals:
                    return ', '.join(vals)
        return ""

    # ──────────────────────────────────────────
    #  CC BY 라이선스 감지
    # ──────────────────────────────────────────
    @staticmethod
    def _classify_license(text: str) -> str:
        """라이선스 텍스트/URL에서 CC 라이선스 유형 판별"""
        if not text:
            return ""
        t = text.lower()
        # creativecommons.org URL 패턴
        m = re.search(r'creativecommons\.org/licenses/([\w-]+)', t)
        if m:
            return f"CC {m.group(1).upper()}"
        # "CC BY-SA 4.0" 등 명시적 표기
        m = re.search(r'\bcc\s+(by[\w\s-]*?\d[\.\d]*)', t)
        if m:
            return f"CC {m.group(1).upper().strip()}"
        # "Creative Commons Attribution" 텍스트
        if 'creative commons' in t:
            if 'attribution' in t:
                parts = ['CC BY']
                if 'sharealike' in t or 'share-alike' in t:
                    parts.append('SA')
                if 'noncommercial' in t or 'non-commercial' in t:
                    parts.append('NC')
                if 'noderivatives' in t or 'no-derivatives' in t or 'noderivs' in t:
                    parts.append('ND')
                return '-'.join(parts) if len(parts) > 1 else parts[0]
            return "Creative Commons"
        # 단순 "open government licence" (UK 등)
        if 'open government licence' in t or 'open government license' in t:
            return "OGL"
        return ""

    @staticmethod
    def _extract_license_from_meta(soup: BeautifulSoup) -> str:
        """meta 태그에서 라이선스 추출 (dc.rights, dcterms.license 등)"""
        for attr in ('dc.rights', 'dcterms.license', 'dcterms.rights',
                      'dc.license', 'rights'):
            meta = soup.find('meta', attrs={'name': attr})
            if meta and meta.get('content', '').strip():
                result = GenericMetaResolver._classify_license(meta['content'])
                if result:
                    return result
        return ""

    @staticmethod
    def _extract_license_from_jsonld(soup: BeautifulSoup) -> str:
        """JSON-LD schema.org license 필드에서 추출"""
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    lic = item.get('license') or item.get('copyrightHolder', '')
                    if isinstance(lic, dict):
                        lic = lic.get('url', '') or lic.get('name', '')
                    if lic:
                        result = GenericMetaResolver._classify_license(str(lic))
                        if result:
                            return result
            except (json.JSONDecodeError, TypeError):
                continue
        return ""

    @staticmethod
    def _extract_license_from_link_rel(soup: BeautifulSoup) -> str:
        """<link rel="license"> 태그에서 추출"""
        link = soup.find('link', rel='license')
        if link and link.get('href', ''):
            return GenericMetaResolver._classify_license(link['href'])
        # <a rel="license"> 도 확인
        a = soup.find('a', rel='license')
        if a:
            href = a.get('href', '')
            text = a.get_text(strip=True)
            return (GenericMetaResolver._classify_license(href)
                    or GenericMetaResolver._classify_license(text))
        return ""

    @staticmethod
    def _extract_license_from_text(soup: BeautifulSoup) -> str:
        """footer/하단 영역에서 CC 라이선스 텍스트 검색"""
        for selector in ['footer', '.footer', '#footer',
                         '.license', '.copyright', '[class*="license"]']:
            for el in soup.select(selector):
                text = el.get_text(separator=' ', strip=True)
                if len(text) > 2000:
                    text = text[:2000]
                result = GenericMetaResolver._classify_license(text)
                if result:
                    return result
        return ""

    # ──────────────────────────────────────────
    #  Copyright (ⓒ) 감지
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_copyright_from_page(soup: BeautifulSoup) -> str:
        """footer/하단 영역에서 Copyright(ⓒ) 텍스트 검색"""
        for selector in ['footer', '.footer', '#footer',
                         '.copyright', '[class*="copyright"]',
                         '.legal', '#legal', '.site-info']:
            for el in soup.select(selector):
                text = el.get_text(separator=' ', strip=True)
                if len(text) > 2000:
                    text = text[:2000]
                # ⓒ 또는 © 또는 (c) 또는 "Copyright" 텍스트 감지
                if re.search(r'[©ⓒ]|copyright|\(c\)', text, re.IGNORECASE):
                    return "C"
        # 페이지 전체에서 마지막 수단으로 검색
        body = soup.find('body')
        if body:
            full_text = body.get_text(separator=' ', strip=True)
            # 마지막 500자에서 copyright 표시 검색
            tail = full_text[-500:] if len(full_text) > 500 else full_text
            if re.search(r'[©ⓒ]|copyright|\(c\)', tail, re.IGNORECASE):
                return "C"
        return ""

    # ──────────────────────────────────────────
    #  ISBN/ISSN 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_isbn_issn(soup: BeautifulSoup) -> tuple[str, str]:
        """웹페이지에서 ISBN과 ISSN 추출. 하이픈 있는 ISBN 우선.
        Returns: (isbn, issn) tuple
        """
        isbn_candidates = []
        issn_candidates = []

        # 1. meta 태그에서 추출
        for attr in ('citation_isbn', 'isbn', 'dc.identifier', 'DC.identifier'):
            meta = soup.find('meta', attrs={'name': attr})
            if meta and meta.get('content', '').strip():
                val = meta['content'].strip()
                if re.search(r'\d{1,5}-?\d{1,7}-?\d{1,7}-?[\dXx]', val):
                    isbn_candidates.append(val)

        for attr in ('citation_issn', 'issn'):
            meta = soup.find('meta', attrs={'name': attr})
            if meta and meta.get('content', '').strip():
                val = meta['content'].strip()
                if re.search(r'\d{4}-?\d{3}[\dXx]', val):
                    issn_candidates.append(val)

        # 2. JSON-LD에서 추출
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    isbn_val = item.get('isbn', '')
                    if isbn_val:
                        if isinstance(isbn_val, list):
                            isbn_candidates.extend(str(v) for v in isbn_val)
                        else:
                            isbn_candidates.append(str(isbn_val))
                    issn_val = item.get('issn', '')
                    if issn_val:
                        if isinstance(issn_val, list):
                            issn_candidates.extend(str(v) for v in issn_val)
                        else:
                            issn_candidates.append(str(issn_val))
            except (json.JSONDecodeError, TypeError):
                continue

        # 3. 본문 텍스트에서 ISBN/ISSN 패턴 추출
        # 검색 범위를 제한 (전체 본문이 아닌 특정 영역)
        search_areas = []
        for sel in ['.publication-details', '.book-info', '.document-info',
                    '.metadata', '.details', '.bibliographic',
                    '[class*="isbn"]', '[class*="issn"]', '[class*="identifier"]',
                    'dl', 'table']:
            for el in soup.select(sel):
                search_areas.append(el.get_text(separator=' ', strip=True))

        # footer 영역도 확인 (종종 ISSN 포함)
        for sel in ['footer', '.footer', '#footer']:
            for el in soup.select(sel):
                search_areas.append(el.get_text(separator=' ', strip=True))

        search_text = ' '.join(search_areas)
        if search_text:
            # ISBN-13 (하이픈 포함/미포함)
            for m in re.finditer(r'ISBN[:\s-]*(\d{3}-\d{1,5}-\d{1,7}-\d{1,7}-\d)', search_text, re.IGNORECASE):
                isbn_candidates.append(m.group(1))
            for m in re.finditer(r'ISBN[:\s-]*(\d{13})', search_text, re.IGNORECASE):
                isbn_candidates.append(m.group(1))
            # ISBN-10
            for m in re.finditer(r'ISBN[:\s-]*(\d{1,5}-\d{1,7}-\d{1,7}-[\dXx])', search_text, re.IGNORECASE):
                isbn_candidates.append(m.group(1))
            for m in re.finditer(r'ISBN[:\s-]*(\d{9}[\dXx])', search_text, re.IGNORECASE):
                isbn_candidates.append(m.group(1))
            # ISSN
            for m in re.finditer(r'ISSN[:\s-]*(\d{4}-\d{3}[\dXx])', search_text, re.IGNORECASE):
                issn_candidates.append(m.group(1))
            for m in re.finditer(r'ISSN[:\s-]*(\d{7}[\dXx])', search_text, re.IGNORECASE):
                issn_candidates.append(m.group(1))

        # ISBN 선택: 하이픈 있는 번호 우선
        isbn = ""
        if isbn_candidates:
            hyphenated = [c for c in isbn_candidates if '-' in c]
            isbn = hyphenated[0] if hyphenated else isbn_candidates[0]

        # ISSN 선택: 하이픈 있는 번호 우선
        issn = ""
        if issn_candidates:
            hyphenated = [c for c in issn_candidates if '-' in c]
            issn = hyphenated[0] if hyphenated else issn_candidates[0]

        return isbn, issn

    # ──────────────────────────────────────────
    #  문서 유형 분류 (5가지)
    # ──────────────────────────────────────────
    @staticmethod
    def _classify_doc_type(soup: BeautifulSoup | None, doc: dict) -> str:
        """문서를 5가지 유형으로 분류: 정책자료, 통계자료, 발간자료, 보고서, 회의자료
        우선순위: 메타태그 → document_type 필드 → 제목/URL 패턴
        """
        doc_type_raw = (doc.get('document_type', '') or '').lower()
        title = (doc.get('title', '') or '').lower()
        link = (doc.get('link', '') or '').lower()
        combined = f"{doc_type_raw} {title} {link}"

        # 회의자료 패턴
        if re.search(r'\b(conference|summit|symposium|workshop|seminar|forum|'
                      r'proceeding|meeting|roundtable|panel|colloquium)\b', combined):
            return "회의자료"

        # 통계자료 패턴
        if re.search(r'\b(statistic|census|survey data|data\s*set|indicator|'
                      r'demographic data|statistical|national_statistics|'
                      r'official_statistics|statistical_data_set)\b', combined):
            return "통계자료"

        # 보고서 패턴 (가장 일반적)
        if re.search(r'\b(report|evaluation|assessment|audit|review|inquiry|'
                      r'investigation|inspection|examination|corporate_report|'
                      r'independent_report|annual report|technical report)\b', combined):
            return "보고서"

        # 정책자료 패턴
        if re.search(r'\b(policy|regulation|legislation|directive|guidance|'
                      r'strategy|framework|action plan|white paper|green paper|'
                      r'policy_paper|impact_assessment|consultation|bill|act|'
                      r'executive order|memorandum)\b', combined):
            return "정책자료"

        # HTML 메타태그에서 추가 힌트
        if soup is not None:
            # og:type, article:section 등
            for attr_name in ('og:type', 'article:section', 'dc.type', 'DC.type'):
                meta = soup.find('meta', attrs={'property': attr_name}) or \
                       soup.find('meta', attrs={'name': attr_name})
                if meta and meta.get('content', ''):
                    content = meta['content'].lower()
                    if any(w in content for w in ['conference', 'meeting', 'event']):
                        return "회의자료"
                    if any(w in content for w in ['statistic', 'dataset', 'data']):
                        return "통계자료"
                    if any(w in content for w in ['report', 'review']):
                        return "보고서"
                    if any(w in content for w in ['policy', 'regulation', 'law']):
                        return "정책자료"

        # 기본값: 발간자료 (위 카테고리에 해당하지 않는 일반 출판물)
        return "발간자료"

    # ──────────────────────────────────────────
    #  SITE_SERIES_MAP 조회
    # ──────────────────────────────────────────
    @staticmethod
    def _get_series_default(site_code: str, doc: dict) -> str:
        mapping = SITE_SERIES_MAP.get(site_code, "")
        if isinstance(mapping, str):
            return mapping
        if isinstance(mapping, dict):
            doc_type = doc.get('document_type', '').lower()
            link = doc.get('link', '').lower()
            for key, value in mapping.items():
                if key == '_default':
                    continue
                if key in doc_type or key in link:
                    return value
            return mapping.get('_default', '')
        return ""

    # ──────────────────────────────────────────
    #  URL/제목에서 권호정보 패턴 추출
    # ──────────────────────────────────────────
    @staticmethod
    def _extract_volume_from_url_or_title(url: str, title: str) -> str:
        text = f"{url} {title}"

        patterns = [
            r'(WP\d{4}-\d+)',                    # WP2026-02
            r'(FEDS\s*\d{4}-\d+)',                # FEDS 2026-01
            r'(IFDP\s*\d{4}-\d+)',                # IFDP 2026-01
            r'\b((?:R|RL|IF|IN)\d{4,6})\b',       # CRS: R12345, RL12345
            r'(GAO-\d{2}-\d+)',                    # GAO-26-123456
            # CMCC: tn0302 → TN0302
            r'\b[Tt][Nn](\d{3,5})\b',
            # RAND: RRA4386-1, PTA4155-1
            r'\b((?:RR|PT)[A-Z]?\d{3,5}(?:-\d+)?)\b',
            # Kiel: /34930/ (5자리 숫자 ID)
            r'/(\d{5})/?(?:\s|$|\.)',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = m.group(1) if m.lastindex else m.group(0)
                return val.strip()

        return ""


# ═══════════════════════════════════════════════
#  GOV.UK — Content API 활용
# ═══════════════════════════════════════════════
class GovukMetaResolver(BaseMetadataResolver):
    """
    GOV.UK Content API에서 메타데이터 추출
    - document_collections → 수록잡지 대용
    - taxons[].title → 키워드
    """

    def resolve(self, doc: dict) -> dict:
        link = doc.get('link', '')
        result = {"journal": "", "volume_info": "", "keywords": "", "license": "",
                  "isbn": "", "issn": "", "doc_type": ""}

        if not link:
            return result

        parsed = urlparse(link)
        path = parsed.path
        if not path or path == '/':
            return result

        api_url = f"https://www.gov.uk/api/content{path}"
        resp = self._get(api_url)
        if resp is None:
            return result

        try:
            data = resp.json()
        except (ValueError, AttributeError):
            return result

        # 라이선스: GOV.UK Content API의 license 필드
        api_license = data.get('license', '')
        if api_license:
            result["license"] = GenericMetaResolver._classify_license(api_license)
        if not result["license"]:
            # GOV.UK 기본: Open Government Licence
            result["license"] = "OGL"

        # 수록잡지: document_type → 시리즈명
        doc_type = data.get('document_type', '')
        schema_name = data.get('schema_name', '')
        result["journal"] = self._map_govuk_type(doc_type, schema_name)

        # document_collections → 더 구체적인 시리즈명
        links = data.get('links', {})
        collections = links.get('document_collections', [])
        if collections:
            collection_titles = [c.get('title', '') for c in collections if c.get('title')]
            if collection_titles:
                result["journal"] = collection_titles[0]

        # 키워드: taxons
        taxons = links.get('taxons', [])
        if taxons:
            taxon_titles = [t.get('title', '') for t in taxons if t.get('title')]
            if taxon_titles:
                result["keywords"] = _normalize_keywords(', '.join(taxon_titles))

        # 키워드 폴백: topical_events
        if not result["keywords"]:
            topical = links.get('topical_events', [])
            if topical:
                event_titles = [t.get('title', '') for t in topical if t.get('title')]
                if event_titles:
                    result["keywords"] = _normalize_keywords(', '.join(event_titles))

        # 문서 유형 분류 (GOV.UK document_type 활용)
        govuk_type_map = {
            'policy_paper': '정책자료', 'guidance': '정책자료',
            'impact_assessment': '정책자료', 'consultation': '정책자료',
            'official_statistics': '통계자료', 'national_statistics': '통계자료',
            'statistical_data_set': '통계자료',
            'corporate_report': '보고서', 'independent_report': '보고서',
            'research': '발간자료', 'foi_release': '발간자료',
            'transparency': '발간자료', 'notice': '발간자료',
        }
        result["doc_type"] = govuk_type_map.get(doc_type, '발간자료')

        return result

    @staticmethod
    def _map_govuk_type(doc_type: str, schema_name: str) -> str:
        type_map = {
            'policy_paper': 'Policy Paper',
            'research': 'Research and Analysis',
            'official_statistics': 'Official Statistics',
            'corporate_report': 'Corporate Report',
            'impact_assessment': 'Impact Assessment',
            'guidance': 'Guidance',
            'statistical_data_set': 'Statistical Data Set',
            'independent_report': 'Independent Report',
            'foi_release': 'FOI Release',
            'transparency': 'Transparency Data',
            'notice': 'Notice',
            'consultation': 'Consultation',
            'national_statistics': 'National Statistics',
        }
        if doc_type in type_map:
            return type_map[doc_type]
        if schema_name:
            return schema_name.replace('_', ' ').title()
        return "GOV.UK Publication"


# ═══════════════════════════════════════════════
#  BEA 전용
# ═══════════════════════════════════════════════
class BeaMetaResolver(GenericMetaResolver):
    """BEA 전용: WP번호 추출 + paper-abstract 키워드"""

    def resolve(self, doc: dict) -> dict:
        result = super().resolve(doc)

        if not result["journal"]:
            result["journal"] = "BEA Working Paper Series"

        link = doc.get('link', '')
        if not result["volume_info"] and link:
            m = re.search(r'(?:BEA-?)?(WP\d{4}-\d+)', link, re.IGNORECASE)
            if m:
                result["volume_info"] = m.group(1)

        # BEA 전용 키워드 추출 (이미 super()에서 soup 방문했으므로 재방문 방지)
        if not result["keywords"] and link and link.startswith('http'):
            soup = self._soup(link)
            if soup:
                kw = self._extract_bea_keywords(soup)
                if kw:
                    result["keywords"] = _normalize_keywords(kw)

        return result

    @staticmethod
    def _extract_bea_keywords(soup: BeautifulSoup) -> str:
        abstract_div = soup.find('div', class_='paper-abstract')
        if abstract_div:
            text = abstract_div.get_text()
            m = re.search(r'Keywords?\s*:\s*(.+?)(?:JEL|$)', text,
                          re.IGNORECASE | re.DOTALL)
            if m:
                return m.group(1).strip().rstrip('.')

        for tag in soup.find_all(['p', 'div', 'span']):
            text = tag.get_text()
            m = re.match(r'Keywords?\s*:\s*(.+)', text, re.IGNORECASE)
            if m:
                kw = m.group(1).strip().rstrip('.')
                if len(kw) < 500:
                    return kw
        return ""


# ═══════════════════════════════════════════════
#  FRB (Federal Reserve Board) 전용
# ═══════════════════════════════════════════════
class FrbMetaResolver(GenericMetaResolver):
    """FRB 전용: FEDS/IFDP/Notes 시리즈 구분 + URL 번호 추출"""

    def resolve(self, doc: dict) -> dict:
        result = super().resolve(doc)
        link = doc.get('link', '').lower()
        doc_type = doc.get('document_type', '').lower()

        if not result["journal"] or result["journal"] == SITE_SERIES_MAP.get("Z00063", {}).get("_default", ""):
            if 'feds' in doc_type and 'note' in doc_type:
                result["journal"] = "FEDS Notes"
            elif 'feds' in link or 'feds' in doc_type:
                result["journal"] = "FEDS Working Paper"
            elif 'ifdp' in link or 'ifdp' in doc_type:
                result["journal"] = "IFDP"
            else:
                result["journal"] = "Federal Reserve Board Publication"

        if not result["volume_info"]:
            for pat, fmt in [
                (r'/feds/(\d{4})/\1(\d{2,3})/', 'FEDS {}-{}'),
                (r'/ifdp/(\d{4})/\1(\d{2,3})/', 'IFDP {}-{}'),
                (r'/feds/(\d{4})/(\d+)/', 'FEDS {}-{}'),
            ]:
                m = re.search(pat, link)
                if m:
                    result["volume_info"] = fmt.format(m.group(1), m.group(2))
                    break

        return result


# ═══════════════════════════════════════════════
#  ERS (Economic Research Service) 전용
# ═══════════════════════════════════════════════
class ErsMetaResolver(GenericMetaResolver):
    """ERS 전용: report_number 활용"""

    def resolve(self, doc: dict) -> dict:
        result = super().resolve(doc)

        if not result["journal"]:
            result["journal"] = "ERS Report"

        report_num = (doc.get('report_number') or '').strip()
        if report_num and not result["volume_info"]:
            result["volume_info"] = report_num

        return result


# ═══════════════════════════════════════════════
#  리졸버 레지스트리
# ═══════════════════════════════════════════════
RESOLVER_REGISTRY: dict[str, type[BaseMetadataResolver]] = {
    'Z00057': BeaMetaResolver,     # BEA
    'Z00063': FrbMetaResolver,     # FRB
    'Z00038': ErsMetaResolver,     # ERS
}


def get_resolver(country: str, site_code: str) -> BaseMetadataResolver:
    """국가/사이트코드에 맞는 메타데이터 리졸버 반환"""
    if country == "UK":
        return GovukMetaResolver()
    if site_code in RESOLVER_REGISTRY:
        return RESOLVER_REGISTRY[site_code]()
    return GenericMetaResolver()


# ═══════════════════════════════════════════════
#  메인 실행 로직
# ═══════════════════════════════════════════════
def process_json_file(fpath: str, force: bool = False,
                      completed: set | None = None) -> dict:
    """
    단일 JSON 파일의 모든 문서에 대해 메타데이터를 추출하여 업데이트
    Returns: {'total': N, 'resolved': N, 'skipped': N, 'failed': N}
    """
    if completed is None:
        completed = set()
    fname = os.path.basename(fpath)

    with open(fpath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    documents = data.get('documents', [])
    if not documents:
        return {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}

    # 국가 판별 (pdf_url_resolver.py와 동일 로직)
    if fname.startswith("govuk_"):
        country = "UK"
    elif fname.startswith("us_"):
        country = "US"
    elif fname.startswith("se_"):
        country = "SW"
    elif fname.startswith("sg_"):
        country = "SI"
    else:
        parts = fname.split("_")
        if len(parts) >= 2 and len(parts[0]) == 2 and parts[0].isalpha():
            country = parts[0].upper()
        else:
            country = ""

    site_code = data.get('metadata', {}).get('site_code', '')
    acronym = data.get('metadata', {}).get('acronym', site_code)

    resolver = get_resolver(country, site_code)
    logger.info(f"처리 중: {fname} ({len(documents)}건) → {type(resolver).__name__}")

    site_name = data.get('metadata', {}).get('site_name', '')
    stats = {'total': len(documents), 'resolved': 0, 'skipped': 0, 'failed': 0}
    modified = False

    for i, doc in enumerate(documents):
        # 이미 처리된 문서는 건너뛰기 (--force가 아닌 경우)
        if not force and doc.get('metadata_resolved_at') is not None:
            stats['skipped'] += 1
            continue

        # 2025-12 이전 발행 자료 스킵
        pub_date = _parse_date(doc.get('published_date', ''))
        if pub_date and pub_date < MIN_DATE:
            stats['skipped'] += 1
            continue

        # completed sites.csv 중복 스킵
        title = doc.get('title', '').strip()
        if completed and (site_name, title) in completed:
            stats['skipped'] += 1
            continue

        try:
            meta = resolver.resolve(doc)

            doc['journal'] = meta.get('journal', '')
            doc['volume_info'] = meta.get('volume_info', '')
            doc['keywords'] = meta.get('keywords', '')
            doc['license'] = meta.get('license', '')
            doc['isbn'] = meta.get('isbn', '')
            doc['issn'] = meta.get('issn', '')
            doc['doc_type'] = meta.get('doc_type', '')
            doc['metadata_resolved_at'] = datetime.now().isoformat()
            modified = True

            has_any = any([meta.get('journal'), meta.get('volume_info'), meta.get('keywords')])
            if has_any:
                stats['resolved'] += 1
                logger.debug(f"  [{i+1}/{len(documents)}] 메타 추출: "
                             f"잡지={meta.get('journal', '')[:30]}, "
                             f"권호={meta.get('volume_info', '')[:20]}, "
                             f"키워드={meta.get('keywords', '')[:40]}")
            else:
                stats['failed'] += 1
                logger.debug(f"  [{i+1}/{len(documents)}] 메타 없음: {doc.get('title', '')[:50]}")

        except Exception as e:
            doc['journal'] = ''
            doc['volume_info'] = ''
            doc['keywords'] = ''
            doc['license'] = ''
            doc['isbn'] = ''
            doc['issn'] = ''
            doc['doc_type'] = ''
            doc['metadata_resolved_at'] = datetime.now().isoformat()
            modified = True
            stats['failed'] += 1
            logger.warning(f"  [{i+1}/{len(documents)}] 오류: {e}")

        # 진행상황 로그 (50건마다)
        processed = stats['resolved'] + stats['failed']
        if processed > 0 and processed % 50 == 0:
            logger.info(f"  {acronym}: {processed}/{stats['total']} 처리 "
                        f"(성공: {stats['resolved']}, 실패: {stats['failed']})")

        # 요청 간 대기
        if i < len(documents) - 1:
            time.sleep(REQUEST_DELAY)

    # 파일 저장
    if modified:
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"  저장 완료: {fname} "
                    f"(성공: {stats['resolved']}, 실패: {stats['failed']}, 건너뜀: {stats['skipped']})")

    return stats


def run(country: str | None = None, force: bool = False):
    """전체 또는 국가별 메타데이터 추출 실행"""
    json_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))

    # summary 파일 제외
    json_files = [f for f in json_files if "summary" not in os.path.basename(f)]

    # 국가 필터
    if country:
        prefix_map = {"UK": "govuk_", "US": "us_", "SW": "se_", "SI": "sg_"}
        prefix = prefix_map.get(country, country.lower() + "_")
        json_files = [f for f in json_files if os.path.basename(f).startswith(prefix)]

    if not json_files:
        logger.warning("처리할 JSON 파일이 없습니다.")
        return

    completed = _load_completed()

    logger.info(f"메타데이터 추출 시작: {len(json_files)}개 파일")
    logger.info(f"  국가 필터: {country or '전체'}, 강제 재처리: {force}")
    logger.info("=" * 60)

    total_stats = {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}
    start_time = time.time()

    for i, fpath in enumerate(json_files, 1):
        fname = os.path.basename(fpath)
        logger.info(f"[{i}/{len(json_files)}] {fname}")

        stats = process_json_file(fpath, force=force, completed=completed)
        for k in total_stats:
            total_stats[k] += stats[k]

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"메타데이터 추출 완료! (소요시간: {elapsed:.0f}초)")
    logger.info(f"  전체: {total_stats['total']}건")
    logger.info(f"  성공: {total_stats['resolved']}건")
    logger.info(f"  실패: {total_stats['failed']}건")
    logger.info(f"  건너뜀: {total_stats['skipped']}건")


def main():
    parser = argparse.ArgumentParser(description='수록잡지·권호정보·키워드 메타데이터 수집기')
    parser.add_argument('--country', '-c',
                        help='국가 필터 (예: UK, US, AT, CA, NO 등)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='이미 처리된 문서도 재처리')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='상세 로그 출력')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run(country=args.country, force=args.force)


if __name__ == '__main__':
    main()
