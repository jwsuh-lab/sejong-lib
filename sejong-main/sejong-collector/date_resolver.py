"""
발행일 추출기 — 문서 랜딩 페이지에서 published_date를 추출
- pdf_url_resolver.py와 동일한 패턴: JSON 파일을 순회하며 업데이트
- 전략 우선순위:
  1. JSON-LD (script type="application/ld+json") → datePublished / date
  2. meta 태그 → article:published_time, DC.date, dcterms.date 등
  3. <time> 태그 → datetime 속성
  4. 본문 텍스트 → 날짜 패턴 정규식 (제목 근처 우선)
  5. 페이지 하단 "last updated" / "date modified" 텍스트
"""
import argparse
import json
import glob
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Playwright 사용 가능 여부
try:
    import playwright  # noqa: F401
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
REQUEST_DELAY = 1.0

# ═══════════════════════════════════════════════
#  날짜 파싱 유틸
# ═══════════════════════════════════════════════

# 영문 월 이름 → 숫자 매핑
_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9,
    'oct': 10, 'nov': 11, 'dec': 12,
}

# ISO 날짜 패턴: 2026-02-23, 2026-02-23T11:18:00Z
_ISO_RE = re.compile(r'(\d{4})-(\d{2})-(\d{2})')

# 영문 날짜 패턴: February 23, 2026 / 23 February 2026 / Feb 23, 2026
_EN_DATE_RE = re.compile(
    r'(\d{1,2})\s+'
    r'(January|February|March|April|May|June|July|August|September|October|November|December'
    r'|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+'
    r'(\d{4})',
    re.IGNORECASE,
)
_EN_DATE_RE2 = re.compile(
    r'(January|February|March|April|May|June|July|August|September|October|November|December'
    r'|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+'
    r'(\d{1,2}),?\s+(\d{4})',
    re.IGNORECASE,
)

# US 형식: 2/23/2026, 02/23/2026
_US_DATE_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{4})')

# 2자리 연도: 01/30/26
_US_DATE_SHORT_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{2})(?!\d)')

# Drupal 날짜: "Fri, 02/20/2026 - 14:45"
_DRUPAL_DATE_RE = re.compile(r'[A-Z][a-z]{2},\s*(\d{1,2}/\d{1,2}/\d{4})\s*-\s*\d{2}:\d{2}')


def _parse_date_str(s: str) -> str | None:
    """다양한 형식의 날짜 문자열을 ISO 형식(YYYY-MM-DD)으로 변환"""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()

    # ISO 형식: 2026-02-23...
    m = _ISO_RE.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2020 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"

    # 영문: 23 February 2026
    m = _EN_DATE_RE.search(s)
    if m:
        d, mon_str, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        mo = _MONTH_MAP.get(mon_str)
        if mo and 2020 <= y <= 2030 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"

    # 영문: February 23, 2026
    m = _EN_DATE_RE2.search(s)
    if m:
        mon_str, d, y = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        mo = _MONTH_MAP.get(mon_str)
        if mo and 2020 <= y <= 2030 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"

    # Drupal 형식: "Fri, 02/20/2026 - 14:45"
    m = _DRUPAL_DATE_RE.search(s)
    if m:
        return _parse_date_str(m.group(1))

    # US 형식: 2/23/2026
    m = _US_DATE_RE.search(s)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2020 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"

    # 2자리 연도: 01/30/26 → 2026
    m = _US_DATE_SHORT_RE.search(s)
    if m:
        mo, d, y2 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        y = 2000 + y2
        if 2020 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"

    return None


# ═══════════════════════════════════════════════
#  HTML 기반 날짜 추출 전략
# ═══════════════════════════════════════════════

def _extract_jsonld(soup: BeautifulSoup) -> str | None:
    """전략1: JSON-LD에서 datePublished/dateModified/date 추출"""
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            # 리스트인 경우 첫 번째 항목
            if isinstance(data, list):
                data = data[0] if data else {}
            for key in ('datePublished', 'dateCreated', 'dateModified', 'date'):
                val = data.get(key)
                if val:
                    parsed = _parse_date_str(str(val))
                    if parsed:
                        return parsed
            # @graph 내부도 확인
            for item in data.get('@graph', []):
                if isinstance(item, dict):
                    for key in ('datePublished', 'dateCreated', 'dateModified', 'date'):
                        val = item.get(key)
                        if val:
                            parsed = _parse_date_str(str(val))
                            if parsed:
                                return parsed
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return None


def _extract_meta(soup: BeautifulSoup) -> str | None:
    """전략2: meta 태그에서 날짜 추출"""
    meta_names = [
        'article:published_time', 'og:article:published_time',
        'article:modified_time', 'og:updated_time',
        'DC.date', 'DC.date.created', 'DC.date.issued',
        'dcterms.date', 'dcterms.created', 'dcterms.issued', 'dcterms.modified',
        'date', 'pubdate', 'sailthru.date',
        'citation_publication_date', 'citation_date',
    ]
    for name in meta_names:
        # property 또는 name 속성으로 검색
        tag = soup.find('meta', attrs={'property': name})
        if not tag:
            tag = soup.find('meta', attrs={'name': name})
        if tag:
            content = tag.get('content', '')
            parsed = _parse_date_str(content)
            if parsed:
                return parsed
    return None


def _extract_time_tag(soup: BeautifulSoup) -> str | None:
    """전략3: <time> 태그의 datetime 속성"""
    for time_tag in soup.find_all('time', attrs={'datetime': True}):
        parsed = _parse_date_str(time_tag['datetime'])
        if parsed:
            return parsed
    # datetime 없는 time 태그의 텍스트
    for time_tag in soup.find_all('time'):
        text = time_tag.get_text(strip=True)
        parsed = _parse_date_str(text)
        if parsed:
            return parsed
    return None


def _extract_label_date(soup: BeautifulSoup) -> str | None:
    """전략4: 라벨 기반 날짜 추출 — "Date Published:", "Published:", "Release Date:" 등"""
    # 라벨 키워드 목록
    date_labels = [
        'date published', 'published', 'publication date', 'release date',
        'date released', 'posted', 'post date', 'created',
        'date posted', 'issued', 'date issued',
    ]

    # 전체 텍스트에서 "Label: Date" 패턴 검색
    main = soup.find('main') or soup.find('article') or soup.find('body')
    if not main:
        return None
    text = main.get_text(separator='\n', strip=True)
    for line in text.split('\n'):
        line_lower = line.strip().lower()
        for label in date_labels:
            if line_lower.startswith(label):
                # "Date Published: February 1, 2026" or "Published February 1, 2026"
                remainder = line[len(label):].strip().lstrip(':').strip()
                parsed = _parse_date_str(remainder)
                if parsed:
                    return parsed

    # dt/dd, label/value 구조 검색
    for dl in soup.find_all(['dl', 'table']):
        for dt in dl.find_all(['dt', 'th', 'strong']):
            dt_text = dt.get_text(strip=True).lower().rstrip(':')
            if any(lbl in dt_text for lbl in date_labels):
                # 다음 sibling에서 날짜 추출
                dd = dt.find_next_sibling(['dd', 'td', 'span', 'div', 'p'])
                if dd:
                    parsed = _parse_date_str(dd.get_text(strip=True))
                    if parsed:
                        return parsed

    return None


def _extract_visible_date(soup: BeautifulSoup) -> str | None:
    """전략5: 본문에서 날짜 패턴 추출 (date 클래스 우선)"""
    # date 관련 클래스/id를 가진 요소 우선
    for selector in [
        '[class*="date"]', '[class*="Date"]', '[class*="publish"]',
        '[class*="time"]', '[class*="byline"]',
        '[id*="date"]', '[id*="publish"]',
        '.field--name-field-date', '.date-display-single',
        '.publication-date', '.article-date', '.post-date',
        'dl dt', '.metadata',
    ]:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            parsed = _parse_date_str(text)
            if parsed:
                return parsed

    # Canada.ca 특화: "Date modified" 패턴
    for dl in soup.find_all('dl'):
        dt_tags = dl.find_all('dt')
        dd_tags = dl.find_all('dd')
        for dt, dd in zip(dt_tags, dd_tags):
            dt_text = dt.get_text(strip=True).lower()
            if 'date modified' in dt_text or 'date' in dt_text:
                parsed = _parse_date_str(dd.get_text(strip=True))
                if parsed:
                    return parsed

    return None


def _extract_text_fallback(soup: BeautifulSoup) -> str | None:
    """전략5: 전체 텍스트에서 가장 최근 날짜 패턴 추출"""
    # 본문 영역만 대상 (nav, footer 제외)
    main = soup.find('main') or soup.find('article') or soup.find('body')
    if not main:
        return None

    text = main.get_text(separator=' ', strip=True)
    # 텍스트 앞부분 2000자만 확인 (제목/발행일 근처)
    text = text[:2000]

    dates_found = []

    # ISO 패턴
    for m in _ISO_RE.finditer(text):
        parsed = _parse_date_str(m.group(0))
        if parsed:
            dates_found.append(parsed)

    # 영문 패턴
    for m in _EN_DATE_RE.finditer(text):
        parsed = _parse_date_str(m.group(0))
        if parsed:
            dates_found.append(parsed)
    for m in _EN_DATE_RE2.finditer(text):
        parsed = _parse_date_str(m.group(0))
        if parsed:
            dates_found.append(parsed)

    if dates_found:
        # 가장 최근 날짜 반환
        dates_found.sort(reverse=True)
        return dates_found[0]

    return None


def extract_date_from_html(soup: BeautifulSoup) -> tuple[str | None, str]:
    """HTML에서 발행일 추출 — 전략 cascade

    Returns:
        (date_str, strategy_name) 또는 (None, 'none')
    """
    strategies = [
        ('jsonld', _extract_jsonld),
        ('meta', _extract_meta),
        ('time-tag', _extract_time_tag),
        ('label', _extract_label_date),
        ('visible', _extract_visible_date),
        ('text-fallback', _extract_text_fallback),
    ]
    for name, fn in strategies:
        result = fn(soup)
        if result:
            return result, name
    return None, 'none'


# ═══════════════════════════════════════════════
#  HTTP/Playwright 요청
# ═══════════════════════════════════════════════

_session = requests.Session()
_session.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/131.0.0.0 Safari/537.36 '
        'SejongLibrary-DateResolver/1.0'
    ),
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})

# Playwright subprocess script (403 폴백용)
_PW_SCRIPT = '''
import sys, json
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
url = sys.argv[1]
pw = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
context = browser.new_context(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    locale="en-US",
)
page = context.new_page()
Stealth().apply_stealth_sync(page)
resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
page.wait_for_timeout(3000)
html = page.content()
status = resp.status if resp else 200
context.close()
browser.close()
pw.stop()
result = json.dumps({"status": status, "html": html}, ensure_ascii=False)
sys.stdout.reconfigure(encoding="utf-8")
print(result)
'''


def _fetch_soup(url: str) -> BeautifulSoup | None:
    """URL에서 BeautifulSoup 객체를 가져옴 (403은 빠르게 실패 — 날짜 추출에 Playwright 불필요)"""
    if not url or not url.startswith('http'):
        return None
    try:
        resp = _session.get(url, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'lxml')
    except requests.RequestException:
        return None


def _pw_fetch_soup(url: str) -> BeautifulSoup | None:
    """Playwright subprocess로 페이지 가져오기"""
    try:
        result = subprocess.run(
            [sys.executable, '-c', _PW_SCRIPT, url],
            capture_output=True, text=True, timeout=60, encoding='utf-8',
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        return BeautifulSoup(data['html'], 'lxml')
    except Exception:
        return None


# ═══════════════════════════════════════════════
#  JSON 파일 처리
# ═══════════════════════════════════════════════

def process_json_file(fpath: str, force: bool = False) -> dict:
    """단일 JSON 파일의 published_date 없는 문서에 날짜 추출"""
    fname = os.path.basename(fpath)

    with open(fpath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    documents = data.get('documents', [])
    if not documents:
        return {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}

    acronym = data.get('metadata', {}).get('acronym', fname[:20])
    logger.info(f"처리 중: {fname} ({len(documents)}건)")

    stats = {'total': len(documents), 'resolved': 0, 'skipped': 0, 'failed': 0}
    modified = False
    strategies_used = {}

    for i, doc in enumerate(documents):
        existing_date = doc.get('published_date', '')

        # 이미 날짜가 있으면 건너뛰기
        if not force and existing_date:
            stats['skipped'] += 1
            continue

        link = doc.get('link', '')
        if not link or not link.startswith('http'):
            stats['failed'] += 1
            continue

        soup = _fetch_soup(link)
        if not soup:
            stats['failed'] += 1
            continue

        date_str, strategy = extract_date_from_html(soup)

        if date_str:
            doc['published_date'] = date_str
            doc['_date_resolved_by'] = strategy
            stats['resolved'] += 1
            strategies_used[strategy] = strategies_used.get(strategy, 0) + 1
            modified = True
        else:
            stats['failed'] += 1

        # 요청 간 대기
        if i < len(documents) - 1:
            time.sleep(REQUEST_DELAY)

    # 파일 저장
    if modified:
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    strategy_str = ', '.join(f'{k}:{v}' for k, v in strategies_used.items()) if strategies_used else 'none'
    logger.info(f"  완료: {fname} "
                f"(성공: {stats['resolved']}, 실패: {stats['failed']}, "
                f"건너뜀: {stats['skipped']}) [전략: {strategy_str}]")

    return stats


def run(country: str | None = None, force: bool = False):
    """전체 또는 국가별 날짜 추출"""
    json_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    json_files = [f for f in json_files if "summary" not in os.path.basename(f)]

    # 국가 필터
    if country:
        prefix_map = {"UK": "govuk_", "US": "us_", "SW": "se_", "SI": "sg_"}
        prefix = prefix_map.get(country.upper(), country.lower() + "_")
        json_files = [f for f in json_files if os.path.basename(f).startswith(prefix)]

    if not json_files:
        logger.warning("처리할 JSON 파일이 없습니다.")
        return

    # 날짜 없는 문서가 있는 파일만 필터 (force가 아닌 경우)
    if not force:
        files_needing_dates = []
        for fpath in json_files:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            docs = data.get('documents', [])
            missing = sum(1 for d in docs if not d.get('published_date', ''))
            if missing > 0:
                files_needing_dates.append((fpath, missing, len(docs)))
        if not files_needing_dates:
            logger.info("모든 문서에 이미 날짜가 있습니다.")
            return
        logger.info(f"날짜 추출 대상: {len(files_needing_dates)}개 파일 "
                    f"(날짜 없는 문서 {sum(m for _, m, _ in files_needing_dates)}건)")
        json_files = [f for f, _, _ in files_needing_dates]
    else:
        logger.info(f"날짜 추출 시작: {len(json_files)}개 파일 (강제 재처리)")

    logger.info("=" * 60)

    total_stats = {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}
    start_time = time.time()

    for i, fpath in enumerate(json_files, 1):
        fname = os.path.basename(fpath)
        logger.info(f"[{i}/{len(json_files)}] {fname}")

        stats = process_json_file(fpath, force=force)
        for k in total_stats:
            total_stats[k] += stats[k]

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"날짜 추출 완료! (소요시간: {elapsed:.0f}초)")
    logger.info(f"  전체: {total_stats['total']}건")
    logger.info(f"  성공: {total_stats['resolved']}건")
    logger.info(f"  실패: {total_stats['failed']}건")
    logger.info(f"  건너뜀: {total_stats['skipped']}건")


def main():
    parser = argparse.ArgumentParser(description='발행일 추출기 — 문서 페이지에서 날짜 추출')
    parser.add_argument('--country', '-c',
                        help='국가 필터 (예: US, AT, CA, GB 등)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='이미 날짜가 있는 문서도 재처리')
    parser.add_argument('--file', help='특정 JSON 파일만 처리')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='상세 로그 출력')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.file:
        stats = process_json_file(args.file, force=args.force)
        print(f"완료: {stats}")
    else:
        run(country=args.country, force=args.force)


if __name__ == '__main__':
    main()
