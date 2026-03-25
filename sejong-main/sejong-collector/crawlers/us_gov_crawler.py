"""
미국 정부/연구기관 사이트 크롤러 모음
- 사이트마다 구조가 다르므로 패턴별 전용 크롤러를 등록하여 사용
- 전용 크롤러가 없는 사이트는 GenericHtmlCrawler(범용)로 폴백
"""
import json
import logging
import re
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup, Tag

# Playwright (선택적 의존성 — 미설치 시 기존 동작 유지)
try:
    import playwright  # noqa: F401 — 설치 여부만 확인
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

sys.path.insert(0, str(Path(__file__).parent.parent))
from site_manager import SiteManager, Site
from completed_filter import load_completed_titles, filter_completed
from date_filter import filter_by_date
from relevance_filter import filter_by_relevance

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

REQUEST_DELAY = 1.5


# ═══════════════════════════════════════════════
#  베이스 클래스
# ═══════════════════════════════════════════════
class BaseSiteCrawler(ABC):
    """미국 사이트 크롤러 베이스 클래스"""

    # Playwright 폴백 대상 HTTP 상태 코드
    _PW_FALLBACK_CODES = {403, 429, 503}

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/131.0.0.0 Safari/537.36 '
                'SejongLibrary-Crawler/1.0'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    @abstractmethod
    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        ...

    def save(self, site: Site, results: list[dict]) -> Path | None:
        if not results:
            return None
        timestamp = datetime.now().strftime('%Y%m%d')
        tag = (site.acronym or site.code).replace(' ', '').replace('&', '')
        filepath = self.data_dir / f"us_{site.code}_{tag}_{timestamp}.json"
        output = {
            'metadata': {
                'site_code': site.code,
                'site_name': site.name,
                'site_name_kr': site.name_kr,
                'acronym': site.acronym,
                'source_url': site.url,
                'crawled_at': datetime.now().isoformat(),
                'total_collected': len(results),
            },
            'documents': results,
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logger.info(f"  저장: {filepath.name} ({len(results)}건)")
        return filepath

    # ── Playwright 헬퍼 (subprocess 격리 실행) ──
    # Playwright sync API는 asyncio 이벤트 루프와 충돌하므로
    # 별도 프로세스에서 실행하여 완전 격리한다.

    _PW_SCRIPT = '''
import sys, json
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

url = sys.argv[1]
wait_ms = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

pw = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
context = browser.new_context(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    locale="en-US",
)
page = context.new_page()
Stealth().apply_stealth_sync(page)
resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
page.wait_for_timeout(wait_ms)
html = page.content()
status = resp.status if resp else 200
context.close()
browser.close()
pw.stop()

result = json.dumps({"status": status, "html": html}, ensure_ascii=False)
sys.stdout.reconfigure(encoding="utf-8")
print(result)
'''

    @classmethod
    def _pw_get(cls, url: str, wait_ms: int = 5000) -> requests.Response | None:
        """Playwright를 subprocess로 실행하여 페이지 HTML 획득"""
        if not HAS_PLAYWRIGHT:
            return None
        import subprocess
        try:
            result = subprocess.run(
                [sys.executable, '-c', cls._PW_SCRIPT, url, str(wait_ms)],
                capture_output=True, text=True, timeout=60, encoding='utf-8',
            )
            if result.returncode != 0:
                stderr = result.stderr.strip()[-200:] if result.stderr else ''
                logger.error(f"  Playwright subprocess 실패 (rc={result.returncode}): {stderr}")
                return None

            data = json.loads(result.stdout)
            status = data['status']
            html = data['html']

            # fake requests.Response 생성
            fake = requests.Response()
            fake.status_code = status
            fake._content = html.encode('utf-8')
            fake.encoding = 'utf-8'
            fake.url = url
            fake.headers['Content-Type'] = 'text/html; charset=utf-8'
            logger.info(f"  Playwright 폴백 성공: {url} (status={status})")
            return fake
        except subprocess.TimeoutExpired:
            logger.error(f"  Playwright subprocess 타임아웃: {url}")
            return None
        except Exception as e:
            logger.error(f"  Playwright 폴백 실패: {e}")
            return None

    # ── 공통 헬퍼 ──
    def _get(self, url: str, max_retries: int = 3, **kw) -> requests.Response | None:
        timeout = kw.pop('timeout', 60)
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, timeout=timeout, **kw)
                # 429 Rate Limited — 대기 후 재시도
                if resp.status_code == 429:
                    wait = int(resp.headers.get('Retry-After', 2 ** attempt * 5))
                    logger.warning(f"  429 Rate Limited, {wait}초 대기 (attempt {attempt}/{max_retries}): {url}")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.SSLError:
                logger.warning(f"  SSL 오류, verify=False로 재시도: {url}")
                try:
                    resp = self.session.get(url, timeout=30, verify=False)
                    resp.raise_for_status()
                    return resp
                except requests.RequestException as e2:
                    logger.error(f"  SSL 재시도 실패: {e2}")
                    return None
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if status in self._PW_FALLBACK_CODES and HAS_PLAYWRIGHT:
                    logger.warning(f"  HTTP {status} → Playwright 폴백 시도: {url}")
                    pw_resp = self._pw_get(url)
                    if pw_resp is not None:
                        return pw_resp
                logger.error(f"  HTTP 오류 ({status}): {url}")
                return None
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning(f"  {type(e).__name__} (attempt {attempt}/{max_retries}), {wait}초 후 재시도: {url}")
                    time.sleep(wait)
                    continue
                logger.error(f"  {max_retries}회 재시도 후 실패: {url} — {e}")
                return None
            except requests.RequestException as e:
                logger.error(f"  요청 실패: {url} — {e}")
                return None
        return None

    def _soup(self, url: str) -> BeautifulSoup | None:
        resp = self._get(url)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, 'lxml')
        # Cloudflare 챌린지 감지 → Playwright 폴백
        if self._is_cloudflare_challenge(soup) and HAS_PLAYWRIGHT:
            logger.warning(f"  Cloudflare 챌린지 감지 → Playwright 폴백: {url}")
            pw_resp = self._pw_get(url, wait_ms=8000)
            if pw_resp is not None:
                return BeautifulSoup(pw_resp.text, 'lxml')
        return soup

    @staticmethod
    def _is_cloudflare_challenge(soup: BeautifulSoup) -> bool:
        """Cloudflare/봇 챌린지 페이지인지 감지"""
        title = soup.title.get_text(strip=True).lower() if soup.title else ''
        if 'just a moment' in title or 'attention required' in title:
            return True
        # Cloudflare 챌린지 폼
        if soup.select_one('form#challenge-form, div#challenge-stage'):
            return True
        return False

    def _doc(self, site: Site, **fields) -> dict:
        """표준 문서 dict 생성"""
        base = {
            'site_code': site.code,
            'site_name': site.name,
            'site_acronym': site.acronym,
        }
        base.update(fields)
        return base

    @staticmethod
    def _filter_pdf_direct_links(results: list[dict]) -> list[dict]:
        """URL 수집 원칙: PDF 직접 링크(Direct Link) 제거"""
        filtered = []
        removed = 0
        for doc in results:
            link = doc.get('link', '')
            if link and link.lower().endswith('.pdf'):
                removed += 1
                continue
            filtered.append(doc)
        if removed:
            logger.info(f"  PDF 직접 링크 제거: {removed}건")
        return filtered

    @staticmethod
    def _strip_html(text: str) -> str:
        """HTML 태그 제거"""
        return re.sub(r'<[^>]+>', '', unescape(text)).strip()

    @staticmethod
    def _abs_url(href: str, base: str) -> str:
        if not href:
            return ''
        if href.startswith('http'):
            return href
        return urljoin(base, href)


# ═══════════════════════════════════════════════
#  1. GAO — GovInfo API
# ═══════════════════════════════════════════════
class GaoCrawler(BaseSiteCrawler):
    """GAO: gao.gov 직접 차단 → GovInfo API 사용"""
    GOVINFO_API = "https://api.govinfo.gov"

    def __init__(self, data_dir: Path, api_key: str = "DEMO_KEY"):
        super().__init__(data_dir)
        self.api_key = api_key

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("GAO 크롤링 (GovInfo API)")
        results, offset = [], "*"
        while len(results) < max_results:
            params = {'offsetMark': offset, 'pageSize': min(max_results, 100),
                      'api_key': self.api_key}
            url = f"{self.GOVINFO_API}/collections/GAOREPORTS/2024-01-01T00:00:00Z?{urlencode(params)}"
            resp = self._get(url)
            if not resp:
                break
            data = resp.json()
            for pkg in data.get('packages', []):
                results.append(self._doc(site,
                    title=pkg.get('title', ''),
                    link=pkg.get('packageLink', ''),
                    published_date=pkg.get('lastModified', ''),
                    document_type=pkg.get('docClass', 'REPORT'),
                    package_id=pkg.get('packageId', ''),
                ))
            logger.info(f"  GAO: {len(results)}/{data.get('count', '?')} 건")
            nxt = data.get('nextPage', '')
            if not nxt or len(results) >= max_results:
                break
            try:
                offset = nxt.split('offsetMark=')[1].split('&')[0]
            except (IndexError, AttributeError):
                offset = None
            if not offset:
                break
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  2. NIST — HTML 스크래핑
# ═══════════════════════════════════════════════
class NistCrawler(BaseSiteCrawler):
    """NIST: nist.gov/publications/search  페이지당 25건"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("NIST 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            soup = self._soup(f"https://www.nist.gov/publications/search?page={page}")
            if not soup:
                break
            found = 0
            for h3 in soup.select('h3'):
                a = h3.find('a')
                if not a:
                    continue
                title = a.get_text(strip=True)
                href = self._abs_url(a.get('href', ''), 'https://www.nist.gov')
                date_text, authors = '', ''
                for sib in h3.next_siblings:
                    txt = getattr(sib, 'get_text', lambda **k: str(sib).strip)(strip=True)
                    if not txt:
                        continue
                    dm = re.search(r'(?:January|February|March|April|May|June|July|August|'
                                   r'September|October|November|December)\s+\d{1,2},?\s+\d{4}', txt)
                    if dm and not date_text:
                        date_text = dm.group()
                    elif txt.startswith('Author(s)'):
                        authors = txt.replace('Author(s)', '').strip()
                results.append(self._doc(site, title=title, link=href,
                    published_date=date_text, authors=authors, document_type='publication'))
                found += 1
            logger.info(f"  NIST: {len(results)} 건 (page={page})")
            if found < 25:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  3. BEA — Drupal HTML
# ═══════════════════════════════════════════════
class BeaCrawler(BaseSiteCrawler):
    """BEA: bea.gov/research/papers  Drupal, 페이지당 10건"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("BEA 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            soup = self._soup(f"https://www.bea.gov/research/papers?page={page}")
            if not soup:
                break
            cards = soup.select('div.card.card-document')
            if not cards:
                break
            for card in cards:
                title_el = card.select_one('h2.paper-title a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = self._abs_url(title_el.get('href', ''), 'https://www.bea.gov')
                # 날짜
                time_el = card.select_one('.paper-publication-date time')
                date = time_el.get('datetime', time_el.get_text(strip=True)) if time_el else ''
                # 저자 (첫 번째 .paper-mod-date 중 views-field 아닌 것)
                authors = ''
                for div in card.select('div.paper-mod-date'):
                    if 'views-field' not in div.get('class', []):
                        authors = div.get_text(strip=True)
                        break
                # 초록
                desc_el = card.select_one('div.paper-abstract')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, authors=authors,
                    description=desc, document_type='working_paper'))
            logger.info(f"  BEA: {len(results)} 건 (page={page})")
            if len(cards) < 10:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  4. CBO — Drupal (Cloudflare 주의)
# ═══════════════════════════════════════════════
class CboCrawler(BaseSiteCrawler):
    """CBO: cbo.gov/search  Cloudflare 차단 가능 → 시도 후 실패시 경고"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("CBO 크롤링 (HTML, Cloudflare 차단 가능)")
        results, page = [], 0
        while len(results) < max_results:
            url = f"https://www.cbo.gov/search?page={page}"
            soup = self._soup(url)
            if not soup:
                logger.warning("  CBO: 접속 차단. 브라우저 자동화(Playwright) 필요.")
                break
            rows = soup.select('li.views-row')
            if not rows:
                # Cloudflare challenge 페이지일 수 있음
                if 'challenge' in soup.get_text().lower() or not soup.select('.view-content'):
                    logger.warning("  CBO: Cloudflare 챌린지 감지. 브라우저 자동화 필요.")
                    break
                break
            for row in rows:
                title_el = row.select_one('.views-field-title h3 a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = self._abs_url(title_el.get('href', ''), 'https://www.cbo.gov')
                time_el = row.select_one('time')
                date = time_el.get('datetime', time_el.get_text(strip=True)) if time_el else ''
                type_el = row.select_one('.views-field-type .field-content')
                doc_type = type_el.get_text(strip=True) if type_el else ''
                desc_el = row.select_one('.views-field-search-api-excerpt .field-content')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, description=desc, document_type=doc_type))
            logger.info(f"  CBO: {len(results)} 건 (page={page})")
            if len(rows) < 10:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  5. FRB — 연도별 HTML 페이지
# ═══════════════════════════════════════════════
class FrbCrawler(BaseSiteCrawler):
    """FRB: FEDS + IFDP + FEDS Notes  연도별 정적 HTML"""

    SERIES = [
        ('FEDS', 'https://www.federalreserve.gov/econres/feds/{year}.htm'),
        ('IFDP', 'https://www.federalreserve.gov/econres/ifdp/{year}.htm'),
        ('FEDS Notes', 'https://www.federalreserve.gov/econres/notes/feds-notes/{year}-index.htm'),
    ]

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("FRB 크롤링 (연도별 HTML)")
        results = []
        current_year = datetime.now().year
        for series_name, url_tpl in self.SERIES:
            for year in range(current_year, current_year - 3, -1):
                if len(results) >= max_results:
                    break
                url = url_tpl.format(year=year)
                soup = self._soup(url)
                if not soup:
                    continue
                for entry in soup.select('div.feds-note, div.col-md-9.heading'):
                    h5 = entry.select_one('h5 a')
                    if not h5:
                        continue
                    title = h5.get_text(strip=True)
                    link = self._abs_url(h5.get('href', ''), 'https://www.federalreserve.gov')
                    time_el = entry.select_one('time')
                    date = time_el.get('datetime', time_el.get_text(strip=True)) if time_el else ''
                    auth_el = entry.select_one('div.authors')
                    authors = auth_el.get_text(strip=True) if auth_el else ''
                    results.append(self._doc(site, title=title, link=link,
                        published_date=date, authors=authors,
                        document_type=series_name))
                logger.info(f"  FRB {series_name} {year}: 누적 {len(results)} 건")
                time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  6. CRS — Congress.gov REST API
# ═══════════════════════════════════════════════
class CrsCrawler(BaseSiteCrawler):
    """CRS: Congress.gov API (api.congress.gov/v3/crsreport)"""

    def __init__(self, data_dir: Path, api_key: str = "DEMO_KEY"):
        super().__init__(data_dir)
        self.api_key = api_key

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("CRS 크롤링 (Congress.gov API)")
        results, offset = [], 0
        limit = min(max_results, 250)
        while len(results) < max_results:
            url = (f"https://api.congress.gov/v3/crsreport"
                   f"?format=json&limit={limit}&offset={offset}&api_key={self.api_key}")
            resp = self._get(url)
            if not resp:
                break
            data = resp.json()
            reports = data.get('CRSReports') or data.get('reports', [])
            if not reports:
                break
            for r in reports:
                results.append(self._doc(site,
                    title=r.get('title', ''),
                    link=r.get('url', ''),
                    published_date=r.get('publishDate', ''),
                    document_type=r.get('contentType', 'Report'),
                    report_id=r.get('id', ''),
                ))
            total = data.get('pagination', {}).get('count', '?')
            logger.info(f"  CRS: {len(results)}/{total} 건")
            if not data.get('pagination', {}).get('next'):
                break
            offset += limit
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  7. Census — AEM AJAX
# ═══════════════════════════════════════════════
class CensusCrawler(BaseSiteCrawler):
    """Census Bureau: census.gov working papers  AEM AJAX 연도별"""

    SERIES = [
        ('CES', 'ces-wp'),
        ('SEHSD', 'sehsd-wp'),
        ('CARRA', 'carra-wp'),
    ]

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Census Bureau 크롤링 (AEM AJAX)")
        results = []
        for series_label, series_slug in self.SERIES:
            if len(results) >= max_results:
                break
            # 'all' 탭으로 전체 로드
            url = (f"https://www.census.gov/content/census/en/library/working-papers"
                   f"/series/{series_slug}/jcr:content/root/responsivegrid/"
                   f"text_list.page.all.html")
            soup = self._soup(url)
            if not soup:
                # 폴백: 메인 페이지
                soup = self._soup(f"https://www.census.gov/library/working-papers/series/{series_slug}.html")
                if not soup:
                    continue
            for li in soup.select('li.cmp-list__item, li.uscb-list-text__item'):
                title_el = li.select_one('div.cmp-list__item-title')
                if not title_el:
                    continue
                link_el = li.select_one('a.cmp-list__item-link, a.uscb-list-text__item-container')
                href = self._abs_url(link_el.get('href', ''), 'https://www.census.gov') if link_el else ''
                title = title_el.get_text(strip=True)
                date_el = li.select_one('div.cmp-list__item-date')
                date = date_el.get_text(strip=True) if date_el else ''
                auth_el = li.select_one('div.cmp-list__item-author')
                authors = auth_el.get_text(strip=True).replace('Written by:', '').strip() if auth_el else ''
                desc_el = li.select_one('div.cmp-list__item-description')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=href,
                    published_date=date, authors=authors, description=desc,
                    document_type=f'working_paper ({series_label})'))
            logger.info(f"  Census {series_label}: 누적 {len(results)} 건")
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  8. RAND — AEM async HTML
# ═══════════════════════════════════════════════
class RandCrawler(BaseSiteCrawler):
    """RAND: AEM async endpoint  페이지당 12건"""
    BASE = "https://www.rand.org/content/rand/pubs/jcr:content/par/columnwrap/col1/pubsearch.html"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("RAND 크롤링 (AEM async)")
        results, start = [], 0
        while len(results) < max_results:
            url = f"{self.BASE}?q=&start={start}&content_type_ss=Research&sortBy=date"
            soup = self._soup(url)
            if not soup:
                break
            items = soup.select('li[data-relevancy]')
            if not items:
                break
            for li in items:
                a = li.select_one('a')
                if not a:
                    continue
                href = self._abs_url(a.get('href', ''), 'https://www.rand.org')
                title_el = li.select_one('h3.title')
                title = title_el.get_text(strip=True) if title_el else a.get_text(strip=True)
                date_el = li.select_one('p.date')
                date = date_el.get_text(strip=True) if date_el else ''
                type_el = li.select_one('p.type')
                doc_type = type_el.get_text(strip=True) if type_el else ''
                desc_el = li.select_one('p.desc')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=href,
                    published_date=date, description=desc, document_type=doc_type))
            logger.info(f"  RAND: {len(results)} 건 (start={start})")
            if len(items) < 12:
                break
            start += 12
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
#  9. Urban Institute — Drupal (lbj-lede 웹컴포넌트)
# ═══════════════════════════════════════════════
class UrbanCrawler(BaseSiteCrawler):
    """Urban Institute: urban.org/research  lbj-lede 속성 파싱 (403 차단 가능)"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Urban Institute 크롤링 (HTML, 403 차단 가능)")
        results, page = [], 0
        while len(results) < max_results:
            resp = self._get(f"https://www.urban.org/research?page={page}")
            if not resp:
                if page == 0:
                    logger.warning("  Urban: 403 차단. 브라우저 자동화(Playwright) 필요.")
                break
            soup = BeautifulSoup(resp.text, 'lxml')
            items = soup.select('lbj-lede')
            if not items:
                # lbj-lede가 없으면 일반 h2/h3 링크 시도
                for a in soup.select('h2 a, h3 a'):
                    title = a.get_text(strip=True)
                    if title and len(title) > 10:
                        link = self._abs_url(a.get('href', ''), 'https://www.urban.org')
                        results.append(self._doc(site, title=title, link=link,
                            document_type='publication'))
                break
            for lede in items:
                title = lede.get('headline', '')
                href = lede.get('href', '')
                if not title:
                    continue
                link = self._abs_url(href, 'https://www.urban.org')
                date = lede.get('date', '')
                doc_type = lede.get('eyebrow', '')
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, document_type=doc_type))
            logger.info(f"  Urban: {len(results)} 건 (page={page})")
            if len(items) < 30:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 10. Heritage Foundation — Drupal result-card
# ═══════════════════════════════════════════════
class HeritageCrawler(BaseSiteCrawler):
    """Heritage: heritage.org/search  보고서 타입 필터링"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Heritage 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            url = f"https://www.heritage.org/search?contains=&type%5B%5D=report&page={page}"
            soup = self._soup(url)
            if not soup:
                break
            rows = soup.select('div.view-content div.views-row')
            if not rows:
                break
            for row in rows:
                title_el = row.select_one('a.result-card__title')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = self._abs_url(title_el.get('href', ''), 'https://www.heritage.org')
                date = ''
                date_el = row.select_one('p.result-card__date span')
                if date_el:
                    date = date_el.get_text(strip=True)
                eyebrow = row.select_one('p.result-card__eyebrow')
                doc_type = eyebrow.get_text(strip=True) if eyebrow else ''
                auth = row.select_one('a.result-card__link, span.result-card__link')
                authors = auth.get_text(strip=True) if auth else ''
                desc_el = row.select_one('div.result-card__description')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, authors=authors, description=desc,
                    document_type=doc_type))
            logger.info(f"  Heritage: {len(results)} 건 (page={page})")
            if len(rows) < 12:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 11. ERS — JSON REST API
# ═══════════════════════════════════════════════
class ErsCrawler(BaseSiteCrawler):
    """USDA ERS: ers.usda.gov/api/publications  JSON API"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("ERS 크롤링 (JSON API)")
        results, start = [], 0
        while len(results) < max_results:
            url = f"https://www.ers.usda.gov/api/publications/v1.0?size=20&start={start}"
            resp = self._get(url)
            if not resp:
                break
            data = resp.json()
            rows = data.get('rows', [])
            if not rows:
                break
            for pub in rows:
                authors = ', '.join(a.get('name', '') for a in pub.get('authors', []))
                results.append(self._doc(site,
                    title=pub.get('title', ''),
                    link=self._abs_url(pub.get('url', ''), 'https://www.ers.usda.gov'),
                    published_date=pub.get('releaseDate', ''),
                    authors=authors,
                    description=(pub.get('shortDescription', '') or '')[:500],
                    document_type=pub.get('pubType', ''),
                    report_number=pub.get('reportNumber', ''),
                ))
            total = data.get('pager', {}).get('total_items', '?')
            logger.info(f"  ERS: {len(results)}/{total} 건")
            if len(rows) < 20:
                break
            start += 20
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 12. Atlantic Council — WordPress REST API
# ═══════════════════════════════════════════════
class AtlanticCouncilCrawler(BaseSiteCrawler):
    """Atlantic Council: WP REST API  카테고리 2523 (in-depth reports)"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Atlantic Council 크롤링 (WP REST API)")
        results, page = [], 1
        per_page = min(max_results, 100)
        while len(results) < max_results:
            url = (f"https://www.atlanticcouncil.org/wp-json/wp/v2/posts"
                   f"?categories=2523&per_page={per_page}&page={page}")
            resp = self._get(url)
            if not resp or resp.status_code == 400:
                break
            try:
                posts = resp.json()
            except ValueError:
                break
            if not posts or not isinstance(posts, list):
                break
            for post in posts:
                title = self._strip_html(post.get('title', {}).get('rendered', ''))
                link = post.get('link', '')
                date = post.get('date', '')
                excerpt = self._strip_html(post.get('excerpt', {}).get('rendered', ''))[:500]
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, description=excerpt, document_type='report'))
            logger.info(f"  Atlantic Council: {len(results)} 건 (page={page})")
            if len(posts) < per_page:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 13. CNAS — HTML 스크래핑
# ═══════════════════════════════════════════════
class CnasCrawler(BaseSiteCrawler):
    """CNAS: cnas.org/reports  페이지당 20건, 링크 쌍(이미지+텍스트) 중복 제거"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("CNAS 크롤링 (HTML)")
        results, page = [], 1
        seen = set()
        while len(results) < max_results:
            url = "https://www.cnas.org/reports" if page == 1 else f"https://www.cnas.org/reports/p{page}"
            soup = self._soup(url)
            if not soup:
                break
            links = soup.select('a[href*="/publications/"]')
            if not links:
                break
            found = 0
            for a in links:
                href = a.get('href', '')
                title = a.get_text(strip=True)
                if not title or len(title) < 5 or href in seen:
                    continue
                seen.add(href)
                link = self._abs_url(href, 'https://www.cnas.org')
                results.append(self._doc(site, title=title, link=link,
                    document_type='report'))
                found += 1
            logger.info(f"  CNAS: {len(results)} 건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 14. PIIE — Drupal 서브섹션
# ═══════════════════════════════════════════════
class PiieCrawler(BaseSiteCrawler):
    """PIIE: piie.com/publications  article + h2 a 패턴"""

    SECTIONS = [
        ('policy-briefs', 'Policy Brief'),
        ('working-papers', 'Working Paper'),
        ('piie-briefings', 'Briefing'),
    ]

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("PIIE 크롤링 (HTML, 서브섹션별)")
        results = []
        for section_slug, section_label in self.SECTIONS:
            if len(results) >= max_results:
                break
            base = f"https://www.piie.com/publications/{section_slug}"
            soup = self._soup(base)
            if not soup:
                logger.warning(f"  PIIE {section_label}: 접속 실패 (403 가능)")
                continue
            # article 내 h2 a 또는 직접 h2 a
            for article in (soup.select('article') or [soup]):
                title_el = article.select_one('h2 a, h3 a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                link = self._abs_url(title_el.get('href', ''), 'https://www.piie.com')
                # 날짜: article 내 time 또는 텍스트 패턴
                date = ''
                time_el = article.select_one('time')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, document_type=section_label))
            logger.info(f"  PIIE {section_label}: 누적 {len(results)} 건")
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 15. BJS — ojp.gov Drupal
# ═══════════════════════════════════════════════
class BjsCrawler(BaseSiteCrawler):
    """BJS: bjs.ojp.gov/library  Drupal Views"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("BJS 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            url = f"https://bjs.ojp.gov/library?page={page}"
            soup = self._soup(url)
            if not soup:
                break
            rows = soup.select('.views-row')
            if not rows:
                break
            for row in rows:
                title_el = row.select_one('h2 a, h3 a, .views-field-title a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = self._abs_url(title_el.get('href', ''), 'https://bjs.ojp.gov')
                date_el = row.select_one('time, .date-display-single, .views-field-created .field-content')
                date = ''
                if date_el:
                    date = date_el.get('datetime', date_el.get_text(strip=True))
                desc_el = row.select_one('.views-field-body .field-content, .field--name-body')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, description=desc, document_type='publication'))
            logger.info(f"  BJS: {len(results)} 건 (page={page})")
            if len(rows) < 10:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 16. OPRE — acf.hhs.gov Drupal
# ═══════════════════════════════════════════════
class OpreCrawler(BaseSiteCrawler):
    """OPRE: acf.gov/opre/topic  토픽별 연구 프로젝트 수집"""

    TOPICS = [
        'abuse-neglect-adoption-foster-care', 'child-care', 'head-start',
        'home-visiting', 'human-trafficking', 'self-sufficiency',
        'strengthening-families', 'youth-development',
    ]

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("OPRE 크롤링 (토픽별 HTML)")
        results = []
        seen = set()
        for topic in self.TOPICS:
            if len(results) >= max_results:
                break
            url = f"https://acf.gov/opre/topic/overview/{topic}"
            soup = self._soup(url)
            if not soup:
                url = f"https://acf.gov/opre/topic/{topic}"
                soup = self._soup(url)
                if not soup:
                    continue
            for a in soup.select('h3 a, article h3 a'):
                title = a.get_text(strip=True)
                href = a.get('href', '')
                if not title or len(title) < 10 or href in seen:
                    continue
                seen.add(href)
                link = self._abs_url(href, 'https://acf.gov')
                results.append(self._doc(site, title=title, link=link,
                    document_type='research'))
            logger.info(f"  OPRE {topic}: 누적 {len(results)} 건")
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 17. FDA — Drupal guidance search
# ═══════════════════════════════════════════════
class FdaCrawler(BaseSiteCrawler):
    """FDA: guidance 검색이 JS 렌더링 → 브라우저 자동화 필요 (폴백: 허브 링크 수집)"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("FDA 크롤링 (JS 렌더링 → 허브 링크 폴백)")
        logger.warning("  FDA 가이던스 검색은 JS 렌더링. 전체 수집에는 Playwright 필요.")
        # 폴백: 약물/기기/식품 허브에서 카테고리 링크라도 수집
        results = []
        soup = self._soup("https://www.fda.gov/drugs/guidance-compliance-regulatory-information/guidances-drugs")
        if not soup:
            return []
        seen = set()
        for a in soup.select('a[href*="/drugs/guidances-drugs/"]'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 15 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.fda.gov')
            results.append(self._doc(site, title=title, link=link,
                document_type='guidance'))
        logger.info(f"  FDA: {len(results)} 건 (카테고리 링크)")
        return results[:max_results]


# ═══════════════════════════════════════════════
# 18. Hudson Institute — HTML
# ═══════════════════════════════════════════════
class HudsonCrawler(BaseSiteCrawler):
    """Hudson Institute: hudson.org/research  article 요소에서 추출"""

    # 콘텐츠 링크 패턴 (topic/slug 형식, 짧은 카테고리 링크 제외)
    _CONTENT_RE = re.compile(r'^/[a-z-]+/[a-z0-9-]{15,}')

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Hudson 크롤링 (HTML)")
        results = []
        soup = self._soup("https://www.hudson.org/research")
        if not soup:
            return []
        seen = set()
        for article in soup.select('article'):
            for a in article.select('a[href]'):
                href = a.get('href', '')
                title = a.get_text(strip=True)
                if not title or len(title) < 15 or href in seen:
                    continue
                if not self._CONTENT_RE.match(href):
                    continue
                if '/experts/' in href or '/events/' in href:
                    continue
                seen.add(href)
                link = self._abs_url(href, 'https://www.hudson.org')
                results.append(self._doc(site, title=title, link=link,
                    document_type='research'))
                if len(results) >= max_results:
                    break
        logger.info(f"  Hudson: {len(results)} 건")
        return results[:max_results]


# ═══════════════════════════════════════════════
# 19. WRI — World Resources Institute
# ═══════════════════════════════════════════════
class WriCrawler(BaseSiteCrawler):
    """WRI: wri.org/resources  h3 a 링크 패턴"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("WRI 크롤링 (HTML)")
        results, page = [], 0
        seen = set()
        while len(results) < max_results:
            url = f"https://www.wri.org/resources?page={page}"
            soup = self._soup(url)
            if not soup:
                break
            links = soup.select('h3 a, h2 a')
            if not links:
                break
            found = 0
            for a in links:
                href = a.get('href', '')
                title = a.get_text(strip=True)
                if not title or len(title) < 10:
                    continue
                link = self._abs_url(href, 'https://www.wri.org')
                if link in seen:
                    continue
                seen.add(link)
                results.append(self._doc(site, title=title, link=link,
                    document_type='publication'))
                found += 1
            logger.info(f"  WRI: {len(results)} 건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 20. CGD — Center for Global Development
# ═══════════════════════════════════════════════
class CgdCrawler(BaseSiteCrawler):
    """CGD: cgdev.org (403 차단 가능)"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("CGD 크롤링 (HTML, 403 차단 가능)")
        resp = self._get("https://www.cgdev.org/commentary-and-analysis")
        if not resp:
            logger.warning("  CGD: 403 차단. 브라우저 자동화(Playwright) 필요.")
            return []
        soup = BeautifulSoup(resp.text, 'lxml')
        results = []
        for a in soup.select('h3 a, h2 a'):
            title = a.get_text(strip=True)
            if not title or len(title) < 10:
                continue
            link = self._abs_url(a.get('href', ''), 'https://www.cgdev.org')
            results.append(self._doc(site, title=title, link=link,
                document_type='publication'))
            if len(results) >= max_results:
                break
        logger.info(f"  CGD: {len(results)} 건")
        return results[:max_results]


# ═══════════════════════════════════════════════
# 21. Belfer Center — Harvard
# ═══════════════════════════════════════════════
class BelferCrawler(BaseSiteCrawler):
    """Belfer Center: belfercenter.org reports"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Belfer Center 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            url = f"https://www.belfercenter.org/research/publication-type/reports-papers?page={page}"
            soup = self._soup(url)
            if not soup:
                break
            rows = soup.select('.views-row, article, .node--type-publication')
            if not rows:
                break
            for row in rows:
                title_el = row.select_one('h2 a, h3 a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                link = self._abs_url(title_el.get('href', ''), 'https://www.belfercenter.org')
                date_el = row.select_one('time, .date, .field--name-field-date')
                date = date_el.get('datetime', date_el.get_text(strip=True)) if date_el else ''
                desc_el = row.select_one('.field--name-body, .teaser__body, p')
                desc = desc_el.get_text(strip=True)[:500] if desc_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, description=desc, document_type='report'))
            logger.info(f"  Belfer: {len(results)} 건 (page={page})")
            if len(rows) < 10:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 22. Wilson Center
# ═══════════════════════════════════════════════
class WilsonCrawler(BaseSiteCrawler):
    """Wilson Center: wilsoncenter.org/insight-analysis"""

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Wilson Center 크롤링 (HTML)")
        results, page = [], 0
        while len(results) < max_results:
            url = f"https://www.wilsoncenter.org/insight-analysis?page={page}"
            soup = self._soup(url)
            if not soup:
                break
            rows = soup.select('.views-row, article')
            if not rows:
                break
            for row in rows:
                title_el = row.select_one('h2 a, h3 a')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                link = self._abs_url(title_el.get('href', ''), 'https://www.wilsoncenter.org')
                date_el = row.select_one('time, .date')
                date = date_el.get('datetime', date_el.get_text(strip=True)) if date_el else ''
                results.append(self._doc(site, title=title, link=link,
                    published_date=date, document_type='analysis'))
            logger.info(f"  Wilson: {len(results)} 건 (page={page})")
            if len(rows) < 10:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        return results[:max_results]


# ═══════════════════════════════════════════════
# 범용 HTML 크롤러 (폴백)
# ═══════════════════════════════════════════════
class GenericHtmlCrawler(BaseSiteCrawler):
    """범용 크롤러: 제목+링크 패턴 최대한 추출"""

    def crawl(self, site: Site, max_results: int = 50) -> list[dict]:
        logger.info(f"범용 크롤링: {site.code} | {site.acronym or site.name}")
        url = site.url.split('\n')[0].strip()
        if not url or not url.startswith('http'):
            logger.warning(f"  유효한 URL 없음: {url}")
            return []
        soup = self._soup(url)
        if not soup:
            return []
        docs = []
        for tag in ['h3', 'h2', 'h4']:
            for heading in soup.select(f'{tag} a'):
                title = heading.get_text(strip=True)
                href = heading.get('href', '')
                if not title or len(title) < 5:
                    continue
                href = self._abs_url(href, url)
                docs.append(self._doc(site, title=title, link=href,
                    document_type='publication'))
                if len(docs) >= max_results:
                    break
            if docs:
                break
        logger.info(f"  범용 결과: {len(docs)}건")
        return docs[:max_results]


# ═══════════════════════════════════════════════
#  크롤러 레지스트리
# ═══════════════════════════════════════════════
# GAO(Z00014)는 수집 정책상 제외 — 수집 대상에서 영구 배제
GAO_EXCLUDED_CODE = 'Z00014'

CRAWLER_REGISTRY: dict[str, type[BaseSiteCrawler]] = {
    # ── API 기반 ──
    # 'Z00014': GaoCrawler,         # GAO — 수집 정책상 제외
    'Z00038': ErsCrawler,           # USDA ERS (JSON API)
    'Z00320': AtlanticCouncilCrawler,  # Atlantic Council (WP REST API)
    'Z00412': CrsCrawler,           # CRS (Congress.gov API)
    # ── HTML 스크래핑 ──
    'Z00048': BjsCrawler,           # BJS
    'Z00050': OpreCrawler,          # OPRE
    'Z00054': NistCrawler,          # NIST
    'Z00057': BeaCrawler,           # BEA
    'Z00058': FdaCrawler,           # FDA
    'Z00063': FrbCrawler,           # FRB (연도별)
    'Z00065': CboCrawler,           # CBO (Cloudflare 주의)
    'Z00071': CensusCrawler,        # Census Bureau
    'Z00083': CnasCrawler,          # CNAS
    'Z00088': PiieCrawler,          # PIIE
    'Z00089': HeritageCrawler,      # Heritage Foundation
    'Z00236': WilsonCrawler,        # Wilson Center
    'Z00304': UrbanCrawler,         # Urban Institute
    'Z00318': BelferCrawler,        # Belfer Center
    'Z00323': RandCrawler,          # RAND
    'Z00345': HudsonCrawler,        # Hudson Institute
    'Z00350': WriCrawler,           # WRI
    'Z00354': CgdCrawler,           # CGD
}


# ═══════════════════════════════════════════════
#  실행기 & CLI
# ═══════════════════════════════════════════════
class UsGovCrawlerRunner:
    """미국 사이트 크롤링 실행기"""

    def __init__(self, data_dir: str | Path = None, api_key: str = "DEMO_KEY"):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = api_key
        self._completed = load_completed_titles()

    def _get_crawler(self, site: Site) -> BaseSiteCrawler:
        crawler_cls = CRAWLER_REGISTRY.get(site.code)
        if crawler_cls is None:
            return GenericHtmlCrawler(self.data_dir)
        # API 키가 필요한 크롤러 처리
        if crawler_cls in (GaoCrawler, CrsCrawler):
            return crawler_cls(self.data_dir, api_key=self.api_key)
        return crawler_cls(self.data_dir)

    def crawl_site(self, site_code: str, max_results: int = 100) -> list[dict]:
        if site_code == GAO_EXCLUDED_CODE:
            logger.warning(f"GAO({GAO_EXCLUDED_CODE})는 수집 정책상 제외됩니다.")
            return []
        manager = SiteManager()
        site = manager.get_by_code(site_code)
        if not site:
            logger.error(f"기관코드 '{site_code}'를 찾을 수 없습니다.")
            return []
        if site.current_use == 'X':
            logger.warning(f"기관 '{site_code}'는 현재사용X 상태입니다. 수집 불가.")
            return []
        crawler = self._get_crawler(site)
        logger.info(f"[{site.code}] {site.acronym or site.name} → {type(crawler).__name__}")
        results = crawler.crawl(site, max_results=max_results)
        results = BaseSiteCrawler._filter_pdf_direct_links(results)
        results = filter_completed(results, self._completed)
        results = filter_by_date(results)
        results = filter_by_relevance(results)
        if results:
            crawler.save(site, results)
        return results

    def crawl_all_us(self, max_results_per_site: int = 50):
        manager = SiteManager()
        us_sites = [s for s in manager.get_by_country('US')
                    if not s.exclude and s.current_use != 'X'
                    and s.code != GAO_EXCLUDED_CODE]
        logger.info(f"미국 사이트 {len(us_sites)}개 크롤링 시작")
        logger.info("=" * 60)
        summary = []
        for i, site in enumerate(us_sites, 1):
            crawler = self._get_crawler(site)
            logger.info(f"[{i}/{len(us_sites)}] {site.code} {site.acronym or site.name}"
                        f" → {type(crawler).__name__}")
            results = crawler.crawl(site, max_results=max_results_per_site)
            results = BaseSiteCrawler._filter_pdf_direct_links(results)
            results = filter_completed(results, self._completed)
            results = filter_by_date(results)
            results = filter_by_relevance(results)
            if results:
                crawler.save(site, results)
            summary.append({'code': site.code, 'name': site.name,
                           'acronym': site.acronym, 'crawler': type(crawler).__name__,
                           'count': len(results)})
            if i < len(us_sites):
                time.sleep(REQUEST_DELAY)

        summary_path = self.data_dir / f"us_summary_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                'crawled_at': datetime.now().isoformat(),
                'total_sites': len(us_sites),
                'total_documents': sum(s['count'] for s in summary),
                'by_crawler': _group_counts(summary, 'crawler'),
                'sites': summary,
            }, f, ensure_ascii=False, indent=2)
        total = sum(s['count'] for s in summary)
        logger.info("=" * 60)
        logger.info(f"완료! {len(us_sites)} 사이트, 총 {total}건")
        return summary


def _group_counts(items: list[dict], key: str) -> dict:
    d: dict[str, int] = {}
    for it in items:
        k = it.get(key, 'unknown')
        d[k] = d.get(k, 0) + it.get('count', 0)
    return d


def main():
    import argparse
    parser = argparse.ArgumentParser(description='미국 정부/연구기관 사이트 크롤러')
    parser.add_argument('--site', '-s', help='기관코드 (예: Z00057=BEA)')
    parser.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    parser.add_argument('--api-key', '-k', default='DEMO_KEY', help='API 키 (GAO/CRS)')
    parser.add_argument('--data-dir', '-d', default=None)
    parser.add_argument('--list', '-l', action='store_true', help='지원 목록')
    parser.add_argument('--all', '-a', action='store_true', help='전체 US 사이트 크롤링')
    args = parser.parse_args()

    if args.list:
        manager = SiteManager()
        print(f"\n전용 크롤러 ({len(CRAWLER_REGISTRY)}개):")
        print(f"  {'코드':8s} {'약어':8s} {'크롤러':25s} 기관명")
        print(f"  {'-'*8} {'-'*8} {'-'*25} {'-'*45}")
        for code, cls in sorted(CRAWLER_REGISTRY.items()):
            site = manager.get_by_code(code)
            if site:
                print(f"  {code:8s} {site.acronym:8s} {cls.__name__:25s} {site.name[:45]}")
        us_total = len(manager.get_by_country('US'))
        print(f"\n  전용: {len(CRAWLER_REGISTRY)}개 / 전체 US: {us_total}개"
              f" (나머지 → GenericHtmlCrawler)")
        return

    runner = UsGovCrawlerRunner(data_dir=args.data_dir, api_key=args.api_key)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_us(max_results_per_site=args.max)
    else:
        print("사용법:")
        print("  python us_gov_crawler.py --list              지원 사이트 목록")
        print("  python us_gov_crawler.py -s Z00057           BEA 크롤링")
        print("  python us_gov_crawler.py -s Z00057 -m 50     BEA 50건")
        print("  python us_gov_crawler.py --all               전체 US 크롤링")
        print("  python us_gov_crawler.py --all -m 20         전체 US, 사이트당 20건")


if __name__ == '__main__':
    main()
