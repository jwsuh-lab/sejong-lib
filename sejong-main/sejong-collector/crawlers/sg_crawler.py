"""
싱가포르(SI) 정부기관 사이트 크롤러 모음
- MAS: mas.gov.sg/publications (.mas-media-card + 페이징)
- MOM: stats.mom.gov.sg/Publications (React DataTable JSON)
- IRAS, GovTech, MCCY, MOH, MFA, IMDA, MDDI: HTML 스크래핑
- Z00103(EDB): 로그인 필요 → 제외
- Z00256(IMDA 중복): Z00110과 동일 → 제외
"""
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from site_manager import SiteManager, Site
from completed_filter import load_completed_titles, filter_completed
from crawlers.us_gov_crawler import BaseSiteCrawler, REQUEST_DELAY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# 건너뛸 사이트
SKIP_CODES = {
    'Z00103',  # EDB: PDF 다운로드 시 로그인 필요
    'Z00256',  # IMDA 중복 (Z00110과 동일)
}


# ═══════════════════════════════════════════════
#  싱가포르 크롤러 베이스 — 파일명 prefix 'sg_'
# ═══════════════════════════════════════════════
class _SgBase(BaseSiteCrawler):
    """싱가포르 크롤러 공통 베이스 — save()에서 'sg_' prefix 사용"""

    def save(self, site: Site, results: list[dict]) -> Path | None:
        if not results:
            return None
        timestamp = datetime.now().strftime('%Y%m%d')
        tag = (site.acronym or site.code).replace(' ', '').replace('&', '')
        filepath = self.data_dir / f"sg_{site.code}_{tag}_{timestamp}.json"
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


# ═══════════════════════════════════════════════
#  1. MAS — Monetary Authority of Singapore
# ═══════════════════════════════════════════════
class MasCrawler(_SgBase):
    """MAS: mas.gov.sg/publications — .mas-media-card 패턴, 페이징"""
    BASE_URL = "https://www.mas.gov.sg/publications"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MAS 크롤링 (HTML)")
        results, page = [], 1
        seen = set()

        while len(results) < max_results:
            url = f"{self.BASE_URL}?page={page}" if page > 1 else self.BASE_URL
            soup = self._soup(url)
            if not soup:
                break

            found = 0
            # .mas-media-card 패턴
            cards = soup.select('.mas-media-card, .media-card, .card')
            if cards:
                for card in cards:
                    title_el = card.select_one('h3 a, h2 a, a.card-title, a.media-card__title')
                    if not title_el:
                        # 카드 내부 첫 번째 링크 시도
                        title_el = card.select_one('a[href]')
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    href = title_el.get('href', '')
                    if not title or len(title) < 5 or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://www.mas.gov.sg')

                    # 날짜
                    date = ''
                    date_el = card.select_one('time, .date, .card-date, .media-card__date')
                    if date_el:
                        date = date_el.get('datetime', date_el.get_text(strip=True))

                    # 문서유형
                    type_el = card.select_one('.card-type, .media-card__type, .tag')
                    doc_type = type_el.get_text(strip=True) if type_el else 'publication'

                    # 설명
                    desc_el = card.select_one('.card-description, .media-card__description, p')
                    desc = desc_el.get_text(strip=True)[:500] if desc_el else ''

                    results.append(self._doc(site,
                        title=title, link=link,
                        published_date=date, description=desc,
                        document_type=doc_type))
                    found += 1
            else:
                # 폴백: h2/h3 링크
                for a in soup.select('h2 a, h3 a'):
                    title = a.get_text(strip=True)
                    href = a.get('href', '')
                    if not title or len(title) < 5 or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://www.mas.gov.sg')
                    results.append(self._doc(site,
                        title=title, link=link,
                        document_type='publication'))
                    found += 1

            logger.info(f"  MAS: {len(results)}건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        return results[:max_results]


# ═══════════════════════════════════════════════
#  2. MOM — Ministry of Manpower (React DataTable)
# ═══════════════════════════════════════════════
class MomCrawler(_SgBase):
    """MOM: stats.mom.gov.sg/Publications — React DataTable + JSON API"""
    BASE_URL = "https://stats.mom.gov.sg/Publications"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MOM 크롤링 (React DataTable)")
        results = []
        seen = set()

        # 1차 시도: JSON API 엔드포인트
        api_urls = [
            "https://stats.mom.gov.sg/api/publications",
            "https://stats.mom.gov.sg/api/Publication",
        ]
        for api_url in api_urls:
            resp = self._get(api_url)
            if resp and resp.headers.get('content-type', '').startswith('application/json'):
                try:
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get('data', data.get('items', []))
                    for item in items[:max_results]:
                        title = item.get('title', item.get('Title', ''))
                        href = item.get('url', item.get('Url', item.get('link', '')))
                        if not title:
                            continue
                        link = self._abs_url(href, 'https://stats.mom.gov.sg') if href else ''
                        date = item.get('date', item.get('Date', item.get('publishDate', '')))
                        doc_type = item.get('type', item.get('Type', 'publication'))
                        results.append(self._doc(site,
                            title=title, link=link,
                            published_date=date, document_type=doc_type))
                    if results:
                        logger.info(f"  MOM (JSON API): {len(results)}건")
                        return results[:max_results]
                except (ValueError, KeyError, AttributeError, TypeError) as e:
                    logger.debug(f"  MOM JSON API 파싱 실패 ({api_url}): {e}")

        # 2차 시도: HTML 스크래핑
        soup = self._soup(self.BASE_URL)
        if not soup:
            return []

        # DataTable 또는 일반 테이블에서 추출
        for row in soup.select('table tr, .datatable tr, .publication-item, .list-item'):
            title_el = row.select_one('a[href], td a')
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            href = title_el.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://stats.mom.gov.sg')

            # 날짜: 테이블 셀 또는 날짜 요소에서
            date = ''
            for td in row.select('td'):
                text = td.get_text(strip=True)
                if re.match(r'\d{4}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', text):
                    date = text
                    break

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date, document_type='publication'))
            if len(results) >= max_results:
                break

        # 3차 시도: h2/h3 링크 폴백
        if not results:
            for a in soup.select('h2 a, h3 a, .card a, .publication a'):
                title = a.get_text(strip=True)
                href = a.get('href', '')
                if not title or len(title) < 5 or href in seen:
                    continue
                seen.add(href)
                link = self._abs_url(href, 'https://stats.mom.gov.sg')
                results.append(self._doc(site,
                    title=title, link=link,
                    document_type='publication'))
                if len(results) >= max_results:
                    break

        logger.info(f"  MOM: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  3. IRAS — Inland Revenue Authority
# ═══════════════════════════════════════════════
class IrasCrawler(_SgBase):
    """IRAS: iras.gov.sg — 연간보고서 정적 HTML"""
    BASE_URL = "https://www.iras.gov.sg/who-we-are/what-we-do/annual-reports-and-publications/annual-reports"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("IRAS 크롤링 (정적 HTML)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            return []

        results = []
        seen = set()
        # 연간보고서 링크 추출
        for a in soup.select('a[href]'):
            href = a.get('href', '')
            title = a.get_text(strip=True)
            if not title or len(title) < 5:
                continue
            # PDF 또는 보고서 링크 필터
            is_report = (
                '.pdf' in href.lower()
                or 'annual' in title.lower()
                or 'report' in title.lower()
                or '/annual-report' in href.lower()
            )
            if not is_report:
                continue
            if href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.iras.gov.sg')

            # PDF URL
            pdf_url = link if '.pdf' in link.lower() else ''

            results.append(self._doc(site,
                title=title, link=link,
                pdf_url=pdf_url,
                document_type='annual_report'))
            if len(results) >= max_results:
                break

        logger.info(f"  IRAS: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  4. GovTech — Government Technology Agency
# ═══════════════════════════════════════════════
class GovTechCrawler(_SgBase):
    """GovTech: tech.gov.sg/media/corporate-publications/ — 정적 HTML"""
    BASE_URL = "https://www.tech.gov.sg/media/corporate-publications/"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("GovTech 크롤링 (정적 HTML)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            return []

        results = []
        seen = set()
        for a in soup.select('h2 a, h3 a, .card a, article a, a[href*="publication"]'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.tech.gov.sg')

            date = ''
            parent = a.find_parent(['article', 'div', 'li'])
            if parent:
                time_el = parent.select_one('time, .date')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date,
                document_type='corporate_publication'))
            if len(results) >= max_results:
                break

        logger.info(f"  GovTech: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  5. MCCY — Ministry of Culture, Community and Youth
# ═══════════════════════════════════════════════
class MccyCrawler(_SgBase):
    """MCCY: mccy.gov.sg/about-us/news-and-resources — HTML"""
    BASE_URL = "https://www.mccy.gov.sg/about-us/news-and-resources"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MCCY 크롤링 (HTML)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            return []

        results = []
        seen = set()
        for a in soup.select('h2 a, h3 a, .card a, article a, .resource-item a'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.mccy.gov.sg')

            date = ''
            parent = a.find_parent(['article', 'div', 'li'])
            if parent:
                time_el = parent.select_one('time, .date')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date,
                document_type='publication'))
            if len(results) >= max_results:
                break

        logger.info(f"  MCCY: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  6. MOH — Ministry of Health
# ═══════════════════════════════════════════════
class MohCrawler(_SgBase):
    """MOH: moh.gov.sg/resources-statistics — HTML (403 가능)"""
    BASE_URL = "https://www.moh.gov.sg/resources-statistics"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MOH 크롤링 (HTML, 403 차단 가능)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            logger.warning("  MOH: 접속 실패 (403 차단 가능)")
            return []

        results = []
        seen = set()
        for a in soup.select('h2 a, h3 a, .card a, article a, a[href*="resources"]'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.moh.gov.sg')

            date = ''
            parent = a.find_parent(['article', 'div', 'li'])
            if parent:
                time_el = parent.select_one('time, .date')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date,
                document_type='publication'))
            if len(results) >= max_results:
                break

        logger.info(f"  MOH: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  7. MFA — Ministry of Foreign Affairs
# ═══════════════════════════════════════════════
class MfaCrawler(_SgBase):
    """MFA: mfa.gov.sg/Newsroom — HTML"""
    BASE_URL = "https://www.mfa.gov.sg/Newsroom"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MFA 크롤링 (HTML)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            return []

        results = []
        seen = set()
        for a in soup.select('h2 a, h3 a, .card a, article a, .news-item a'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.mfa.gov.sg')

            date = ''
            parent = a.find_parent(['article', 'div', 'li'])
            if parent:
                time_el = parent.select_one('time, .date')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date,
                document_type='publication'))
            if len(results) >= max_results:
                break

        logger.info(f"  MFA: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  8. IMDA — Info-communications Media Development Authority
# ═══════════════════════════════════════════════
class ImdaCrawler(_SgBase):
    """IMDA: imda.gov.sg — HTML"""
    BASE_URL = "https://www.imda.gov.sg"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("IMDA 크롤링 (HTML)")
        # IMDA 메인 → 리서치/출판물 페이지 탐색
        target_urls = [
            f"{self.BASE_URL}/resources",
            f"{self.BASE_URL}/about-imda/research-and-statistics",
            self.BASE_URL,
        ]
        results = []
        seen = set()

        for target_url in target_urls:
            soup = self._soup(target_url)
            if not soup:
                continue
            for a in soup.select('h2 a, h3 a, .card a, article a'):
                title = a.get_text(strip=True)
                href = a.get('href', '')
                if not title or len(title) < 5 or href in seen:
                    continue
                seen.add(href)
                link = self._abs_url(href, self.BASE_URL)

                date = ''
                parent = a.find_parent(['article', 'div', 'li'])
                if parent:
                    time_el = parent.select_one('time, .date')
                    if time_el:
                        date = time_el.get('datetime', time_el.get_text(strip=True))

                results.append(self._doc(site,
                    title=title, link=link,
                    published_date=date,
                    document_type='publication'))
                if len(results) >= max_results:
                    break
            if results:
                break
            time.sleep(REQUEST_DELAY)

        logger.info(f"  IMDA: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  9. MDDI — Ministry of Digital Development and Information
# ═══════════════════════════════════════════════
class MddiCrawler(_SgBase):
    """MDDI: mddi.gov.sg/newsroom — HTML (403 가능)"""
    BASE_URL = "https://www.mddi.gov.sg/newsroom"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("MDDI 크롤링 (HTML, 403 차단 가능)")
        soup = self._soup(self.BASE_URL)
        if not soup:
            logger.warning("  MDDI: 접속 실패 (403 차단 가능)")
            return []

        results = []
        seen = set()
        for a in soup.select('h2 a, h3 a, .card a, article a, .news-item a'):
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if not title or len(title) < 5 or href in seen:
                continue
            seen.add(href)
            link = self._abs_url(href, 'https://www.mddi.gov.sg')

            date = ''
            parent = a.find_parent(['article', 'div', 'li'])
            if parent:
                time_el = parent.select_one('time, .date')
                if time_el:
                    date = time_el.get('datetime', time_el.get_text(strip=True))

            results.append(self._doc(site,
                title=title, link=link,
                published_date=date,
                document_type='publication'))
            if len(results) >= max_results:
                break

        logger.info(f"  MDDI: {len(results)}건")
        return results[:max_results]


# ═══════════════════════════════════════════════
#  범용 HTML 크롤러 (싱가포르 — sg_ prefix)
# ═══════════════════════════════════════════════
class SgGenericHtmlCrawler(_SgBase):
    """범용 크롤러 (싱가포르): 제목+링크 패턴 최대한 추출"""

    def crawl(self, site: Site, max_results: int = 50) -> list[dict]:
        logger.info(f"범용 크롤링(SG): {site.code} | {site.acronym or site.name}")
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
        logger.info(f"  범용 결과(SG): {len(docs)}건")
        return docs[:max_results]


# ═══════════════════════════════════════════════
#  크롤러 레지스트리
# ═══════════════════════════════════════════════
SG_CRAWLER_REGISTRY: dict[str, type[_SgBase]] = {
    'Z00104': IrasCrawler,       # IRAS 국세청
    'Z00105': GovTechCrawler,    # GovTech 정부기술청
    'Z00106': MccyCrawler,       # MCCY 문화지역사회청소년부
    'Z00107': MohCrawler,        # MOH 보건부
    'Z00108': MfaCrawler,        # MFA 외무부
    'Z00109': MomCrawler,        # MOM 인력부
    'Z00110': ImdaCrawler,       # IMDA 정보통신미디어개발청
    'Z00111': MddiCrawler,       # MDDI 디지털발전정보부
    'Z00112': MasCrawler,        # MAS 통화청
}


# ═══════════════════════════════════════════════
#  실행기 & CLI
# ═══════════════════════════════════════════════
class SgCrawlerRunner:
    """싱가포르 사이트 크롤링 실행기"""

    def __init__(self, data_dir: str | Path = None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._completed = load_completed_titles()

    def _get_crawler(self, site: Site) -> _SgBase:
        crawler_cls = SG_CRAWLER_REGISTRY.get(site.code)
        if crawler_cls is None:
            return SgGenericHtmlCrawler(self.data_dir)
        return crawler_cls(self.data_dir)

    def crawl_site(self, site_code: str, max_results: int = 100) -> list[dict]:
        manager = SiteManager()
        site = manager.get_by_code(site_code)
        if not site:
            logger.error(f"기관코드 '{site_code}'를 찾을 수 없습니다.")
            return []
        if site_code in SKIP_CODES:
            logger.info(f"[{site.code}] {site.acronym or site.name} → 건너뜀 (수집 제외)")
            return []
        crawler = self._get_crawler(site)
        logger.info(f"[{site.code}] {site.acronym or site.name} → {type(crawler).__name__}")
        results = crawler.crawl(site, max_results=max_results)
        results = filter_completed(results, self._completed)
        if results:
            crawler.save(site, results)
        return results

    def crawl_all_sg(self, max_results_per_site: int = 50):
        """전체 싱가포르 사이트 크롤링"""
        manager = SiteManager()
        sg_sites = [s for s in manager.get_by_country('SI')
                    if not s.exclude and s.code not in SKIP_CODES]
        logger.info(f"싱가포르 사이트 {len(sg_sites)}개 크롤링 시작")
        logger.info("=" * 60)

        summary = []
        for i, site in enumerate(sg_sites, 1):
            crawler = self._get_crawler(site)
            logger.info(f"[{i}/{len(sg_sites)}] {site.code} {site.acronym or site.name}"
                        f" → {type(crawler).__name__}")
            results = crawler.crawl(site, max_results=max_results_per_site)
            results = filter_completed(results, self._completed)
            if results:
                crawler.save(site, results)
            summary.append({'code': site.code, 'name': site.name,
                           'acronym': site.acronym, 'crawler': type(crawler).__name__,
                           'count': len(results)})
            if i < len(sg_sites):
                time.sleep(REQUEST_DELAY)

        # 요약 저장
        summary_path = self.data_dir / f"sg_summary_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                'crawled_at': datetime.now().isoformat(),
                'total_sites': len(sg_sites),
                'total_documents': sum(s['count'] for s in summary),
                'sites': summary,
            }, f, ensure_ascii=False, indent=2)
        total = sum(s['count'] for s in summary)
        logger.info("=" * 60)
        logger.info(f"완료! {len(sg_sites)} 사이트, 총 {total}건")
        return summary


def main():
    import argparse
    parser = argparse.ArgumentParser(description='싱가포르 정부기관 사이트 크롤러')
    parser.add_argument('--site', '-s', help='기관코드 (예: Z00112=MAS)')
    parser.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    parser.add_argument('--data-dir', '-d', default=None)
    parser.add_argument('--list', '-l', action='store_true', help='지원 목록')
    parser.add_argument('--all', '-a', action='store_true', help='전체 SG 사이트 크롤링')
    args = parser.parse_args()

    if args.list:
        manager = SiteManager()
        print(f"\n전용 크롤러 ({len(SG_CRAWLER_REGISTRY)}개):")
        print(f"  {'코드':8s} {'약어':8s} {'크롤러':25s} 기관명")
        print(f"  {'-'*8} {'-'*8} {'-'*25} {'-'*45}")
        for code, cls in sorted(SG_CRAWLER_REGISTRY.items()):
            site = manager.get_by_code(code)
            if site:
                print(f"  {code:8s} {(site.acronym or ''):8s} {cls.__name__:25s} {site.name[:45]}")
        print(f"\n  건너뛰는 사이트: {', '.join(sorted(SKIP_CODES))}")
        sg_total = len(manager.get_by_country('SI'))
        print(f"  전용: {len(SG_CRAWLER_REGISTRY)}개 / 전체 SI: {sg_total}개")
        return

    runner = SgCrawlerRunner(data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_sg(max_results_per_site=args.max)
    else:
        print("사용법:")
        print("  python sg_crawler.py --list              지원 사이트 목록")
        print("  python sg_crawler.py -s Z00112           MAS 크롤링")
        print("  python sg_crawler.py -s Z00112 -m 50     MAS 50건")
        print("  python sg_crawler.py --all               전체 SG 크롤링")
        print("  python sg_crawler.py --all -m 20         전체 SG, 사이트당 20건")


if __name__ == '__main__':
    main()
