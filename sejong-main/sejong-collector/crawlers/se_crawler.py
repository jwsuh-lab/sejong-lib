"""
스웨덴(SW) 정부/연구기관 사이트 크롤러 모음
- government.se/reports/: 11개 부처 공통 URL (한 번만 수집)
- SIPRI: sipri.org/publications/search (Drupal 페이징)
- Timbro: timbro.se/in-english/publications/ (WordPress)
"""
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from site_manager import SiteManager, Site
from completed_filter import load_completed_titles, filter_completed
from crawlers.us_gov_crawler import BaseSiteCrawler, GenericHtmlCrawler, REQUEST_DELAY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# government.se 부처 코드 (11개, 모두 같은 URL)
GOVSE_CODES = [f'Z0009{i}' for i in range(1, 10)] + ['Z00100', 'Z00101']


# ═══════════════════════════════════════════════
#  스웨덴 크롤러 베이스 — 파일명 prefix 'se_'
# ═══════════════════════════════════════════════
class _SeBase(BaseSiteCrawler):
    """스웨덴 크롤러 공통 베이스 — save()에서 'se_' prefix 사용"""

    def save(self, site: Site, results: list[dict]) -> Path | None:
        if not results:
            return None
        timestamp = datetime.now().strftime('%Y%m%d')
        tag = (site.acronym or site.code).replace(' ', '').replace('&', '')
        filepath = self.data_dir / f"se_{site.code}_{tag}_{timestamp}.json"
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
#  1. government.se — 11개 부처 공통 URL
# ═══════════════════════════════════════════════
class GovSeCrawler(_SeBase):
    """government.se/reports/ — 11개 부처 공통 URL
    모든 부처가 같은 /reports/ URL을 사용하므로,
    첫 번째 부처(Z00091)에서만 전체 수집하고
    나머지 부처(Z00092~Z00101)는 빈 결과 반환 (중복 방지).
    Runner에서 중복 제어를 담당한다.
    """
    BASE_URL = "https://www.government.se/reports/"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info(f"government.se 크롤링 ({site.code} {site.name_kr})")
        results, page = [], 1
        seen = set()

        while len(results) < max_results:
            url = f"{self.BASE_URL}?page={page}" if page > 1 else self.BASE_URL
            soup = self._soup(url)
            if not soup:
                break

            # government.se/reports/ 페이지에서 리포트 링크 추출
            # 각 리포트는 ul.list li 또는 h2/h3 a 패턴
            found = 0
            for a in soup.select('a[href*="/reports/"]'):
                href = a.get('href', '')
                title = a.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                link = self._abs_url(href, 'https://www.government.se')
                # 카테고리/네비게이션 링크 제외
                if link == self.BASE_URL or link.rstrip('/') == self.BASE_URL.rstrip('/'):
                    continue
                if link in seen:
                    continue
                seen.add(link)

                # 날짜 추출 시도 (부모/형제 요소에서)
                date = ''
                parent = a.find_parent(['li', 'article', 'div'])
                if parent:
                    time_el = parent.find('time')
                    if time_el:
                        date = time_el.get('datetime', time_el.get_text(strip=True))
                    else:
                        date_match = re.search(
                            r'\d{1,2}\s+(?:January|February|March|April|May|June|'
                            r'July|August|September|October|November|December)\s+\d{4}',
                            parent.get_text()
                        )
                        if date_match:
                            date = date_match.group()

                results.append(self._doc(site,
                    title=title, link=link,
                    published_date=date,
                    document_type='report'))
                found += 1

            # h2, h3 링크도 시도 (다른 레이아웃 대비)
            if found == 0:
                for heading in soup.select('h2 a, h3 a'):
                    href = heading.get('href', '')
                    title = heading.get_text(strip=True)
                    if not title or len(title) < 5:
                        continue
                    link = self._abs_url(href, 'https://www.government.se')
                    if link in seen:
                        continue
                    seen.add(link)
                    results.append(self._doc(site,
                        title=title, link=link,
                        document_type='report'))
                    found += 1

            logger.info(f"  government.se: {len(results)}건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        return results[:max_results]


# ═══════════════════════════════════════════════
#  2. SIPRI — Drupal 페이징
# ═══════════════════════════════════════════════
class SipriCrawler(_SeBase):
    """SIPRI: sipri.org/publications/search — Drupal, ?page=N"""
    BASE_URL = "https://www.sipri.org/publications/search"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("SIPRI 크롤링 (Drupal HTML)")
        results, page = [], 0
        seen = set()

        while len(results) < max_results:
            url = f"{self.BASE_URL}?page={page}" if page > 0 else self.BASE_URL
            soup = self._soup(url)
            if not soup:
                break

            found = 0
            # Drupal views 패턴: .views-row 또는 article 내 h2/h3 a
            rows = soup.select('.views-row, .view-content article, .search-result')
            if rows:
                for row in rows:
                    title_el = row.select_one('h2 a, h3 a, .node-title a')
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    href = title_el.get('href', '')
                    if not title or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://www.sipri.org')

                    # 날짜
                    date = ''
                    time_el = row.select_one('time, .date-display-single, .field--name-field-date')
                    if time_el:
                        date = time_el.get('datetime', time_el.get_text(strip=True))

                    # 저자
                    auth_el = row.select_one('.field--name-field-author, .views-field-field-author')
                    authors = auth_el.get_text(strip=True) if auth_el else ''

                    # 설명
                    desc_el = row.select_one('.field--name-body, .views-field-body .field-content')
                    desc = desc_el.get_text(strip=True)[:500] if desc_el else ''

                    # 문서유형
                    type_el = row.select_one('.field--name-field-pub-type, .views-field-type .field-content')
                    doc_type = type_el.get_text(strip=True) if type_el else 'publication'

                    results.append(self._doc(site,
                        title=title, link=link,
                        published_date=date, authors=authors,
                        description=desc, document_type=doc_type))
                    found += 1
            else:
                # 폴백: 일반 h2/h3 링크
                for a in soup.select('h2 a, h3 a'):
                    title = a.get_text(strip=True)
                    href = a.get('href', '')
                    if not title or len(title) < 5 or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://www.sipri.org')
                    results.append(self._doc(site,
                        title=title, link=link,
                        document_type='publication'))
                    found += 1

            logger.info(f"  SIPRI: {len(results)}건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        return results[:max_results]


# ═══════════════════════════════════════════════
#  3. Timbro — WordPress
# ═══════════════════════════════════════════════
class TimbroCrawler(_SeBase):
    """Timbro: timbro.se/in-english/publications/ — WordPress article 카드"""
    BASE_URL = "https://timbro.se/in-english/publications/"

    def crawl(self, site: Site, max_results: int = 100) -> list[dict]:
        logger.info("Timbro 크롤링 (WordPress)")
        results, page = [], 1
        seen = set()

        while len(results) < max_results:
            url = f"{self.BASE_URL}page/{page}/" if page > 1 else self.BASE_URL
            soup = self._soup(url)
            if not soup:
                break

            found = 0
            # WordPress article 카드 패턴
            articles = soup.select('article, .post, .entry')
            if articles:
                for article in articles:
                    title_el = article.select_one('h2 a, h3 a, .entry-title a')
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    href = title_el.get('href', '')
                    if not title or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://timbro.se')

                    # 날짜
                    date = ''
                    time_el = article.select_one('time')
                    if time_el:
                        date = time_el.get('datetime', time_el.get_text(strip=True))

                    # 설명
                    desc_el = article.select_one('.entry-summary, .excerpt, p')
                    desc = desc_el.get_text(strip=True)[:500] if desc_el else ''

                    results.append(self._doc(site,
                        title=title, link=link,
                        published_date=date, description=desc,
                        document_type='publication'))
                    found += 1
            else:
                # 폴백: h2/h3 링크
                for a in soup.select('h2 a, h3 a'):
                    title = a.get_text(strip=True)
                    href = a.get('href', '')
                    if not title or len(title) < 5 or href in seen:
                        continue
                    seen.add(href)
                    link = self._abs_url(href, 'https://timbro.se')
                    results.append(self._doc(site,
                        title=title, link=link,
                        document_type='publication'))
                    found += 1

            logger.info(f"  Timbro: {len(results)}건 (page={page})")
            if found == 0:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        return results[:max_results]


# ═══════════════════════════════════════════════
#  범용 HTML 크롤러 (스웨덴 — se_ prefix)
# ═══════════════════════════════════════════════
class SeGenericHtmlCrawler(_SeBase):
    """범용 크롤러 (스웨덴): 제목+링크 패턴 최대한 추출"""

    def crawl(self, site: Site, max_results: int = 50) -> list[dict]:
        logger.info(f"범용 크롤링(SE): {site.code} | {site.acronym or site.name}")
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
        logger.info(f"  범용 결과(SE): {len(docs)}건")
        return docs[:max_results]


# ═══════════════════════════════════════════════
#  크롤러 레지스트리
# ═══════════════════════════════════════════════
SE_CRAWLER_REGISTRY: dict[str, type[_SeBase]] = {
    # ── government.se (11개 부처 공통) ──
    'Z00091': GovSeCrawler,
    'Z00092': GovSeCrawler,
    'Z00093': GovSeCrawler,
    'Z00094': GovSeCrawler,
    'Z00095': GovSeCrawler,
    'Z00096': GovSeCrawler,
    'Z00097': GovSeCrawler,
    'Z00098': GovSeCrawler,
    'Z00099': GovSeCrawler,
    'Z00100': GovSeCrawler,
    'Z00101': GovSeCrawler,
    # ── SIPRI ──
    'Z00102': SipriCrawler,
    # ── Timbro ──
    'Z00365': TimbroCrawler,
}


# ═══════════════════════════════════════════════
#  실행기 & CLI
# ═══════════════════════════════════════════════
class SeCrawlerRunner:
    """스웨덴 사이트 크롤링 실행기"""

    def __init__(self, data_dir: str | Path = None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._completed = load_completed_titles()

    def _get_crawler(self, site: Site) -> _SeBase:
        crawler_cls = SE_CRAWLER_REGISTRY.get(site.code)
        if crawler_cls is None:
            return SeGenericHtmlCrawler(self.data_dir)
        return crawler_cls(self.data_dir)

    def crawl_site(self, site_code: str, max_results: int = 100) -> list[dict]:
        manager = SiteManager()
        site = manager.get_by_code(site_code)
        if not site:
            logger.error(f"기관코드 '{site_code}'를 찾을 수 없습니다.")
            return []
        crawler = self._get_crawler(site)
        logger.info(f"[{site.code}] {site.acronym or site.name} → {type(crawler).__name__}")
        results = crawler.crawl(site, max_results=max_results)
        results = filter_completed(results, self._completed)
        if results:
            crawler.save(site, results)
        return results

    def crawl_all_se(self, max_results_per_site: int = 50):
        """전체 스웨덴 사이트 크롤링
        government.se는 11개 부처가 같은 URL이므로 Z00091에서만 수집하고
        Z00092~Z00101은 건너뛴다."""
        manager = SiteManager()
        se_sites = [s for s in manager.get_by_country('SW') if not s.exclude]
        logger.info(f"스웨덴 사이트 {len(se_sites)}개 크롤링 시작")
        logger.info("=" * 60)

        summary = []
        govse_crawled = False  # government.se 중복 방지 플래그
        govse_results = []     # government.se 수집 결과 (중복 방지용)

        for i, site in enumerate(se_sites, 1):
            # government.se 중복 처리: 성공적으로 수집된 후에만 건너뜀
            if site.code in GOVSE_CODES:
                if govse_crawled:
                    logger.info(f"[{i}/{len(se_sites)}] {site.code} {site.name_kr}"
                                f" → 건너뜀 (government.se 이미 수집됨)")
                    summary.append({'code': site.code, 'name': site.name,
                                   'acronym': site.acronym, 'crawler': 'GovSeCrawler',
                                   'count': 0, 'skipped': True})
                    continue

            crawler = self._get_crawler(site)
            logger.info(f"[{i}/{len(se_sites)}] {site.code} {site.acronym or site.name}"
                        f" → {type(crawler).__name__}")
            results = crawler.crawl(site, max_results=max_results_per_site)
            results = filter_completed(results, self._completed)
            if results:
                crawler.save(site, results)
                # government.se 성공 시 중복 방지 플래그 설정
                if site.code in GOVSE_CODES:
                    govse_crawled = True
            summary.append({'code': site.code, 'name': site.name,
                           'acronym': site.acronym, 'crawler': type(crawler).__name__,
                           'count': len(results)})
            if i < len(se_sites):
                time.sleep(REQUEST_DELAY)

        # 요약 저장
        summary_path = self.data_dir / f"se_summary_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                'crawled_at': datetime.now().isoformat(),
                'total_sites': len(se_sites),
                'total_documents': sum(s['count'] for s in summary),
                'sites': summary,
            }, f, ensure_ascii=False, indent=2)
        total = sum(s['count'] for s in summary)
        logger.info("=" * 60)
        logger.info(f"완료! {len(se_sites)} 사이트, 총 {total}건")
        return summary


def main():
    import argparse
    parser = argparse.ArgumentParser(description='스웨덴 정부/연구기관 사이트 크롤러')
    parser.add_argument('--site', '-s', help='기관코드 (예: Z00102=SIPRI)')
    parser.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    parser.add_argument('--data-dir', '-d', default=None)
    parser.add_argument('--list', '-l', action='store_true', help='지원 목록')
    parser.add_argument('--all', '-a', action='store_true', help='전체 SW 사이트 크롤링')
    args = parser.parse_args()

    if args.list:
        manager = SiteManager()
        print(f"\n전용 크롤러 ({len(SE_CRAWLER_REGISTRY)}개):")
        print(f"  {'코드':8s} {'약어':8s} {'크롤러':25s} 기관명")
        print(f"  {'-'*8} {'-'*8} {'-'*25} {'-'*45}")
        for code, cls in sorted(SE_CRAWLER_REGISTRY.items()):
            site = manager.get_by_code(code)
            if site:
                print(f"  {code:8s} {(site.acronym or ''):8s} {cls.__name__:25s} {site.name[:45]}")
        se_total = len(manager.get_by_country('SW'))
        print(f"\n  전용: {len(SE_CRAWLER_REGISTRY)}개 / 전체 SW: {se_total}개")
        return

    runner = SeCrawlerRunner(data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_se(max_results_per_site=args.max)
    else:
        print("사용법:")
        print("  python se_crawler.py --list              지원 사이트 목록")
        print("  python se_crawler.py -s Z00102           SIPRI 크롤링")
        print("  python se_crawler.py -s Z00102 -m 50     SIPRI 50건")
        print("  python se_crawler.py --all               전체 SW 크롤링")
        print("  python se_crawler.py --all -m 20         전체 SW, 사이트당 20건")


if __name__ == '__main__':
    main()
