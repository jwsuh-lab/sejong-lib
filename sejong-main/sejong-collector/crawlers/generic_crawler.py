"""
범용 국가 크롤러 — 전용 크롤러가 없는 모든 국가를 지원
- CountryBaseCrawler: 파일명 prefix를 파라미터로 받는 베이스 클래스
- CountryGenericHtmlCrawler: h2/h3/h4 링크 추출 범용 크롤러
- GenericCrawlerRunner: 국가코드 기반 실행기

사용법:
  python crawlers/generic_crawler.py AT --all           # 오스트리아 전체
  python crawlers/generic_crawler.py CA --site Z00139   # 캐나다 특정 사이트
  python crawlers/generic_crawler.py NO --all -m 30     # 노르웨이, 사이트당 30건
  python crawlers/generic_crawler.py AT --list           # 오스트리아 사이트 목록
"""
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from site_manager import SiteManager, Site
from completed_filter import load_completed_titles, filter_completed
from date_filter import filter_by_date
from relevance_filter import filter_by_relevance
from crawlers.us_gov_crawler import BaseSiteCrawler, REQUEST_DELAY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# 전용 크롤러가 있는 국가 — 이 국가들은 전용 크롤러 사용을 안내
COUNTRIES_WITH_DEDICATED_CRAWLER = {
    'GB': 'crawl-uk (govuk_crawler.py)',
    'US': 'crawl-us (us_gov_crawler.py)',
    'SW': 'crawl-se (se_crawler.py)',
    'SI': 'crawl-sg (sg_crawler.py)',
}


# ═══════════════════════════════════════════════
#  베이스 클래스 — prefix 파라미터화
# ═══════════════════════════════════════════════
class CountryBaseCrawler(BaseSiteCrawler):
    """범용 국가 크롤러 베이스 — save()에서 파라미터화된 prefix 사용"""

    def __init__(self, data_dir: Path, country_prefix: str):
        super().__init__(data_dir)
        self.country_prefix = country_prefix

    def save(self, site: Site, results: list[dict]) -> Path | None:
        if not results:
            return None
        timestamp = datetime.now().strftime('%Y%m%d')
        tag = (site.acronym or site.code).replace(' ', '').replace('&', '')
        filepath = self.data_dir / f"{self.country_prefix}_{site.code}_{tag}_{timestamp}.json"
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
#  범용 HTML 크롤러
# ═══════════════════════════════════════════════
class CountryGenericHtmlCrawler(CountryBaseCrawler):
    """범용 크롤러: 다중 셀렉터 전략으로 문서 링크 추출"""

    # 네비게이션/UI 링크 제외용 키워드
    _NAV_KEYWORDS = {
        'home', 'about', 'contact', 'login', 'sign in', 'menu', 'search',
        'skip to', 'back to', 'next', 'previous', 'page', 'cookie',
        'privacy', 'terms', 'sitemap', 'accessibility', 'français',
        'twitter', 'facebook', 'linkedin', 'youtube', 'instagram',
    }

    def _is_nav_link(self, title: str) -> bool:
        """네비게이션/UI 링크인지 판별"""
        t = title.lower().strip()
        if len(t) < 5:
            return True
        return any(kw in t for kw in self._NAV_KEYWORDS)

    def _extract_date_near(self, element) -> str:
        """링크 주변에서 날짜 추출 시도"""
        import re
        # 날짜 패턴들
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2026-03-12
            r'\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}',
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
            r'\d{1,2}/\d{1,2}/\d{4}',  # 12/03/2026
            r'\d{1,2}\.\d{1,2}\.\d{4}',  # 12.03.2026
        ]
        combined = '|'.join(f'({p})' for p in date_patterns)

        # 1) 부모 요소에서 <time> 태그 확인
        parent = element.find_parent(['li', 'div', 'article', 'tr', 'section'])
        if parent:
            time_tag = parent.find('time')
            if time_tag:
                dt = time_tag.get('datetime', '') or time_tag.get_text(strip=True)
                if dt:
                    return dt
            # 2) 부모 텍스트에서 날짜 패턴 검색
            text = parent.get_text(' ', strip=True)
            m = re.search(combined, text, re.IGNORECASE)
            if m:
                return m.group()

        # 3) 형제 요소에서 검색
        for sib in element.find_next_siblings(limit=3):
            txt = sib.get_text(strip=True) if hasattr(sib, 'get_text') else str(sib).strip()
            m = re.search(combined, txt, re.IGNORECASE)
            if m:
                return m.group()

        return ''

    def _extract_strategy_heading_contains_link(self, soup, url, max_results):
        """전략1: h2/h3/h4 안에 a 태그 (기존 방식)"""
        docs = []
        for tag in ['h3', 'h2', 'h4']:
            for heading in soup.select(f'{tag} a'):
                title = heading.get_text(strip=True)
                href = heading.get('href', '')
                if self._is_nav_link(title):
                    continue
                href = self._abs_url(href, url)
                docs.append({'title': title, 'link': href, '_element': heading})
                if len(docs) >= max_results:
                    break
            if docs:
                break
        return docs

    def _extract_strategy_link_contains_heading(self, soup, url, max_results):
        """전략2: a 태그 안에 h2/h3/h4 (FNI 등 역순 패턴)"""
        docs = []
        seen = set()
        for tag in ['h3', 'h2', 'h4']:
            for heading in soup.select(f'a {tag}'):
                a_tag = heading.find_parent('a')
                if not a_tag:
                    continue
                title = heading.get_text(strip=True)
                href = a_tag.get('href', '')
                if self._is_nav_link(title) or title in seen:
                    continue
                seen.add(title)
                href = self._abs_url(href, url)
                docs.append({'title': title, 'link': href, '_element': heading})
                if len(docs) >= max_results:
                    break
            if docs:
                break
        return docs

    def _extract_strategy_publication_links(self, soup, url, max_results):
        """전략3: href에 publications/research/report 등 키워드 포함"""
        import re as _re
        pattern = _re.compile(
            r'/(publications?|research|reports?|papers?|studies|analysis|documents?)/\S',
            _re.IGNORECASE)
        docs = []
        seen = set()
        for a in soup.select('a[href]'):
            href = a.get('href', '')
            if not pattern.search(href):
                continue
            title = a.get_text(strip=True)
            if self._is_nav_link(title) or title in seen:
                continue
            # 목록 링크가 아니라 콘텐츠 링크인지 확인
            if len(title) < 10:
                continue
            seen.add(title)
            href = self._abs_url(href, url)
            docs.append({'title': title, 'link': href, '_element': a})
            if len(docs) >= max_results:
                break
        return docs

    def _extract_strategy_list_items(self, soup, url, max_results):
        """전략4: li > a 패턴 (목록 기반 사이트)"""
        docs = []
        seen = set()
        for li in soup.select('li'):
            a = li.find('a')
            if not a:
                continue
            title = a.get_text(strip=True)
            href = a.get('href', '')
            if self._is_nav_link(title) or title in seen:
                continue
            if len(title) < 15:
                continue
            seen.add(title)
            href = self._abs_url(href, url)
            docs.append({'title': title, 'link': href, '_element': a})
            if len(docs) >= max_results:
                break
        return docs

    def crawl(self, site: Site, max_results: int = 50) -> list[dict]:
        logger.info(f"범용 크롤링({self.country_prefix.upper()}): "
                     f"{site.code} | {site.acronym or site.name}")
        url = site.url.split('\n')[0].strip()
        if not url or not url.startswith('http'):
            logger.warning(f"  유효한 URL 없음: {url}")
            return []
        soup = self._soup(url)
        if not soup:
            return []

        # 전략 cascade: 첫 번째로 결과가 나오는 전략 채택
        strategies = [
            ('heading>link', self._extract_strategy_heading_contains_link),
            ('link>heading', self._extract_strategy_link_contains_heading),
            ('pub-href',     self._extract_strategy_publication_links),
            ('list-items',   self._extract_strategy_list_items),
        ]

        docs = []
        used_strategy = None
        for name, strategy_fn in strategies:
            docs = strategy_fn(soup, url, max_results)
            if docs:
                used_strategy = name
                break

        results = []
        for d in docs[:max_results]:
            date = self._extract_date_near(d.get('_element')) if d.get('_element') else ''
            results.append(self._doc(site, title=d['title'], link=d['link'],
                                     published_date=date, document_type='publication'))

        logger.info(f"  범용 결과({self.country_prefix.upper()}): "
                     f"{len(results)}건 [전략: {used_strategy or 'none'}]")
        return results


# ═══════════════════════════════════════════════
#  실행기
# ═══════════════════════════════════════════════
class GenericCrawlerRunner:
    """범용 국가 크롤링 실행기"""

    def __init__(self, country_code: str, data_dir: str | Path = None):
        self.country_code = country_code.upper()
        self.prefix = country_code.lower()
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._completed = load_completed_titles()

    def _get_crawler(self) -> CountryGenericHtmlCrawler:
        return CountryGenericHtmlCrawler(self.data_dir, self.prefix)

    def crawl_site(self, site_code: str, max_results: int = 100) -> list[dict]:
        manager = SiteManager()
        site = manager.get_by_code(site_code)
        if not site:
            logger.error(f"기관코드 '{site_code}'를 찾을 수 없습니다.")
            return []
        if site.current_use == 'X':
            logger.warning(f"기관 '{site_code}'는 현재사용X 상태입니다. 수집 불가.")
            return []
        if site.country_code != self.country_code:
            logger.warning(f"기관코드 '{site_code}'는 {site.country_code} 소속입니다. "
                           f"(요청: {self.country_code})")
        crawler = self._get_crawler()
        logger.info(f"[{site.code}] {site.acronym or site.name} → "
                     f"{type(crawler).__name__} ({self.prefix}_)")
        results = crawler.crawl(site, max_results=max_results)
        results = BaseSiteCrawler._filter_pdf_direct_links(results)
        results = filter_completed(results, self._completed)
        results = filter_by_date(results)
        results = filter_by_relevance(results)
        if results:
            crawler.save(site, results)
        return results

    def crawl_all(self, max_results_per_site: int = 50, force: bool = False):
        """해당 국가 전체 사이트 크롤링 (force=True면 현재사용 무시)"""
        manager = SiteManager()
        if force:
            sites = manager.get_by_country(self.country_code)
        else:
            sites = [s for s in manager.get_by_country(self.country_code)
                     if s.current_use != 'X']
        if not sites:
            logger.warning(f"국가코드 '{self.country_code}'에 해당하는 사이트가 없습니다.")
            return []

        logger.info(f"{self.country_code} 사이트 {len(sites)}개 크롤링 시작")
        logger.info("=" * 60)

        summary = []
        for i, site in enumerate(sites, 1):
            crawler = self._get_crawler()
            logger.info(f"[{i}/{len(sites)}] {site.code} {site.acronym or site.name}"
                        f" → {type(crawler).__name__}")
            results = crawler.crawl(site, max_results=max_results_per_site)
            results = BaseSiteCrawler._filter_pdf_direct_links(results)
            results = filter_completed(results, self._completed)
            results = filter_by_date(results)
            results = filter_by_relevance(results)
            if results:
                crawler.save(site, results)
            summary.append({
                'code': site.code, 'name': site.name,
                'acronym': site.acronym,
                'crawler': type(crawler).__name__,
                'count': len(results),
            })
            if i < len(sites):
                time.sleep(REQUEST_DELAY)

        # 요약 저장
        summary_path = self.data_dir / f"{self.prefix}_summary_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                'crawled_at': datetime.now().isoformat(),
                'country_code': self.country_code,
                'total_sites': len(sites),
                'total_documents': sum(s['count'] for s in summary),
                'sites': summary,
            }, f, ensure_ascii=False, indent=2)
        total = sum(s['count'] for s in summary)
        logger.info("=" * 60)
        logger.info(f"완료! {len(sites)} 사이트, 총 {total}건")
        return summary


# ═══════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description='범용 국가 크롤러')
    parser.add_argument('country', help='국가코드 (예: AT, CA, NO, IT, CN, BE, IN, EU)')
    parser.add_argument('--site', '-s', help='기관코드 (예: Z00139)')
    parser.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    parser.add_argument('--data-dir', '-d', default=None)
    parser.add_argument('--list', '-l', action='store_true', help='사이트 목록 출력')
    parser.add_argument('--all', '-a', action='store_true', help='전체 사이트 크롤링')
    parser.add_argument('--force', '-f', action='store_true', help='현재사용 상태 무시')
    args = parser.parse_args()

    country = args.country.upper()

    # 전용 크롤러 가드
    if country in COUNTRIES_WITH_DEDICATED_CRAWLER:
        dedicated = COUNTRIES_WITH_DEDICATED_CRAWLER[country]
        print(f"'{country}'는 전용 크롤러가 있습니다: {dedicated}")
        print(f"  python main.py {dedicated.split(' ')[0]} 명령을 사용하세요.")
        return

    if args.list:
        manager = SiteManager()
        sites = manager.get_by_country(country)
        if not sites:
            print(f"국가코드 '{country}'에 해당하는 사이트가 없습니다.")
            return
        print(f"\n{country} 사이트 목록 ({len(sites)}개):")
        print(f"  {'코드':8s} {'약어':10s} {'현재사용':8s} 기관명")
        print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*50}")
        for site in sites:
            use = 'X' if site.current_use == 'X' else 'O'
            print(f"  {site.code:8s} {(site.acronym or ''):10s} {use:8s} {site.name[:50]}")
        active = len([s for s in sites if s.current_use != 'X'])
        print(f"\n  전체: {len(sites)}개 (현재사용 가능: {active}개)")
        return

    runner = GenericCrawlerRunner(country, data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all(max_results_per_site=args.max, force=args.force)
    else:
        print(f"사용법 ({country}):")
        print(f"  python crawlers/generic_crawler.py {country} --list"
              f"              사이트 목록")
        print(f"  python crawlers/generic_crawler.py {country} -s ZXXXXX"
              f"         특정 사이트 크롤링")
        print(f"  python crawlers/generic_crawler.py {country} --all"
              f"               전체 크롤링")
        print(f"  python crawlers/generic_crawler.py {country} --all -m 20"
              f"         전체, 사이트당 20건")


if __name__ == '__main__':
    main()
