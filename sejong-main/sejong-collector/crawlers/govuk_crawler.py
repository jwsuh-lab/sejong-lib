"""
GOV.UK 크롤러: GOV.UK Search API를 사용하여 연구/통계 문서를 수집
"""
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))
from site_manager import SiteManager, Site
from policy_filter import UK_POLICY_TYPES
from completed_filter import load_completed_titles, filter_completed
from date_filter import filter_by_date
from relevance_filter import filter_by_relevance

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# GOV.UK Search API 설정
GOVUK_API_BASE = "https://www.gov.uk/api/search.json"
RESULTS_PER_PAGE = 50  # API 최대 허용값
REQUEST_DELAY = 1.0    # 요청 간 대기 시간 (초)
RESULT_FIELDS = [
    "title",
    "link",
    "description",
    "public_timestamp",
    "content_store_document_type",
    "organisations",
]


class GovukCrawler:
    """GOV.UK 연구/통계 문서 크롤러"""

    def __init__(self, data_dir: str | Path = None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._completed = load_completed_titles()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SejongLibrary-Crawler/1.0 (Research Purpose)',
            'Accept': 'application/json',
        })

    def _build_api_url(self, org_slug: str, start: int = 0, count: int = RESULTS_PER_PAGE) -> str:
        """GOV.UK Search API URL 생성"""
        params = {
            'filter_organisations': org_slug,
            'count': count,
            'start': start,
            'order': '-public_timestamp',
            'fields': ','.join(RESULT_FIELDS),
        }
        return f"{GOVUK_API_BASE}?{urlencode(params)}"

    def fetch_page(self, org_slug: str, start: int = 0, max_retries: int = 3) -> dict | None:
        """API에서 한 페이지 결과 가져오기 (재시도 로직 포함)"""
        url = self._build_api_url(org_slug, start)
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get('Retry-After', 2 ** attempt * 5))
                    logger.warning(f"429 Rate Limited, {wait}초 대기 (attempt {attempt}): org={org_slug}")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning(f"  {type(e).__name__} (attempt {attempt}/{max_retries}), {wait}초 후 재시도: org={org_slug}")
                    time.sleep(wait)
                    continue
                logger.error(f"  {max_retries}회 재시도 후 실패 (org={org_slug}): {e}")
                return None
            except ValueError as e:
                logger.error(f"  JSON 파싱 실패 (org={org_slug}): {e}")
                return None
            except requests.RequestException as e:
                logger.error(f"  API 요청 실패 (org={org_slug}, start={start}): {e}")
                return None
        return None

    def crawl_site(self, site: Site, max_results: int = 500) -> list[dict]:
        """단일 사이트의 문서 목록 수집"""
        slug = site.govuk_org_slug
        if not slug:
            logger.warning(f"{site.code} ({site.name}): GOV.UK slug를 추출할 수 없습니다.")
            return []

        logger.info(f"크롤링 시작: {site.code} | {site.acronym or site.name} | slug={slug}")

        all_results = []
        start = 0

        while start < max_results:
            data = self.fetch_page(slug, start)
            if data is None:
                break

            total = data.get('total', 0)
            results = data.get('results', [])

            if not results:
                break

            for item in results:
                doc = {
                    'site_code': site.code,
                    'site_name': site.name,
                    'site_acronym': site.acronym,
                    'org_slug': slug,
                    'title': item.get('title', ''),
                    'link': f"https://www.gov.uk{item.get('link', '')}",
                    'description': item.get('description', ''),
                    'published_date': item.get('public_timestamp', ''),
                    'document_type': item.get('content_store_document_type', ''),
                }
                all_results.append(doc)

            logger.info(f"  {slug}: {len(all_results)}/{total} 건 수집 완료")

            start += RESULTS_PER_PAGE
            if start >= total:
                break

            time.sleep(REQUEST_DELAY)

        # 정책동향 관련 문서유형만 필터링
        before = len(all_results)
        all_results = [
            doc for doc in all_results
            if doc.get('document_type', '') in UK_POLICY_TYPES
        ]
        if before != len(all_results):
            logger.info(f"  {slug}: 문서유형 필터링 {before} → {len(all_results)}건")

        all_results = filter_completed(all_results, self._completed)
        all_results = filter_by_date(all_results)
        all_results = filter_by_relevance(all_results)

        return all_results

    def save_results(self, site: Site, results: list[dict]):
        """수집 결과를 JSON 파일로 저장"""
        if not results:
            return

        timestamp = datetime.now().strftime('%Y%m%d')
        filename = f"govuk_{site.code}_{site.acronym or 'unknown'}_{timestamp}.json"
        filepath = self.data_dir / filename

        output = {
            'metadata': {
                'site_code': site.code,
                'site_name': site.name,
                'site_name_kr': site.name_kr,
                'acronym': site.acronym,
                'org_slug': site.govuk_org_slug,
                'crawled_at': datetime.now().isoformat(),
                'total_collected': len(results),
            },
            'documents': results,
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        logger.info(f"저장 완료: {filepath.name} ({len(results)}건)")
        return filepath

    def crawl_all_govuk(self, max_results_per_site: int = 100):
        """모든 GOV.UK 사이트 크롤링"""
        manager = SiteManager()
        govuk_sites = manager.get_govuk_sites()

        logger.info(f"GOV.UK 사이트 {len(govuk_sites)}개 크롤링 시작")
        logger.info("=" * 60)

        summary = []
        for i, site in enumerate(govuk_sites, 1):
            logger.info(f"[{i}/{len(govuk_sites)}] {site.code} - {site.name}")
            results = self.crawl_site(site, max_results=max_results_per_site)
            if results:
                self.save_results(site, results)
            summary.append({
                'code': site.code,
                'name': site.name,
                'acronym': site.acronym,
                'count': len(results),
            })
            if i < len(govuk_sites):
                time.sleep(REQUEST_DELAY)

        # 전체 요약 저장
        summary_path = self.data_dir / f"govuk_summary_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                'crawled_at': datetime.now().isoformat(),
                'total_sites': len(govuk_sites),
                'total_documents': sum(s['count'] for s in summary),
                'sites': summary,
            }, f, ensure_ascii=False, indent=2)

        logger.info("=" * 60)
        logger.info(f"크롤링 완료! 총 {sum(s['count'] for s in summary)}건 수집")
        logger.info(f"요약 파일: {summary_path.name}")

        return summary


def main():
    import argparse

    parser = argparse.ArgumentParser(description='GOV.UK 연구/통계 문서 크롤러')
    parser.add_argument('--site', '-s', type=str, help='특정 기관코드만 크롤링 (예: Z00113)')
    parser.add_argument('--max', '-m', type=int, default=100, help='사이트당 최대 수집 건수 (기본: 100)')
    parser.add_argument('--data-dir', '-d', type=str, default=None, help='결과 저장 디렉토리')
    args = parser.parse_args()

    crawler = GovukCrawler(data_dir=args.data_dir)

    if args.site:
        manager = SiteManager()
        site = manager.get_by_code(args.site)
        if not site:
            logger.error(f"기관코드 '{args.site}'를 찾을 수 없습니다.")
            sys.exit(1)
        results = crawler.crawl_site(site, max_results=args.max)
        if results:
            crawler.save_results(site, results)
            print(f"\n수집 완료: {len(results)}건")
        else:
            print("\n수집된 문서가 없습니다.")
    else:
        crawler.crawl_all_govuk(max_results_per_site=args.max)


if __name__ == '__main__':
    main()
