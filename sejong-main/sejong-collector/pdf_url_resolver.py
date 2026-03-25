"""
PDF 다운로드 URL 추출기
- 수집된 문서 데이터의 랜딩 페이지에서 실제 PDF 다운로드 URL을 추출
- GOV.UK: Content API 활용
- GAO: GovInfo API 활용
- 기타 US 사이트: HTML 파싱으로 PDF 링크 탐색
"""
import argparse
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
REQUEST_DELAY_UK = 1.0
REQUEST_DELAY_US = 1.5


# ═══════════════════════════════════════════════
#  베이스 클래스
# ═══════════════════════════════════════════════
class BasePdfResolver(ABC):
    """PDF URL 추출 베이스 클래스"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SejongLibrary-PDFResolver/1.0 (Research Purpose)',
            'Accept': 'text/html,application/json',
        })

    @abstractmethod
    def resolve(self, doc: dict) -> list[str]:
        """문서에서 PDF URL 목록 추출. 빈 리스트 = PDF 없음."""
        ...

    def _get(self, url: str, timeout: int = 30) -> requests.Response | None:
        try:
            resp = self.session.get(url, timeout=timeout)
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
#  GOV.UK — Content API
# ═══════════════════════════════════════════════
class GovukPdfResolver(BasePdfResolver):
    """
    GOV.UK Content API에서 PDF 첨부파일 URL 추출
    API: https://www.gov.uk/api/content/{path}
    PDF는 details.attachments[] 또는 details.documents[] 에 존재
    """

    def resolve(self, doc: dict) -> list[str]:
        link = doc.get('link', '')
        if not link:
            return []

        # URL에서 path 추출: https://www.gov.uk/government/publications/xxx → /government/publications/xxx
        parsed = urlparse(link)
        path = parsed.path
        if not path or path == '/':
            return []

        api_url = f"https://www.gov.uk/api/content{path}"
        resp = self._get(api_url)
        if resp is None:
            return []

        try:
            data = resp.json()
        except (ValueError, AttributeError):
            return []

        pdf_urls = []
        details = data.get('details', {})

        # 방법 1: details.attachments[]
        for att in details.get('attachments', []):
            if att.get('content_type') == 'application/pdf':
                url = att.get('url', '')
                if url:
                    pdf_urls.append(url)

        # 방법 2: details.documents[] (HTML 조각에서 href 추출)
        if not pdf_urls:
            for doc_html in details.get('documents', []):
                if isinstance(doc_html, str) and '.pdf' in doc_html:
                    for match in re.findall(r'href=["\']([^"\']*\.pdf[^"\']*)', doc_html):
                        pdf_urls.append(match)

        # 방법 3: body HTML 내 PDF 링크
        if not pdf_urls:
            body = details.get('body', '')
            if isinstance(body, str) and '.pdf' in body:
                for match in re.findall(r'href=["\']([^"\']*\.pdf[^"\']*)', body):
                    pdf_urls.append(match)

        # 중복 제거 (순서 유지)
        seen = set()
        unique = []
        for u in pdf_urls:
            if u not in seen:
                seen.add(u)
                unique.append(u)

        return unique


# ═══════════════════════════════════════════════
#  GAO — GovInfo API
# ═══════════════════════════════════════════════
class GaoPdfResolver(BasePdfResolver):
    """
    GAO: GovInfo API의 summary에서 PDF 링크 추출
    link 형태: https://api.govinfo.gov/packages/{packageId}/summary
    PDF: https://api.govinfo.gov/packages/{packageId}/pdf?api_key=...
    또는 summary 응답의 download.pdfLink 필드
    """

    def __init__(self, api_key: str = "DEMO_KEY"):
        super().__init__()
        self.api_key = api_key

    def resolve(self, doc: dict) -> list[str]:
        link = doc.get('link', '')
        package_id = doc.get('package_id', '')

        if not link and not package_id:
            return []

        # summary API에서 PDF 링크 추출
        summary_url = link if link.endswith('/summary') else ''
        if not summary_url and package_id:
            summary_url = f"https://api.govinfo.gov/packages/{package_id}/summary"

        if summary_url:
            api_url = f"{summary_url}?api_key={self.api_key}"
            resp = self._get(api_url)
            if resp:
                try:
                    data = resp.json()
                    pdf_link = data.get('download', {}).get('pdfLink', '')
                    if pdf_link:
                        # API 키 추가
                        if '?' in pdf_link:
                            return [f"{pdf_link}&api_key={self.api_key}"]
                        return [f"{pdf_link}?api_key={self.api_key}"]
                except (ValueError, AttributeError):
                    pass

        # 폴백: packageId 기반 직접 PDF URL 구성
        if package_id:
            return [f"https://api.govinfo.gov/packages/{package_id}/pdf?api_key={self.api_key}"]

        return []


# ═══════════════════════════════════════════════
#  범용 HTML 파싱 PDF 추출기
# ═══════════════════════════════════════════════
class GenericHtmlPdfResolver(BasePdfResolver):
    """
    랜딩 페이지 HTML에서 PDF 링크를 탐색
    1. a[href$=".pdf"] 직접 매칭
    2. a[href*=".pdf"] 부분 매칭 (쿼리스트링 포함)
    3. meta tag / og:url 등에서 PDF 참조 확인
    """

    def resolve(self, doc: dict) -> list[str]:
        link = doc.get('link', '')
        if not link or not link.startswith('http'):
            return []

        soup = self._soup(link)
        if soup is None:
            return []

        return self._extract_pdf_urls(soup, link)

    def _extract_pdf_urls(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        pdf_urls = []
        seen = set()

        # 1. a 태그에서 .pdf 링크 수집
        for a in soup.find_all('a', href=True):
            href = a['href']
            if self._is_pdf_url(href):
                full_url = self._abs_url(href, base_url)
                if full_url and full_url not in seen:
                    seen.add(full_url)
                    pdf_urls.append(full_url)

        # 2. 메타 태그에서 PDF URL 확인
        for meta in soup.find_all('meta', attrs={'content': True}):
            content = meta.get('content', '')
            if self._is_pdf_url(content):
                if content not in seen:
                    seen.add(content)
                    pdf_urls.append(content)

        return pdf_urls

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        if not url:
            return False
        url_lower = url.lower().split('?')[0].split('#')[0]
        return url_lower.endswith('.pdf')

    @staticmethod
    def _abs_url(href: str, base: str) -> str:
        if not href:
            return ''
        if href.startswith('http'):
            return href
        return urljoin(base, href)


# ═══════════════════════════════════════════════
#  사이트별 특화 HTML PDF 추출기
# ═══════════════════════════════════════════════
class NistPdfResolver(GenericHtmlPdfResolver):
    """NIST: 출판물 페이지 → PDF 다운로드 링크"""

    def resolve(self, doc: dict) -> list[str]:
        link = doc.get('link', '')
        if not link:
            return []
        soup = self._soup(link)
        if soup is None:
            return []

        # NIST 전용: doi.org 링크나 직접 PDF 링크
        pdf_urls = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True).lower()
            if self._is_pdf_url(href):
                pdf_urls.append(self._abs_url(href, link))
            elif 'pdf' in text and ('download' in text or 'full' in text):
                full = self._abs_url(href, link)
                if full:
                    pdf_urls.append(full)

        if not pdf_urls:
            pdf_urls = self._extract_pdf_urls(soup, link)

        return list(dict.fromkeys(pdf_urls))


class BeaPdfResolver(GenericHtmlPdfResolver):
    """BEA: 논문 페이지 → PDF 다운로드 링크"""
    pass


class FrbPdfResolver(GenericHtmlPdfResolver):
    """FRB: FEDS/IFDP 페이지 → PDF 다운로드 링크"""

    def resolve(self, doc: dict) -> list[str]:
        link = doc.get('link', '')
        if not link:
            return []
        soup = self._soup(link)
        if soup is None:
            return []

        pdf_urls = []
        # FRB: "Full Paper (PDF)" 등의 링크 패턴
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True).lower()
            if self._is_pdf_url(href):
                pdf_urls.append(self._abs_url(href, 'https://www.federalreserve.gov'))
            elif 'pdf' in text and href:
                full = self._abs_url(href, 'https://www.federalreserve.gov')
                if full and self._is_pdf_url(full):
                    pdf_urls.append(full)

        if not pdf_urls:
            pdf_urls = self._extract_pdf_urls(soup, link)

        return list(dict.fromkeys(pdf_urls))


class BjsPdfResolver(GenericHtmlPdfResolver):
    """BJS: ojp.gov 출판물 → PDF 다운로드 링크"""
    pass


# ═══════════════════════════════════════════════
#  리졸버 레지스트리
# ═══════════════════════════════════════════════
# site_code → resolver class 매핑
RESOLVER_REGISTRY: dict[str, type[BasePdfResolver]] = {
    'Z00054': NistPdfResolver,    # NIST
    'Z00057': BeaPdfResolver,     # BEA
    'Z00063': FrbPdfResolver,     # FRB
    'Z00048': BjsPdfResolver,     # BJS
}


def get_resolver(country: str, site_code: str, api_key: str = "DEMO_KEY") -> BasePdfResolver:
    """국가/사이트코드에 맞는 리졸버 반환"""
    if country == "UK":
        return GovukPdfResolver()
    if site_code == 'Z00014':  # GAO
        return GaoPdfResolver(api_key=api_key)
    if site_code in RESOLVER_REGISTRY:
        return RESOLVER_REGISTRY[site_code]()
    return GenericHtmlPdfResolver()


# ═══════════════════════════════════════════════
#  메인 실행 로직
# ═══════════════════════════════════════════════
def process_json_file(fpath: str, force: bool = False, api_key: str = "DEMO_KEY") -> dict:
    """
    단일 JSON 파일의 모든 문서에 대해 PDF URL을 추출하여 업데이트
    Returns: {'total': N, 'resolved': N, 'skipped': N, 'failed': N}
    """
    fname = os.path.basename(fpath)

    with open(fpath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    documents = data.get('documents', [])
    if not documents:
        return {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}

    # 국가 판별
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
    delay = REQUEST_DELAY_UK if country == "UK" else REQUEST_DELAY_US

    resolver = get_resolver(country, site_code, api_key=api_key)
    logger.info(f"처리 중: {fname} ({len(documents)}건) → {type(resolver).__name__}")

    stats = {'total': len(documents), 'resolved': 0, 'skipped': 0, 'failed': 0}
    modified = False

    for i, doc in enumerate(documents):
        # 이미 처리된 문서는 건너뛰기 (--force가 아닌 경우)
        if not force and doc.get('pdf_url') is not None:
            stats['skipped'] += 1
            continue

        try:
            pdf_urls = resolver.resolve(doc)

            doc['pdf_url'] = pdf_urls[0] if pdf_urls else ""
            doc['pdf_urls'] = pdf_urls
            doc['pdf_resolved_at'] = datetime.now().isoformat()
            modified = True

            if pdf_urls:
                stats['resolved'] += 1
                logger.debug(f"  [{i+1}/{len(documents)}] PDF 발견: {pdf_urls[0][:80]}...")
            else:
                stats['failed'] += 1
                logger.debug(f"  [{i+1}/{len(documents)}] PDF 없음: {doc.get('title', '')[:50]}")

        except Exception as e:
            doc['pdf_url'] = ""
            doc['pdf_urls'] = []
            doc['pdf_resolved_at'] = datetime.now().isoformat()
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
            time.sleep(delay)

    # 파일 저장
    if modified:
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"  저장 완료: {fname} "
                    f"(성공: {stats['resolved']}, 실패: {stats['failed']}, 건너뜀: {stats['skipped']})")

    return stats


def run(country: str | None = None, force: bool = False, api_key: str = "DEMO_KEY"):
    """전체 또는 국가별 PDF URL 추출 실행"""
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

    logger.info(f"PDF URL 추출 시작: {len(json_files)}개 파일")
    logger.info(f"  국가 필터: {country or '전체'}, 강제 재처리: {force}")
    logger.info("=" * 60)

    total_stats = {'total': 0, 'resolved': 0, 'skipped': 0, 'failed': 0}
    start_time = time.time()

    for i, fpath in enumerate(json_files, 1):
        fname = os.path.basename(fpath)
        logger.info(f"[{i}/{len(json_files)}] {fname}")

        stats = process_json_file(fpath, force=force, api_key=api_key)
        for k in total_stats:
            total_stats[k] += stats[k]

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"PDF URL 추출 완료! (소요시간: {elapsed:.0f}초)")
    logger.info(f"  전체: {total_stats['total']}건")
    logger.info(f"  성공: {total_stats['resolved']}건")
    logger.info(f"  실패: {total_stats['failed']}건")
    logger.info(f"  건너뜀: {total_stats['skipped']}건")


def main():
    parser = argparse.ArgumentParser(description='PDF 다운로드 URL 추출기')
    parser.add_argument('--country', '-c',
                        help='국가 필터 (예: UK, US, AT, CA, NO 등)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='이미 처리된 문서도 재처리')
    parser.add_argument('--api-key', '-k', default='DEMO_KEY',
                        help='GovInfo API 키 (GAO용, 기본: DEMO_KEY)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='상세 로그 출력')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run(country=args.country, force=args.force, api_key=args.api_key)


if __name__ == '__main__':
    main()
