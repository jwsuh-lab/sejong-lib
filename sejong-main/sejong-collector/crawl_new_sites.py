"""
신규 사이트 3곳 크롤러:
1. EPRS (European Parliament Think Tank)
2. IEA (International Energy Agency) — 403 차단으로 WebFetch 기반
3. Oxfam International

2026년 발행, PDF 다운로드 가능, 영문 정책보고서 수집.
"""
import json
import logging
import re
import sys
import io
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / 'data'
REQUEST_DELAY = 1.5

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/json',
    'Accept-Language': 'en-US,en;q=0.9',
})


def safe_get(url, timeout=30, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get('Retry-After', 2 ** attempt * 5))
                logger.warning(f"  429 Rate Limited, {wait}s wait")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.SSLError:
            try:
                return session.get(url, timeout=timeout, verify=False)
            except Exception:
                pass
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"  요청 실패: {url[:80]} — {e}")
            else:
                time.sleep(2 ** attempt)
    return None


def save_results(prefix, site_code, site_name, site_name_kr, acronym, source_url, documents):
    if not documents:
        logger.info(f"  {site_name}: 수집 결과 없음")
        return None
    timestamp = datetime.now().strftime('%Y%m%d')
    tag = acronym or site_code
    filepath = DATA_DIR / f"{prefix}_{site_code}_{tag}_{timestamp}.json"
    output = {
        'metadata': {
            'site_code': site_code,
            'site_name': site_name,
            'site_name_kr': site_name_kr,
            'acronym': acronym,
            'source_url': source_url,
            'crawled_at': datetime.now().isoformat(),
            'total_collected': len(documents),
        },
        'documents': documents,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"  저장: {filepath.name} ({len(documents)}건)")
    return filepath


# ═══════════════════════════════════════════════
#  EPRS PDF URL 생성 헬퍼
# ═══════════════════════════════════════════════
# Document type code → folder name mapping
EPRS_TYPE_FOLDER = {
    'BRI': 'BRIE',   # Briefing
    'STU': 'STUD',   # Study
    'IDA': 'IDAN',   # In-Depth Analysis
    'ATA': 'ATAG',   # At a Glance
}


def eprs_pdf_url(doc_id):
    """
    EPRS_BRI(2026)774726 → https://www.europarl.europa.eu/RegData/etudes/BRIE/2026/774726/EPRS_BRI(2026)774726_EN.pdf
    """
    m = re.match(r'(\w+)_(\w+)\((\d{4})\)(\d+)', doc_id)
    if not m:
        return ''
    prefix, dtype, year, num = m.groups()
    folder = EPRS_TYPE_FOLDER.get(dtype, dtype)
    return f"https://www.europarl.europa.eu/RegData/etudes/{folder}/{year}/{num}/{doc_id}_EN.pdf"


def verify_url(url):
    """HEAD 요청으로 URL 접근 가능 여부 확인"""
    try:
        resp = session.head(url, timeout=10, allow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False


# ═══════════════════════════════════════════════
#  1. EPRS (European Parliament Think Tank)
# ═══════════════════════════════════════════════
def crawl_eprs(max_results=200):
    """
    EPRS advanced-search 페이지에서 2026년 Briefing/Study/In-Depth Analysis 수집.
    페이지가 "Load more" AJAX 방식이므로 document 페이지를 직접 파싱.
    """
    logger.info("=" * 60)
    logger.info("EPRS (European Parliament Think Tank) 크롤링 시작")
    logger.info("=" * 60)

    documents = []
    seen_ids = set()

    # EPRS advanced search — 필터링된 페이지 로드
    # Note: The page uses "Load more" AJAX, so we need to find the actual data source
    # Try fetching the search page and extracting document links
    search_url = 'https://www.europarl.europa.eu/thinktank/en/research/advanced-search'
    params_list = [
        # Briefings + Studies + In-Depth Analysis, 2026
        '?documentType=BRIEFINGS&documentType=STUDIES&documentType=IN-DEPTH+ANALYSIS&startDate=01%2F01%2F2026&endDate=31%2F12%2F2026',
    ]

    for params in params_list:
        url = search_url + params
        logger.info(f"  EPRS search: {url[:100]}")
        resp = safe_get(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract document links
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Match document URLs like /thinktank/en/document/EPRS_BRI(2026)774726
            m = re.search(r'/document/(\w+_\w+\(\d{4}\)\d+)', href)
            if not m:
                continue
            doc_id = m.group(1)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            title = a.get_text(strip=True)
            if not title or len(title) < 5:
                parent = a.find_parent(['div', 'li', 'article'])
                if parent:
                    h = parent.find(['h2', 'h3', 'h4', 'span'])
                    if h:
                        title = h.get_text(strip=True)
            if not title or len(title) < 5:
                title = doc_id

            doc_url = f"https://www.europarl.europa.eu/thinktank/en/document/{doc_id}"
            pdf_url = eprs_pdf_url(doc_id)

            documents.append({
                'site_code': 'NEW_EPRS',
                'site_name': 'European Parliamentary Research Service',
                'site_acronym': 'EPRS',
                'title': title,
                'link': doc_url,
                'published_date': '',  # 개별 페이지에서 실제 날짜 추출
                'document_type': 'research',
                'pdf_url': pdf_url,
            })

    logger.info(f"  EPRS 검색 페이지에서 {len(documents)}건 발견")

    # 검색 결과가 10건(첫 페이지)뿐이므로 각 문서 페이지에서 추가 정보 수집
    # + 관련 문서 링크 탐색으로 확장
    logger.info(f"  EPRS: 개별 문서 페이지에서 메타데이터 + 추가 문서 탐색")
    enriched = []
    extra_found = 0

    for i, doc in enumerate(documents[:max_results]):
        resp = safe_get(doc['link'], timeout=15)
        if not resp:
            enriched.append(doc)
            time.sleep(0.5)
            continue

        page_soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract date from page (메타태그 우선, 본문 텍스트 fallback)
        pub_date = ''
        for attr, val in [('property', 'article:published_time'), ('name', 'citation_publication_date'),
                          ('name', 'DC.date.issued'), ('name', 'DC.date'), ('name', 'date')]:
            meta = page_soup.find('meta', attrs={attr: val})
            if meta and meta.get('content'):
                ym = re.search(r'(\d{4})', meta['content'])
                if ym and int(ym.group(1)) >= 2026:
                    pub_date = meta['content'].strip()
                    break
        if not pub_date:
            date_el = page_soup.find('time') or page_soup.find(class_=re.compile(r'date'))
            if date_el:
                dt_text = date_el.get('datetime', '') or date_el.get_text(strip=True)
                ym = re.search(r'(\d{4})', dt_text)
                if ym and int(ym.group(1)) >= 2026:
                    pub_date = dt_text
        if pub_date:
            doc['published_date'] = pub_date

        # Extract authors
        authors = []
        for meta in page_soup.find_all('meta', attrs={'name': 'author'}):
            if meta.get('content'):
                authors.append(meta['content'])
        if not authors:
            author_el = page_soup.find(class_=re.compile(r'author'))
            if author_el:
                authors.append(author_el.get_text(strip=True))
        if authors:
            doc['authors'] = ', '.join(authors)

        # Extract description
        desc_meta = page_soup.find('meta', attrs={'name': 'description'})
        if desc_meta and desc_meta.get('content'):
            doc['description'] = desc_meta['content']

        # Verify PDF URL exists
        if doc.get('pdf_url'):
            if not verify_url(doc['pdf_url']):
                # Try finding PDF on the page directly
                for a_tag in page_soup.find_all('a', href=True):
                    if a_tag['href'].lower().endswith('.pdf'):
                        doc['pdf_url'] = urljoin(doc['link'], a_tag['href'])
                        break

        # Find additional document links on this page
        for a_tag in page_soup.find_all('a', href=True):
            href = a_tag['href']
            m2 = re.search(r'/document/(\w+_\w+\(\d{4}\)\d+)', href)
            if m2:
                new_id = m2.group(1)
                if new_id not in seen_ids and '2026' in new_id:
                    seen_ids.add(new_id)
                    new_title = a_tag.get_text(strip=True) or new_id
                    new_pdf = eprs_pdf_url(new_id)
                    documents.append({
                        'site_code': 'NEW_EPRS',
                        'site_name': 'European Parliamentary Research Service',
                        'site_acronym': 'EPRS',
                        'title': new_title,
                        'link': f"https://www.europarl.europa.eu/thinktank/en/document/{new_id}",
                        'published_date': '',  # 개별 페이지에서 실제 날짜 추출
                        'document_type': 'research',
                        'pdf_url': new_pdf,
                    })
                    extra_found += 1

        enriched.append(doc)
        logger.info(f"    [{i+1}/{len(documents)}] {doc['title'][:50]}")
        time.sleep(REQUEST_DELAY)

    if extra_found:
        logger.info(f"  EPRS: 관련 문서 {extra_found}건 추가 발견")

    # Now let's also try browsing by policy area pages for more coverage
    policy_areas = [
        'economic-and-monetary-affairs',
        'environment',
        'foreign-affairs',
        'industry-research-and-energy',
        'internal-market-and-consumer-protection',
        'international-trade',
        'security-and-defence',
        'transport-and-tourism',
        'employment-and-social-affairs',
        'civil-liberties-justice-and-home-affairs',
        'development',
        'budgets',
        'agriculture-and-rural-development',
        'education-and-culture',
        'public-health',
    ]

    for area in policy_areas:
        if len(documents) >= max_results:
            break
        area_url = f"https://www.europarl.europa.eu/thinktank/en/research/by-policy-area/{area}"
        resp = safe_get(area_url, timeout=15)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        area_count = 0
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            m2 = re.search(r'/document/(\w+_\w+\(\d{4}\)\d+)', href)
            if m2:
                new_id = m2.group(1)
                if new_id not in seen_ids and '2026' in new_id:
                    seen_ids.add(new_id)
                    new_title = a_tag.get_text(strip=True) or new_id
                    if len(new_title) < 5:
                        parent = a_tag.find_parent(['div', 'li'])
                        if parent:
                            h = parent.find(['h2', 'h3', 'h4', 'span'])
                            if h:
                                new_title = h.get_text(strip=True)

                    dtype_m = re.search(r'_(\w+)\(', new_id)
                    dtype = dtype_m.group(1) if dtype_m else ''
                    # Only collect Briefings, Studies, In-Depth Analysis
                    if dtype not in ('BRI', 'STU', 'IDA', 'ATA'):
                        continue

                    new_pdf = eprs_pdf_url(new_id)
                    documents.append({
                        'site_code': 'NEW_EPRS',
                        'site_name': 'European Parliamentary Research Service',
                        'site_acronym': 'EPRS',
                        'title': new_title,
                        'link': f"https://www.europarl.europa.eu/thinktank/en/document/{new_id}",
                        'published_date': '',  # 개별 페이지에서 실제 날짜 추출
                        'document_type': 'research',
                        'pdf_url': new_pdf,
                    })
                    area_count += 1

        if area_count:
            logger.info(f"  EPRS policy area [{area}]: +{area_count}건")
        time.sleep(REQUEST_DELAY)

    # Filter: only keep docs with PDF URL
    with_pdf = [d for d in documents if d.get('pdf_url')]
    # Deduplicate by link
    seen_links = set()
    unique = []
    for d in with_pdf:
        if d['link'] not in seen_links:
            seen_links.add(d['link'])
            unique.append(d)

    logger.info(f"  EPRS 최종: 전체 {len(documents)}건, PDF+중복제거 {len(unique)}건")

    save_results('eu', 'NEW_EPRS', 'European Parliamentary Research Service',
                 '유럽의회 연구서비스', 'EPRS',
                 'https://www.europarl.europa.eu/thinktank', unique)
    return unique


# ═══════════════════════════════════════════════
#  2. IEA (International Energy Agency)
#  403 차단되므로 알려진 2026 보고서 목록 직접 수집
# ═══════════════════════════════════════════════
def crawl_iea(max_results=80):
    """
    IEA는 자동 요청을 403 차단.
    알려진 2026 보고서 목록 + sitemap에서 수집 시도.
    """
    logger.info("=" * 60)
    logger.info("IEA (International Energy Agency) 크롤링 시작")
    logger.info("=" * 60)

    documents = []

    # Try sitemap first
    sitemap_urls = [
        'https://www.iea.org/sitemap.xml',
        'https://www.iea.org/sitemap-reports.xml',
    ]

    for smap_url in sitemap_urls:
        resp = safe_get(smap_url, timeout=15)
        if not resp:
            continue

        # Parse sitemap XML
        soup = BeautifulSoup(resp.text, 'xml')
        for loc in soup.find_all('loc'):
            url = loc.get_text(strip=True)
            if '/reports/' in url and url != 'https://www.iea.org/reports':
                # Extract report slug
                slug = url.rstrip('/').split('/')[-1]
                if not slug:
                    continue

                documents.append({
                    'site_code': 'NEW_IEA',
                    'site_name': 'International Energy Agency',
                    'site_acronym': 'IEA',
                    'title': slug.replace('-', ' ').title(),
                    'link': url,
                    'published_date': '',
                    'document_type': 'report',
                    'pdf_url': '',
                })

        if documents:
            logger.info(f"  IEA sitemap에서 {len(documents)}건 발견")
            break

    # If sitemap didn't work, use known 2026 reports from web search
    if not documents:
        logger.info("  IEA: sitemap 실패, 알려진 2026 보고서 목록 사용")
        known_reports = [
            ('Electricity 2026', 'https://www.iea.org/reports/electricity-2026'),
            ('The State of Energy Innovation 2026', 'https://www.iea.org/reports/the-state-of-energy-innovation-2026'),
            ('World Energy Outlook 2025', 'https://www.iea.org/reports/world-energy-outlook-2025'),
            ('Energy Efficiency 2025', 'https://www.iea.org/reports/energy-efficiency-2025'),
            ('Renewables 2025', 'https://www.iea.org/reports/renewables-2025'),
            ('Global EV Outlook 2026', 'https://www.iea.org/reports/global-ev-outlook-2026'),
            ('CO2 Emissions in 2025', 'https://www.iea.org/reports/co2-emissions-in-2025'),
            ('Oil Market Report', 'https://www.iea.org/reports/oil-market-report-march-2026'),
            ('Gas Market Report Q1 2026', 'https://www.iea.org/reports/gas-market-report-q1-2026'),
            ('Coal 2025', 'https://www.iea.org/reports/coal-2025'),
            ('Critical Minerals Market Review 2025', 'https://www.iea.org/reports/critical-minerals-market-review-2025'),
            ('Nuclear Power and Secure Energy Transitions', 'https://www.iea.org/reports/nuclear-power-and-secure-energy-transitions-2025'),
            ('Southeast Asia Energy Outlook 2025', 'https://www.iea.org/reports/southeast-asia-energy-outlook-2025'),
            ('Africa Energy Outlook 2025', 'https://www.iea.org/reports/africa-energy-outlook-2025'),
            ('Batteries and Secure Energy Transitions', 'https://www.iea.org/reports/batteries-and-secure-energy-transitions'),
            ('Global Hydrogen Review 2025', 'https://www.iea.org/reports/global-hydrogen-review-2025'),
            ('Energy Technology Perspectives 2026', 'https://www.iea.org/reports/energy-technology-perspectives-2026'),
            ('World Energy Investment 2025', 'https://www.iea.org/reports/world-energy-investment-2025'),
            ('Net Zero Roadmap 2025 Update', 'https://www.iea.org/reports/net-zero-roadmap-a-global-pathway-to-keep-the-15-0c-goal-in-reach'),
            ('Tracking Clean Energy Progress 2026', 'https://www.iea.org/reports/tracking-clean-energy-progress-2026'),
        ]

        for title, url in known_reports:
            documents.append({
                'site_code': 'NEW_IEA',
                'site_name': 'International Energy Agency',
                'site_acronym': 'IEA',
                'title': title,
                'link': url,
                'published_date': '',  # HEAD/GET으로 실제 날짜 확인 필요
                'document_type': 'report',
                'pdf_url': '',
            })

    # Try to resolve PDF URLs — IEA PDFs are typically on iea.blob.core.windows.net
    logger.info(f"  IEA: {len(documents)}건 PDF 확인 중...")

    # Known PDF patterns for IEA
    iea_pdf_patterns = {
        'electricity-2026': 'https://iea.blob.core.windows.net/assets/electricity-2026/Electricity2026.pdf',
    }

    verified = []
    for doc in documents:
        # Only keep 2026 publications
        if '2026' not in doc['published_date'] and '2026' not in doc['title']:
            continue

        # Try direct PDF URL patterns
        slug = doc['link'].rstrip('/').split('/')[-1]
        if slug in iea_pdf_patterns:
            doc['pdf_url'] = iea_pdf_patterns[slug]

        # Try HEAD request to check if report page is accessible
        try:
            resp = session.head(doc['link'], timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                verified.append(doc)
                logger.info(f"    OK: {doc['title'][:50]}")
            else:
                logger.info(f"    {resp.status_code}: {doc['title'][:50]}")
        except Exception:
            logger.info(f"    FAIL: {doc['title'][:50]}")

        time.sleep(0.5)

    logger.info(f"  IEA 결과: 전체 {len(documents)}건, 접근 가능 {len(verified)}건")

    # Note: IEA PDFs typically require free account login
    # Save all verified reports (PDF may need manual resolution)
    save_results('intl', 'NEW_IEA', 'International Energy Agency',
                 '국제에너지기구', 'IEA',
                 'https://www.iea.org/reports', verified)
    return verified


# ═══════════════════════════════════════════════
#  3. Oxfam International
# ═══════════════════════════════════════════════
def crawl_oxfam(max_results=60):
    """Oxfam research publications at oxfam.org/en/research"""
    logger.info("=" * 60)
    logger.info("Oxfam International 크롤링 시작")
    logger.info("=" * 60)

    documents = []

    page = 0
    while len(documents) < max_results:
        url = f'https://www.oxfam.org/en/research?page={page}'
        logger.info(f"  Oxfam page {page}: {url}")

        resp = safe_get(url)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, 'html.parser')
        items = soup.select('.views-row, article, .teaser')
        if not items:
            items = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/research/' in href and href != '/en/research':
                    items.append(a)

        if not items:
            break

        new_count = 0
        for item in items:
            a = item if item.name == 'a' else item.find('a', href=True)
            if not a:
                continue
            href = a.get('href', '')
            if not href:
                continue
            title = a.get_text(strip=True)
            if not title or len(title) < 10:
                parent = a.find_parent(['article', 'div', 'li'])
                if parent:
                    h = parent.find(['h2', 'h3', 'h4'])
                    if h:
                        title = h.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            doc_url = urljoin('https://www.oxfam.org', href)
            if any(d['link'] == doc_url for d in documents):
                continue

            documents.append({
                'site_code': 'NEW_OXFAM',
                'site_name': 'Oxfam International',
                'site_acronym': 'Oxfam',
                'title': title,
                'link': doc_url,
                'published_date': '',
                'document_type': 'research',
                'pdf_url': '',
            })
            new_count += 1

        logger.info(f"  Oxfam page {page}: +{new_count}건 (누적 {len(documents)}건)")
        if new_count == 0:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
        if page > 15:
            break

    # Resolve dates and PDFs
    logger.info(f"  Oxfam: {len(documents)}건 날짜/PDF 확인 중...")
    verified = []
    for i, doc in enumerate(documents):
        resp = safe_get(doc['link'], timeout=15)
        if not resp:
            continue

        ps = BeautifulSoup(resp.text, 'html.parser')

        # Extract date
        date_str = ''
        for prop in ['article:published_time', 'datePublished', 'DC.date']:
            m = ps.find('meta', attrs={'property': prop}) or ps.find('meta', attrs={'name': prop})
            if m and m.get('content'):
                date_str = m['content']
                break

        if not date_str:
            for script in ps.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '')
                    if isinstance(data, dict):
                        date_str = data.get('datePublished', '') or data.get('dateCreated', '')
                        if date_str:
                            break
                except Exception:
                    pass

        if not date_str:
            time_tag = ps.find('time')
            if time_tag:
                date_str = time_tag.get('datetime', '') or time_tag.get_text(strip=True)

        if '2026' not in str(date_str):
            text = ps.get_text()[:3000]
            for pattern in [
                r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+2026',
                r'\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+2026',
                r'2026-\d{2}-\d{2}',
            ]:
                dm = re.search(pattern, text)
                if dm:
                    date_str = dm.group(0)
                    break
            else:
                time.sleep(0.5)
                continue

        doc['published_date'] = date_str

        # Find PDF
        pdf_url = ''
        for a_tag in ps.find_all('a', href=True):
            h = a_tag['href']
            if h.lower().endswith('.pdf'):
                pdf_url = urljoin(doc['link'], h)
                break
        if not pdf_url:
            for a_tag in ps.find_all('a', href=True):
                h = a_tag['href']
                t = a_tag.get_text(strip=True).lower()
                if any(kw in t for kw in ['download', 'pdf', 'full report', 'read report']):
                    full = urljoin(doc['link'], h)
                    if '.pdf' in full.lower():
                        pdf_url = full
                        break

        if pdf_url:
            doc['pdf_url'] = pdf_url
            verified.append(doc)
            logger.info(f"    [{i+1}] OK: {doc['title'][:40]} | {date_str}")
        else:
            logger.info(f"    [{i+1}] No PDF: {doc['title'][:40]}")

        time.sleep(0.8)

    logger.info(f"  Oxfam 결과: 전체 {len(documents)}건, 2026+PDF {len(verified)}건")

    save_results('intl', 'NEW_OXFAM', 'Oxfam International',
                 '옥스팜 인터내셔널', 'Oxfam',
                 'https://www.oxfam.org/en/research', verified)
    return verified


# ═══════════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', choices=['eprs', 'iea', 'oxfam', 'all'], default='all')
    args = parser.parse_args()

    all_docs = []

    if args.site in ('eprs', 'all'):
        eprs_docs = crawl_eprs()
        all_docs.extend(eprs_docs)
        logger.info(f"  EPRS: {len(eprs_docs)}건")

    if args.site in ('iea', 'all'):
        iea_docs = crawl_iea()
        all_docs.extend(iea_docs)
        logger.info(f"  IEA:  {len(iea_docs)}건")

    if args.site in ('oxfam', 'all'):
        oxfam_docs = crawl_oxfam()
        all_docs.extend(oxfam_docs)
        logger.info(f"  Oxfam: {len(oxfam_docs)}건")

    logger.info("=" * 60)
    logger.info(f"전체 수집 결과: {len(all_docs)}건")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
