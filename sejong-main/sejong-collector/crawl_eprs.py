"""
EPRS (European Parliament Think Tank) 크롤러.
JS 렌더링 없이 날짜 범위를 좁혀서 10건씩 수집.
309건 중 최대한 많이 수집.
"""
import json
import logging
import re
import sys
import io
import time
from datetime import datetime, timedelta
from pathlib import Path

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

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})

# Document type code → folder name mapping for PDF URL
EPRS_TYPE_FOLDER = {
    'BRI': 'BRIE',
    'STU': 'STUD',
    'IDA': 'IDAN',
    'ATA': 'ATAG',
}


def eprs_pdf_url(doc_id):
    m = re.match(r'(\w+)_(\w+)\((\d{4})\)(\d+)', doc_id)
    if not m:
        return ''
    prefix, dtype, year, num = m.groups()
    folder = EPRS_TYPE_FOLDER.get(dtype, dtype)
    return f"https://www.europarl.europa.eu/RegData/etudes/{folder}/{year}/{num}/{doc_id}_EN.pdf"


def safe_get(url, timeout=30):
    for attempt in range(1, 4):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(2 ** attempt * 5)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt == 3:
                logger.error(f"  요청 실패: {url[:80]} — {e}")
            else:
                time.sleep(2 ** attempt)
    return None


def fetch_eprs_page(start_date, end_date, doc_types=None):
    """지정 날짜 범위의 EPRS 문서 목록 가져오기 (최대 10건)"""
    if doc_types is None:
        doc_types = ['BRIEFINGS', 'STUDIES', 'IN-DEPTH+ANALYSIS']

    type_params = '&'.join(f'documentType={t}' for t in doc_types)
    sd = start_date.strftime('%d/%m/%Y')
    ed = end_date.strftime('%d/%m/%Y')
    url = f"https://www.europarl.europa.eu/thinktank/en/research/advanced-search?{type_params}&startDate={sd}&endDate={ed}"

    resp = safe_get(url)
    if not resp:
        return [], 0

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract total count
    total = 0
    text = soup.get_text()
    m = re.search(r'Showing\s+\d+\s+of\s+(\d+)', text)
    if m:
        total = int(m.group(1))

    docs = []
    seen = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        m2 = re.search(r'/document/(\w+_\w+\(\d{4}\)\d+)', href)
        if not m2:
            continue
        doc_id = m2.group(1)
        if doc_id in seen:
            continue
        seen.add(doc_id)

        title = a.get_text(strip=True)
        if not title or len(title) < 5:
            parent = a.find_parent(['div', 'li', 'article'])
            if parent:
                h = parent.find(['h2', 'h3', 'h4', 'span'])
                if h:
                    title = h.get_text(strip=True)
        if not title or len(title) < 5:
            title = doc_id

        # Skip non-English titles (simple heuristic)
        if re.search(r'[àáâãäåèéêëìíîïòóôõöùúûüÀ-Ü]', title) and not re.search(r'[a-zA-Z]{5,}', title):
            continue

        pdf = eprs_pdf_url(doc_id)

        docs.append({
            'site_code': 'NEW_EPRS',
            'site_name': 'European Parliamentary Research Service',
            'site_acronym': 'EPRS',
            'title': title,
            'link': f"https://www.europarl.europa.eu/thinktank/en/document/{doc_id}",
            'published_date': '',
            'document_type': 'research',
            'pdf_url': pdf,
            '_doc_id': doc_id,
        })

    return docs, total


def generate_date_windows(start, end, days=5):
    """날짜 범위를 작은 윈도우로 분할"""
    windows = []
    current = start
    while current < end:
        window_end = min(current + timedelta(days=days - 1), end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def main():
    logger.info("=" * 60)
    logger.info("EPRS 크롤링 시작 (날짜 윈도우 방식)")
    logger.info("=" * 60)

    all_docs = {}  # doc_id → doc

    # Phase 1: 큰 윈도우(월별)로 총 건수 파악
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 13)

    # 먼저 5일 단위 윈도우로 시작
    windows = generate_date_windows(start, end, days=5)
    logger.info(f"  총 {len(windows)}개 윈도우 (5일 단위)")

    need_split = []  # 10건 초과 윈도우

    for i, (ws, we) in enumerate(windows):
        docs, total = fetch_eprs_page(ws, we)
        for d in docs:
            did = d['_doc_id']
            if did not in all_docs:
                all_docs[did] = d

        if total > 10:
            need_split.append((ws, we, total))
            logger.info(f"  [{i+1}/{len(windows)}] {ws.strftime('%m/%d')}-{we.strftime('%m/%d')}: {len(docs)}건 (총 {total}, 분할 필요)")
        else:
            logger.info(f"  [{i+1}/{len(windows)}] {ws.strftime('%m/%d')}-{we.strftime('%m/%d')}: {len(docs)}건")

        time.sleep(1.0)

    # Phase 2: 10건 초과 윈도우를 더 작게 분할 (2일)
    if need_split:
        logger.info(f"\n  Phase 2: {len(need_split)}개 윈도우 재분할 (2일 단위)")
        sub_need = []
        for ws, we, total in need_split:
            sub_windows = generate_date_windows(ws, we, days=2)
            for sw_s, sw_e in sub_windows:
                docs, sub_total = fetch_eprs_page(sw_s, sw_e)
                for d in docs:
                    did = d['_doc_id']
                    if did not in all_docs:
                        all_docs[did] = d

                if sub_total > 10:
                    sub_need.append((sw_s, sw_e, sub_total))
                    logger.info(f"    {sw_s.strftime('%m/%d')}-{sw_e.strftime('%m/%d')}: {len(docs)}건 (총 {sub_total}, 추가 분할 필요)")
                else:
                    logger.info(f"    {sw_s.strftime('%m/%d')}-{sw_e.strftime('%m/%d')}: {len(docs)}건")
                time.sleep(1.0)

    # Phase 3: 여전히 10건 초과면 문서 유형별로 분할
    if sub_need:
        logger.info(f"\n  Phase 3: {len(sub_need)}개 윈도우 유형별 분할")
        for ws, we, total in sub_need:
            for dtype in ['BRIEFINGS', 'STUDIES', 'IN-DEPTH+ANALYSIS']:
                docs, sub_total = fetch_eprs_page(ws, we, doc_types=[dtype])
                for d in docs:
                    did = d['_doc_id']
                    if did not in all_docs:
                        all_docs[did] = d
                if docs:
                    logger.info(f"    {ws.strftime('%m/%d')}-{we.strftime('%m/%d')} [{dtype}]: {len(docs)}건")
                time.sleep(1.0)

    logger.info(f"\n  총 고유 문서: {len(all_docs)}건")

    # Phase 4: 각 문서 페이지에서 메타데이터 보강
    documents = list(all_docs.values())
    logger.info(f"  메타데이터 보강 중...")

    for i, doc in enumerate(documents):
        resp = safe_get(doc['link'], timeout=15)
        if not resp:
            time.sleep(0.5)
            continue

        ps = BeautifulSoup(resp.text, 'html.parser')

        # Date
        time_el = ps.find('time')
        if time_el:
            doc['published_date'] = time_el.get('datetime', '') or time_el.get_text(strip=True)

        # Authors
        authors = []
        for meta in ps.find_all('meta', attrs={'name': 'author'}):
            if meta.get('content'):
                authors.append(meta['content'])
        if not authors:
            auth_el = ps.find(class_=re.compile(r'author'))
            if auth_el:
                auth_text = auth_el.get_text(strip=True)
                if auth_text:
                    authors.append(auth_text)
        if authors:
            doc['authors'] = ', '.join(authors)

        # Description
        desc = ps.find('meta', attrs={'name': 'description'})
        if desc and desc.get('content'):
            doc['description'] = desc['content']

        if (i + 1) % 20 == 0:
            logger.info(f"    [{i+1}/{len(documents)}] 진행 중...")

        time.sleep(0.8)

    # Clean up internal fields
    for d in documents:
        d.pop('_doc_id', None)

    # Save
    timestamp = datetime.now().strftime('%Y%m%d')
    filepath = DATA_DIR / f"eu_NEW_EPRS_EPRS_{timestamp}.json"
    output = {
        'metadata': {
            'site_code': 'NEW_EPRS',
            'site_name': 'European Parliamentary Research Service',
            'site_name_kr': '유럽의회 연구서비스',
            'acronym': 'EPRS',
            'source_url': 'https://www.europarl.europa.eu/thinktank',
            'crawled_at': datetime.now().isoformat(),
            'total_collected': len(documents),
        },
        'documents': documents,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"\n  저장: {filepath.name} ({len(documents)}건)")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
