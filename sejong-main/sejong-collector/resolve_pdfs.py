"""
PDF URL이 없는 문서들의 랜딩 페이지에서 PDF 다운로드 링크를 찾아 추출.
"""
import json
import re
import sys
import io
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_400.json'

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})


def find_pdf_link(url):
    """랜딩 페이지에서 PDF 다운로드 링크 탐색"""
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        # 1. Direct PDF links in <a> tags
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True).lower()

            # Check href ends with .pdf
            if href.lower().endswith('.pdf'):
                return urljoin(url, href)

            # Check link text suggests PDF download
            if any(kw in text for kw in ['download pdf', 'pdf download', 'full report',
                                          'download report', 'read the report',
                                          'download the full', 'view pdf',
                                          'download publication', 'full text pdf',
                                          'pdf version']):
                full_url = urljoin(url, href)
                if '.pdf' in full_url.lower():
                    return full_url

        # 2. Links containing /pdf/ or /files/ with pdf-like patterns
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/pdf/' in href.lower() or '/pdfs/' in href.lower():
                return urljoin(url, href)

        # 3. Check for embedded PDF viewers (iframe/embed/object with PDF src)
        for tag in soup.find_all(['iframe', 'embed', 'object']):
            src = tag.get('src', '') or tag.get('data', '')
            if src and '.pdf' in src.lower():
                return urljoin(url, src)

        # 4. Check meta citation_pdf_url (academic papers)
        meta = soup.find('meta', attrs={'name': 'citation_pdf_url'})
        if meta and meta.get('content'):
            return meta['content']

        return ''
    except Exception:
        return ''


def verify_pdf(url):
    """PDF URL이 실제로 접근 가능한지 HEAD 요청으로 확인"""
    if not url:
        return False
    try:
        resp = session.head(url, timeout=8, allow_redirects=True)
        content_type = resp.headers.get('content-type', '').lower()
        return resp.status_code == 200 and ('pdf' in content_type or url.lower().endswith('.pdf'))
    except Exception:
        return False


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    targets = [d for d in docs if d.get('_relevance_score', 0) > -1]
    targets.sort(key=lambda x: -x.get('_relevance_score', 0))

    need = sum(1 for d in targets if not d.get('pdf_url', '').strip())
    print(f"Need PDF: {need}/{len(targets)}", flush=True)

    resolved = 0
    failed = 0

    for i, doc in enumerate(targets):
        if doc.get('pdf_url', '').strip():
            continue

        url = doc.get('link', '')
        pdf_url = find_pdf_link(url)

        if pdf_url:
            doc['pdf_url'] = pdf_url
            resolved += 1
            print(f"  OK [{doc.get('_country','')}] {doc.get('title','')[:45]}", flush=True)
            print(f"     -> {pdf_url[:70]}", flush=True)
        else:
            failed += 1

        time.sleep(0.3)

        if (resolved + failed) % 20 == 0:
            print(f"  [{i+1}/{len(targets)}] resolved: {resolved}, failed: {failed}", flush=True)

    has_pdf = sum(1 for d in targets if d.get('pdf_url', '').strip())
    print(f"\nFinal: {has_pdf}/{len(targets)} with PDF", flush=True)
    print(f"Resolved: {resolved}, Failed: {failed}", flush=True)

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Saved", flush=True)


if __name__ == '__main__':
    main()
