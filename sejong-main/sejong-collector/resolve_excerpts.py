"""
선별된 문서에서 발췌(description)를 웹페이지 meta description/og:description에서 추출
"""
import json
import sys
import io
import time
import re
import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_400.json'
REQUEST_DELAY = 0.2

session = requests.Session()
session.headers.update({
    'User-Agent': 'SejongLibrary-MetaResolver/1.0 (Research Purpose)',
    'Accept': 'text/html',
})


def extract_excerpt(url):
    """웹페이지에서 발췌 텍스트 추출"""
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        # 1. meta description
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta and meta.get('content', '').strip():
            text = meta['content'].strip()
            if len(text) > 30:
                return text[:500]

        # 2. og:description
        og = soup.find('meta', attrs={'property': 'og:description'})
        if og and og.get('content', '').strip():
            text = og['content'].strip()
            if len(text) > 30:
                return text[:500]

        # 3. twitter:description
        tw = soup.find('meta', attrs={'name': 'twitter:description'})
        if tw and tw.get('content', '').strip():
            text = tw['content'].strip()
            if len(text) > 30:
                return text[:500]

        # 4. First meaningful paragraph
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if len(text) > 50 and not any(skip in text.lower() for skip in
                ['cookie', 'javascript', 'browser', 'privacy policy', 'terms of use',
                 'sign up', 'subscribe', 'newsletter', 'copyright']):
                return text[:500]

        return ''
    except Exception:
        return ''


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    # Sort and take top 300
    docs.sort(key=lambda x: -x.get('_relevance_score', 0))

    need_excerpt = sum(1 for d in docs[:300] if not d.get('description', '').strip())
    print(f"Need excerpt: {need_excerpt}/300", flush=True)

    resolved = 0
    failed = 0

    for i, doc in enumerate(docs[:300]):
        if doc.get('description', '').strip():
            continue

        url = doc.get('link', '')
        excerpt = extract_excerpt(url)
        if excerpt:
            doc['description'] = excerpt
            resolved += 1
        else:
            failed += 1

        time.sleep(REQUEST_DELAY)

        if (resolved + failed) % 20 == 0:
            print(f"  [{i+1}/300] resolved: {resolved}, failed: {failed}", flush=True)

    has_desc = sum(1 for d in docs[:300] if d.get('description', '').strip())
    print(f"\nFinal: {has_desc}/300 with excerpt", flush=True)
    print(f"Resolved: {resolved}, Failed: {failed}", flush=True)

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Saved", flush=True)


if __name__ == '__main__':
    main()
