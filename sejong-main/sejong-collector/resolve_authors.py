"""
선별된 문서들의 저자(authors)를 웹페이지에서 추출.
meta tags, JSON-LD, byline 등에서 저자 정보 추출.
"""
import json
import re
import sys
import io
import time
import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_400.json'

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})


def clean_author(name):
    """Clean up author name."""
    name = name.strip()
    # Remove common prefixes/suffixes
    name = re.sub(r'^(by|author[s]?:?|written by|prepared by)\s+', '', name, flags=re.IGNORECASE)
    name = name.strip(' ,;|')
    # Skip if looks like org name or junk
    if len(name) < 3 or len(name) > 100:
        return ''
    if any(x in name.lower() for x in ['cookie', 'javascript', 'subscribe', 'privacy',
                                         'copyright', 'admin', 'editor', 'webmaster',
                                         'http', 'www.', '.com', '.org', '.gov']):
        return ''
    return name


def extract_authors(url):
    """웹페이지에서 저자 추출"""
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        authors = []

        # 1. meta author tag
        meta = soup.find('meta', attrs={'name': 'author'})
        if meta and meta.get('content'):
            a = clean_author(meta['content'])
            if a:
                authors.append(a)

        # 2. meta article:author
        meta = soup.find('meta', attrs={'property': 'article:author'})
        if meta and meta.get('content'):
            a = clean_author(meta['content'])
            if a:
                authors.append(a)

        # 3. meta citation_author (academic papers)
        for meta in soup.find_all('meta', attrs={'name': 'citation_author'}):
            if meta.get('content'):
                a = clean_author(meta['content'])
                if a:
                    authors.append(a)

        # 4. JSON-LD author
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    author_data = item.get('author', '')
                    if isinstance(author_data, str) and author_data:
                        a = clean_author(author_data)
                        if a:
                            authors.append(a)
                    elif isinstance(author_data, dict):
                        name = author_data.get('name', '')
                        if name:
                            a = clean_author(name)
                            if a:
                                authors.append(a)
                    elif isinstance(author_data, list):
                        for ad in author_data:
                            if isinstance(ad, dict):
                                name = ad.get('name', '')
                            elif isinstance(ad, str):
                                name = ad
                            else:
                                continue
                            if name:
                                a = clean_author(name)
                                if a:
                                    authors.append(a)
            except (json.JSONDecodeError, TypeError):
                pass

        # 5. DC.creator
        for meta in soup.find_all('meta', attrs={'name': 'DC.creator'}):
            if meta.get('content'):
                a = clean_author(meta['content'])
                if a:
                    authors.append(a)

        # 6. Byline CSS selectors
        byline_selectors = [
            '.author', '.byline', '.by-author', '.post-author',
            '.article-author', '.contributor', '.writer',
            '[class*="author"]', '[class*="byline"]',
            '[rel="author"]', '[itemprop="author"]',
        ]
        for sel in byline_selectors:
            for el in soup.select(sel):
                text = el.get_text(strip=True)
                # Clean "By Name" patterns
                text = re.sub(r'^by\s+', '', text, flags=re.IGNORECASE)
                a = clean_author(text)
                if a and len(a) < 80:
                    authors.append(a)

        # Deduplicate
        seen = set()
        unique = []
        for a in authors:
            a_lower = a.lower()
            if a_lower not in seen:
                seen.add(a_lower)
                unique.append(a)

        return ', '.join(unique[:5])  # Max 5 authors
    except Exception:
        return ''


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    # Only process non-excluded docs
    targets = [d for d in docs if d.get('_relevance_score', 0) > -1]
    targets.sort(key=lambda x: -x.get('_relevance_score', 0))

    need = sum(1 for d in targets if not d.get('authors', '').strip())
    print(f"Need authors: {need}/{len(targets)}", flush=True)

    resolved = 0
    failed = 0

    for i, doc in enumerate(targets):
        if doc.get('authors', '').strip():
            continue

        url = doc.get('link', '')
        author_str = extract_authors(url)
        if author_str:
            doc['authors'] = author_str
            resolved += 1
            print(f"  OK [{doc.get('_country','')}] {author_str[:40]} | {doc.get('title','')[:45]}", flush=True)
        else:
            failed += 1

        time.sleep(0.3)

        if (resolved + failed) % 20 == 0:
            print(f"  [{i+1}/{len(targets)}] resolved: {resolved}, failed: {failed}", flush=True)

    has_author = sum(1 for d in targets if d.get('authors', '').strip())
    print(f"\nFinal: {has_author}/{len(targets)} with author", flush=True)
    print(f"Resolved: {resolved}, Failed: {failed}", flush=True)

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Saved", flush=True)


if __name__ == '__main__':
    main()
