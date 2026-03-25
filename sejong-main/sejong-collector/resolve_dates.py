"""
selection_400.json에서 published_date가 없는 문서들의 발행일을 웹페이지에서 추출.
meta tags: article:published_time, datePublished, DC.date, citation_publication_date 등
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
    'User-Agent': 'SejongLibrary-DateResolver/1.0 (Research Purpose)',
    'Accept': 'text/html',
})


def extract_date(url):
    """웹페이지에서 발행일 추출"""
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        # 1. article:published_time
        for prop in ['article:published_time', 'article:published']:
            meta = soup.find('meta', attrs={'property': prop})
            if meta and meta.get('content'):
                return meta['content'].strip()

        # 2. datePublished (schema.org)
        meta = soup.find('meta', attrs={'itemprop': 'datePublished'})
        if meta and meta.get('content'):
            return meta['content'].strip()

        # 3. DC.date
        for name in ['DC.date', 'DC.date.issued', 'dcterms.issued',
                      'citation_publication_date', 'citation_date',
                      'date', 'publish_date', 'pubdate']:
            meta = soup.find('meta', attrs={'name': name})
            if meta and meta.get('content'):
                return meta['content'].strip()

        # 4. JSON-LD datePublished
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                if isinstance(data, dict):
                    dp = data.get('datePublished', '')
                    if dp:
                        return dp
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            dp = item.get('datePublished', '')
                            if dp:
                                return dp
            except (json.JSONDecodeError, TypeError):
                pass

        # 5. time tag with datetime
        time_tag = soup.find('time', attrs={'datetime': True})
        if time_tag:
            return time_tag['datetime'].strip()

        return ''
    except Exception:
        return ''


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    # Sort and focus on top 300
    docs.sort(key=lambda x: -x.get('_relevance_score', 0))

    need_date = sum(1 for d in docs[:300] if not d.get('published_date', '').strip())
    print(f"Need date: {need_date}/300", flush=True)

    resolved = 0
    failed = 0

    for i, doc in enumerate(docs[:300]):
        if doc.get('published_date', '').strip():
            continue

        url = doc.get('link', '')
        date_str = extract_date(url)
        if date_str:
            # Validate it contains a year
            year_match = re.search(r'(\d{4})', date_str)
            if year_match:
                doc['published_date'] = date_str
                year = int(year_match.group(1))
                resolved += 1
                if year < 2026:
                    print(f"  [{doc.get('_country','')}] PRE-2026: {year} | {doc.get('title','')[:60]}", flush=True)
            else:
                failed += 1
        else:
            failed += 1

        time.sleep(0.2)

        if (resolved + failed) % 20 == 0:
            print(f"  [{i+1}/300] resolved: {resolved}, failed: {failed}", flush=True)

    has_date = sum(1 for d in docs[:300] if d.get('published_date', '').strip())
    print(f"\nFinal: {has_date}/300 with date", flush=True)
    print(f"Resolved: {resolved}, Failed: {failed}", flush=True)

    # Year distribution
    from collections import Counter
    years = []
    for d in docs[:300]:
        pd = d.get('published_date', '')
        if pd:
            m = re.search(r'(\d{4})', pd)
            if m:
                years.append(int(m.group(1)))
    print(f"\nYear distribution:")
    for y, c in sorted(Counter(years).items()):
        print(f"  {y}: {c}")

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Saved", flush=True)


if __name__ == '__main__':
    main()
