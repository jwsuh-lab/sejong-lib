"""
발행일 미확인 문서들의 날짜를 웹페이지 본문 텍스트에서 추출 (v2).
기존 resolve_dates.py의 메타태그 방식 + HTML 본문 텍스트 날짜 패턴 매칭.
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

# Date patterns to search in visible text
# Matches: "28 October 2025", "March 9, 2026", "2026-03-01", "January 2026", etc.
DATE_PATTERNS = [
    # "28 October 2025" or "28 Oct 2025"
    re.compile(r'\b(\d{1,2})\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})\b', re.IGNORECASE),
    # "October 28, 2025"
    re.compile(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2}),?\s+(\d{4})\b', re.IGNORECASE),
    # "January 2026"
    re.compile(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})\b', re.IGNORECASE),
    # ISO: "2026-01-15"
    re.compile(r'\b(\d{4})-(\d{2})-(\d{2})\b'),
]


def extract_date_from_text(text):
    """Extract date from visible page text, prefer most recent 2026+ date."""
    dates_found = []

    for pat in DATE_PATTERNS:
        for m in pat.finditer(text):
            full = m.group(0)
            year_match = re.search(r'(\d{4})', full)
            if year_match:
                year = int(year_match.group(1))
                if 2026 <= year <= 2030:
                    dates_found.append(full.strip())

    return dates_found[0] if dates_found else ''


def extract_date(url):
    """웹페이지에서 발행일 추출 (메타태그 + 본문 텍스트)"""
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        # === Phase 1: Meta tags (기존 로직) ===
        # 1. article:published_time
        for prop in ['article:published_time', 'article:published']:
            meta = soup.find('meta', attrs={'property': prop})
            if meta and meta.get('content'):
                content = meta['content'].strip()
                ym = re.search(r'(\d{4})', content)
                if ym and int(ym.group(1)) >= 2026:
                    return content

        # 2. datePublished (schema.org)
        meta = soup.find('meta', attrs={'itemprop': 'datePublished'})
        if meta and meta.get('content'):
            content = meta['content'].strip()
            ym = re.search(r'(\d{4})', content)
            if ym and int(ym.group(1)) >= 2026:
                return content

        # 3. DC.date and similar
        for name in ['DC.date', 'DC.date.issued', 'dcterms.issued',
                      'citation_publication_date', 'citation_date',
                      'date', 'publish_date', 'pubdate']:
            meta = soup.find('meta', attrs={'name': name})
            if meta and meta.get('content'):
                content = meta['content'].strip()
                ym = re.search(r'(\d{4})', content)
                if ym and int(ym.group(1)) >= 2026:
                    return content

        # 4. JSON-LD datePublished
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in items:
                    if isinstance(item, dict):
                        dp = item.get('datePublished', '')
                        if dp:
                            ym = re.search(r'(\d{4})', dp)
                            if ym and int(ym.group(1)) >= 2026:
                                return dp
            except (json.JSONDecodeError, TypeError):
                pass

        # 5. time tag with datetime
        time_tag = soup.find('time', attrs={'datetime': True})
        if time_tag:
            dt = time_tag['datetime'].strip()
            ym = re.search(r'(\d{4})', dt)
            if ym and int(ym.group(1)) >= 2026:
                return dt

        # === Phase 2: Visible text date extraction (신규) ===
        # Look in common date containers first
        date_selectors = [
            '.date', '.published', '.pub-date', '.post-date',
            '.article-date', '.entry-date', '.meta-date',
            '.publication-date', '.field-date', '.content-date',
            '[class*="date"]', '[class*="publish"]',
        ]
        for sel in date_selectors:
            elements = soup.select(sel)
            for el in elements:
                text = el.get_text(strip=True)
                date = extract_date_from_text(text)
                if date:
                    return date

        # Look near common header areas (first 30% of page text)
        body = soup.find('body')
        if body:
            # Remove script/style
            for tag in body.find_all(['script', 'style', 'nav', 'footer']):
                tag.decompose()

            full_text = body.get_text(' ', strip=True)
            # Check first portion of text (more likely to have pub date)
            first_portion = full_text[:3000]
            date = extract_date_from_text(first_portion)
            if date:
                return date

            # Check full text as fallback
            date = extract_date_from_text(full_text)
            if date:
                return date

        return ''
    except Exception as e:
        return ''


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    docs.sort(key=lambda x: -x.get('_relevance_score', 0))

    need_date = sum(1 for d in docs[:300] if not d.get('published_date', '').strip())
    print(f"Need date: {need_date}/300", flush=True)

    resolved = 0
    failed = 0
    pre2026 = 0

    for i, doc in enumerate(docs[:300]):
        if doc.get('published_date', '').strip():
            continue

        url = doc.get('link', '')
        date_str = extract_date(url)
        if date_str:
            year_match = re.search(r'(\d{4})', date_str)
            if year_match:
                year = int(year_match.group(1))
                if year >= 2026:
                    doc['published_date'] = date_str
                    resolved += 1
                    print(f"  OK [{doc.get('_country','')}] {year} | {doc.get('title','')[:55]}", flush=True)
                else:
                    pre2026 += 1
                    print(f"  PRE-2026 [{doc.get('_country','')}] {year} | {doc.get('title','')[:55]}", flush=True)
            else:
                failed += 1
        else:
            failed += 1

        time.sleep(0.3)

        if (resolved + failed + pre2026) % 15 == 0:
            print(f"  [{i+1}/300] resolved: {resolved}, pre2026: {pre2026}, failed: {failed}", flush=True)

    has_date = sum(1 for d in docs[:300] if d.get('published_date', '').strip())
    print(f"\nFinal: {has_date}/300 with date", flush=True)
    print(f"Resolved: {resolved}, Pre-2026: {pre2026}, Failed: {failed}", flush=True)

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Saved", flush=True)


if __name__ == '__main__':
    main()
