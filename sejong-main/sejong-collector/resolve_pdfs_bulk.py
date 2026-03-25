"""
기존 데이터 파일에서 2026 날짜 있지만 PDF 없는 문서들의 PDF URL 해결.
현재사용 X 사이트와 GAO, GB 제외.
"""
import json
import re
import sys
import io
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / 'data'

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})

# Excluded site codes (현재사용 X or GAO or GB)
EXCLUDE_CODES = {'Z00014'}  # GAO

def find_pdf_link(url):
    if not url or not url.startswith('http'):
        return ''
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 1. Direct .pdf links
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.lower().endswith('.pdf'):
                return urljoin(url, href)

        # 2. Links with download text + .pdf
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True).lower()
            if any(kw in text for kw in ['download pdf', 'pdf download', 'full report',
                                          'download report', 'read the report', 'view pdf',
                                          'download publication', 'full text', 'read report',
                                          'download', 'get pdf']):
                full = urljoin(url, href)
                if '.pdf' in full.lower():
                    return full

        # 3. Links with /pdf/ or /pdfs/ path
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/pdf/' in href.lower() or '/pdfs/' in href.lower():
                return urljoin(url, href)

        # 4. iframe/embed with PDF
        for tag in soup.find_all(['iframe', 'embed', 'object']):
            src = tag.get('src', '') or tag.get('data', '')
            if src and '.pdf' in src.lower():
                return urljoin(url, src)

        # 5. meta citation_pdf_url
        meta = soup.find('meta', attrs={'name': 'citation_pdf_url'})
        if meta and meta.get('content'):
            return meta['content']

        return ''
    except Exception:
        return ''


def main():
    # Load selection to check existing titles
    with open(DATA_DIR / 'selection_400.json', 'r', encoding='utf-8') as f:
        sel = json.load(f)
    sel_titles = set(d.get('title', '').lower().strip() for d in sel)

    # Scan all data files
    candidates = []
    for f in sorted(DATA_DIR.glob('*_Z00*_*.json')):
        # Skip GB files
        if f.name.startswith('govuk_'):
            continue

        with open(f, 'r', encoding='utf-8') as fh:
            try:
                data = json.load(fh)
            except:
                continue

        meta = data.get('metadata', {})
        code = meta.get('site_code', '')
        if code in EXCLUDE_CODES:
            continue

        for doc in data.get('documents', []):
            # Only 2026 docs without PDF
            if '2026' not in str(doc.get('published_date', '')):
                continue
            if doc.get('pdf_url', '').strip():
                continue
            # Skip if already in selection
            if doc.get('title', '').lower().strip() in sel_titles:
                continue

            candidates.append(doc)

    print(f"Candidates (2026, no PDF, not in selection): {len(candidates)}", flush=True)

    # Group by site
    from collections import Counter
    site_counts = Counter(d.get('site_name', '')[:40] for d in candidates)
    for name, cnt in site_counts.most_common():
        print(f"  {cnt:3d} | {name}", flush=True)

    # Resolve PDFs
    resolved = []
    failed = 0
    for i, doc in enumerate(candidates):
        url = doc.get('link', '')
        pdf = find_pdf_link(url)
        if pdf:
            doc['pdf_url'] = pdf
            resolved.append(doc)
            print(f"  [{i+1}/{len(candidates)}] OK: {doc.get('title','')[:45]}", flush=True)
            print(f"     -> {pdf[:70]}", flush=True)
        else:
            failed += 1

        time.sleep(0.5)

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(candidates)} | resolved: {len(resolved)}, failed: {failed}", flush=True)

    print(f"\nResolved: {len(resolved)}, Failed: {failed}", flush=True)

    # Add resolved docs to selection
    if resolved:
        for d in resolved:
            d.setdefault('_relevance_score', 0.3)
            d.setdefault('keywords', '')
            d.setdefault('authors', '')
            d.setdefault('description', '')
            # Set country
            code = d.get('site_code', '')
            country_map = {
                'US': ['Z00015','Z00027','Z00031','Z00035','Z00038','Z00044','Z00047','Z00048',
                       'Z00050','Z00051','Z00054','Z00057','Z00058','Z00060','Z00063','Z00065',
                       'Z00066','Z00067','Z00071','Z00076','Z00077','Z00078','Z00079','Z00080',
                       'Z00082','Z00083','Z00087','Z00089','Z00236','Z00243','Z00249','Z00260',
                       'Z00304','Z00307','Z00316','Z00318','Z00320','Z00323','Z00345','Z00347',
                       'Z00348','Z00350','Z00351','Z00352','Z00353','Z00412','Z00583'],
                'CA': ['Z00139','Z00140','Z00141','Z00142','Z00143','Z00144','Z00145','Z00147',
                       'Z00148','Z00149','Z00150','Z00151','Z00152','Z00153','Z00154','Z00155',
                       'Z00238','Z00239','Z00241','Z00242','Z00250','Z00252','Z00253','Z00258',
                       'Z00264','Z00313','Z00325','Z00394'],
                'NO': ['Z00001','Z00002','Z00003','Z00005','Z00287'],
                'AT': ['Z00156','Z00157','Z00158','Z00159','Z00160','Z00161','Z00162','Z00163',
                       'Z00164','Z00165','Z00166','Z00167','Z00168','Z00169','Z00171','Z00172',
                       'Z00173','Z00174','Z00175','Z00240','Z00244','Z00246','Z00248','Z00254',
                       'Z00255','Z00261','Z00263','Z00265','Z00283','Z00284','Z00285','Z00286',
                       'Z00289','Z00298','Z00302','Z00400'],
                'IN': ['Z00308','Z00385'],
                'IT': ['Z00321','Z00380'],
                'SA': ['Z00361'],
            }
            for country, codes in country_map.items():
                if code in codes:
                    d['_country'] = country.lower()
                    break
            else:
                d.setdefault('_country', 'us')

            d['_site_name'] = d.get('site_name', '')

        sel.extend(resolved)
        with open(DATA_DIR / 'selection_400.json', 'w', encoding='utf-8') as f:
            json.dump(sel, f, ensure_ascii=False, indent=2, default=str)
        print(f"Added {len(resolved)} docs to selection_400.json", flush=True)


if __name__ == '__main__':
    main()
