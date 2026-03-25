"""
선별된 330건에 대해서만 PDF URL + 메타데이터를 추출하는 타겟 스크립트.
기존 pdf_url_resolver.py, metadata_resolver.py의 로직을 재사용.
"""
import json
import sys
import io
import time
import logging
from datetime import datetime
from collections import defaultdict

from pdf_url_resolver import get_resolver, GenericHtmlPdfResolver
from metadata_resolver import SITE_SERIES_MAP

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

SELECTION_FILE = 'data/selection_330.json'
REQUEST_DELAY = 0.5


def resolve_pdf_for_doc(doc):
    """단일 문서의 PDF URL 추출"""
    country = doc.get('_country', '').upper()
    site_code = doc.get('site_code', '')

    # country mapping for resolver
    if country == 'GOVUK':
        country = 'UK'

    resolver = get_resolver(country, site_code)
    try:
        pdf_urls = resolver.resolve(doc)
        if pdf_urls:
            return pdf_urls[0], pdf_urls
    except Exception as e:
        logger.debug(f"PDF resolve failed: {e}")
    return '', []


def resolve_metadata_for_doc(doc):
    """단일 문서의 journal/volume/keywords 추출"""
    site_code = doc.get('site_code', '')

    # journal from site series map
    journal = ''
    series = SITE_SERIES_MAP.get(site_code, '')
    if isinstance(series, str):
        journal = series
    elif isinstance(series, dict):
        journal = series.get('_default', '')

    # keywords from existing data
    keywords = doc.get('keywords', '')

    return journal, keywords


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    total = len(docs)
    need_pdf = sum(1 for d in docs if not d.get('pdf_url'))
    print(f"Total: {total}, Need PDF: {need_pdf}")

    stats = defaultdict(int)
    resolved_count = 0
    failed_count = 0

    for i, doc in enumerate(docs):
        # PDF resolution
        if not doc.get('pdf_url'):
            pdf_url, pdf_urls = resolve_pdf_for_doc(doc)
            if pdf_url:
                doc['pdf_url'] = pdf_url
                doc['pdf_urls'] = pdf_urls
                doc['pdf_resolved_at'] = datetime.now().isoformat()
                resolved_count += 1
                stats[doc.get('_country', '?') + '_ok'] += 1
            else:
                doc['pdf_url'] = ''
                doc['pdf_urls'] = []
                failed_count += 1
                stats[doc.get('_country', '?') + '_fail'] += 1
            time.sleep(REQUEST_DELAY)

        # Metadata: journal (if missing)
        if not doc.get('journal'):
            journal, _ = resolve_metadata_for_doc(doc)
            if journal:
                doc['journal'] = journal

        # Progress
        if (i + 1) % 10 == 0 or (i + 1) == total:
            pct = (i + 1) / total * 100
            print(f"[{i+1}/{total}] {pct:.0f}% | PDF resolved: {resolved_count}, failed: {failed_count}", flush=True)

    # Final stats
    has_pdf = sum(1 for d in docs if d.get('pdf_url'))
    has_journal = sum(1 for d in docs if d.get('journal'))
    has_keywords = sum(1 for d in docs if d.get('keywords'))

    print(f"\n{'='*50}")
    print(f"FINAL STATS")
    print(f"{'='*50}")
    print(f"Total docs: {total}")
    print(f"Has PDF URL: {has_pdf}")
    print(f"No PDF URL: {total - has_pdf}")
    print(f"Has journal: {has_journal}")
    print(f"Has keywords: {has_keywords}")
    print(f"\nPDF resolution by country:")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")

    # Save updated selection
    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nSaved to {SELECTION_FILE}")


if __name__ == '__main__':
    main()
