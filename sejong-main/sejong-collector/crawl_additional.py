"""
Additional crawlers for Federal Reserve FEDS, ECB, Amnesty, HRW, WHO.
Goal: get 80+ more docs to replace EPRS dominance.
"""
import json
import re
import sys
import io
import time
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / 'data'


def extract_pub_date(soup):
    """Extract publication date from a BeautifulSoup page, checking multiple sources.

    Returns the date string if found, or '' if not.
    Checks in order:
      1. meta name="citation_publication_date"
      2. meta property="article:published_time"
      3. meta name="DC.date.issued" / "DC.date" / "date"
      4. JSON-LD datePublished
      5. <time> tag with datetime attribute
      6. Visible text date pattern in first 3000 chars (Month D, YYYY or YYYY-MM-DD)
    """
    # 1. citation_publication_date
    meta = soup.find('meta', attrs={'name': 'citation_publication_date'})
    if meta and meta.get('content', '').strip():
        return meta['content'].strip()

    # 2. article:published_time
    meta = soup.find('meta', attrs={'property': 'article:published_time'})
    if meta and meta.get('content', '').strip():
        return meta['content'].strip()

    # 3. DC.date.issued / DC.date / date
    for name in ('DC.date.issued', 'DC.date', 'date'):
        meta = soup.find('meta', attrs={'name': name})
        if meta and meta.get('content', '').strip():
            return meta['content'].strip()

    # 4. JSON-LD datePublished
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            if isinstance(data, dict):
                dp = data.get('datePublished', '')
                if dp:
                    return str(dp).strip()
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        dp = item.get('datePublished', '')
                        if dp:
                            return str(dp).strip()
        except (json.JSONDecodeError, TypeError):
            pass

    # 5. <time> tag with datetime attribute
    time_tag = soup.find('time', attrs={'datetime': True})
    if time_tag and time_tag['datetime'].strip():
        return time_tag['datetime'].strip()

    # 6. Visible text date pattern in first 3000 chars
    text = soup.get_text(' ', strip=True)[:3000]
    # YYYY-MM-DD
    m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    if m:
        return m.group(1)
    # Month D, YYYY  (e.g. January 15, 2026)
    m = re.search(
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        text
    )
    if m:
        return m.group(1)

    return ''


def extract_year_from_date(date_str):
    """Extract a 4-digit year from a date string. Returns int or None."""
    if not date_str:
        return None
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else None


def extract_year_from_url(url):
    """Try to extract a 4-digit year from the URL path (e.g. /2026/)."""
    m = re.search(r'/(\d{4})/', url)
    return int(m.group(1)) if m else None

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})


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
                logger.warning(f"  Failed: {url[:60]} - {e}")
            else:
                time.sleep(2 ** attempt)
    return None


def crawl_fed_feds():
    """Federal Reserve FEDS Working Papers"""
    logger.info("=== Federal Reserve FEDS Papers ===")
    base = 'https://www.federalreserve.gov'
    docs = []

    resp = safe_get(f'{base}/econres/feds/index.htm')
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')
    paper_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/econres/feds/' in href and href.endswith('.htm') and 'index' not in href and 'all-years' not in href:
            title = a.get_text(strip=True)
            if len(title) > 15:
                paper_links.append({'title': title, 'link': urljoin(base, href)})

    logger.info(f"  Found {len(paper_links)} paper links")

    for p in paper_links:
        try:
            resp2 = safe_get(p['link'], timeout=15)
            if not resp2:
                continue
            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            pdf = ''
            for a in soup2.find_all('a', href=True):
                if '.pdf' in a['href'].lower():
                    pdf = urljoin(p['link'], a['href'])
                    break

            if not pdf:
                continue

            authors = ''
            for meta in soup2.find_all('meta', attrs={'name': 'author'}):
                if meta.get('content'):
                    authors = meta['content']
                    break

            desc = ''
            meta_desc = soup2.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc['content']

            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(p['link'])
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {p['title'][:50]}")
                continue

            docs.append({
                'title': p['title'],
                'link': p['link'],
                'pdf_url': pdf,
                'published_date': pub_date,
                'authors': authors,
                'description': desc,
                'site_code': 'NEW_FED_FEDS',
                'site_name': 'Federal Reserve Board - FEDS',
                'document_type': 'working paper',
            })
            logger.info(f"  OK: {p['title'][:50]}")
            time.sleep(0.8)
        except Exception as e:
            logger.warning(f"  Error: {e}")

    return docs


def crawl_fed_notes():
    """Federal Reserve FEDS Notes"""
    logger.info("=== Federal Reserve FEDS Notes ===")
    base = 'https://www.federalreserve.gov'
    docs = []

    resp = safe_get(f'{base}/econres/notes/feds-notes/default.htm')
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')
    paper_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/econres/notes/feds-notes/' in href and href.endswith('.htm') and 'default' not in href:
            title = a.get_text(strip=True)
            if len(title) > 15:
                paper_links.append({'title': title, 'link': urljoin(base, href)})

    logger.info(f"  Found {len(paper_links)} note links")

    for p in paper_links:
        try:
            resp2 = safe_get(p['link'], timeout=15)
            if not resp2:
                continue
            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            pdf = ''
            for a in soup2.find_all('a', href=True):
                if '.pdf' in a['href'].lower():
                    pdf = urljoin(p['link'], a['href'])
                    break

            if not pdf:
                continue

            authors = ''
            for meta in soup2.find_all('meta', attrs={'name': 'author'}):
                if meta.get('content'):
                    authors = meta['content']
                    break

            desc = ''
            meta_desc = soup2.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc['content']

            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(p['link'])
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {p['title'][:50]}")
                continue

            docs.append({
                'title': p['title'],
                'link': p['link'],
                'pdf_url': pdf,
                'published_date': pub_date,
                'authors': authors,
                'description': desc,
                'site_code': 'NEW_FED_NOTES',
                'site_name': 'Federal Reserve Board - FEDS Notes',
                'document_type': 'policy note',
            })
            logger.info(f"  OK: {p['title'][:50]}")
            time.sleep(0.8)
        except Exception as e:
            logger.warning(f"  Error: {e}")

    return docs


def crawl_ecb():
    """ECB Working Papers 2026"""
    logger.info("=== ECB Working Papers ===")
    docs = []

    resp = safe_get('https://www.ecb.europa.eu/pub/research/working-papers/html/papers-2026.en.html')
    if not resp:
        resp = safe_get('https://www.ecb.europa.eu/pub/research/working-papers/html/index.en.html')
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Find paper entries
    for a in soup.find_all('a', href=True):
        href = a['href']
        title = a.get_text(strip=True)

        # Match WP links
        if 'wp' in href and ('.en.html' in href or '.pdf' in href) and len(title) > 15:
            m = re.search(r'wp(\d+)', href)
            if m:
                num = m.group(1)
                pdf = f'https://www.ecb.europa.eu/pub/pdf/scpwps/ecb.wp{num}.en.pdf'
                link = f'https://www.ecb.europa.eu/pub/pdf/scpwps/ecb.wp{num}.en.html'

                # Try to extract date from the detail page
                pub_date = ''
                try:
                    resp2 = safe_get(link, timeout=15)
                    if resp2:
                        soup2 = BeautifulSoup(resp2.text, 'html.parser')
                        pub_date = extract_pub_date(soup2)
                        time.sleep(0.5)
                except Exception:
                    pass
                if not pub_date:
                    # Page is from 2026 list, so fallback
                    pub_date = '2026'
                year = extract_year_from_date(pub_date)
                if not year or year < 2026:
                    logger.info(f"  Skipped (date={pub_date}): {title[:50]}")
                    continue

                docs.append({
                    'title': title,
                    'link': link,
                    'pdf_url': pdf,
                    'published_date': pub_date,
                    'authors': '',
                    'description': '',
                    'site_code': 'NEW_ECB',
                    'site_name': 'European Central Bank',
                    'document_type': 'working paper',
                })
                logger.info(f"  OK: {title[:50]}")

    # Deduplicate
    seen = set()
    unique = []
    for d in docs:
        if d['pdf_url'] not in seen:
            seen.add(d['pdf_url'])
            unique.append(d)

    return unique


def crawl_amnesty():
    """Amnesty International reports"""
    logger.info("=== Amnesty International ===")
    docs = []

    for page in range(1, 5):
        resp = safe_get(f'https://www.amnesty.org/en/latest/research/?page={page}', timeout=20)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')

        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text(strip=True)

            if '/document/' not in href or len(title) < 10:
                continue

            link = urljoin('https://www.amnesty.org', href)

            # Check detail page
            resp2 = safe_get(link, timeout=15)
            if not resp2:
                continue

            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            # Date extraction
            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(link)
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {title[:50]}")
                continue

            # PDF
            pdf = ''
            for a2 in soup2.find_all('a', href=True):
                h = a2['href']
                if '.pdf' in h.lower():
                    pdf = urljoin(link, h)
                    break

            if not pdf:
                continue

            docs.append({
                'title': title,
                'link': link,
                'pdf_url': pdf,
                'published_date': pub_date,
                'authors': 'Amnesty International',
                'description': '',
                'site_code': 'NEW_AMNESTY',
                'site_name': 'Amnesty International',
                'document_type': 'research report',
            })
            logger.info(f"  OK: {title[:50]}")
            time.sleep(1)

        time.sleep(1)

    # Deduplicate
    seen = set()
    unique = []
    for d in docs:
        if d['title'] not in seen:
            seen.add(d['title'])
            unique.append(d)

    return unique


def crawl_hrw():
    """Human Rights Watch reports"""
    logger.info("=== Human Rights Watch ===")
    docs = []

    resp = safe_get('https://www.hrw.org/publications?keyword=&date%5Bvalue%5D%5Byear%5D=2026', timeout=20)
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')
    seen = set()

    for a in soup.find_all('a', href=True):
        href = a['href']
        title = a.get_text(strip=True)
        if ('/report/' in href or '/news/' in href) and len(title) > 20:
            link = urljoin('https://www.hrw.org', href)
            if link in seen:
                continue
            seen.add(link)

            resp2 = safe_get(link, timeout=15)
            if not resp2:
                continue

            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(link)
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {title[:50]}")
                time.sleep(1)
                continue

            pdf = ''
            for a2 in soup2.find_all('a', href=True):
                if '.pdf' in a2['href'].lower():
                    pdf = urljoin(link, a2['href'])
                    break

            if pdf:
                docs.append({
                    'title': title,
                    'link': link,
                    'pdf_url': pdf,
                    'published_date': pub_date,
                    'authors': 'Human Rights Watch',
                    'description': '',
                    'site_code': 'NEW_HRW',
                    'site_name': 'Human Rights Watch',
                    'document_type': 'research report',
                })
                logger.info(f"  OK: {title[:50]}")
            time.sleep(1)

            if len(docs) >= 20:
                break

    return docs


def crawl_who():
    """WHO recent publications"""
    logger.info("=== WHO Publications ===")
    docs = []

    resp = safe_get('https://www.who.int/publications/i?order=publicationDate&page=1', timeout=20)
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')

    for a in soup.find_all('a', href=True):
        href = a['href']
        title = a.get_text(strip=True)

        if '/publications/i/' in href and len(title) > 15:
            link = urljoin('https://www.who.int', href)

            resp2 = safe_get(link, timeout=15)
            if not resp2:
                continue

            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(link)
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {title[:50]}")
                continue

            pdf = ''
            for a2 in soup2.find_all('a', href=True):
                h = a2['href']
                if '.pdf' in h.lower():
                    pdf = urljoin(link, h)
                    break

            if not pdf:
                continue

            docs.append({
                'title': title,
                'link': link,
                'pdf_url': pdf,
                'published_date': pub_date,
                'authors': 'WHO',
                'description': '',
                'site_code': 'NEW_WHO',
                'site_name': 'World Health Organization',
                'document_type': 'research report',
            })
            logger.info(f"  OK: {title[:50]}")
            time.sleep(1)

            if len(docs) >= 25:
                break

    return docs


def crawl_undp():
    """UNDP publications"""
    logger.info("=== UNDP Publications ===")
    docs = []

    resp = safe_get('https://www.undp.org/publications', timeout=20)
    if not resp:
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')

    for a in soup.find_all('a', href=True):
        href = a['href']
        title = a.get_text(strip=True)

        if '/publications/' in href and len(title) > 15 and href.count('/') >= 3:
            link = urljoin('https://www.undp.org', href)

            resp2 = safe_get(link, timeout=15)
            if not resp2:
                continue

            soup2 = BeautifulSoup(resp2.text, 'html.parser')

            pub_date = extract_pub_date(soup2)
            if not pub_date:
                url_year = extract_year_from_url(link)
                if url_year and url_year >= 2026:
                    pub_date = str(url_year)
            year = extract_year_from_date(pub_date)
            if not year or year < 2026:
                logger.info(f"  Skipped (date={pub_date}): {title[:50]}")
                continue

            pdf = ''
            for a2 in soup2.find_all('a', href=True):
                h = a2['href']
                if '.pdf' in h.lower():
                    pdf = urljoin(link, h)
                    break

            if not pdf:
                continue

            docs.append({
                'title': title,
                'link': link,
                'pdf_url': pdf,
                'published_date': pub_date,
                'authors': 'UNDP',
                'description': '',
                'site_code': 'NEW_UNDP',
                'site_name': 'United Nations Development Programme',
                'document_type': 'research report',
            })
            logger.info(f"  OK: {title[:50]}")
            time.sleep(1)

            if len(docs) >= 20:
                break

    return docs


def main():
    all_docs = []

    for name, func in [
        ('FEDS', crawl_fed_feds),
        ('FEDS Notes', crawl_fed_notes),
        ('ECB', crawl_ecb),
        ('Amnesty', crawl_amnesty),
        ('HRW', crawl_hrw),
        ('WHO', crawl_who),
        ('UNDP', crawl_undp),
    ]:
        docs = func()
        all_docs.extend(docs)
        logger.info(f"  {name} total: {len(docs)}")

    logger.info(f"\n=== TOTAL: {len(all_docs)} documents ===")

    timestamp = datetime.now().strftime('%Y%m%d')
    filepath = DATA_DIR / f"additional_sources_{timestamp}.json"
    output = {
        'metadata': {
            'crawled_at': datetime.now().isoformat(),
            'total_collected': len(all_docs),
        },
        'documents': all_docs,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved: {filepath.name}")


if __name__ == '__main__':
    main()
