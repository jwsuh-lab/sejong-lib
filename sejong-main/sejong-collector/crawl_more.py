"""Crawl additional sites: Fed IFDP, ILO more, NCES, CATO, IES, ABARES"""
import requests, time, re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
})

DATA_DIR = Path(__file__).parent / 'data'
all_new = []


def extract_pub_date(soup):
    """Extract publication date from a page.

    Checks these sources in order:
      1. meta name="citation_publication_date"
      2. meta property="article:published_time"
      3. meta name="DC.date.issued" / "DC.date" / "date"
      4. JSON-LD datePublished
      5. <time> tag with datetime attribute
      6. Visible text date patterns in first 3000 chars of body

    Returns the date string if a year >= 2026 is found, or '' otherwise.
    """
    year_pat = re.compile(r'(20[2-9]\d)')

    def _check_year(text):
        if not text:
            return ''
        m = year_pat.search(text)
        if m and int(m.group(1)) >= 2026:
            return text.strip()
        return ''

    # 1. citation_publication_date
    tag = soup.find('meta', attrs={'name': 'citation_publication_date'})
    if tag and tag.get('content'):
        result = _check_year(tag['content'])
        if result:
            return result

    # 2. article:published_time
    tag = soup.find('meta', attrs={'property': 'article:published_time'})
    if tag and tag.get('content'):
        result = _check_year(tag['content'])
        if result:
            return result

    # 3. DC.date.issued, DC.date, date
    for name in ('DC.date.issued', 'DC.date', 'date'):
        tag = soup.find('meta', attrs={'name': name})
        if tag and tag.get('content'):
            result = _check_year(tag['content'])
            if result:
                return result

    # 4. JSON-LD datePublished
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    dp = item.get('datePublished', '')
                    result = _check_year(str(dp))
                    if result:
                        return result
        except (json.JSONDecodeError, TypeError):
            pass

    # 5. <time> tag with datetime attribute
    for time_tag in soup.find_all('time', attrs={'datetime': True}):
        result = _check_year(time_tag['datetime'])
        if result:
            return result

    # 6. Visible text date patterns in first 3000 chars of body
    body = soup.find('body')
    if body:
        text = body.get_text(' ', strip=True)[:3000]
        m = year_pat.search(text)
        if m and int(m.group(1)) >= 2026:
            return m.group(1)

    return ''


def crawl_fed_ifdp():
    """Federal Reserve IFDP papers"""
    print("=== Fed IFDP ===")
    docs = []
    resp = session.get('https://www.federalreserve.gov/econres/ifdp/index.htm', timeout=15)
    if resp.status_code != 200:
        print(f"  Status: {resp.status_code}")
        return docs

    soup = BeautifulSoup(resp.text, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href']
        title = a.get_text(strip=True)
        if '/econres/ifdp/' in href and href.endswith('.htm') and 'index' not in href and len(title) > 15:
            link = urljoin('https://www.federalreserve.gov', href)
            try:
                resp2 = session.get(link, timeout=15)
                soup2 = BeautifulSoup(resp2.text, 'html.parser')
                pdfs = [urljoin(link, a2['href']) for a2 in soup2.find_all('a', href=True) if '.pdf' in a2['href'].lower()]
                if pdfs:
                    authors = ''
                    for meta in soup2.find_all('meta', attrs={'name': 'author'}):
                        if meta.get('content'):
                            authors = meta['content']
                            break
                    desc = ''
                    md = soup2.find('meta', attrs={'name': 'description'})
                    if md and md.get('content'):
                        desc = md['content']

                    pub_date = extract_pub_date(soup2)
                    if pub_date:
                        docs.append({
                            'title': title, 'link': link, 'pdf_url': pdfs[0],
                            'published_date': pub_date, 'site_code': 'NEW_FED_IFDP',
                            'site_name': 'Federal Reserve Board - IFDP',
                            'document_type': 'working paper',
                            'authors': authors, 'description': desc, 'keywords': '',
                        })
                        print(f"  OK: {title[:50]}")
                    else:
                        print(f"  SKIP (no 2026+ date): {title[:50]}")
                time.sleep(0.8)
            except Exception as e:
                print(f"  ERR: {e}")

    print(f"  Total: {len(docs)}")
    return docs


def crawl_ilo_more():
    """ILO - crawl more pages"""
    print("=== ILO (more) ===")
    docs = []
    seen = set()

    for page in range(1, 4):
        url = f'https://www.ilo.org/publications?page={page}'
        try:
            resp = session.get(url, timeout=20)
        except:
            continue
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text(strip=True)
            if '/publications/' in href and len(title) > 15 and title not in seen:
                seen.add(title)
                link = urljoin('https://www.ilo.org', href)
                try:
                    resp2 = session.get(link, timeout=15)
                    soup2 = BeautifulSoup(resp2.text, 'html.parser')
                    pub_date = extract_pub_date(soup2)
                    if not pub_date:
                        continue
                    pdfs = [urljoin(link, a2['href']) for a2 in soup2.find_all('a', href=True) if '.pdf' in a2['href'].lower()]
                    if pdfs:
                        docs.append({
                            'title': title, 'link': link, 'pdf_url': pdfs[0],
                            'published_date': pub_date, 'authors': 'ILO',
                            'description': '', 'site_code': 'NEW_ILO',
                            'site_name': 'International Labour Organization',
                            'document_type': 'research report', 'keywords': '',
                        })
                        print(f"  OK: {title[:50]}")
                    time.sleep(1)
                except:
                    pass
                if len(docs) >= 20:
                    break
        time.sleep(1)

    print(f"  Total: {len(docs)}")
    return docs


def crawl_nces():
    """NCES publications"""
    print("=== NCES ===")
    docs = []
    try:
        resp = session.get('https://nces.ed.gov/pubsearch/index.asp', timeout=15)
        if resp.status_code != 200:
            print(f"  Status: {resp.status_code}")
            return docs

        # Try search with year
        resp2 = session.get(
            'https://nces.ed.gov/pubsearch/getpubcats.asp?sid=011',
            timeout=15
        )
        soup = BeautifulSoup(resp2.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text(strip=True)
            if 'pubsinfo' in href and len(title) > 10:
                link = urljoin('https://nces.ed.gov', href)
                try:
                    resp3 = session.get(link, timeout=15)
                    soup3 = BeautifulSoup(resp3.text, 'html.parser')
                    pub_date = extract_pub_date(soup3)
                    pdfs = [urljoin(link, a2['href']) for a2 in soup3.find_all('a', href=True) if '.pdf' in a2['href'].lower()]
                    if pub_date and pdfs:
                        docs.append({
                            'title': title, 'link': link, 'pdf_url': pdfs[0],
                            'published_date': pub_date, 'site_code': 'Z00015',
                            'site_name': 'National Center for Education Statistics',
                            'document_type': 'research report',
                            'authors': '', 'description': '', 'keywords': '',
                        })
                        print(f"  OK: {title[:50]}")
                    time.sleep(0.8)
                except:
                    pass
                if len(docs) >= 15:
                    break
    except Exception as e:
        print(f"  ERR: {e}")

    print(f"  Total: {len(docs)}")
    return docs


def crawl_abares():
    """ABARES (Australia)"""
    print("=== ABARES ===")
    docs = []
    try:
        resp = session.get('https://www.agriculture.gov.au/abares/research-topics/trade/australian-crop-report', timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            pdfs = [urljoin(resp.url, a['href']) for a in soup.find_all('a', href=True) if '.pdf' in a['href'].lower()]
            pub_date = extract_pub_date(soup)
            if pdfs and pub_date:
                docs.append({
                    'title': 'Australian Crop Report', 'link': resp.url,
                    'pdf_url': pdfs[0], 'published_date': pub_date,
                    'site_code': 'Z00240', 'site_name': 'ABARES',
                    'document_type': 'research report',
                    'authors': 'ABARES', 'description': '', 'keywords': '',
                })
                print(f"  OK: Australian Crop Report")

        # Try more ABARES pages
        for slug in ['agricultural-outlook', 'demand-for-rural-water']:
            url = f'https://www.agriculture.gov.au/abares/research-topics/{slug}'
            try:
                resp2 = session.get(url, timeout=15)
                soup2 = BeautifulSoup(resp2.text, 'html.parser')
                pdfs = [urljoin(url, a['href']) for a in soup2.find_all('a', href=True) if '.pdf' in a['href'].lower()]
                pub_date2 = extract_pub_date(soup2)
                if pdfs and pub_date2:
                    docs.append({
                        'title': slug.replace('-', ' ').title(), 'link': url,
                        'pdf_url': pdfs[0], 'published_date': pub_date2,
                        'site_code': 'Z00240', 'site_name': 'ABARES',
                        'document_type': 'research report',
                        'authors': 'ABARES', 'description': '', 'keywords': '',
                    })
                    print(f"  OK: {slug}")
                time.sleep(0.8)
            except:
                pass
    except Exception as e:
        print(f"  ERR: {e}")

    print(f"  Total: {len(docs)}")
    return docs


def crawl_fed_sr():
    """Federal Reserve Staff Reports (FRBNY)"""
    print("=== FRBNY Staff Reports ===")
    docs = []
    try:
        resp = session.get('https://www.newyorkfed.org/research/staff_reports/index', timeout=15)
        if resp.status_code != 200:
            print(f"  Status: {resp.status_code}")
            return docs

        soup = BeautifulSoup(resp.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            title = a.get_text(strip=True)
            if '/staff_reports/sr' in href and len(title) > 15:
                link = urljoin('https://www.newyorkfed.org', href)
                # FRBNY SR PDF pattern: /medialibrary/media/research/staff_reports/sr{num}.pdf
                m = re.search(r'sr(\d+)', href)
                if m:
                    num = m.group(1)
                    pdf = f'https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr{num}.pdf'

                    # Fetch the detail page to get the actual publication date
                    try:
                        resp_detail = session.get(link, timeout=15)
                        soup_detail = BeautifulSoup(resp_detail.text, 'html.parser')
                        pub_date = extract_pub_date(soup_detail)
                    except Exception:
                        pub_date = ''

                    if pub_date:
                        docs.append({
                            'title': title, 'link': link, 'pdf_url': pdf,
                            'published_date': pub_date,
                            'site_code': 'NEW_FRBNY',
                            'site_name': 'Federal Reserve Bank of New York',
                            'document_type': 'staff report',
                            'authors': '', 'description': '', 'keywords': '',
                        })
                        print(f"  OK: {title[:50]}")
                    else:
                        print(f"  SKIP (no 2026+ date): {title[:50]}")
                    time.sleep(0.5)

                if len(docs) >= 20:
                    break
    except Exception as e:
        print(f"  ERR: {e}")

    print(f"  Total: {len(docs)}")
    return docs


def crawl_bis_wp():
    """BIS Working Papers via proper URL"""
    print("=== BIS Working Papers ===")
    docs = []
    try:
        resp = session.get('https://www.bis.org/list/wppubls/index.htm', timeout=15)
        if resp.status_code != 200:
            # Try alternate
            resp = session.get('https://www.bis.org/doclist/wppubls.rss', timeout=15)
        if resp.status_code != 200:
            print(f"  Status: {resp.status_code}")
            return docs

        soup = BeautifulSoup(resp.text, 'html.parser')
        for item in soup.find_all(['item', 'entry']):
            title_el = item.find('title')
            link_el = item.find('link')
            if title_el and link_el:
                title = title_el.get_text(strip=True)
                link = link_el.get_text(strip=True) or link_el.get('href', '')
                if len(title) > 10:
                    # Check pubDate from RSS item
                    pub_date = ''
                    pub_date_el = item.find('pubdate') or item.find('pubDate')
                    if pub_date_el:
                        pd_text = pub_date_el.get_text(strip=True)
                        ym = re.search(r'(20[2-9]\d)', pd_text)
                        if ym and int(ym.group(1)) >= 2026:
                            pub_date = pd_text
                    # Fallback: check <updated> or <dc:date> for Atom feeds
                    if not pub_date:
                        for tag_name in ['updated', 'dc:date', 'date']:
                            date_el = item.find(tag_name)
                            if date_el:
                                dt_text = date_el.get_text(strip=True)
                                ym = re.search(r'(20[2-9]\d)', dt_text)
                                if ym and int(ym.group(1)) >= 2026:
                                    pub_date = dt_text
                                    break

                    if pub_date:
                        pdf = link.replace('.htm', '.pdf')
                        docs.append({
                            'title': title, 'link': link, 'pdf_url': pdf,
                            'published_date': pub_date,
                            'site_code': 'NEW_BIS',
                            'site_name': 'Bank for International Settlements',
                            'document_type': 'working paper',
                            'authors': '', 'description': '', 'keywords': '',
                        })
                        print(f"  OK: {title[:50]}")
                    else:
                        print(f"  SKIP (no 2026+ date): {title[:50]}")
                    if len(docs) >= 15:
                        break
    except Exception as e:
        print(f"  ERR: {e}")

    print(f"  Total: {len(docs)}")
    return docs


def main():
    all_docs = []

    for name, func in [
        ('Fed IFDP', crawl_fed_ifdp),
        ('ILO', crawl_ilo_more),
        ('NCES', crawl_nces),
        ('ABARES', crawl_abares),
        ('FRBNY', crawl_fed_sr),
        ('BIS', crawl_bis_wp),
    ]:
        docs = func()
        all_docs.extend(docs)

    print(f"\n=== TOTAL: {len(all_docs)} ===")

    with open(DATA_DIR / 'additional_sources3_20260314.json', 'w', encoding='utf-8') as f:
        json.dump({'documents': all_docs}, f, ensure_ascii=False, indent=2)
    print("Saved")


if __name__ == '__main__':
    main()
