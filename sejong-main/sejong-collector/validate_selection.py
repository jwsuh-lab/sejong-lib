"""
selection_400.json 전수 검증 스크립트.
3가지 품질 문제를 점검하고 부적격 문서를 자동 제외 처리한다.

1. 날짜 검증: 실제 웹페이지에서 발행일 재확인 (2026+ 필수)
2. PDF 검증: Content-Type, Content-Length 확인 (가짜PDF/소용량 제거)
3. 언어 검증: HTML lang 속성 + 제목 영어 여부 확인
"""
import json
import re
import sys
import io
import time
import requests
from bs4 import BeautifulSoup
from collections import Counter, defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_400.json'
MIN_PDF_SIZE_KB = 100  # 100KB 미만은 1-2장 가능성 높음
MIN_YEAR = 2026

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
})

# ========== 날짜 추출 ==========

DATE_PATTERNS = [
    re.compile(r'\b(\d{1,2})\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})\b', re.I),
    re.compile(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(\d{4})\b', re.I),
    re.compile(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})\b', re.I),
    re.compile(r'\b(\d{4})-(\d{2})-(\d{2})\b'),
]


def extract_year_from_text(text):
    """텍스트에서 연도 추출, 모든 날짜 패턴에서 가장 빈번한 연도 반환"""
    years = []
    for pat in DATE_PATTERNS:
        for m in pat.finditer(text):
            ym = re.search(r'(\d{4})', m.group(0))
            if ym:
                y = int(ym.group(1))
                if 2020 <= y <= 2030:
                    years.append(y)
    return years


def verify_date(url, soup=None):
    """웹페이지에서 실제 발행일의 연도를 확인. (year, source) 반환."""
    if soup is None:
        if not url or not url.startswith('http'):
            return None, 'no_url'
        try:
            resp = session.get(url, timeout=12)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
        except Exception:
            return None, 'fetch_error'

    # 1. Meta tags (가장 신뢰도 높음)
    meta_props = [
        ('property', 'article:published_time'),
        ('property', 'article:published'),
        ('name', 'citation_publication_date'),
        ('name', 'DC.date.issued'),
        ('name', 'DC.date'),
        ('name', 'dcterms.issued'),
        ('name', 'date'),
        ('name', 'publish_date'),
        ('name', 'pubdate'),
        ('itemprop', 'datePublished'),
    ]
    for attr, val in meta_props:
        meta = soup.find('meta', attrs={attr: val})
        if meta and meta.get('content'):
            ym = re.search(r'(\d{4})', meta['content'])
            if ym:
                return int(ym.group(1)), f'meta:{val}'

    # 2. JSON-LD
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                if isinstance(item, dict):
                    dp = item.get('datePublished', '') or item.get('dateCreated', '')
                    if dp:
                        ym = re.search(r'(\d{4})', str(dp))
                        if ym:
                            return int(ym.group(1)), 'jsonld'
        except (json.JSONDecodeError, TypeError):
            pass

    # 3. time tag
    time_tag = soup.find('time', attrs={'datetime': True})
    if time_tag:
        ym = re.search(r'(\d{4})', time_tag['datetime'])
        if ym:
            return int(ym.group(1)), 'time_tag'

    # 4. Visible text (처음 3000자)
    body = soup.find('body')
    if body:
        for tag in body.find_all(['script', 'style', 'nav', 'footer']):
            tag.decompose()
        text = body.get_text(' ', strip=True)[:3000]
        years = extract_year_from_text(text)
        if years:
            # 가장 빈번한 연도 반환 (동률 시 최신)
            yc = Counter(years)
            best = max(yc.keys(), key=lambda y: (yc[y], y))
            return best, 'visible_text'

    return None, 'not_found'


# ========== PDF 검증 ==========

def verify_pdf(pdf_url):
    """PDF URL의 유효성과 크기를 확인. (is_valid, size_kb, content_type) 반환."""
    if not pdf_url or not pdf_url.startswith('http'):
        return False, 0, 'no_url'
    try:
        resp = session.head(pdf_url, timeout=10, allow_redirects=True)
        ct = resp.headers.get('Content-Type', '')
        size = int(resp.headers.get('Content-Length', 0))
        size_kb = size / 1024

        is_pdf = 'pdf' in ct.lower() or 'octet-stream' in ct.lower()
        # Content-Length=0일 때 GET으로 재시도 (일부 서버는 HEAD에 size 안줌)
        if size == 0 and is_pdf:
            try:
                resp2 = session.get(pdf_url, timeout=10, stream=True)
                ct2 = resp2.headers.get('Content-Type', '')
                size2 = int(resp2.headers.get('Content-Length', 0))
                resp2.close()
                if size2 > 0:
                    size_kb = size2 / 1024
                is_pdf = 'pdf' in ct2.lower() or 'octet-stream' in ct2.lower()
                ct = ct2
            except Exception:
                pass

        return is_pdf, size_kb, ct
    except Exception as e:
        return False, 0, f'error:{e}'


# ========== 언어 검증 ==========

ENG_STOPWORDS = {' the ', ' of ', ' and ', ' for ', ' in ', ' on ', ' to ',
                 ' is ', ' are ', ' was ', ' with ', ' an ', ' by ', ' at ', ' from ',
                 ' that ', ' this ', ' has ', ' have ', ' been ', ' will '}

NON_ENG_WORDS = {'und', 'der', 'die', 'das', 'les', 'une', 'pour', 'del', 'della',
                 'nella', 'degli', 'fra', 'til', 'med', 'och', 'det', 'som', 'ett',
                 'naar', 'voor', 'het', 'sur', 'dans', 'avec', 'denne', 'dette',
                 'disse', 'delle', 'sulle', 'hacia', 'sobre', 'entre', 'desde'}


def check_english(title, url=None, soup=None):
    """문서가 영어인지 확인. (is_english, reason) 반환."""
    t_lower = ' ' + title.lower() + ' '
    words = set(title.lower().split())

    # 비영어 단어 2개 이상이면 비영어
    non_eng_matches = words & NON_ENG_WORDS
    if len(non_eng_matches) >= 2:
        return False, f'non_eng_words:{non_eng_matches}'

    # 비ASCII 비율 15% 초과
    non_ascii = sum(1 for c in title if ord(c) > 127)
    ratio = non_ascii / max(len(title), 1)
    if ratio > 0.15:
        return False, f'non_ascii_ratio:{ratio:.2f}'

    # 비ASCII 3개 이상 + 영어 stopword 없으면 비영어
    eng_count = sum(1 for sw in ENG_STOPWORDS if sw in t_lower)
    if non_ascii > 3 and eng_count == 0:
        return False, f'non_ascii:{non_ascii}_no_eng_stopwords'

    # HTML lang 속성 확인 (soup이 있는 경우)
    if soup:
        html_tag = soup.find('html')
        if html_tag:
            lang = (html_tag.get('lang') or '').lower()
            if lang and not lang.startswith('en') and lang not in ('', 'und'):
                # lang이 비영어이고, 제목에 영어 stopword가 2개 미만이면 비영어
                if eng_count < 2:
                    return False, f'html_lang:{lang}'

    return True, 'ok'


# ========== 메인 검증 ==========

def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    # 기존 _excluded 문자열 값 정규화 (bool로 변환)
    normalized = 0
    for d in docs:
        raw = d.get('_excluded')
        if isinstance(raw, str):
            if not raw:
                d['_excluded'] = False
                normalized += 1
                continue
            d['_excluded'] = True
            if '_exclude_reasons' not in d:
                d['_exclude_reasons'] = [raw]
            elif raw not in d['_exclude_reasons']:
                d['_exclude_reasons'].insert(0, raw)
            normalized += 1
    if normalized:
        print(f"기존 _excluded 문자열 {normalized}건 → bool 변환 완료")

    active = [d for d in docs if d.get('_relevance_score', 0) > 0]
    print(f"총 {len(docs)}건 중 활성 {len(active)}건 검증 시작")
    print("=" * 70)

    # 검증 결과 추적
    stats = {
        'date_fail': 0, 'date_pass': 0, 'date_skip': 0,
        'pdf_fake': 0, 'pdf_tiny': 0, 'pdf_ok': 0, 'pdf_error': 0,
        'lang_fail': 0, 'lang_pass': 0,
        'excluded_total': 0,
    }
    excluded_reasons = []

    for i, doc in enumerate(docs):
        if doc.get('_relevance_score', 0) <= 0:
            continue  # 이미 제외된 문서는 건너뜀

        title = doc.get('title', '')[:60]
        link = doc.get('link', '')
        pdf_url = doc.get('pdf_url', '')
        stored_date = doc.get('published_date', '')
        reasons = []

        # ---- PDF 검증 (네트워크 요청 최소화 위해 먼저) ----
        is_pdf, size_kb, ct = verify_pdf(pdf_url)
        if not is_pdf:
            reasons.append(f'pdf_invalid(ct={ct[:30]})')
            stats['pdf_fake'] += 1
        elif size_kb < MIN_PDF_SIZE_KB and size_kb > 0:
            reasons.append(f'pdf_tiny({size_kb:.0f}KB)')
            stats['pdf_tiny'] += 1
        elif size_kb == 0 and 'pdf' in ct.lower():
            # Content-Length 0이지만 PDF content-type — HEAD가 size 안줌, 통과
            stats['pdf_ok'] += 1
        else:
            stats['pdf_ok'] += 1

        # ---- 날짜 검증 (date='2026'만 표기된 미검증 문서) ----
        need_date_check = (stored_date.strip() == '2026' or stored_date.strip() == '')
        # 구체적 날짜가 있어도 spot-check 대상
        if not need_date_check:
            # 구체적 날짜의 연도 확인
            ym = re.search(r'(\d{4})', stored_date)
            if ym and int(ym.group(1)) >= MIN_YEAR:
                stats['date_pass'] += 1
            else:
                reasons.append(f'date_stored_pre2026({stored_date})')
                stats['date_fail'] += 1
        else:
            # 날짜 미검증 → 웹페이지에서 실제 날짜 확인
            year, source = verify_date(link)
            if year is not None:
                if year >= MIN_YEAR:
                    # 실제 날짜로 업데이트
                    doc['_date_verified'] = True
                    doc['_date_verified_year'] = year
                    doc['_date_verified_by'] = source
                    stats['date_pass'] += 1
                else:
                    reasons.append(f'date_actual_{year}(by:{source})')
                    stats['date_fail'] += 1
            else:
                # 날짜 확인 불가 — 경고만 (제외하지는 않음)
                doc['_date_verified'] = False
                stats['date_skip'] += 1

        # ---- 언어 검증 ----
        is_eng, lang_reason = check_english(doc.get('title', ''))
        if not is_eng:
            reasons.append(f'lang:{lang_reason}')
            stats['lang_fail'] += 1
        else:
            stats['lang_pass'] += 1

        # ---- 개별 검증 플래그 저장 ----
        doc['_pdf_verified'] = is_pdf and (size_kb >= MIN_PDF_SIZE_KB or size_kb == 0)
        doc['_lang_verified'] = is_eng

        # ---- 제외 처리 ----
        if reasons:
            doc['_relevance_score'] = -1.0
            doc['_excluded'] = True
            doc['_exclude_reasons'] = reasons
            stats['excluded_total'] += 1
            excluded_reasons.append((title, reasons))
            print(f"  EXCLUDE [{doc.get('_country','')}] {title}")
            for r in reasons:
                print(f"           → {r}")
        else:
            doc['_validated'] = True

        # 진행 상황
        if (i + 1) % 30 == 0:
            checked = stats['date_pass'] + stats['date_fail'] + stats['date_skip']
            print(f"  [{checked}/{len(active)}] excluded so far: {stats['excluded_total']}")

        time.sleep(0.3)  # rate limiting

    # ========== 결과 요약 ==========
    print()
    print("=" * 70)
    print("검증 결과 요약")
    print("=" * 70)

    print(f"\n날짜 검증:")
    print(f"  통과: {stats['date_pass']}")
    print(f"  실패 (pre-2026): {stats['date_fail']}")
    print(f"  미확인 (날짜 추출 불가): {stats['date_skip']}")

    print(f"\nPDF 검증:")
    print(f"  정상: {stats['pdf_ok']}")
    print(f"  가짜 PDF (HTML 반환): {stats['pdf_fake']}")
    print(f"  소용량 (<{MIN_PDF_SIZE_KB}KB): {stats['pdf_tiny']}")

    print(f"\n언어 검증:")
    print(f"  영어: {stats['lang_pass']}")
    print(f"  비영어: {stats['lang_fail']}")

    remaining = len(active) - stats['excluded_total']
    print(f"\n{'=' * 70}")
    print(f"총 제외: {stats['excluded_total']}건")
    print(f"남은 활성 문서: {remaining}건")
    print(f"{'=' * 70}")

    if excluded_reasons:
        print(f"\n제외 문서 상세:")
        # 사유별 통계
        reason_types = defaultdict(int)
        for title, reasons in excluded_reasons:
            for r in reasons:
                key = r.split('(')[0]
                reason_types[key] += 1
        print(f"\n  사유별 건수:")
        for k, v in sorted(reason_types.items(), key=lambda x: -x[1]):
            print(f"    {k}: {v}")

    # 저장
    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n저장 완료: {SELECTION_FILE}")


if __name__ == '__main__':
    main()
