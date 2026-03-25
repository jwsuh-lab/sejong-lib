"""
수집 우선순위 기반 관련성 필터링
- 국가적 이슈 키워드 매칭으로 정책 관련성 점수 부여
- 문서유형별 우선순위 가중치 적용
- 점수순 정렬로 관련성 높은 자료 우선 배치
"""
import logging
import re

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
#  비정책 배제 키워드 — 국내 정책 수요와 거리가 먼 주제
#  (예: 특정 아프리카 국가의 국한된 식량 문제, WFP 일반 구호 등)
# ═══════════════════════════════════════════════
EXCLUSION_KEYWORDS = [
    # 일반 구호/인도주의 (정책 시사점 낮음)
    'food aid', 'humanitarian relief', 'emergency relief', 'disaster relief',
    'famine relief', 'refugee camp', 'internally displaced',
    # WFP/구호기관 일반 자료
    'WFP annual report', 'WFP country brief', 'situation report',
    # 지엽적 지역 이슈
    'locust', 'drought relief', 'flood relief',

    # ── GOV.UK 비정책 문서 배제 (2026-03-12 추가) ──

    # 개별 시설 점검/조치계획
    'prison action plan', 'inspection response', 'approved premises inspection',

    # 특정 사건 (영국 국내 한정)
    'grenfell tower',

    # 행정 안내/메타 페이지 (발간 일정, 데이터 목차 등)
    'pre-release access list', 'upcoming releases', 'upcoming publications',
    'list of upcoming',

    # 주간·일간 반복 서베일런스 게시판
    'weekly bulletin', 'syndromic surveillance', 'weekly summary',

    # 내부 행정/조직 관리
    'humble address', 'statement of excesses', 'performance update',
    'business critical models',

    # 지엽적 기술규격/물품
    'spectacle frames', 'spectacle lenses', 'reading spectacles',
    'equipment theft',

    # 개별 급여/인사 구조
    'judicial salary', 'member contributions',
]
_EXCLUSION_PATTERNS: list[re.Pattern] = []


def _build_exclusion_patterns():
    global _EXCLUSION_PATTERNS
    if _EXCLUSION_PATTERNS:
        return
    for kw in EXCLUSION_KEYWORDS:
        _EXCLUSION_PATTERNS.append(re.compile(
            r'\b' + re.escape(kw.lower()) + r'\b', re.IGNORECASE
        ))


def is_excluded_topic(doc: dict) -> bool:
    """비정책 주제 배제 여부 판별"""
    _build_exclusion_patterns()
    title = (doc.get('title') or '').lower()
    desc = (doc.get('description') or '').lower()
    text = f"{title} {desc}"
    for pattern in _EXCLUSION_PATTERNS:
        if pattern.search(text):
            return True
    return False


# ═══════════════════════════════════════════════
#  짧은 description / 비정책 문서유형 배제
# ═══════════════════════════════════════════════
MIN_DESCRIPTION_LENGTH = 30  # description이 30자 미만이면 정책자료로 보기 어려움

# 정책 시사점이 낮은 문서유형 (GOV.UK 기준)
LOW_VALUE_DOC_TYPES = {
    'statistics',           # 통계 포털 안내 페이지
    'statistical_data_set', # 원시 데이터 테이블 (분석 없음)
}


def is_low_quality(doc: dict) -> bool:
    """내용이 지나치게 짧거나 정책 시사점이 낮은 문서 유형 여부 판별

    Returns:
        True이면 제외 대상
    """
    title = (doc.get('title') or '').strip()
    desc = (doc.get('description') or '').strip()

    # 제목이 없거나 5자 미만
    if len(title) < 5:
        return True

    # description이 짧되, 제목이 충분히 길면(20자+) 허용
    # (범용 크롤러는 description 추출이 어려운 경우가 많음)
    if len(desc) < MIN_DESCRIPTION_LENGTH and len(title) < 20:
        return True

    # 정책 시사점이 낮은 문서유형
    doc_type = doc.get('document_type') or ''
    if doc_type in LOW_VALUE_DOC_TYPES:
        return True

    return False


def deduplicate_results(results: list[dict]) -> list[dict]:
    """동일 title의 중복 문서 제거 (첫 번째만 유지)

    GOV.UK에서 DOH/DHSC 등 기관이 재편되면서
    동일 문서가 복수 기관에서 수집되는 경우 방지
    """
    seen_titles: set[str] = set()
    deduped = []
    removed = 0
    for doc in results:
        title_key = (doc.get('title') or '').strip().lower()
        if title_key in seen_titles:
            removed += 1
            continue
        seen_titles.add(title_key)
        deduped.append(doc)
    if removed:
        logger.info(f"  중복 제거: {removed}건 제거 (동일 title)")
    return deduped


# ═══════════════════════════════════════════════
#  국가 이슈 키워드 사전 (2025-2026 기준)
#  - 국내 정부·공공기관의 정책 수립·집행·평가에
#    시사점을 줄 수 있는 해외 정책자료 수집용
#  - 키워드 추가/수정 시 이 사전만 변경하면 됨
# ═══════════════════════════════════════════════
ISSUE_KEYWORDS = {
    '경제·통상': [
        'trade', 'tariff', 'supply chain', 'inflation', 'recession',
        'economic growth', 'fiscal policy', 'monetary policy', 'interest rate',
        'debt', 'GDP', 'FDI', 'sanctions', 'export control',
        'industrial policy', 'subsidy', 'trade war', 'WTO',
        'economic outlook', 'budget', 'tax reform', 'competitiveness',
    ],
    'AI·반도체·디지털': [
        'artificial intelligence', ' AI ', 'semiconductor', 'chip',
        'digital transformation', 'cybersecurity', 'quantum computing',
        'AI regulation', 'AI governance', 'big data', 'platform regulation',
        'machine learning', 'autonomous', 'data privacy', 'digital economy',
        'tech regulation', 'innovation', 'R&D',
        'digital identity', 'digital ID',
    ],
    '기후·에너지': [
        'climate change', 'carbon', 'net zero', 'renewable energy',
        'green transition', 'ESG', 'emission', 'sustainability',
        'clean energy', 'hydrogen', 'energy transition', 'energy security',
        'decarbonization', 'electric vehicle', 'solar', 'wind power',
        'Paris Agreement', 'COP', 'biodiversity',
    ],
    '인구·복지': [
        'aging', 'demographic', 'fertility', 'population decline',
        'pension', 'welfare', 'childcare', 'immigration', 'migration',
        'labor shortage', 'workforce', 'social security', 'inequality',
        'poverty', 'gender', 'disability', 'elderly care',
        'flexible working', 'home-based working',
    ],
    '안보·외교': [
        'defense', 'security', 'geopolitics', 'Indo-Pacific',
        'nuclear', 'missile', 'alliance', 'NATO', 'deterrence',
        'intelligence', 'arms control', 'military', 'foreign policy',
        'diplomacy', 'conflict', 'peacekeeping', 'terrorism',
        'maritime security', 'space security', 'sanctions',
    ],
    '보건': [
        'public health', 'pandemic', 'healthcare', 'pharmaceutical',
        'mental health', 'biotech', 'health policy', 'WHO',
        'vaccination', 'disease', 'health system', 'medical',
        'drug', 'antimicrobial', 'health insurance', 'epidem',
    ],
    '주거·도시': [
        'housing', 'real estate', 'urban planning', 'infrastructure',
        'transportation', 'smart city', 'construction', 'affordable housing',
        'public transport', 'land use', 'urban development',
    ],
    '교육': [
        'education', 'higher education', 'STEM', 'workforce development',
        'skills', 'university', 'student', 'vocational training',
        'lifelong learning', 'digital literacy', 'school',
    ],
}

# 전체 키워드를 소문자 정규식 패턴으로 전처리
_ALL_PATTERNS: list[re.Pattern] = []


def _build_patterns():
    """키워드 사전에서 정규식 패턴 리스트 생성 (1회만 실행)"""
    global _ALL_PATTERNS
    if _ALL_PATTERNS:
        return
    seen = set()
    for keywords in ISSUE_KEYWORDS.values():
        for kw in keywords:
            kw_lower = kw.strip().lower()
            if kw_lower not in seen:
                seen.add(kw_lower)
                # 단어 경계 매칭 (\b)으로 정확도 향상
                _ALL_PATTERNS.append(re.compile(
                    r'\b' + re.escape(kw_lower) + r'\b', re.IGNORECASE
                ))


# ═══════════════════════════════════════════════
#  문서 유형 우선순위
# ═══════════════════════════════════════════════
DOC_TYPE_BONUS = {
    # Tier 1: 정책 발간자료 (가장 높음)
    'report': 0.3, 'policy_paper': 0.3, 'corporate_report': 0.3,
    'independent_report': 0.3, 'official_statistics': 0.3,
    'national_statistics': 0.3, 'impact_assessment': 0.3,
    'government_response': 0.3, 'REPORT': 0.3,
    # Tier 2: 연구·분석 자료
    'research': 0.2, 'working_paper': 0.2,
    'policy_brief': 0.2, 'consultation_outcome': 0.2,
    'Policy Brief': 0.2, 'Working Paper': 0.2,
    # Tier 3: 일반 발간물
    'publication': 0.1, 'guidance': 0.1, 'analysis': 0.1,
}


# ═══════════════════════════════════════════════
#  스코어링 함수
# ═══════════════════════════════════════════════
def score_relevance(doc: dict) -> float:
    """문서의 정책 관련성 점수 계산 (0.0 ~ 1.0)

    점수 구성:
      - 키워드 매칭 (최대 0.7): title 매치당 0.15, description 매치당 0.05
      - 문서유형 보너스 (최대 0.3): tier1=0.3, tier2=0.2, tier3=0.1

    Returns:
        0.0 ~ 1.0 사이의 관련성 점수
    """
    _build_patterns()

    title = (doc.get('title') or '').lower()
    desc = (doc.get('description') or '').lower()
    doc_type = doc.get('document_type') or ''

    # 키워드 매칭 점수
    keyword_score = 0.0
    for pattern in _ALL_PATTERNS:
        if pattern.search(title):
            keyword_score += 0.15
        elif desc and pattern.search(desc):
            keyword_score += 0.05
    keyword_score = min(keyword_score, 0.7)

    # 문서유형 보너스
    type_bonus = DOC_TYPE_BONUS.get(doc_type, 0.0)

    return min(keyword_score + type_bonus, 1.0)


def filter_by_relevance(results: list[dict], min_score: float = 0.0) -> list[dict]:
    """관련성 점수 부여 + 점수순 정렬

    Args:
        results: 크롤링 결과 목록
        min_score: 최소 점수 (이하 제거). 기본 0.0이면 제거 없이 정렬만.

    Returns:
        _relevance_score 필드가 추가된 결과 목록 (점수 내림차순)
    """
    if not results:
        return results

    # Step 0: 중복 제거 (동일 title)
    results = deduplicate_results(results)

    # Step 1: 비정책 주제 사전 배제
    excluded = 0
    pre_filtered = []
    for doc in results:
        if is_excluded_topic(doc):
            excluded += 1
            continue
        pre_filtered.append(doc)
    if excluded:
        logger.info(f"  비정책 주제 배제: {excluded}건 제거")

    # Step 2: 짧은 description / 낮은 가치 문서유형 배제
    low_q = 0
    quality_filtered = []
    for doc in pre_filtered:
        if is_low_quality(doc):
            low_q += 1
            continue
        quality_filtered.append(doc)
    if low_q:
        logger.info(f"  품질 필터: {low_q}건 제거 (짧은 desc 또는 낮은 가치 문서유형)")
    pre_filtered = quality_filtered

    scored = []
    for doc in pre_filtered:
        doc['_relevance_score'] = round(score_relevance(doc), 3)
        scored.append(doc)

    # 최소 점수 이하 제거
    if min_score > 0:
        before = len(scored)
        scored = [d for d in scored if d['_relevance_score'] >= min_score]
        removed = before - len(scored)
        if removed:
            logger.info(f"  관련성 필터: {before} → {len(scored)}건 "
                        f"(-{removed}건, min={min_score})")

    # 점수 내림차순 정렬 (동점이면 원래 순서 유지)
    scored.sort(key=lambda d: d['_relevance_score'], reverse=True)

    # 통계 로깅
    if scored:
        scores = [d['_relevance_score'] for d in scored]
        high = sum(1 for s in scores if s >= 0.3)
        logger.info(f"  관련성 스코어링: {len(scored)}건 "
                    f"(상위={high}건, 최고={max(scores):.2f}, 평균={sum(scores)/len(scores):.2f})")

    return scored
