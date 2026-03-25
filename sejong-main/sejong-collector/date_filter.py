"""
발행일 기반 필터링 — 2026년 1월 이후 발행 자료만 수집
- published_date 필드가 있으면 파싱하여 날짜 확인
- 날짜 없는 문서는 유지 (판별 불가)
"""
import logging
import re
from datetime import datetime, date

logger = logging.getLogger(__name__)

# 수집 기준일: 2026년 발행 자료만 수집
CUTOFF_DATE = date(2026, 1, 1)


def parse_date(date_str: str) -> date | None:
    """다양한 날짜 형식을 파싱하여 date 객체 반환"""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    if not date_str:
        return None

    # ISO 8601 변형들: 2025-12-01T00:00:00Z, 2025-12-01T00:00:00, 2025-12-01
    # 그 외: January 1, 2025 / 1 January 2025 / 2025/12/01
    for fmt in (
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d',
        '%B %d, %Y',
        '%d %B %Y',
        '%Y/%m/%d',
        '%b %d, %Y',
        '%d %b %Y',
    ):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    # ISO 8601 with timezone offset: 2025-12-01T00:00:00+00:00
    try:
        clean = re.sub(r'[+-]\d{2}:\d{2}$', '', date_str)
        return datetime.fromisoformat(clean).date()
    except (ValueError, TypeError):
        pass

    # 날짜만 추출 시도 (문자열 내 YYYY-MM-DD 패턴)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    return None


def filter_by_date(results: list[dict], cutoff: date = CUTOFF_DATE) -> list[dict]:
    """발행일 기준 필터링

    - published_date가 cutoff 이전이면 제거
    - published_date가 없거나 파싱 불가면 유지 (판별 불가)

    Args:
        results: 크롤링 결과 목록
        cutoff: 기준일 (이 날짜 이후만 유지). 기본 2025-12-01
    """
    if not results:
        return results

    filtered = []
    removed = 0
    no_date = 0

    for doc in results:
        pub_date_str = doc.get('published_date', '')
        if not pub_date_str:
            no_date += 1
            filtered.append(doc)
            continue

        pub_date = parse_date(pub_date_str)
        if pub_date is None:
            no_date += 1
            filtered.append(doc)
            continue

        if pub_date >= cutoff:
            filtered.append(doc)
        else:
            removed += 1

    if removed:
        logger.info(f"  날짜 필터({cutoff}): {len(results)} → {len(filtered)}건 "
                    f"(-{removed}건 제거, {no_date}건 날짜없음)")

    return filtered
