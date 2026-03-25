"""수집완료 문서 중복 필터링 유틸리티"""
import csv, logging
from pathlib import Path

logger = logging.getLogger(__name__)
CSV_PATH = Path(__file__).parent / 'completed sites.csv'


def load_completed_titles(csv_path=None) -> set[str]:
    """completed sites.csv에서 자료명 목록을 정규화하여 set으로 반환"""
    path = Path(csv_path) if csv_path else CSV_PATH
    titles = set()
    if not path.exists():
        logger.warning(f"수집완료 목록 파일 없음: {path}")
        return titles
    with open(path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 건너뛰기
        for row in reader:
            if len(row) >= 5:
                title = row[4].strip()  # 자료명 = 5번째 컬럼
                if title:
                    titles.add(title.lower())
    logger.info(f"수집완료 목록 로드: {len(titles)}건 ({path.name})")
    return titles


def filter_completed(results: list[dict], completed: set[str]) -> list[dict]:
    """이미 수집 완료된 문서를 제거하고, 제거 건수를 로깅"""
    before = len(results)
    filtered = [r for r in results if r.get('title', '').strip().lower() not in completed]
    removed = before - len(filtered)
    if removed:
        logger.info(f"  수집완료 중복 제거: {before} → {len(filtered)}건 (-{removed}건)")
    return filtered
