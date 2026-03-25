"""
국외정책동향 문서유형 필터링
- UK: 화이트리스트 방식 (정책동향 관련 유형만 유지)
- US: 블랙리스트 방식 (대부분 유지, 명확한 비관련만 제외)
"""
import json
import logging
import os
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ── 정책동향 관련 문서유형 (UK 화이트리스트) ──
UK_POLICY_TYPES = {
    'policy_paper',
    'research',
    'official_statistics',
    'national_statistics',
    'corporate_report',
    'independent_report',
    'statistical_data_set',
    'consultation_outcome',
    'open_consultation',
    'closed_consultation',
    'impact_assessment',
    'government_response',
    'statistics',
    'call_for_evidence_outcome',
    'open_call_for_evidence',
    'closed_call_for_evidence',
}

# ── US 제외 유형 (블랙리스트) ──
US_EXCLUDE_TYPES = {'Video'}

# ── SW 제외 유형 (블랙리스트 — 스웨덴: 전부 정책관련) ──
SW_EXCLUDE_TYPES: set[str] = set()

# ── SI 제외 유형 (블랙리스트 — 싱가포르: 전부 정책관련) ──
SI_EXCLUDE_TYPES: set[str] = set()

DATA_DIR = Path(__file__).parent / 'data'
BACKUP_DIR = DATA_DIR / 'backup_원본'


def is_policy_document(doc: dict, country: str) -> bool:
    """문서가 정책동향 관련인지 판정"""
    doc_type = doc.get('document_type', '')
    # document_type이 int(0 등)인 경우 문자열로 변환
    if not isinstance(doc_type, str):
        doc_type = str(doc_type) if doc_type else ''

    if country == 'UK':
        return doc_type in UK_POLICY_TYPES
    elif country == 'US':
        return doc_type not in US_EXCLUDE_TYPES
    elif country == 'SW':
        return doc_type not in SW_EXCLUDE_TYPES
    elif country == 'SI':
        return doc_type not in SI_EXCLUDE_TYPES
    return True


def filter_json_file(fpath: str | Path, backup: bool = True) -> dict:
    """단일 JSON 파일 필터링. 원본 백업 후 덮어쓰기.

    Returns:
        dict with keys: filename, country, before, after, removed_types
    """
    fpath = Path(fpath)
    fname = fpath.name

    # 국가 판별
    if fname.startswith('govuk_'):
        country = 'UK'
    elif fname.startswith('us_'):
        country = 'US'
    elif fname.startswith('se_'):
        country = 'SW'
    elif fname.startswith('sg_'):
        country = 'SI'
    else:
        # 동적 국가 감지: {2자리소문자}_ 패턴
        parts = fname.split('_')
        if len(parts) >= 2 and len(parts[0]) == 2 and parts[0].isalpha():
            country = parts[0].upper()
        else:
            return {'filename': fname, 'country': '?', 'before': 0, 'after': 0, 'removed_types': {}}

    with open(fpath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    documents = data.get('documents', [])
    before = len(documents)

    # 필터링
    removed_types = Counter()
    filtered = []
    for doc in documents:
        if is_policy_document(doc, country):
            filtered.append(doc)
        else:
            doc_type = doc.get('document_type', '')
            if not isinstance(doc_type, str):
                doc_type = str(doc_type) if doc_type else ''
            removed_types[doc_type or '(빈값)'] += 1

    after = len(filtered)

    if before == after:
        # 변경 없음 — 백업/덮어쓰기 불필요
        return {
            'filename': fname, 'country': country,
            'before': before, 'after': after,
            'removed_types': dict(removed_types),
        }

    # 백업
    if backup:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = BACKUP_DIR / fname
        if not backup_path.exists():
            shutil.copy2(fpath, backup_path)
            logger.info(f"  백업: {backup_path.name}")

    # 덮어쓰기
    data['documents'] = filtered
    data['metadata']['total_collected'] = after
    data['metadata']['filtered_at'] = datetime.now().isoformat()
    data['metadata']['original_count'] = before

    with open(fpath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {
        'filename': fname, 'country': country,
        'before': before, 'after': after,
        'removed_types': dict(removed_types),
    }


def filter_all(data_dir: str | Path = None):
    """전체 JSON 파일 필터링 + 통계 출력"""
    if data_dir is None:
        data_dir = DATA_DIR
    data_dir = Path(data_dir)

    import glob
    json_files = sorted(glob.glob(str(data_dir / '*.json')))

    total_before = 0
    total_after = 0
    all_removed = Counter()
    results = []

    print("=" * 70)
    print("  국외정책동향 문서유형 필터링")
    print("=" * 70)

    for fpath in json_files:
        fname = os.path.basename(fpath)
        if 'summary' in fname:
            continue

        result = filter_json_file(fpath)
        results.append(result)

        total_before += result['before']
        total_after += result['after']
        for t, c in result['removed_types'].items():
            all_removed[t] += c

        removed = result['before'] - result['after']
        if removed > 0:
            logger.info(
                f"  {fname}: {result['before']} → {result['after']} "
                f"(-{removed}건)"
            )
        else:
            logger.info(f"  {fname}: {result['before']}건 (변경 없음)")

    # 요약 출력
    print()
    print("=" * 70)
    print(f"  필터링 결과 요약")
    print("=" * 70)
    print(f"  전체: {total_before}건 → {total_after}건 (-{total_before - total_after}건)")
    print()

    # 국가별 집계 (동적)
    all_countries = sorted(set(r['country'] for r in results if r['country'] != '?'))
    for country in all_countries:
        c_results = [r for r in results if r['country'] == country]
        c_before = sum(r['before'] for r in c_results)
        c_after = sum(r['after'] for r in c_results)
        print(f"  {country}: {c_before}건 → {c_after}건 (-{c_before - c_after}건)")

    print()
    if all_removed:
        print("  제거된 문서유형:")
        for doc_type, count in all_removed.most_common():
            print(f"    {doc_type:40s} {count}건")

    print()
    print(f"  백업 위치: {BACKUP_DIR}")
    print("=" * 70)

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description='국외정책동향 문서유형 필터링')
    parser.add_argument('--data-dir', '-d', default=None, help='데이터 디렉토리')
    parser.add_argument('--no-backup', action='store_true', help='백업 생략')
    parser.add_argument('--dry-run', action='store_true', help='실제 파일 변경 없이 통계만 출력')
    args = parser.parse_args()

    if args.dry_run:
        # dry-run: 통계만 출력
        data_dir = Path(args.data_dir) if args.data_dir else DATA_DIR
        import glob
        json_files = sorted(glob.glob(str(data_dir / '*.json')))

        total_before = 0
        total_after = 0
        all_removed = Counter()

        print("=" * 70)
        print("  [DRY-RUN] 필터링 시뮬레이션 (파일 변경 없음)")
        print("=" * 70)

        for fpath in json_files:
            fname = os.path.basename(fpath)
            if 'summary' in fname:
                continue

            if fname.startswith('govuk_'):
                country = 'UK'
            elif fname.startswith('us_'):
                country = 'US'
            elif fname.startswith('se_'):
                country = 'SW'
            elif fname.startswith('sg_'):
                country = 'SI'
            else:
                parts = fname.split('_')
                if len(parts) >= 2 and len(parts[0]) == 2 and parts[0].isalpha():
                    country = parts[0].upper()
                else:
                    country = '?'
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            documents = data.get('documents', [])
            before = len(documents)
            after = sum(1 for d in documents if is_policy_document(d, country))
            removed = before - after

            total_before += before
            total_after += after

            for doc in documents:
                if not is_policy_document(doc, country):
                    dt = doc.get('document_type', '')
                    if not isinstance(dt, str):
                        dt = str(dt) if dt else ''
                    all_removed[dt or '(빈값)'] += 1

            if removed > 0:
                print(f"  {fname}: {before} → {after} (-{removed}건)")
            else:
                print(f"  {fname}: {before}건 (변경 없음)")

        print()
        print(f"  전체: {total_before}건 → {total_after}건 (-{total_before - total_after}건)")
        if all_removed:
            print("\n  제거될 문서유형:")
            for doc_type, count in all_removed.most_common():
                print(f"    {doc_type:40s} {count}건")
        return

    filter_all(data_dir=args.data_dir)


if __name__ == '__main__':
    main()
