"""BRM code.csv를 로드하여 사이트별 BRM 코드를 매핑"""
import csv
import re
from pathlib import Path


# sites.csv의 BRM대분류 토큰 → BRM CODE1 매핑
TOKEN_TO_CODE1 = {
    "공공질서": "0001", "안전": "0001",
    "과학기술": "0002",
    "교육": "0003",
    "교통": "0004", "물류": "0004",
    "국방": "0005",
    "지역개발": "0006",
    "농림": "0007",
    "문화": "0008", "문화체육관광": "0008", "체육": "0008", "관광": "0008",
    "보건": "0009",
    "사회복지": "0010",
    "산업": "0011", "통상": "0011", "중소기업": "0011",
    "일반공공행정": "0012",
    "금융": "0013", "재정": "0013", "세제": "0013", "세재": "0013",
    "통신": "0014",
    "외교": "0015", "통일": "0015",
    "해양수산": "0016",
    "환경": "0017",
}

# 각 CODE1의 기본 CODE2 ("~일반" 서브카테고리)
DEFAULT_CODE2 = {
    "0001": "0024",  # 안전관리
    "0002": "0030",  # 과학기술연구
    "0003": "0039",  # 교육일반
    "0004": "0049",  # 물류등기타
    "0005": "0063",  # 병무행정
    "0006": "0067",  # 지역및도시
    "0007": "0077",  # 농업·농촌
    "0008": "0118",  # 문화체육관광일반
    "0009": "0125",  # 보건의료
    "0010": "0154",  # 사회복지일반
    "0011": "0165",  # 산업·중소기업일반
    "0012": "0175",  # 일반행정
    "0013": "0187",  # 금융
    "0014": "0194",  # 방송통신
    "0015": "0199",  # 외교
    "0016": "0204",  # 해양수산·어촌
    "0017": "0221",  # 환경일반
}

# 무시할 BRM 값
IGNORE_PATTERNS = {"링크를 찾을 수 없음", "접속 x", "접속x", "접속불가", ""}


class BrmMapper:
    """BRM code.csv를 로드하여 사이트의 brm_category를 CODE1/CODE2로 변환"""

    def __init__(self, brm_csv_path=None):
        if brm_csv_path is None:
            brm_csv_path = Path(__file__).parent / "BRM code.csv"
        self.brm_csv_path = Path(brm_csv_path)
        self.code1_by_name = {}   # {"환경": "0017", ...}
        self.code2_by_name = {}   # {"대기": "0208", ...}
        self.country_codes = {}   # {"노르웨이": "NO", ...}
        self._load()

    def _load(self):
        """BRM code.csv를 cp949로 파싱"""
        with open(self.brm_csv_path, "r", encoding="cp949") as f:
            reader = csv.reader(f)
            current_code1 = None
            for row in reader:
                if len(row) < 4:
                    continue
                code1_name = row[0].strip()
                code1_val = row[1].strip()
                code2_name = row[2].strip()
                code2_val = row[3].strip()

                # CODE1 매핑 (대분류명이 있는 행)
                if code1_name and code1_val and code1_val.isdigit():
                    self.code1_by_name[code1_name] = code1_val
                    current_code1 = code1_val

                # CODE2 매핑 (소분류)
                if code2_name and code2_val and code2_val.isdigit():
                    self.code2_by_name[code2_name] = code2_val

                # 국가코드 테이블 (컬럼 5, 6)
                if len(row) >= 7:
                    country_name = row[5].strip()
                    country_code = row[6].strip()
                    if country_name and country_code and country_code != "CODE":
                        self.country_codes[country_name] = country_code

    def get_brm_for_site(self, site) -> list[tuple[str, str]]:
        """site.brm_category를 파싱하여 BRM (CODE1, CODE2) 리스트 반환 (최대 2세트)"""
        brm_raw = getattr(site, "brm_category", "") or ""
        if not brm_raw.strip():
            return []

        # 무효값 체크
        if brm_raw.strip().lower() in IGNORE_PATTERNS:
            return []

        # 줄바꿈과 쉼표로 분할
        tokens = re.split(r"[\n,]+", brm_raw)

        seen_code1 = set()
        results = []

        for token in tokens:
            token = token.strip()
            if not token:
                continue

            # 무효값 체크
            if token.lower() in IGNORE_PATTERNS:
                continue

            # 점 구분 복합명 → 첫 토큰만 사용 (예: "재정.세재.금융" → "재정")
            if "." in token:
                token = token.split(".")[0].strip()

            # "교통 및 물류" → "교통" + "물류" 같은 패턴 처리
            sub_tokens = re.split(r"\s*및\s*", token)

            for sub in sub_tokens:
                sub = sub.strip()
                if not sub:
                    continue

                code1 = TOKEN_TO_CODE1.get(sub)
                if code1 and code1 not in seen_code1:
                    code2 = DEFAULT_CODE2.get(code1, "")
                    results.append((code1, code2))
                    seen_code1.add(code1)

                if len(results) >= 2:
                    break

            if len(results) >= 2:
                break

        return results


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")

    mapper = BrmMapper()
    print(f"CODE1 매핑: {len(mapper.code1_by_name)}개")
    for name, code in mapper.code1_by_name.items():
        print(f"  {name} → {code}")
    print(f"\nCODE2 매핑: {len(mapper.code2_by_name)}개")
    print(f"국가코드: {len(mapper.country_codes)}개")
    for name, code in mapper.country_codes.items():
        print(f"  {name} → {code}")

    # SiteManager 연동 테스트
    from site_manager import SiteManager
    sm = SiteManager()
    print("\n--- 사이트별 BRM 매핑 테스트 ---")
    for site in list(sm.sites)[:20]:
        brm = mapper.get_brm_for_site(site)
        brm_str = ", ".join(f"({c1}/{c2})" for c1, c2 in brm) if brm else "(없음)"
        print(f"  {site.code} {site.acronym:10s} BRM대분류={site.brm_category!r:40s} → {brm_str}")
