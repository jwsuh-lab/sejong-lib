"""
SiteManager: sites.csv를 읽어 사이트 목록을 관리하는 클래스
"""
import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, parse_qs


@dataclass
class Site:
    """개별 수집 대상 사이트 정보"""
    no: int
    code: str                    # 기관코드 (Z00001 등)
    name: str                    # 기관명 (영문)
    name_prev: str               # 수정전 기관명
    country_code: str            # 국가코드 (GB, US, NO 등)
    country: str                 # 국가 (한글)
    org_type: str                # 유형 (정부기관, 연구기관)
    name_kr: str                 # 기관명 (한글)
    name_kr_prev: str            # 수정전 기관명 (한글)
    acronym: str                 # 기관명 약어
    url: str                     # 수집 URL
    url_prev: str                # 수정전 수집 URL
    note: str                    # 비고
    brm_category: str            # BRM 대분류
    expected_count: str          # 연간 수집 예상 수량
    exclude: str                 # 수집제외
    alias1: str                  # 기관명(기타1)
    alias2: str                  # 기관명(기타2)
    alias3: str                  # 기관명(기타3)
    collected_2025: str          # 2025년 수집 건수
    current_use: str = ''        # 현재사용 (빈값=사용가능, X=미사용)
    tags: list = field(default_factory=list)

    @property
    def is_govuk(self) -> bool:
        """GOV.UK 사이트 여부"""
        return 'gov.uk' in self.url

    @property
    def govuk_org_slug(self) -> str | None:
        """GOV.UK URL에서 기관 slug 추출"""
        if not self.is_govuk:
            return None
        parsed = urlparse(self.url)
        params = parse_qs(parsed.query)
        orgs = params.get('organisations[]', params.get('organisations%5B%5D', []))
        if orgs:
            return orgs[0]
        # URL path에서 추출 시도
        match = re.search(r'/government/organisations/([^/?#]+)', self.url)
        if match:
            return match.group(1)
        return None

    def __repr__(self):
        return f"Site({self.code}, {self.acronym or self.name[:20]})"


class SiteManager:
    """sites.csv를 읽어 사이트 목록을 관리"""

    def __init__(self, csv_path: str | Path = None):
        if csv_path is None:
            csv_path = Path(__file__).parent / 'sites.csv'
        self.csv_path = Path(csv_path)
        self.sites: list[Site] = []
        self._load()

    def _load(self):
        """CSV 파일에서 사이트 목록 로드"""
        with open(self.csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)  # 헤더 건너뛰기
            for row in reader:
                if len(row) < 22:
                    row.extend([''] * (22 - len(row)))
                site = Site(
                    no=int(row[0]) if row[0].isdigit() else 0,
                    code=row[1].strip(),
                    name_prev=row[2].strip(),
                    name=row[3].strip(),
                    country_code=row[4].strip(),
                    country=row[5].strip(),
                    org_type=row[6].strip(),
                    name_kr_prev=row[7].strip(),
                    name_kr=row[8].strip(),
                    # row[9] 는 빈 컬럼
                    acronym=row[10].strip(),
                    url_prev=row[11].strip(),
                    url=row[12].strip(),
                    note=row[13].strip(),
                    brm_category=row[14].strip(),
                    expected_count=row[15].strip(),
                    exclude=row[16].strip(),
                    alias1=row[17].strip(),
                    alias2=row[18].strip(),
                    alias3=row[19].strip(),
                    collected_2025=row[20].strip(),
                    current_use=row[21].strip(),
                )
                self.sites.append(site)
        self._apply_url_fixes()

    # CSV를 직접 수정하기 어려운 경우를 위한 URL 보정 테이블
    _URL_FIXES = {
        'Z00001': 'https://www.norad.no/en/news/publications/',
        'Z00258': 'https://www.pbo-dpb.ca/en/publications',
        'Z00240': 'https://www.agriculture.gov.au/abares/products',
        'Z00327': 'https://www.eliamep.gr/en/dimosieuseis/',
        'Z00308': 'https://www.orfonline.org/expert-speak',
        'Z00321': 'https://www.iai.it/en/publications',
        'Z00380': 'https://www.feem.it/en/publication/feem-working-papers/',
        'Z00361': 'https://www.kapsarc.org/our-offerings/publications/',
        'Z00704': 'https://www.euipo.europa.eu/en/observatory/publications',
    }

    def _apply_url_fixes(self):
        """알려진 URL 변경사항 적용"""
        for site in self.sites:
            if site.code in self._URL_FIXES:
                site.url = self._URL_FIXES[site.code]

    def get_by_code(self, code: str) -> Site | None:
        """기관코드로 사이트 검색"""
        for site in self.sites:
            if site.code == code:
                return site
        return None

    def get_by_country(self, country_code: str) -> list[Site]:
        """국가코드로 사이트 필터링"""
        return [s for s in self.sites if s.country_code == country_code]

    def get_govuk_sites(self) -> list[Site]:
        """GOV.UK 그룹 사이트만 반환 (gov.uk URL 사용하는 영국 사이트)"""
        return [s for s in self.sites if s.country_code == 'GB' and s.is_govuk]

    def get_non_govuk_uk_sites(self) -> list[Site]:
        """GOV.UK가 아닌 영국 사이트 반환"""
        return [s for s in self.sites if s.country_code == 'GB' and not s.is_govuk]

    def get_by_type(self, org_type: str) -> list[Site]:
        """기관 유형으로 필터링 (정부기관, 연구기관)"""
        return [s for s in self.sites if s.org_type == org_type]

    def get_countries(self) -> list[str]:
        """등록된 국가코드 목록"""
        return sorted(set(s.country_code for s in self.sites))

    def summary(self) -> dict:
        """전체 사이트 요약 통계"""
        countries = {}
        for s in self.sites:
            countries.setdefault(s.country_code, []).append(s)
        return {
            'total': len(self.sites),
            'countries': {k: len(v) for k, v in sorted(countries.items())},
            'govuk_count': len(self.get_govuk_sites()),
            'uk_non_govuk': len(self.get_non_govuk_uk_sites()),
        }

    def __len__(self):
        return len(self.sites)

    def __iter__(self):
        return iter(self.sites)


if __name__ == '__main__':
    manager = SiteManager()
    info = manager.summary()
    print(f"전체 사이트: {info['total']}개")
    print(f"국가별: {info['countries']}")
    print(f"\n영국 GOV.UK 사이트: {info['govuk_count']}개")
    print(f"영국 비-GOV.UK 사이트: {info['uk_non_govuk']}개")

    print("\n--- GOV.UK 사이트 목록 ---")
    for site in manager.get_govuk_sites():
        slug = site.govuk_org_slug or '(slug 없음)'
        print(f"  {site.code} | {site.acronym:8s} | {site.name_kr:15s} | slug={slug}")
