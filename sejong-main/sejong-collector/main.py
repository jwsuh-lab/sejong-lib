"""
세종도서관 해외 정책자료 수집 시스템
사용법:
  python main.py summary                사이트 현황
  python main.py crawl-uk               영국 GOV.UK 크롤링
  python main.py crawl-us [--site CODE] 미국 크롤링
  python main.py crawl-se [--site CODE] 스웨덴 크롤링
  python main.py crawl-sg [--site CODE] 싱가포르 크롤링
  python main.py crawl AT --all         범용 국가 크롤링 (AT, CA, NO, IT 등)
"""
import argparse
import sys

from site_manager import SiteManager


DEDICATED_COUNTRIES = {'GB', 'US', 'SW', 'SI'}


def show_site_summary():
    """사이트 현황 출력"""
    manager = SiteManager()
    info = manager.summary()
    print("=" * 50)
    print("세종도서관 해외 정책자료 수집 대상 현황")
    print("=" * 50)
    print(f"전체 사이트: {info['total']}개")
    print(f"국가 수: {len(info['countries'])}개")
    print(f"\n국가별 사이트 수:")
    for code, count in info['countries'].items():
        crawler_type = 'DEDICATED' if code in DEDICATED_COUNTRIES else 'GENERIC'
        print(f"  {code}: {count}개  [{crawler_type}]")
    print(f"\n영국 GOV.UK 그룹: {info['govuk_count']}개")
    print(f"영국 비-GOV.UK: {info['uk_non_govuk']}개")
    print("=" * 50)


def crawl_govuk(args):
    """영국 GOV.UK 크롤링"""
    from crawlers.govuk_crawler import GovukCrawler
    manager = SiteManager()
    crawler = GovukCrawler()

    if args.site:
        site = manager.get_by_code(args.site)
        if site:
            print(f"크롤링: {site.name} ({site.name_kr})")
            results = crawler.crawl_site(site, max_results=args.max)
            if results:
                crawler.save_results(site, results)
            print(f"수집 완료: {len(results)}건")
        else:
            print(f"기관코드 '{args.site}'를 찾을 수 없습니다.")
    elif args.all:
        crawler.crawl_all_govuk(max_results_per_site=args.max)
    else:
        print("--site 또는 --all 옵션을 지정하세요.")
        print("  python main.py crawl-uk --site Z00115   DfE 크롤링")
        print("  python main.py crawl-uk --all            전체 UK 크롤링")


def crawl_us(args):
    """미국 크롤링"""
    from crawlers.us_gov_crawler import UsGovCrawlerRunner
    runner = UsGovCrawlerRunner(data_dir=args.data_dir, api_key=args.api_key)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_us(max_results_per_site=args.max)
    else:
        print("--site 또는 --all 옵션을 지정하세요.")
        print("  python main.py crawl-us --site Z00057   BEA 크롤링")
        print("  python main.py crawl-us --all            전체 US 크롤링")


def crawl_se(args):
    """스웨덴 크롤링"""
    from crawlers.se_crawler import SeCrawlerRunner
    runner = SeCrawlerRunner(data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_se(max_results_per_site=args.max)
    else:
        print("--site 또는 --all 옵션을 지정하세요.")
        print("  python main.py crawl-se --site Z00102   SIPRI 크롤링")
        print("  python main.py crawl-se --all            전체 SW 크롤링")


def crawl_sg(args):
    """싱가포르 크롤링"""
    from crawlers.sg_crawler import SgCrawlerRunner
    runner = SgCrawlerRunner(data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all_sg(max_results_per_site=args.max)
    else:
        print("--site 또는 --all 옵션을 지정하세요.")
        print("  python main.py crawl-sg --site Z00112   MAS 크롤링")
        print("  python main.py crawl-sg --all            전체 SG 크롤링")


def crawl_generic(args):
    """범용 국가 크롤링"""
    from crawlers.generic_crawler import GenericCrawlerRunner, COUNTRIES_WITH_DEDICATED_CRAWLER

    country = args.country.upper()
    if country in COUNTRIES_WITH_DEDICATED_CRAWLER:
        dedicated = COUNTRIES_WITH_DEDICATED_CRAWLER[country]
        print(f"'{country}'는 전용 크롤러가 있습니다: {dedicated}")
        print(f"  python main.py {dedicated.split(' ')[0]} 명령을 사용하세요.")
        return

    if args.list:
        manager = SiteManager()
        sites = manager.get_by_country(country)
        if not sites:
            print(f"국가코드 '{country}'에 해당하는 사이트가 없습니다.")
            return
        print(f"\n{country} 사이트 목록 ({len(sites)}개):")
        print(f"  {'코드':8s} {'약어':10s} {'현재사용':8s} 기관명")
        print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*50}")
        for site in sites:
            use = 'X' if site.current_use == 'X' else 'O'
            print(f"  {site.code:8s} {(site.acronym or ''):10s} {use:8s} {site.name[:50]}")
        return

    runner = GenericCrawlerRunner(country, data_dir=args.data_dir)

    if args.site:
        results = runner.crawl_site(args.site, max_results=args.max)
        print(f"\n수집 완료: {len(results)}건")
    elif args.all:
        runner.crawl_all(max_results_per_site=args.max, force=args.force)
    else:
        print("--site 또는 --all 옵션을 지정하세요.")
        print(f"  python main.py crawl {country} --list          사이트 목록")
        print(f"  python main.py crawl {country} --site ZXXXXX   특정 사이트")
        print(f"  python main.py crawl {country} --all           전체 크롤링")
        print(f"  python main.py crawl {country} --all --force   현재사용 무시, 전체 크롤링")


def build_parser():
    parser = argparse.ArgumentParser(
        description='세종도서관 해외 정책자료 수집 시스템')
    sub = parser.add_subparsers(dest='command', help='실행할 명령')

    # summary
    sub.add_parser('summary', help='사이트 현황 출력')

    # crawl-uk
    p_uk = sub.add_parser('crawl-uk', help='영국 GOV.UK 크롤링')
    p_uk.add_argument('--site', '-s', help='기관코드')
    p_uk.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    p_uk.add_argument('--all', '-a', action='store_true', help='전체 크롤링')

    # crawl-us
    p_us = sub.add_parser('crawl-us', help='미국 크롤링')
    p_us.add_argument('--site', '-s', help='기관코드')
    p_us.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    p_us.add_argument('--all', '-a', action='store_true', help='전체 크롤링')
    p_us.add_argument('--api-key', '-k', default='DEMO_KEY', help='API 키')
    p_us.add_argument('--data-dir', '-d', default=None)

    # crawl-se
    p_se = sub.add_parser('crawl-se', help='스웨덴 크롤링')
    p_se.add_argument('--site', '-s', help='기관코드')
    p_se.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    p_se.add_argument('--all', '-a', action='store_true', help='전체 크롤링')
    p_se.add_argument('--data-dir', '-d', default=None)

    # crawl-sg
    p_sg = sub.add_parser('crawl-sg', help='싱가포르 크롤링')
    p_sg.add_argument('--site', '-s', help='기관코드')
    p_sg.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    p_sg.add_argument('--all', '-a', action='store_true', help='전체 크롤링')
    p_sg.add_argument('--data-dir', '-d', default=None)

    # crawl (범용 국가)
    p_gen = sub.add_parser('crawl', help='범용 국가 크롤링 (AT, CA, NO, IT 등)')
    p_gen.add_argument('country', help='국가코드 (예: AT, CA, NO, IT, CN, BE, IN, EU)')
    p_gen.add_argument('--site', '-s', help='기관코드')
    p_gen.add_argument('--max', '-m', type=int, default=100, help='최대 수집 건수')
    p_gen.add_argument('--all', '-a', action='store_true', help='전체 크롤링')
    p_gen.add_argument('--force', '-f', action='store_true', help='현재사용 상태 무시, 전체 크롤링')
    p_gen.add_argument('--list', '-l', action='store_true', help='사이트 목록 출력')
    p_gen.add_argument('--data-dir', '-d', default=None)

    return parser


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()

    if args.command == 'summary':
        show_site_summary()
    elif args.command == 'crawl-uk':
        crawl_govuk(args)
    elif args.command == 'crawl-us':
        crawl_us(args)
    elif args.command == 'crawl-se':
        crawl_se(args)
    elif args.command == 'crawl-sg':
        crawl_sg(args)
    elif args.command == 'crawl':
        crawl_generic(args)
    else:
        # 명령 없이 실행 시 기존 동작 유지
        show_site_summary()
        parser.print_help()
