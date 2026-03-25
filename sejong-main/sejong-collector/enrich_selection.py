"""
selection_330.json 문서들에 대해 키워드, 수록잡지, 권호정보를 채우는 스크립트.
- 키워드: 문서 제목+설명에서 실제 매칭되는 키워드 추출
- 수록잡지: site_code 기반 SITE_SERIES_MAP + 사이트명 폴백
- 권호정보: 발행일 기반 생성
"""
import json
import re
import sys
import io
from collections import defaultdict
from datetime import datetime

from metadata_resolver import SITE_SERIES_MAP

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_400.json'

# Document-level keyword extraction patterns (word boundary matching)
KEYWORD_PATTERNS = {
    'AI_디지털_반도체': {
        'artificial intelligence': r'\bartificial intelligence\b',
        'AI': r'\bai\b',
        'semiconductor': r'\bsemiconductor\b',
        'chip': r'\bchip\b',
        'cybersecurity': r'\bcyber\s*security\b',
        'quantum computing': r'\bquantum computing\b',
        'machine learning': r'\bmachine learning\b',
        'data privacy': r'\bdata privacy\b',
        'digital economy': r'\bdigital econom\b',
        'platform regulation': r'\bplatform regulation\b',
        'generative AI': r'\bgenerative ai\b',
        'deep learning': r'\bdeep learning\b',
        'autonomous': r'\bautonomous\b',
        'large language model': r'\blarge language model\b',
        'robotics': r'\brobotics\b',
        'algorithm': r'\balgorithm\b',
        'digital transformation': r'\bdigital transformation\b',
        'blockchain': r'\bblockchain\b',
        'cloud computing': r'\bcloud computing\b',
    },
    '기후_에너지': {
        'climate change': r'\bclimate change\b',
        'carbon': r'\bcarbon\b',
        'net zero': r'\bnet zero\b',
        'renewable energy': r'\brenewable energy\b',
        'green transition': r'\bgreen transition\b',
        'ESG': r'\besg\b',
        'emission': r'\bemission\b',
        'hydrogen': r'\bhydrogen\b',
        'energy security': r'\benergy security\b',
        'electric vehicle': r'\belectric vehicle\b',
        'Paris Agreement': r'\bparis agreement\b',
        'biodiversity': r'\bbiodiversity\b',
        'sustainability': r'\bsustainab\b',
        'decarbonization': r'\bdecarboni\b',
        'carbon tax': r'\bcarbon tax\b',
        'nuclear energy': r'\bnuclear energy\b',
        'climate adaptation': r'\bclimate adaptation\b',
        'climate resilience': r'\bclimate resilience\b',
        'critical minerals': r'\bcritical minerals\b',
        'clean energy': r'\bclean energy\b',
        'fossil fuel': r'\bfossil fuel\b',
        'solar energy': r'\bsolar\b',
        'wind power': r'\bwind power\b',
        'green bond': r'\bgreen bond\b',
    },
    '경제_통상': {
        'trade': r'\btrade\b',
        'tariff': r'\btariff\b',
        'supply chain': r'\bsupply chain\b',
        'inflation': r'\binflation\b',
        'recession': r'\brecession\b',
        'GDP': r'\bgdp\b',
        'FDI': r'\bfdi\b',
        'sanctions': r'\bsanctions\b',
        'WTO': r'\bwto\b',
        'tax reform': r'\btax reform\b',
        'fiscal policy': r'\bfiscal\b',
        'monetary policy': r'\bmonetary\b',
        'economic growth': r'\beconomic growth\b',
        'export': r'\bexport\b',
        'import': r'\bimport\b',
        'debt': r'\bdebt\b',
        'interest rate': r'\binterest rate\b',
        'central bank': r'\bcentral bank\b',
        'financial stability': r'\bfinancial stability\b',
    },
    '안보_외교': {
        'defense': r'\bdefen[sc]e\b',
        'security': r'\bsecurity\b',
        'geopolitics': r'\bgeopolitic\b',
        'Indo-Pacific': r'\bindo.pacific\b',
        'nuclear': r'\bnuclear\b',
        'NATO': r'\bnato\b',
        'deterrence': r'\bdeterrence\b',
        'military': r'\bmilitary\b',
        'foreign policy': r'\bforeign policy\b',
        'diplomacy': r'\bdiplomac\b',
        'conflict': r'\bconflict\b',
        'maritime': r'\bmaritime\b',
        'intelligence': r'\bintelligence\b',
        'arms control': r'\barms control\b',
        'alliance': r'\balliance\b',
    },
    '인구_복지': {
        'aging': r'\baging\b',
        'demographic': r'\bdemographic\b',
        'fertility': r'\bfertility\b',
        'pension': r'\bpension\b',
        'welfare': r'\bwelfare\b',
        'childcare': r'\bchild\s*care\b',
        'immigration': r'\bimmigration\b',
        'labor shortage': r'\blabor shortage\b',
        'social security': r'\bsocial security\b',
        'inequality': r'\binequality\b',
        'gender': r'\bgender\b',
        'disability': r'\bdisability\b',
        'elderly': r'\belderly\b',
        'migration': r'\bmigration\b',
        'poverty': r'\bpoverty\b',
        'minimum wage': r'\bminimum wage\b',
        'social protection': r'\bsocial protection\b',
    },
    '보건': {
        'public health': r'\bpublic health\b',
        'pandemic': r'\bpandemic\b',
        'healthcare': r'\bhealth\s*care\b',
        'pharmaceutical': r'\bpharmaceutical\b',
        'mental health': r'\bmental health\b',
        'vaccination': r'\bvaccinat\b',
        'disease': r'\bdisease\b',
        'health system': r'\bhealth system\b',
        'antimicrobial': r'\bantimicrobial\b',
        'health insurance': r'\bhealth insurance\b',
        'health equity': r'\bhealth equity\b',
        'universal health coverage': r'\buniversal health coverage\b',
        'primary care': r'\bprimary care\b',
        'long-term care': r'\blong.term care\b',
        'epidemiology': r'\bepidemi\b',
        'mortality': r'\bmortality\b',
        'chronic disease': r'\bchronic disease\b',
        'hospital': r'\bhospital\b',
        'clinical': r'\bclinical\b',
        'nutrition': r'\bnutrition\b',
        'obesity': r'\bobesity\b',
    },
    '주거_도시': {
        'housing': r'\bhousing\b',
        'real estate': r'\breal estate\b',
        'urban planning': r'\burban planning\b',
        'infrastructure': r'\binfrastructure\b',
        'transportation': r'\btransportation\b',
        'smart city': r'\bsmart city\b',
        'affordable housing': r'\baffordable housing\b',
        'public transport': r'\bpublic transport\b',
        'broadband': r'\bbroadband\b',
        'water supply': r'\bwater supply\b',
        'waste management': r'\bwaste management\b',
    },
    '교육': {
        'education': r'\beducation\b',
        'higher education': r'\bhigher education\b',
        'STEM': r'\bstem\b',
        'workforce development': r'\bworkforce development\b',
        'skills': r'\bskills\b',
        'vocational training': r'\bvocational training\b',
        'lifelong learning': r'\blifelong learning\b',
        'digital literacy': r'\bdigital literacy\b',
        'student': r'\bstudent\b',
        'school': r'\bschool\b',
        'university': r'\buniversity\b',
        'teacher': r'\bteacher\b',
        'curriculum': r'\bcurriculum\b',
    },
}


EXTRA_KEYWORD_PATTERNS = {
    'policy analysis': r'\bpolic(?:y|ies)\b',
    'reform': r'\breform\b',
    'regulation': r'\bregulat\b',
    'governance': r'\bgovernan\b',
    'economic policy': r'\beconomic\b',
    'taxation': r'\btax(?:ation|es|payer)?\b',
    'budget': r'\bbudget\b',
    'investment': r'\binvestment\b',
    'development': r'\bdevelopment\b',
    'research': r'\bresearch\b',
    'assessment': r'\bassessment\b',
    'employment': r'\bemployment\b',
    'labor market': r'\blabou?r\b',
    'justice system': r'\bjustice\b',
    'crime prevention': r'\bcrime\b',
    'agriculture': r'\bagricult\b',
    'food security': r'\bfood\b',
    'water resources': r'\bwater\b',
    'environment': r'\benvironment\b',
    'energy policy': r'\benergy\b',
    'technology': r'\btechnolog\b',
    'innovation': r'\binnovation\b',
    'financial policy': r'\bfinancial\b',
    'children and youth': r'\bchildren\b',
    'youth policy': r'\byouth\b',
    'public safety': r'\bsafety\b',
    'health policy': r'\bhealth\b',
    'climate policy': r'\bclimate\b',
    'transportation': r'\btransport\b',
    'housing policy': r'\bhousing\b',
    'digital policy': r'\bdigital\b',
    'conservation': r'\bconservat\b',
    'rural development': r'\brural\b',
    'urban development': r'\burban\b',
    'geopolitics': r'\bgeopolit\b',
    'diplomacy': r'\bdiploma\b',
    'conflict resolution': r'\bconflict\b',
    'trade policy': r'\btrade\b',
    'sanctions policy': r'\bsanction\b',
    'cybersecurity': r'\bcyber\b',
    'mobility': r'\bmobility\b',
    'inequality': r'\binequality\b',
    'immigration policy': r'\bimmigrat\b',
    'fiscal policy': r'\bfiscal\b',
    'welfare policy': r'\bwelfare\b',
    'social policy': r'\bsocial\b',
    'education policy': r'\beducat\b',
    'military affairs': r'\bmilitary\b',
    'defense policy': r'\bdefen[sc]e\b',
    'nuclear policy': r'\bnuclear\b',
    'international relations': r'\b(?:bilateral|multilateral|international|global|foreign)\b',
    'public administration': r'\b(?:federal|congress|parliament|legislation|oversight|accountability)\b',
    'social research': r'\b(?:survey|study|findings|analysis|longitudinal|cohort)\b',
    'crime and justice': r'\b(?:juvenile|sentencing|maltreatment|trafficking|law enforcement|victim|incarcerat)\b',
    'economic analysis': r'\b(?:cost|income|wage|poverty|wealth|affordability|premium|debt)\b',
    'national security': r'\b(?:militia|deterrence|warfare|nato|indo.pacific|maritime)\b',
    'demographic change': r'\b(?:aging|demographic|fertility|population|migration)\b',
    'public health': r'\b(?:medical|hospital|disease|mental|clinical|nutrition|medicare|medicaid)\b',
    'sustainability': r'\b(?:sustainable|sustainability|emission|green|clean)\b',
    'insurance and benefits': r'\b(?:insurance|premium|benefit|aca|marketplace|medicare)\b',
    'Arctic policy': r'\b(?:arctic|high north)\b',
    'community development': r'\b(?:community|neighborhood|resident|coalition)\b',
    'apprenticeship': r'\b(?:apprentice|vocational|workforce)\b',
    'petroleum and energy': r'\b(?:petroleum|oil|gas|fuel|grid|electricity)\b',
    'data and statistics': r'\b(?:data|statistic|metric|indicator|measure)\b',
    'appropriations': r'\b(?:appropriat|funding|expenditure|spending)\b',
    'veterans affairs': r'\b(?:veteran|military service)\b',
    'small business': r'\b(?:small business|entrepreneur|startup)\b',
    'seabed and oceans': r'\b(?:seabed|ocean|maritime|marine|fishing)\b',
    'elections and democracy': r'\b(?:election|ballot|democrat|authoritarian|voting)\b',
    'peace and security': r'\b(?:peace|ceasefire|truce|war|conflict)\b',
}


def format_title_subtitle(title: str) -> str:
    """제목과 부제를 콜론으로 구분하여 포맷팅.
    규칙: 콜론 앞 공백 없음, 콜론 뒤 반드시 띄어쓰기 한 칸.
    대소문자는 원본 유지.
    예: "Main Title: Subtitle" (O)
        "Main Title : Subtitle" (X)
        "Main Title:Subtitle" (X)
    """
    if not title:
        return title

    # 이미 콜론이 있는 경우: 콜론 주변 공백 정규화
    if ':' in title:
        # 콜론 앞뒤 공백 정리: "Title : Sub" → "Title: Sub", "Title:Sub" → "Title: Sub"
        title = re.sub(r'\s*:\s*', ': ', title)
        return title.strip()

    # 대시(—, –, -)로 구분된 부제를 콜론으로 변환
    # "Main Title — Subtitle" 또는 "Main Title - Subtitle" 패턴
    # 단, 하이픈이 단어 내부에 있는 경우(e.g., "long-term")는 제외
    dash_match = re.match(r'^(.{10,}?)\s+[—–]\s+(.+)$', title)
    if dash_match:
        main = dash_match.group(1).strip()
        sub = dash_match.group(2).strip()
        return f"{main}: {sub}"

    # 긴 하이픈 구분 (단어 내부 하이픈 제외, 앞뒤 공백 있는 경우만)
    dash_match2 = re.match(r'^(.{10,}?)\s+-\s+(.+)$', title)
    if dash_match2:
        main = dash_match2.group(1).strip()
        sub = dash_match2.group(2).strip()
        return f"{main}: {sub}"

    return title.strip()


def extract_document_keywords(doc):
    """Extract actual matching keywords from document title + description.
    Guarantees minimum 3 keywords per document."""
    title = (doc.get('title') or '').lower()
    desc = (doc.get('description') or '').lower()
    text = title + ' ' + desc

    matched_keywords = []
    # Phase 1: Original category-specific patterns (high quality)
    for group_name, patterns in KEYWORD_PATTERNS.items():
        for keyword_label, pattern in patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                matched_keywords.append(keyword_label)

    # Phase 2: Extra broader patterns (fill to minimum 3)
    if len(set(matched_keywords)) < 3:
        for keyword_label, pattern in EXTRA_KEYWORD_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                matched_keywords.append(keyword_label)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for kw in matched_keywords:
        if kw not in seen:
            seen.add(kw)
            unique.append(kw)

    # Phase 3: If still < 3, extract significant words from title as keywords
    if len(unique) < 3:
        STOP_WORDS = {
            'the','a','an','of','and','or','for','in','on','to','is','are','was',
            'were','be','been','being','have','has','had','do','does','did','will',
            'would','could','should','may','might','shall','can','with','at','by',
            'from','up','about','into','through','during','before','after','above',
            'below','between','under','again','further','then','once','here','there',
            'when','where','why','how','all','both','each','few','more','most','other',
            'some','such','no','nor','not','only','own','same','so','than','too',
            'very','just','but','its','their','this','that','these','those','what',
            'which','who','whom','whose','new','report','brief','year','review',
            'key','issues','overview','case','studies','final','update','based',
        }
        title_words = re.findall(r'[a-z]{4,}', title)
        for w in title_words:
            if w not in STOP_WORDS and w not in seen:
                seen.add(w)
                unique.append(w)
            if len(unique) >= 3:
                break

    return ', '.join(unique[:8])  # Max 8 keywords


def get_journal_for_doc(doc):
    """Get journal name from SITE_SERIES_MAP or site name fallback."""
    site_code = doc.get('site_code', '')

    if site_code and site_code in SITE_SERIES_MAP:
        mapping = SITE_SERIES_MAP[site_code]
        if isinstance(mapping, str):
            return mapping
        elif isinstance(mapping, dict):
            return mapping.get('_default', '')

    # Fallback: use site_name + " Publication"
    site_name = doc.get('_site_name', '') or doc.get('site_name', '')
    if site_name:
        return f"{site_name} Publication"

    return ''


def get_volume_info(doc):
    """Generate volume info from published date."""
    pub_date = doc.get('published_date', '')
    if not pub_date:
        return ''

    year_match = re.search(r'(\d{4})', pub_date)
    month_match = re.search(r'(\d{4})-(\d{2})', pub_date)

    if month_match:
        year = month_match.group(1)
        month = month_match.group(2)
        return f"Vol.{year}, No.{month}"
    elif year_match:
        return f"Vol.{year_match.group(1)}"

    return ''


def main():
    with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    print(f"Total docs: {len(docs)}")

    kw_filled = 0
    journal_filled = 0
    volume_filled = 0
    title_formatted = 0

    for doc in docs:
        # Title: 부제 콜론 포맷팅 (대소문자 원본 유지)
        old_title = doc.get('title', '')
        new_title = format_title_subtitle(old_title)
        if new_title != old_title:
            doc['title'] = new_title
            title_formatted += 1

        # Keywords: always extract with min-3 guarantee
        keywords = extract_document_keywords(doc)
        old_kw = doc.get('keywords', '')
        old_count = len([k for k in old_kw.split(',') if k.strip()]) if old_kw else 0
        new_count = len([k for k in keywords.split(',') if k.strip()]) if keywords else 0
        if new_count >= old_count:
            doc['keywords'] = keywords
        if doc.get('keywords'):
            kw_filled += 1

        # Journal
        if not doc.get('journal'):
            journal = get_journal_for_doc(doc)
            if journal:
                doc['journal'] = journal
                journal_filled += 1

        # Volume info
        if not doc.get('volume_info'):
            vol = get_volume_info(doc)
            if vol:
                doc['volume_info'] = vol
                volume_filled += 1

    # Stats
    has_kw = sum(1 for d in docs if d.get('keywords'))
    has_journal = sum(1 for d in docs if d.get('journal'))
    has_volume = sum(1 for d in docs if d.get('volume_info'))
    has_desc = sum(1 for d in docs if d.get('description', '').strip())

    print(f"\nEnrichment results:")
    print(f"  Title formatted: {title_formatted}")
    print(f"  Keywords filled: {kw_filled} (total with keywords: {has_kw})")
    print(f"  Journal filled: {journal_filled} (total with journal: {has_journal})")
    print(f"  Volume filled: {volume_filled} (total with volume: {has_volume})")
    print(f"  Has description: {has_desc}")

    # Keyword distribution
    kw_counts = defaultdict(int)
    for d in docs:
        kw = d.get('keywords', '')
        if kw:
            for k in kw.split(', '):
                kw_counts[k.strip()] += 1
    print(f"\nTop keywords:")
    for k, v in sorted(kw_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  {k}: {v}")

    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nSaved to {SELECTION_FILE}")


if __name__ == '__main__':
    main()
