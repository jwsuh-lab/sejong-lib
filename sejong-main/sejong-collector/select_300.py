import csv, json, os, glob, sys, io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ========= 1. Load completed titles + excluded titles =========
completed_titles = set()
with open('completed sites.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) >= 5:
            title = row[4].strip().lower()
            if title:
                completed_titles.add(title)

# Also load previously excluded titles (pre-2026, junk, non-policy, etc.)
try:
    with open('data/excluded_titles.json', 'r', encoding='utf-8') as f:
        excluded_titles = set(json.load(f))
    completed_titles.update(excluded_titles)
    print(f'Loaded {len(excluded_titles)} excluded titles')
except FileNotFoundError:
    pass

# ========= 2. Keywords ENHANCED =========
keyword_groups = {
    'AI_디지털_반도체': [
        'artificial intelligence', 'ai ', ' ai,', 'semiconductor', 'chip',
        'cybersecurity', 'quantum computing', 'machine learning', 'data privacy',
        'digital economy', 'platform regulation', 'generative ai', 'deep learning',
        'autonomous', 'large language model', 'robotics', 'algorithm'
    ],
    '기후_에너지': [
        'climate change', 'carbon', 'net zero', 'renewable energy', 'green transition',
        'esg', 'emission', 'hydrogen', 'energy security', 'electric vehicle',
        'paris agreement', 'cop2', 'biodiversity', 'sustainability', 'decarboni',
        'carbon tax', 'nuclear energy', 'climate adaptation', 'climate resilience',
        'energy subsidy', 'critical minerals', 'clean energy', 'fossil fuel',
        'solar', 'wind power', 'green bond'
    ],
    '경제_통상': [
        'trade', 'tariff', 'supply chain', 'inflation', 'recession', 'gdp', 'fdi',
        'sanctions', 'wto', 'tax reform', 'fiscal', 'monetary', 'economic growth',
        'export', 'import', 'debt', 'interest rate', 'central bank', 'financial stability'
    ],
    '안보_외교': [
        'defense', 'defence', 'security', 'geopolitics', 'indo-pacific', 'nuclear',
        'nato', 'deterrence', 'military', 'foreign policy', 'diplomacy', 'conflict',
        'maritime', 'space security', 'intelligence', 'arms control', 'alliance'
    ],
    '인구_복지': [
        'aging', 'demographic', 'fertility', 'pension', 'welfare', 'childcare',
        'immigration', 'labor shortage', 'social security', 'inequality', 'gender',
        'disability', 'elderly', 'migration', 'poverty', 'minimum wage', 'social protection'
    ],
    '보건': [
        'public health', 'pandemic', 'healthcare', 'pharmaceutical', 'mental health',
        'vaccination', 'disease', 'health system', 'antimicrobial', 'health insurance',
        'health equity', 'universal health coverage', 'primary care', 'long-term care',
        'health workforce', 'epidemi', 'mortality', 'chronic disease', 'patient',
        'hospital', 'clinical', 'biomedical', 'drug', 'opioid', 'tobacco',
        'nutrition', 'obesity'
    ],
    '주거_도시': [
        'housing', 'real estate', 'urban planning', 'infrastructure', 'transportation',
        'smart city', 'affordable housing', 'public transport', 'broadband',
        'water supply', 'waste management'
    ],
    '교육': [
        'education', 'higher education', 'stem', 'workforce development', 'skills',
        'vocational training', 'lifelong learning', 'digital literacy', 'student',
        'school', 'university', 'teacher', 'curriculum'
    ],
}


def score_document(doc):
    title = (doc.get('title') or '').lower()
    desc = (doc.get('description') or '').lower()
    text = title + ' ' + desc

    score = 0
    matched_groups = []

    for group, keywords in keyword_groups.items():
        group_score = 0
        for kw in keywords:
            if kw in title:
                group_score = max(group_score, 0.15)
            elif kw in text:
                group_score = max(group_score, 0.05)
        if group_score > 0:
            score += group_score
            matched_groups.append(group)

    # Document type bonus
    dtype = (doc.get('document_type') or '').lower()
    if any(t in dtype for t in ['report', 'policy_paper', 'corporate_report']):
        score += 0.3
    elif any(t in dtype for t in ['research', 'working_paper', 'policy_brief']):
        score += 0.2
    elif any(t in dtype for t in ['publication', 'guidance']):
        score += 0.1

    return score, matched_groups


# ========= 3. Process all non-UK files =========
all_docs = []
data_dir = 'data'

for f in sorted(glob.glob(os.path.join(data_dir, '*.json'))):
    fname = os.path.basename(f)
    if 'summary' in fname or fname.startswith('govuk_') or fname.startswith('gb_'):
        continue
    if 'selection_' in fname:
        continue
    try:
        with open(f, 'r', encoding='utf-8') as fp:
            d = json.load(fp)
            docs = d.get('documents', [])
            prefix = fname.split('_')[0]
            meta = d.get('metadata', {})
            for doc in docs:
                t = doc.get('title', '').strip()
                if not t or len(t) < 5:
                    continue
                if t.lower() in completed_titles:
                    continue
                # Quality filter: skip non-policy junk
                t_lower = t.lower()
                skip_patterns = [
                    'events', 'multimedia', 'translations', 'podcasts', 'podcast',
                    'billing address', 'copyright notice', 'language policy',
                    'follow the', 'social media', 'job opportunities',
                    'recruitment@', '@ec.europa.eu', 'www.', 'http',
                    'young voices', 'war fare', 'urban futures', 'terra nova',
                    'space tracker', 'raisina debates', 'post aid world',
                    'digital frontiers', 'atlantic files',
                    'trusted websites', 'subscribe to', 'all publications',
                    'resources for partners', 'report an it', 'languages on our',
                    'india with africa', 'a conversation with',
                    'commonhealth live',
                    # Category/navigation pages
                    'european institutions & policies', 'democracy, identity',
                    'european economy policy briefs', 'european economy',
                    'states weekly:', 'code of practice for',
                    'moratorium on genetic', 'national population health survey',
                    # Junk / navigation / UI elements
                    'read more notices', 'view more videos', 'featured content',
                    'our commitments', 'more information', 'available online',
                    'research & publications', 'part of: digital',
                    'state of the union', 'archive.cdc.gov',
                    'view all', 'show more', 'see all', 'load more',
                    'back to top', 'skip to content', 'main menu',
                    'press release', 'news release', 'media advisory',
                    'foto:', 'photo:', 'video:', 'infographic:',
                    'collectionsremove', 'remove filter',
                    'support new america', 'web performance metrics',
                    'volume 32,', 'volume 31,', 'volume 30,',
                    'angel family day', 'anniversary of the battle',
                    'guidance snapshot pilot', 'guidance agenda',
                    'petroleum & other liquids', 'fact sheet',
                    'mins read', # Singapore CSA format "28 NOV 202512 mins read..."
                    # Canadian navigation/generic pages
                    'forests and forestry', 'energy sources and distribution',
                    'public consultations', 'programs and services',
                    'your canadian summer', 'celebrate black history',
                    'learn how to spot', 'make the most of your money',
                    'participate in a radon', 'measles: what you should know',
                    'starts here', 'the honourable',
                    'corporate management and reporting', 'general publications',
                    'plans and reports', 'corporate publications',
                    'criminal justice', 'indigenous peoples',
                    'other publications', 'health concerns',
                    'environmental and workplace', 'travel and tourism',
                    'business and industry', 'environment and natural',
                    'money and finances', 'building a strong economy',
                    'assault-style firearms', 'milano cortina 2026',
                    'official international reserves', 'red tape review',
                    # Generic/non-substantive
                    'open door forum', 'superintendent at',
                    'blueprint for the canada innovation',
                ]
                if any(sp in t_lower for sp in skip_patterns):
                    continue
                if len(t) < 15:
                    continue

                # Skip non-English titles (applied to ALL countries)
                eng_stopwords = [' the ', ' of ', ' and ', ' for ', ' in ', ' on ',
                                 ' to ', ' is ', ' are ', ' was ', ' with ', ' an ',
                                 ' by ', ' at ', ' from ']
                has_english = any(sw in (' ' + t_lower + ' ') for sw in eng_stopwords)
                eng_count = sum(1 for sw in eng_stopwords if sw in (' ' + t_lower + ' '))
                if not has_english and not any(t_lower.startswith(p) for p in
                    ['report', 'review', 'analysis', 'impact', 'policy', 'study',
                     'assessment', 'evaluation', 'survey', 'strategy', 'plan',
                     'national', 'global', 'international', 'annual', 'best practice']):
                    continue

                # Skip titles with high presence of non-English function words
                non_eng_stopwords = {'und', 'der', 'die', 'das', 'les', 'une', 'pour', 'del',
                                     'della', 'nella', 'degli', 'fra', 'til', 'med', 'och',
                                     'det', 'som', 'ett', 'naar', 'voor', 'het', 'sur', 'dans',
                                     'avec', 'denne', 'dette', 'disse', 'delle', 'sulle',
                                     'hacia', 'sobre', 'entre', 'desde'}
                title_words = set(t_lower.split())
                non_eng_hits = len(title_words & non_eng_stopwords)
                if non_eng_hits >= 2:
                    continue

                # Skip non-English content (high non-ASCII ratio or non-ASCII with few English words)
                non_eng_chars = sum(1 for c in t if ord(c) > 127)
                if non_eng_chars / max(len(t), 1) > 0.15:
                    continue
                if non_eng_chars > 3 and eng_count < 2:
                    continue

                # Date filter: only 2026+ documents
                pub_date = doc.get('published_date', '')
                if pub_date:
                    import re as _re
                    year_match = _re.search(r'(\d{4})', pub_date)
                    if year_match:
                        year = int(year_match.group(1))
                        if year < 2026:
                            continue

                # URL-based date filter: skip URLs with /2020/ through /2024/
                link = doc.get('link', '')
                import re as _re2
                url_year = _re2.search(r'/(\d{4})/', link)
                if url_year:
                    uy = int(url_year.group(1))
                    if 2000 <= uy < 2026:
                        continue

                # Title-based date filter: "Month 2024 (Archived)" etc
                months_pat = r'(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(20[12]\d)'
                title_year = _re2.search(months_pat, t_lower)
                if title_year and int(title_year.group(1)) < 2026:
                    continue
                if 'archived' in t_lower:
                    arc_year = _re2.search(r'(20[12]\d)', t_lower)
                    if arc_year and int(arc_year.group(1)) < 2026:
                        continue

                score, groups = score_document(doc)
                doc['_relevance_score'] = score
                doc['_matched_keywords'] = groups
                doc['_country'] = prefix
                doc['_source_file'] = fname
                doc['_site_name'] = meta.get('site_name', doc.get('site_name', ''))
                doc['_site_acronym'] = meta.get('acronym', doc.get('site_acronym', ''))
                all_docs.append(doc)
    except Exception as e:
        print(f'Error processing {fname}: {e}')

# Deduplicate by title
seen_titles = set()
dedup_docs = []
for d in all_docs:
    t_key = d.get('title', '').strip().lower()
    if t_key not in seen_titles:
        seen_titles.add(t_key)
        dedup_docs.append(d)
print(f'Before dedup: {len(all_docs)}, After dedup: {len(dedup_docs)}')
all_docs = dedup_docs

print(f'Total eligible documents: {len(all_docs)}')

# Check keyword pool
kw_total = defaultdict(int)
for d in all_docs:
    for g in d['_matched_keywords']:
        kw_total[g] += 1
print(f'\nKeyword pool (all docs, enhanced keywords):')
for k, v in sorted(kw_total.items(), key=lambda x: -x[1]):
    print(f'  {k}: {v}')

# ========= 4. Select 330 with country+keyword balance =========
all_docs.sort(key=lambda x: -x['_relevance_score'])

country_quota = {
    'us': 170, 'ca': 45, 'se': 25, 'sg': 25, 'no': 20,
    'it': 20, 'eu': 15, 'at': 12, 'sa': 10, 'in': 10,
    'ge': 8, 'gr': 8, 'jo': 8, 'nz': 8, 'be': 8,
    'nl': 8, 'ch': 3, 'fi': 3, 'hu': 3,
}

# Keyword minimum quotas (enhanced for underrepresented)
kw_min = {
    '보건': 18,
    '기후_에너지': 20,
    '인구_복지': 22,
}

selected = []
country_selected = defaultdict(int)
kw_selected = defaultdict(int)
selected_ids = set()

# Pass 1: Ensure keyword minimums for underrepresented categories
for kw_group, min_count in kw_min.items():
    kw_candidates = [d for d in all_docs if kw_group in d['_matched_keywords'] and id(d) not in selected_ids]
    kw_candidates.sort(key=lambda x: -x['_relevance_score'])
    added = 0
    for doc in kw_candidates:
        c = doc['_country']
        quota = country_quota.get(c, 3)
        if country_selected[c] < quota and added < min_count:
            selected.append(doc)
            selected_ids.add(id(doc))
            country_selected[c] += 1
            for g in doc['_matched_keywords']:
                kw_selected[g] += 1
            added += 1

print(f'\nAfter keyword minimum pass: {len(selected)}')

# Pass 2: Fill country quotas
for doc in all_docs:
    if id(doc) in selected_ids:
        continue
    c = doc['_country']
    quota = country_quota.get(c, 3)
    if country_selected[c] < quota:
        selected.append(doc)
        selected_ids.add(id(doc))
        country_selected[c] += 1
        for g in doc['_matched_keywords']:
            kw_selected[g] += 1
    if len(selected) >= 500:
        break

# Pass 3: Fill remaining from top-scored (still respect country max = quota * 1.5)
if len(selected) < 400:
    for doc in all_docs:
        if id(doc) not in selected_ids:
            c = doc['_country']
            max_quota = int(country_quota.get(c, 3) * 1.5)
            if country_selected[c] < max_quota:
                selected.append(doc)
                selected_ids.add(id(doc))
                country_selected[c] += 1
                for g in doc['_matched_keywords']:
                    kw_selected[g] += 1
                if len(selected) >= 500:
                    break

print(f'Total selected: {len(selected)}')

# ========= 5. Stats =========
print(f'\n{"="*50}')
print(f'COUNTRY DISTRIBUTION')
print(f'{"="*50}')
final_country = defaultdict(int)
for d in selected:
    final_country[d['_country']] += 1
for k, v in sorted(final_country.items(), key=lambda x: -x[1]):
    pct = v / len(selected) * 100
    print(f'  {k}: {v} ({pct:.1f}%)')

print(f'\n{"="*50}')
print(f'KEYWORD DISTRIBUTION (enhanced)')
print(f'{"="*50}')
kw_dist = defaultdict(int)
for d in selected:
    for g in d.get('_matched_keywords', []):
        kw_dist[g] += 1
for k, v in sorted(kw_dist.items(), key=lambda x: -x[1]):
    print(f'  {k}: {v}')

# No keyword match
no_kw = sum(1 for d in selected if not d.get('_matched_keywords'))
print(f'  (no keyword match): {no_kw}')

# Score stats
scores = [d['_relevance_score'] for d in selected]
print(f'\n{"="*50}')
print(f'RELEVANCE SCORES')
print(f'{"="*50}')
print(f'  Average: {sum(scores)/len(scores):.3f}')
print(f'  Min: {min(scores):.3f}')
print(f'  Max: {max(scores):.3f}')

# Score distribution
buckets = defaultdict(int)
for s in scores:
    if s >= 0.5:
        buckets['0.50+'] += 1
    elif s >= 0.3:
        buckets['0.30-0.49'] += 1
    elif s >= 0.15:
        buckets['0.15-0.29'] += 1
    else:
        buckets['0.00-0.14'] += 1
print(f'\n  Score distribution:')
for k in ['0.50+', '0.30-0.49', '0.15-0.29', '0.00-0.14']:
    print(f'    {k}: {buckets[k]}')

# Low score documents for manual review
low_docs = [d for d in selected if d['_relevance_score'] < 0.15]
print(f'\n{"="*50}')
print(f'LOW SCORE DOCS (< 0.15): {len(low_docs)} - MANUAL REVIEW NEEDED')
print(f'{"="*50}')
for d in low_docs:
    title = d.get('title', '')[:90]
    print(f'  [{d["_country"]}] score={d["_relevance_score"]:.2f} | {title}')
    if d['_matched_keywords']:
        print(f'         keywords: {", ".join(d["_matched_keywords"])}')
    else:
        print(f'         keywords: NONE')

# Save
with open('data/selection_500.json', 'w', encoding='utf-8') as fp:
    json.dump(selected, fp, ensure_ascii=False, indent=2, default=str)
print(f'\nSaved to data/selection_500.json')
