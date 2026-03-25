"""
selection_330.json에서 중복 및 2024 이전 자료 제거 후 재정렬.
- 중복: 동일 title 또는 동일 link로 판별
- 2024 이전: URL에 /2024/, /2023/ 포함 또는 제목에 "2024", "2023" 포함 + Archived
"""
import json
import re
import sys
import io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

SELECTION_FILE = 'data/selection_330.json'

with open(SELECTION_FILE, 'r', encoding='utf-8') as f:
    docs = json.load(f)

print(f"Before: {len(docs)} docs")

# 1. Remove duplicates (by title, case-insensitive)
seen_titles = set()
seen_links = set()
unique_docs = []
dup_count = 0

for doc in docs:
    title = doc.get('title', '').strip().lower()
    link = doc.get('link', '').strip()

    if title in seen_titles:
        dup_count += 1
        print(f"  DUP (title): {doc.get('title', '')[:80]}")
        continue
    if link and link in seen_links:
        dup_count += 1
        print(f"  DUP (link): {doc.get('title', '')[:80]}")
        continue

    seen_titles.add(title)
    if link:
        seen_links.add(link)
    unique_docs.append(doc)

print(f"\nRemoved {dup_count} duplicates, remaining: {len(unique_docs)}")

# 2. Remove pre-2026 documents by URL pattern or title
pre_2026_patterns = [
    r'/202[0-4]/',          # URL contains /2020/ through /2024/
    r'/202[0-4]\b',         # URL ends with 2020-2024
]
title_pre_2026 = [
    'archived',             # "Fiscal Monitor - December 2024 (Archived)"
]

filtered_docs = []
removed_pre2026 = 0

for doc in unique_docs:
    link = doc.get('link', '')
    title = doc.get('title', '').lower()

    # Check URL for pre-2026 year patterns
    is_pre_2026 = False
    for pattern in pre_2026_patterns:
        if re.search(pattern, link):
            # Verify it's actually a year in URL path, not something else
            year_in_url = re.search(r'/(\d{4})/', link)
            if year_in_url:
                year = int(year_in_url.group(1))
                if year < 2026:
                    is_pre_2026 = True
                    break

    # Check title for "archived" + year
    if not is_pre_2026 and 'archived' in title:
        year_match = re.search(r'(20[12]\d)', title)
        if year_match and int(year_match.group(1)) < 2026:
            is_pre_2026 = True

    # Check title for explicit old year references like "December 2024"
    if not is_pre_2026:
        months = ['january', 'february', 'march', 'april', 'may', 'june',
                  'july', 'august', 'september', 'october', 'november', 'december']
        for month in months:
            pattern = rf'{month}\s+(20[12]\d)'
            m = re.search(pattern, title)
            if m and int(m.group(1)) < 2026:
                is_pre_2026 = True
                break

    if is_pre_2026:
        removed_pre2026 += 1
        print(f"  PRE-2026: {doc.get('title', '')[:80]} | {link[:60]}")
        continue

    filtered_docs.append(doc)

print(f"\nRemoved {removed_pre2026} pre-2026 docs, remaining: {len(filtered_docs)}")

# 3. Sort by relevance score
filtered_docs.sort(key=lambda x: -x.get('_relevance_score', 0))

# Save
with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
    json.dump(filtered_docs, f, ensure_ascii=False, indent=2, default=str)

print(f"\nFinal: {len(filtered_docs)} docs saved")

# Stats
from collections import Counter
countries = Counter(d.get('_country', '') for d in filtered_docs)
print(f"\nCountry distribution:")
for c, n in countries.most_common():
    print(f"  {c}: {n}")
