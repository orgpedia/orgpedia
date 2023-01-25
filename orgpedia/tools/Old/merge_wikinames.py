import json
import sys
from pathlib import Path

new_path = Path(sys.argv[1])
json_path = Path(sys.argv[2])

assert new_path.suffix.lower() == '.yml'
assert json_path.suffix.lower() == '.json'

json_officers = json.loads(json_path.read_text())['officers']

json_wiki_ids = set(o['officer_id'] for o in json_officers)

new_officer_dict = {}

for line in new_path.read_text().split('\n'):
    line = line.strip()
    if not line:
        continue

    doc_name, wiki_name_stub = line.split(':')
    doc_name = doc_name.strip()

    wiki_id_pos = wiki_name_stub.index('(Q')
    wiki_name = wiki_name_stub[:wiki_id_pos].strip()
    wiki_id = wiki_name_stub[wiki_id_pos:].replace('(', '').replace(')', '').strip()
    if wiki_id in json_wiki_ids:
        print(f'{wiki_id}|{doc_name}|{wiki_name}')
    else:
        new_officer_dict.setdefault((wiki_id, wiki_name), []).append(doc_name)

new_officers = []
for ((wiki_id, wiki_name), aliases) in new_officer_dict.items():
    o = {
        "officer_id": wiki_id,
        "name": wiki_name,
        "full_name": wiki_name,
    }
    new_aliases = [n for n in aliases if n != wiki_name]
    if new_aliases:
        o['aliases'] = [{'name': n, 'full_name': n} for n in new_aliases]

    new_officers.append(o)
Path('new.json').write_text(json.dumps({'officers': new_officers}, indent=2))
