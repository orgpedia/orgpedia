import json
import sys
from pathlib import Path

import yaml
import wikidata


json_path = Path(sys.argv[1])
yaml_path = Path(sys.argv[2])

json_officers = json.loads(json_path.read_text())['officers']
yaml_officers = yaml.load(yaml_path.read_text(), Loader=yaml.FullLoader)['officers']

yaml_officer_dict = dict((yo['officer_id'], yo) for yo in yaml_officers)

from wikidata.client import Client
client = Client()

processed_id_dict = {}
if Path('new.yml').exists():
    processed_officers = yaml.load(Path('new_yaml').read_text(), Loader=yaml.FullLoader)['officers']
    processed_id_dict = dict((o['officer_id'], o) for o in processed_officers)
    

print(f'Processed: {len(processed_id_dict)}')

new_yaml_officers = []
for o in json_officers:
    if o['officer_id'] in processed_id_dict:
        new_yaml_officers.append(processed_id_dict[o['officer_id']])
        print(f'ignoring {o["officer_id"]}')
        continue
    
    yd = {'officer_id': o['officer_id'], 'name': o['name'], 'full_name': o['full_name']}
    
    image_url = yaml_officer_dict.get(o['officer_id'], {}).get('image_url', '')
    if image_url:
        yd['image_url'] = image_url
    else:
        entity = client.get(o['officer_id'], load=True)
        image_prop = client.get('P18')
        image = entity.get(image_prop, None)
        if image: 
            image_url = image.image_url        
            if image_url:
                sys.stderr.write(f'found image_url {o["officer_id"]} {o["name"]} {image_url}\n')
                yd['image_url'] = image_url

    if 'aliases' in o:
        yd['aliases'] = o['aliases']

    new_yaml_officers.append(yd)
    Path('new.yml').write_text(yaml.dump({'officers': new_yaml_officers}, default_flow_style=False, sort_keys=False))
    
