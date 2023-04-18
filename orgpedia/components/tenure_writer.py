import csv
import datetime
import json
import logging
import sys
from operator import attrgetter
from pathlib import Path

import pydantic
import yaml
from docint.hierarchy import Hierarchy
from docint.vision import Vision
from more_itertools import flatten

from ..extracts.orgpedia import OfficerID, Order, Tenure


# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34


@Vision.factory(
    "tenure_writer",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "tenure_writer",
        "formats": ["json", "csv"],
        "cadre_file_dict": {},
        "hierarchy_files": {
            "dept": "dept.yml",
            "role": "role.yml",
        },
        "post_id_fields": [],
        "output_dir": "output",
        "translations_file": "trans.yml",
    },
)
class TenureWriter:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        formats,
        cadre_file_dict,
        hierarchy_files,
        post_id_fields,
        output_dir,
        translations_file,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.formats = formats
        self.output_dir = Path(output_dir)
        self.hierarchy_files = hierarchy_files
        self.translations_file = self.conf_dir / translations_file

        self.officer_infos = []
        self.officer_id_dict = {}
        for cadre, cadre_file in cadre_file_dict.items():
            officers = OfficerID.from_disk(cadre_file)
            for officer in officers:
                self.officer_id_dict[officer.officer_id] = officer
            self.officer_infos.extend(officers)

        self.hierarchy_dict = {}
        for field, file_name in self.hierarchy_files.items():
            hierarchy_path = self.conf_dir / file_name
            hierarchy = Hierarchy(hierarchy_path)
            self.hierarchy_dict[field] = hierarchy

        self.translations = yaml.load(self.translations_file.read_text(), Loader=yaml.FullLoader)

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None

    def add_log_handler(self):
        handler_name = f"{self.conf_stub}.log"
        log_path = Path("logs") / handler_name
        self.file_handler = logging.FileHandler(log_path, mode="w")
        self.lgr.info(f"adding handler {log_path}")

        self.file_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(self.file_handler)

    def remove_log_handler(self):
        self.file_handler.flush()
        self.lgr.removeHandler(self.file_handler)
        self.file_handler = None

    def get_tenures_header(self):
        keys = Tenure.__fields__.keys()
        return ['officer_name'] + list(keys)

    def get_tenures_csv(self, tenures):
        tenure_dicts = []
        for tenure in tenures:
            name = self.officer_id_dict[tenure.officer_id].name
            fields = [(f, getattr(tenure, f)) for f in Tenure.__fields__.keys()]
            tenure_dicts.append(dict([('officer_name', name)] + fields))
        return tenure_dicts

    def pipe(self, docs, **kwargs):
        self.add_log_handler()
        print("Inside tenure_writer")
        self.lgr.info("Entering tenure writer")

        docs = list(docs)

        # for doc in docs:
        #     doc.remove_all_extra_fields(except_fields=['order', 'tenures', 'words', 'lines', 'page_image'])

        # export removes region fields from docs, region is the parent class for all extracts
        orders = [d.order.export() for d in docs]
        (self.output_dir / 'orders.json').write_text(json.dumps(orders, default=pydantic.json.pydantic_encoder))

        self.tenures = list(flatten(doc.tenures for doc in docs))
        for tenure in self.tenures:
            if tenure.end_order_id == "" and tenure.end_date == datetime.date.today():
                tenure.end_date = "to_date"

        self.tenures.sort(key=attrgetter('tenure_id'))
        (self.output_dir / 'tenures.json').write_text(json.dumps(self.tenures, default=pydantic.json.pydantic_encoder))
        with open((self.output_dir / 'tenures.csv'), 'w') as tenures_csv:
            csv_writer = csv.DictWriter(tenures_csv, fieldnames=self.get_tenures_header())
            csv_writer.writeheader()
            csv_writer.writerows(self.get_tenures_csv(self.tenures))

        with open((self.output_dir / 'tenures-sample.csv'), 'w') as tenures_sample_csv:
            csv_writer = csv.DictWriter(tenures_sample_csv, fieldnames=self.get_tenures_header())
            csv_writer.writeheader()
            csv_writer.writerows(self.get_tenures_csv(self.tenures[:100]))

        officer_infos_path = self.output_dir / 'officer_infos.json'
        for officer_info in self.officer_infos:
            officer_info.language_names = self.translations['names'][officer_info.name]

        self.officer_infos.sort(key=attrgetter('officer_id'))
        officer_infos_path.write_text(json.dumps(self.officer_infos, default=pydantic.json.pydantic_encoder))

        post_infos = {}
        for field in self.hierarchy_files:
            post_infos[field] = self.hierarchy_dict[field].to_dict()
            names = self.hierarchy_dict[field].get_names()  # we should remove aliases, like we are doing in officers

            missing_names = [n for n in names if n not in self.translations[field]]
            if missing_names:
                print(f'Unable to find translations for {missing_names}')
            post_infos[f'translations_{field}'] = self.translations[field]

        (self.output_dir / 'post_infos.json').write_text(json.dumps(post_infos, default=pydantic.json.pydantic_encoder))

        # export removes region fields from docs, region is the parent class for all extracts
        # orders = [d.order.export() for d in docs]
        # (self.output_dir / 'orders.json').write_text(json.dumps(orders, default=pydantic.json.pydantic_encoder))

        ministries = yaml.load((self.conf_dir / 'ministries.yml').read_text(), Loader=yaml.FullLoader)
        (self.output_dir / 'ministries.json').write_text(json.dumps(ministries, default=pydantic.json.pydantic_encoder))

        order_path = self.output_dir / "order.schema.json"
        order_path.write_text(Order.schema_json(indent=2))

        tenure_path = self.output_dir / "tenure.schema.json"
        tenure_path.write_text(Tenure.schema_json(indent=2))
        return docs
