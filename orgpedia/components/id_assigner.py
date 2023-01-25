import datetime
import logging
import sys
from pathlib import Path

from dateutil import parser
from docint.data_error import DataError
from docint.region import Region
from docint.util import load_config, read_config_from_disk
from docint.vision import Vision
from enchant import request_pwl_dict
from more_itertools import first

from ..extracts.orgpedia import OfficerID, OfficerIDNotFoundError

# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34


class OfficerIDInfo(Region):
    officer_id: str
    name: str
    method: str

    @classmethod
    def get_relevant_objects(cls, officerIDInfos, path, shape):
        _, path_detail_idx = path.split(".", 1)
        path_detail_idx = int(path_detail_idx[2:])

        officerIDInfo = officerIDInfos[path_detail_idx]
        return [officerIDInfo]

    def get_html_json(self):
        return f'{{ID: {self.officer_id}, name: {self.name}, method: {self.method} }}'

    def get_svg_info(self):
        return {'idxs': {'person': [w.word_idx for w in self.words]}}


@Vision.factory(
    "id_assigner",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "id_assigner",
        "pre_edit": True,
        "cadre_file_dict": {},
        "post_id_fields": [],
        "tenure_name_file": "prime_minister_tenures.yml",
    },
)
class IDAssigner:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        pre_edit,
        cadre_file_dict,
        post_id_fields,
        tenure_name_file,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = Path(conf_stub)
        self.pre_edit = pre_edit
        self.post_id_fields = post_id_fields
        self.cadre_names_dict = {}
        self.cadre_names_dictionary = {}
        self.officerID_dict = {}

        for cadre, cadre_file in cadre_file_dict.items():
            officers = OfficerID.from_disk(cadre_file)
            names_dict = {}
            for o in officers:
                names = [o.name] + [a["name"] for a in o.aliases]
                names_nows = set(n.replace(" ", "") for n in names)
                [names_dict.setdefault(n.lower(), o.officer_id) for n in names_nows]

                self.officerID_dict[o.officer_id] = o
            self.cadre_names_dict[cadre] = names_dict

            dictionary_file = self.conf_dir / f"{cadre}.dict"
            if not dictionary_file.exists():
                dictionary_file.write_text("\n".join(names_dict.keys()))

            self.cadre_names_dictionary[cadre] = request_pwl_dict(str(dictionary_file))
            
        tenure_name_path = self.conf_dir / tenure_name_file
        self.tenure_name_dict = self.load_tenure_name(tenure_name_path)
        special_roles = ["Prime Minister", "P. M.", "P.M"]
        self.special_roles = [n.replace(" ", "").strip().lower() for n in special_roles]

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None

    def load_tenure_name(self, tenures_file):
        tenure_file_dict = read_config_from_disk(tenures_file)
        tenure_name_dict = {}
        import pdb
        pdb.set_trace()
        for tenure in tenure_file_dict.get("ministries", []):
            s, e = tenure["start"], tenure["end"]
            sDate = parser.parse(s).date()
            #eDate = parser.parse(e).date() if e != "current" else datetime.date.today()
            eDate = parser.parse(e).date() if e != "today" else datetime.date.today()            
            #tenure_name_dict[(sDate, eDate)] = tenure.get("name", tenure.get("pm"))
            tenure_name_dict[(sDate, eDate)] = tenure.get("pm")
        return tenure_name_dict

    def add_log_handler(self, doc):
        handler_name = f"{doc.pdf_name}.{self.conf_stub}.log"
        log_path = Path("logs") / handler_name
        self.file_handler = logging.FileHandler(log_path, mode="w")
        self.lgr.info(f"adding handler {log_path}")

        self.file_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(self.file_handler)

    def remove_log_handler(self, doc):
        self.file_handler.flush()
        self.lgr.removeHandler(self.file_handler)
        self.file_handler = None

    def get_post_id(self, post, path):
        post_id_fields = post.fields if not self.post_id_fields else self.post_id_fields

        field_ids = []
        for field in post_id_fields:
            field_path = getattr(post, f"{field}_hpath")
            field_ids.append(f'{field[0].upper()}:{">".join(field_path)}')
        return ",".join(field_ids), []

    def get_officer_id(self, doc, officer, path):
        def fix_name(name):
            assert name.isascii()
            name.strip(" .-")
            name_nows = name.replace(" ", "").lower()
            return name_nows

        def get_role_name(order_date):
            for (s, e), name in self.tenure_name_dict.items():
                if s < order_date < e:  # the tenures are overlapping on last day
                    return name
            return None

        name = officer.name
        name_nows = fix_name(name)
        if not name_nows:
            return None, []

        min_name = name_nows.replace("the", "")
        if any(r == min_name for r in self.special_roles):
            name = get_role_name(doc.order.date)
            name_nows = fix_name(name)

        names_dict = self.cadre_names_dict[officer.cadre]
        officer_id = names_dict.get(name_nows, None)
        if not officer_id:
            names_dictionary = self.cadre_names_dictionary[officer.cadre]
            suggestion = first(names_dictionary.suggest(name_nows), None)
            officer_id = names_dict.get(suggestion, None)

        errors = []
        if not officer_id:
            idxs = ", ".join(f"{w.path_abbr}->{w.text}<" for w in officer.words)
            msg = f"name: {doc.pdf_name} >{name}< {idxs}"
            errors.append(OfficerIDNotFoundError(path=path, msg=msg))

        return officer_id, errors

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"id_assigner: {doc.pdf_name}")

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)
        conf_officer_ids = doc_config.get("officer_ids", {})

        errors = []
        doc.add_extra_field("officerIDs", ("list", __name__, "OfficerIDInfo"))

        doc.officerIDs = []

        for detail in doc.order.details:
            officer = detail.officer
            method = 'computed'
            if detail.detail_idx in conf_officer_ids:
                officer.officer_id = conf_officer_ids[detail.detail_idx]
                self.lgr.info(f'** Setting officer_id: {officer.officer_id} {officer.name}')
                officer_errors = []
                method = 'manual'

            else:
                officer_id, officer_errors = self.get_officer_id(doc, officer, detail.path)
                officer.officer_id = officer_id if officer_id else officer.officer_id

            officerID = self.officerID_dict.get(officer.officer_id, None)
            words = officer.words if officer else []
            word_idxs = officer.word_idxs if officer else []

            if officerID:
                officerIDInfo = OfficerIDInfo(
                    officer_id=officer.officer_id,
                    name=officerID.full_name,
                    method=method,
                    words=words,
                    word_idxs=word_idxs,
                    page_idx_=detail.page_idx,
                )
            else:
                officerIDInfo = OfficerIDInfo(
                    officer_id="UNKNOWN",
                    name="",
                    method=method,
                    words=words,
                    word_idxs=word_idxs,
                    page_idx_=detail.page_idx,
                )

            doc.officerIDs.append(officerIDInfo)

            errors.extend(officer_errors)
            for post in detail.get_posts("all"):
                post_id, post_errors = self.get_post_id(post, detail.path)
                post.post_id = post_id if post_id else post.post_id
                errors.extend(post_errors)

            self.lgr.debug(detail.to_id_str())

        doc.add_errors(errors)
        self.lgr.info(f"=={doc.pdf_name}.id_assigner {len(doc.order.details)} {DataError.error_counts(errors)}")
        [self.lgr.info(str(e)) for e in errors]
        self.remove_log_handler(doc)
        return doc
