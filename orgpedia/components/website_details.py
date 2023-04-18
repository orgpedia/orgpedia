import json
import logging
import sys
from pathlib import Path

import yaml
from docint.vision import Vision
from more_itertools import first, flatten

from .website_lang_gen import (
    DetailInfo,
    DetailPipeInfo,
    LabelsInfo,
    MinistryInfo,
    OfficerInfo,
    OrderInfo,
    format_lang_date,  # noqa
    lang_year,  # noqa
)

DIGIT_LANG_DICT = {}
TODATE_DICT = {}  # Ugliness


@Vision.factory(
    "detail_language_generator",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "website_generator",
        "officer_info_files": ["conf/wiki_officer.yml"],
        "ministry_file": "conf/ministries.json",
        "output_dir": "output",
        "languages": [],
        "translation_file": "conf/trans.yml",
        "post_infos_file": "conf/post_infos.json",
        "template_stub": "miniHTML",
        "tenures_file": "input/tenures.json",
        "orders_file": "input/orders.json",
    },
)
class WebsiteDetailGenerator:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        officer_info_files,
        ministry_file,
        output_dir,
        languages,
        translation_file,
        post_infos_file,
        template_stub,
        tenures_file,
        orders_file,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.officer_info_files = officer_info_files
        self.ministry_path = Path(ministry_file)
        self.output_dir = Path(output_dir)
        self.languages = languages
        self.translation_file = Path(translation_file)
        self.post_infos_path = Path(post_infos_file)
        self.tenures_file = Path(tenures_file)
        self.orders_file = Path(orders_file)

        if self.post_infos_path.exists():
            post_infos = json.loads(self.post_infos_path.read_text())
            self.depts = [d['name'] for d in post_infos['dept']['ministries']]
            self.depts.append('')
        else:
            self.depts = []

        self.officer_info_dict = self.get_officer_infos(self.officer_info_files)
        print(f"#Officer_info: {len(self.officer_info_dict)}")

        if self.translation_file.exists():
            self.translations = yaml.load(self.translation_file.read_text(), Loader=yaml.FullLoader)
        else:
            self.translations = {}

        global DIGIT_LANG_DICT
        DIGIT_LANG_DICT = self.translations['digits']

        self.lang_label_info_dict = self.build_lang_label_infos(self.translations['labels'])

        global TODATE_DICT
        TODATE_DICT = dict((lang, getattr(ld, 'to_date')) for (lang, ld) in self.lang_label_info_dict.items())

        self.post_dict = {}
        self.order_dict = {}

        self.order_idx_dict = {}
        self.order_info_dict = {}

        self.tenure_dict = {}

        if self.ministry_path.exists():
            if self.ministry_path.suffix == '.yml':
                min_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)
            else:
                min_dict = json.loads(self.ministry_path.read_text())

            self.ministry_infos = self.build_ministryinfos(min_dict)
        else:
            self.ministry_infos = []

        self.template_dir = Path("conf") / Path("templates") / Path(template_stub)

        from jinja2 import Environment, FileSystemLoader, select_autoescape

        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.curr_tenure_idx = 0

    def has_ministry(self):
        return True

    def build_lang_label_infos(self, labels_trans_dict):
        labels = [l for l in labels_trans_dict.keys()]
        labels = [l.replace(',', '').replace(' ', '_').lower() for l in labels]

        languages = self.languages + ['en'] if 'en' not in self.languages else self.languages  # noqa F841

        init_dicts = dict((lang, {}) for lang in languages)

        for (attr, label) in zip(labels, labels_trans_dict.keys()):
            for lang in languages:
                init_dicts[lang][attr] = labels_trans_dict[label][lang]

        label_info_dict = {}
        for (lang, init_dict) in init_dicts.items():
            label_info_dict[lang] = LabelsInfo(init_dict, lang)

        return label_info_dict

    def build_ministryinfos(self, ministry_yml):
        return [MinistryInfo(m) for m in ministry_yml["ministries"]]

    def get_ministry(self, dt):
        ministry = first([m for m in self.ministry_infos if m.has_date(dt)], None)
        return ministry

    def add_log_handler(self):
        handler_name = f"{self.conf_stub}.log"
        log_path = Path("logs") / handler_name
        self.file_handler = logging.FileHandler(log_path, mode="w")
        self.file_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(self.file_handler)

    def remove_log_handler(self):
        self.file_handler.flush()
        self.lgr.removeHandler(self.file_handler)
        self.file_handler = None

    def get_officer_infos(self, officer_info_files):
        result_dict = {}
        for officer_info_file in officer_info_files:
            o_path = Path(officer_info_file)
            if o_path.suffix.lower() == ".yml":
                info_dict = yaml.load(o_path.read_text(), Loader=yaml.FullLoader)
                # TODO, move all the code to name NOT full_name,
                for o in info_dict['officers']:
                    o['full_name'] = o['name']
            else:
                info_dict = json.loads(o_path.read_text())

            info_dict = dict((d["officer_id"], OfficerInfo(d, idx + 1)) for idx, d in enumerate(info_dict["officers"]))
            result_dict = {**result_dict, **info_dict}
            print(f"\t{officer_info_file} {len(info_dict)} {len(result_dict)}")
        return result_dict

    def gen_details_page(self, doc, order_info):
        order = doc.order
        detail_pplns_list = [self.build_detail_ppln_infos(d, doc) for d in order.details]

        json_detail_list = [d.to_json() for d in order_info.details]
        order_info.details_json_str = json.dumps(
            json_detail_list, separators=(',', ':'), ensure_ascii=False
        )  # , indent=2)

        json_ppln_list = []
        for detail_pplns in detail_pplns_list:
            detail_json_list = [d.to_json() for d in detail_pplns]
            json_ppln_list.append(detail_json_list)
        order_info.details_ppln_json_str = json.dumps(
            json_ppln_list, separators=(',', ':'), ensure_ascii=False
        )  # , indent=2)

        html_path = self.get_html_path("details", order.order_id, 'en')

        # crumbs

        # rendering it here as no translations.
        site_info = self.lang_label_info_dict['en']
        site_info.title = f'Order Details: ({order.order_id})'
        site_info.page_url = f'details-{order.order_id}.html'
        template = self.env.get_template("details.html")
        first_detail_pplns = detail_pplns_list[0]
        order_info.ppln_crumbs = [
            (site_info.home, 'prime.html'),
            (site_info.orders, f'orders-{order_info.get_ministry_years_str()}.html'),
            (order_info.order_id, f'order-{order_info.order_id}.html'),
            ('Detail-1', 'Detail-1'),
        ]
        html_path.write_text(
            template.render(
                site=site_info, order=order_info, detail=order_info.details[0], detail_ppln=first_detail_pplns
            )
        )

    def build_orderinfo(self, order):
        details = []
        for d in order.details:
            officer_id = d.officer.officer_id
            if not officer_id:
                continue
            officer_info = self.officer_info_dict[officer_id]
            officer_url = officer_info.url
            officer_name = officer_info.full_name
            officer_image_url = officer_info.image_url
            details.append(DetailInfo(d, officer_url, officer_name, officer_image_url, order.order_id))

        num_pages = max(d.page_idx for d in order.details) + 1
        if self.has_ministry():
            m = self.get_ministry(order.date)
            assert m, f'Unknown {order.date} in {order.order_id}'
            order_info = OrderInfo(order, details, m.name, m.start_date, m.end_date, num_pages)
        else:
            order_info = OrderInfo(order, details, '', None, None, num_pages)

        return order_info

    def build_detail_ppln_infos(self, detail, doc):
        def flatten_list(lists):
            if lists and isinstance(lists[0], str):
                return lists
            else:
                return list(flatten(lists))

        detail_pipes = []
        for pipe_idx, pipe in enumerate(reversed(doc.pipe_names)):
            detail_pipes.append(DetailPipeInfo(pipe, pipe_idx, detail, doc))

        all_detail_paths = set(flatten(p.object_paths for p in detail_pipes))

        for pipe in detail_pipes:
            pipe_errors = doc.get_errors(pipe.pipe_name)
            detail_errors = [e for e in pipe_errors if e.path in all_detail_paths]
            pipe.add_errors(detail_errors, pipe_errors)

            # pipe_edits = doc.get_errors(pipe.pipe_name)
            # detail_edits = [e for e in pipe_edits if set(e.paths) & all_detail_paths]
            # pipe.add_edits(detail_edits, pipe_edits)
        return detail_pipes

    def get_html_path(self, entity, idx, lang=None):
        idx = idx.replace(' ', '_')

        print(f'{entity} -> {idx}')
        if lang:
            lang_dir = self.output_dir / lang
            if not lang_dir.exists():
                lang_dir.mkdir(parents=True)

        if idx:
            if lang:
                return self.output_dir / lang / f"{entity}-{idx}.html"
            else:
                return self.output_dir / f"{entity}-{idx}.html"
        else:
            assert entity in ("orders", "prime", "deputy", "ministry"), f"{idx} is empty for {entity}"
            if lang:
                return self.output_dir / lang / f"{entity}.html"
            else:
                return self.output_dir / f"{entity}.html"

    def __call__(self, doc):
        self.add_log_handler()
        order_info = self.build_orderinfo(doc.order)
        self.gen_details_page(doc, order_info)
        self.remove_log_handler()
        return doc
