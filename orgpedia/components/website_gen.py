import datetime
import json
import logging
import sys
from itertools import groupby
from operator import attrgetter
from pathlib import Path

import yaml
from dateutil import parser
from more_itertools import flatten

from docint.vision import Vision

# from jinja2 import Environment, FileSystemLoader, select_autoescape

# b /Users/mukund/Software/docInt/docint/pipeline/website_gen.py:164


class OfficerInfo:
    def __init__(self, yml_dict):
        self.officer_id = yml_dict["officer_id"]
        self.image_url = yml_dict.get("image_url", "")
        self.full_name = yml_dict["full_name"]
        self.key_tenures = []
        self.ministries = []
        self.url = ""
        self.officer_idx = -1

    @property
    def first_char(self):
        return self.full_name[0] if self.full_name else ''

    def get_searchdoc_dict(self):
        doc = {}
        doc["idx"] = self.officer_idx
        doc["full_name"] = self.full_name
        doc["officer_id"] = self.officer_id
        doc["image_url"] = self.image_url
        doc["url"] = self.url
        for (k_idx, key_tenure) in enumerate(self.key_tenures):
            doc[f"key_dept{k_idx+1}"] = key_tenure.dept
            doc[f"key_start{k_idx+1}"] = key_tenure.start_month_year
            doc[f"key_end{k_idx+1}"] = key_tenure.end_month_year
        return doc


class TenureInfo:
    def __init__(self, tenure, post, start_order_url, end_order_url):
        self.tenure = tenure
        self.post = post
        self.start_order_url = start_order_url
        self.end_order_url = end_order_url

    @property
    def dept(self):
        return self.post.dept

    @property
    def role(self):
        return self.post.role

    @property
    def start_month_year(self):
        return self.tenure.start_date.strftime("%b %Y")

    @property
    def end_month_year(self):
        return self.tenure.end_date.strftime("%b %Y")

    @property
    def start_date_str(self):
        return self.tenure.start_date.strftime("%d %b %Y")

    @property
    def end_date_str(self):
        return self.tenure.end_date.strftime("%d %b %Y")

    @property
    def start_order_id(self):
        return self.tenure.start_order_id

    @property
    def end_order_id(self):
        return self.tenure.end_order_id


PAGEURL = "/Users/mukund/orgpedia/rajpol/import/html/"


class OrderInfo:
    def __init__(self, order, details, ministry, ministry_start_date, num_pages):
        def get_image_url(idx):
            idx += 1
            return f'{PAGEURL}{order.order_id.replace(".pdf","")}/svg-{idx:03d}.svg'

        self.order = order
        self.ministry = ministry
        self.ministry_start_date = ministry_start_date
        self.details = details
        self.pages = [get_image_url(idx) for idx in range(num_pages)]

    @property
    def order_id(self):
        return self.order.order_id

    @property
    def order_number(self):
        return self.order.order_number

    @property
    def date(self):
        return self.order.date

    @property
    def date_str(self):
        return self.order.date.strftime("%d %b %Y")

    @property
    def url(self):
        return f"file:///Users/mukund/orgpedia/rajpol/import/html/{self.order.order_id}.html"

    @property
    def url_name(self):
        return f"http://rajasthan.police.gov.in/{self.order.order_id}"


class DetailInfo:
    def __init__(self, detail, officer_idx, officer_name):
        self.officer_url = f"officer-{officer_idx}.html"
        self.name = officer_name
        self.post_str = self.get_all_post_str(detail)
        self.idx = detail.detail_idx

    def get_all_post_str(self, detail):
        def get_posts_str(posts, pType):
            if not posts:
                return ""

            postStrs = [f"<b>{pType}:</b>"]
            for post in posts:
                dept, role = post.dept, post.role
                pStr = dept if not role else f"{dept}[{role}]"
                pStr = "" if pStr is None else pStr
                postStrs.append(pStr)
            return "<br>".join(postStrs)

        strs = []
        for pType in ["continues", "relinquishes", "assumes"]:
            posts = getattr(detail, pType)
            strs.append(get_posts_str(posts, pType))
        return "<br>".join(strs)


@Vision.factory(
    "website_generator",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "website_generator",
        "officer_info_files": ["conf/wiki_officer.yml"],
        "ministry_file": "conf/ministries.yml",
        "output_dir": "output",
    },
)
class WebsiteGenerator:
    def __init__(
        self, conf_dir, conf_stub, officer_info_files, ministry_file, output_dir
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.officer_info_files = officer_info_files
        self.ministry_path = Path(ministry_file)
        self.output_dir = Path(output_dir)

        self.officer_info_dict = self.get_officer_infos(self.officer_info_files)
        print(f"#Officer_info: {len(self.officer_info_dict)}")

        self.post_dict = {}
        self.order_dict = {}

        self.order_idx_dict = {}
        self.officer_idx_dict = {}
        self.order_info_dict = {}

        if self.ministry_path.exists():
            self.ministry_dict = yaml.load(
                self.ministry_path.read_text(), Loader=yaml.FullLoader
            )
            for m in self.ministry_dict["ministries"]:
                s, e = m["start_date"], m["end_date"]
                m["start_date"] = parser.parse(s).date()
                m["end_date"] = (
                    parser.parse(e).date() if e != "today" else datetime.date.today()
                )
        else:
            self.ministry_dict = {}

        from jinja2 import Environment, FileSystemLoader, select_autoescape

        self.env = Environment(
            loader=FileSystemLoader("conf/templates"), autoescape=select_autoescape()
        )

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.curr_tenure_idx = 0

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
            else:
                info_dict = json.loads(o_path.read_text())

            info_dict = dict(
                (d["officer_id"], OfficerInfo(d)) for d in info_dict["officers"]
            )
            result_dict = {**result_dict, **info_dict}
            print(f"\t{officer_info_file} {len(info_dict)} {len(result_dict)}")
        return result_dict

    def build_tenureinfo(self, tenure):
        post = self.post_dict[tenure.post_id]

        start_order = self.order_dict[tenure.start_order_id]
        start_page_idx = tenure.get_start_page_idx(start_order)
        start_order_id = tenure.start_order_id
        start_url = f"order-{start_order_id}.html#Page{start_page_idx + 1}"

        if tenure.end_order_id:
            end_order = self.order_dict[tenure.end_order_id]
            end_page_idx = tenure.get_end_page_idx(end_order)
            end_order_id = tenure.end_order_id
            end_url = f"order-{end_order_id}.html#Page{end_page_idx + 1}"
        else:
            end_url = ""

        return TenureInfo(tenure, post, start_url, end_url)

    def build_orderinfo(self, order):
        details = []
        for d in order.details:
            officer_id = d.officer.officer_id
            if not officer_id:
                continue
            officer_idx = self.officer_idx_dict.get(officer_id, 0)
            officer_name = self.officer_info_dict[officer_id].full_name
            details.append(DetailInfo(d, officer_idx, officer_name))

        num_pages = order.details[-1].page_idx + 1
        ministry, ministry_start_date = self.get_ministry(order.date)

        order_info = OrderInfo(order, details, ministry, ministry_start_date, num_pages)
        return order_info

    def get_html_path(self, entity, idx):
        if idx:
            return self.output_dir / f"{entity}-{idx}.html"
        else:
            return self.output_dir / f"{entity}.html"

    def render_html(self, entity, obj):
        template = self.env.get_template(f"{entity}.html.jinja")
        if entity == "officer":
            return template.render(officer=obj)
        elif entity == "officers":
            return template.render(officer_groups=obj)
        elif entity == "orders":
            return template.render(order_groups=obj)
        else:
            return template.render(order=obj)

    def gen_order_page(self, order_idx, order):
        if not order.details:
            return

        print(f"> {order.order_id} {str(order.date)}")

        order_info = self.build_orderinfo(order)
        self.order_info_dict[order.order_id] = order_info

        html_path = self.get_html_path("order", order.order_id)
        html_path.write_text(self.render_html("order", order_info))

    def get_ministry(self, date):
        if not self.ministry_dict:
            return "No Ministry", None

        for m in self.ministry_dict["ministries"]:
            if m["start_date"] <= date < m["end_date"]:
                return m["name"], m["start_date"]
        return None, None

    def gen_officer_page(self, officer_idx, officer_id, tenures):
        def seniority(tenure):
            # add dept seniority as well
            post = self.post_dict[tenure.post_id]
            return (len(post.role_hpath), -tenure.duration_days)

        def tenure_ministry(tenure):
            if not self.ministry_dict:
                return "No Ministry"

            for m in self.ministry_dict["ministries"]:
                if m["start_date"] <= tenure.start_date < m["end_date"]:
                    return m["name"]
            return None

        tenures = sorted(tenures, key=attrgetter("start_date"))
        self.lgr.info(f"Generating officer page: {officer_id} {len(tenures)}")

        ministry_tenures = groupby(tenures, key=lambda t: tenure_ministry(t))

        ministries = {}
        for (ministry, m_tenures) in ministry_tenures:
            tenure_infos = [self.build_tenureinfo(t) for t in m_tenures]
            ministries[ministry] = tenure_infos

        key_tenures = sorted(tenures, key=seniority)[:3]
        key_tenures = [self.build_tenureinfo(t) for t in key_tenures]

        officer_info = self.officer_info_dict[officer_id]
        officer_info.ministries = ministries
        officer_info.key_tenures = key_tenures
        officer_info.url = f"officer-{officer_idx}.html"

        html_path = self.get_html_path("officer", officer_idx)
        html_path.write_text(self.render_html("officer", officer_info))

    def gen_officers_page(self):
        officer_infos = sorted(
            self.officer_info_dict.values(), key=attrgetter("first_char")
        )
        officer_groups = [
            list(g) for k, g in groupby(officer_infos, key=attrgetter("first_char"))
        ]

        print(f"Officer groups: {len(officer_groups)}")
        html_path = self.get_html_path("officers", "")
        html_path.write_text(self.render_html("officers", officer_groups))

    def gen_orders_page(self):
        order_infos = sorted(
            self.order_info_dict.values(), key=attrgetter("ministry_start_date", "date")
        )
        order_groups = [
            list(g)
            for k, g in groupby(order_infos, key=attrgetter("ministry_start_date"))
        ]

        print(f"Order groups: {len(order_groups)}")
        html_path = self.get_html_path("orders", "")
        html_path.write_text(self.render_html("orders", order_groups))

    def write_search_index(self):
        from lunr import lunr

        docs = [o.get_searchdoc_dict() for o in self.officer_info_dict.values()]

        lunrIdx = lunr(ref="idx", fields=["full_name", "officer_id"], documents=docs)

        search_index_file = self.output_dir / "lunr.idx.json"
        search_index_file.write_text(json.dumps(lunrIdx.serialize()))

        docs_file = self.output_dir / "docs.json"
        docs_file.write_text(json.dumps(docs))

    def pipe(self, docs, **kwargs):
        self.add_log_handler()
        docs = list(docs)
        print("Entering website builder")
        self.lgr.info("Entering website builder")

        self.lgr.info(f"Handling #docs: {len(docs)}")

        orders = [doc.order for doc in docs if doc.order.date]
        orders.sort(key=attrgetter("date"))
        self.order_dict = dict((o.order_id, o) for o in orders)
        self.order_idx_dict = dict((o.order_id, i) for (i, o) in enumerate(orders))

        self.lgr.info(f"Handling #orders: {len(orders)}")

        self.post_dict = dict((p.post_id, p) for o in orders for p in o.get_posts())

        tenures = list(flatten(doc.tenures for doc in docs))
        self.lgr.info(f"Handling #tenures: {len(tenures)}")

        officer_key = attrgetter("officer_id")
        officer_groups = groupby(sorted(tenures, key=officer_key), key=officer_key)
        for (officer_idx, (officer_id, officer_tenures)) in enumerate(officer_groups):
            self.gen_officer_page(officer_idx, officer_id, officer_tenures)
            self.officer_idx_dict[officer_id] = officer_idx
            self.officer_info_dict[officer_id].officer_idx = officer_idx

        [self.gen_order_page(idx, o) for idx, o in enumerate(orders)]

        self.gen_officers_page()
        self.gen_orders_page()
        self.write_search_index()

        self.lgr.info("Leaving website builder")
        self.remove_log_handler()
        return docs
