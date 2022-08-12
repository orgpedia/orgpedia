import datetime
import functools
import json
import logging
import sys
from itertools import groupby
from operator import attrgetter
from pathlib import Path
import copy

import yaml
from dateutil import parser
from more_itertools import flatten
from babel.dates import format_date

from docint.vision import Vision

# from jinja2 import Environment, FileSystemLoader, select_autoescape

# b /Users/mukund/Software/docInt/docint/pipeline/website_gen.py:164


DIGIT_LANG_DICT = {}


def format_lang_date(dt, lang, pattern_str):
    dt_str = format_date(dt, format=pattern_str, locale=lang)
    # digits are not translated by babel

    lang_dt_str = []
    for c in dt_str:
        if c.isdigit():
            lang_dt_str.append(str(DIGIT_LANG_DICT[int(c)][lang]))
        else:
            lang_dt_str.append(c)
    return ''.join(lang_dt_str)


class SiteInfo:
    def __init__(self, orgcode, org_name, officer_name, order_name='Orders', lang='en'):
        self.orgcode = orgcode
        self.org_name = org_name
        self.officer_name = officer_name
        self.order_name = order_name
        self.order_number = 'Order Number'
        self.order_date = 'Order Date'
        self.order_url = 'Order URL'
        self.file_name = 'File Name'
        self.internet_archive = 'Internet Archive'
        self.download_date = 'download date'

        self.order_details = 'Order Details'
        self.order_information = 'Order Information'
        self.num = 'Num'
        self.page = 'Page'
        self.posts = 'Posts'

        self.overview = 'Key Tenures'
        self.id_name = 'Wikidata ID'
        self.lang = lang


class PrimeInfo:
    def __init__(self, officer_info, tenure_str):
        self.officer_info = officer_info
        self.tenure_str = tenure_str

    @property
    def image_url(self):
        return self.officer_info.image_url

    @property
    def image_alt(self):
        return self.officer_info.full_name

    @property
    def name(self):
        return self.officer_info.full_name


class OfficerInfo:
    def __init__(self, yml_dict):
        self.officer_id = yml_dict["officer_id"]
        self.image_url = yml_dict.get("image_url", "")
        self.full_name = yml_dict["full_name"]
        self.key_tenures = []
        self.ministries = {}
        self.url = ""
        self.officer_idx = -1
        self._first_tenure = None
        self._first_ministry = None
        self.tenure_json_str = ''
        # self.lang_names_dict = yml_dict['lang_names']
        # self.lang_names_dict['en'] = self.full_name

    @property
    def first_char(self):
        return self.full_name[0]

    @property
    def first_ministry(self):
        # assuming ministries is reverse sorted        
        if self._first_ministry is  None:
            self._first_ministry = list(self.ministries.keys())[0]
        return self._first_ministry 

    @property
    def first_tenure(self):
        # assuming ministries and tenures is reverse sorted
        if self._first_tenure is None:
            self._first_tenure = self.ministries[self.first_ministry][0]
        return self._first_tenure

    @property
    def first_ministry_tenures(self):
        print(f'{self.ministries.keys()}, {self.first_ministry}')
        return self.ministries[self.first_ministry]
    

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


class PostInfo:
    def __init__(self, post):
        self.dept = post.dept
        self.role = post.role
        self.juri = post.juri
        self.loca = post.loca
        self.stat = post.stat

    @property
    def dept_role_str(self):
        if self.role:
            return f'{self.dept}[{self.role}]'
        else:
            return f'{self.dept}'

class ManagerInfo:
    def __init__(self, full_name, url, role, start_date, end_date):
        self.full_name = full_name
        self.image_url = url
        self.role = role
        self.start_date = start_date
        self.end_date = end_date
        self.lang = 'en'

    @classmethod
    def build(cls, officer_info, tenure):
        o, t = officer_info, tenure
        return ManagerInfo(o.full_name, o.image_url, t.role, t.start_date, t.end_date)


    @classmethod
    def build_pm(cls, ministry, image_url):
        s, e = ministry['start_date'], ministry['end_date']
        return ManagerInfo(ministry['pm'], image_url, 'Prime Minister', s, e)

    @property
    def start_date_str(self):
        return format_lang_date(self.start_date, self.lang, 'd MMMM YYYY')

    @property
    def end_date_str(self):
        # return self.tenure.end_date.strftime("%d %b %Y")
        return format_lang_date(self.end_date, self.lang, 'd MMMM YYYY')

class TenureInfo:
    def __init__(self, tenure, post, start_order_url, end_order_url):
        self.tenure = tenure
        self.post = PostInfo(post)
        self.start_order_url = start_order_url
        self.end_order_url = end_order_url
        self.manager_infos = []
        self.tenure_pos = -1
        self.lang = 'en'

    @property
    def dept(self):
        return self.post.dept

    @property
    def role(self):
        return self.post.role

    @property
    def start_month_year(self):
        # return self.tenure.start_date.strftime("%b %Y")
        return format_lang_date(self.tenure.start_date, self.lang, 'MMMM YYYY')

    @property
    def end_month_year(self):
        # return self.tenure.end_date.strftime("%b %Y")
        return format_lang_date(self.tenure.end_date, self.lang, 'MMMM YYYY')

    @property
    def start_date_str(self):
        # return self.tenure.start_date.strftime("%d %b %Y")
        return format_lang_date(self.tenure.start_date, self.lang, 'd MMMM YYYY')

    @property
    def end_date_str(self):
        # return self.tenure.end_date.strftime("%d %b %Y")
        return format_lang_date(self.tenure.end_date, self.lang, 'd MMMM YYYY')

    @property
    def start_order_id(self):
        return self.tenure.start_order_id

    @property
    def end_order_id(self):
        return self.tenure.end_order_id


PAGEURL = "/Users/mukund/orgpedia/cabsec/import/html/"


class OrderInfo:
    def __init__(self, order, details, ministry, ministry_start_date, num_pages):
        def get_image_url(idx):
            idx += 1
            # return f'{PAGEURL}{order.order_id.replace(".pdf","")}/svg-{idx:03d}.svg'
            order_stub = order.order_id.replace(".pdf", "")
            return Path('order_images') / order_stub / f'orig-{idx:03d}-000.jpg'

        self.order = order
        self.ministry = ministry
        self.ministry_start_date = ministry_start_date
        self.details = details
        self.pages = [get_image_url(idx) for idx in range(num_pages)]
        self.lang = 'en'
        self.url = f"order-{order.order_id}.html"

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
        # return self.order.date.strftime("%d %b %Y")
        return format_lang_date(self.order.date, self.lang, 'd MMMM YYYY')

    # @property
    # def url(self):
    #     return f"file:///Users/mukund/orgpedia/cabsec/import/html/{self.order.order_id}.html"

    @property
    def url_name(self):
        return f"http://cabsec.gov.in/{self.order.order_id}"


class DetailInfo:
    def __init__(self, detail, officer_idx, officer_name):
        self.officer_url = f"officer-{officer_idx}.html"
        self.name = officer_name
        self.post_str = self.get_all_post_str(detail)
        self.idx = detail.detail_idx
        self.postinfo_dict = self.get_postinfo_dict(detail)

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

    def get_postinfo_dict(self, detail):
        postinfo_dict = {}
        for pType in ["continues", "relinquishes", "assumes"]:
            posts = getattr(detail, pType)
            postinfo_dict[pType] = [PostInfo(p) for p in posts]
        return postinfo_dict


@Vision.factory(
    "website_language_generator",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "website_generator",
        "officer_info_files": ["conf/wiki_officer.yml"],
        "ministry_file": "conf/ministries.yml",
        "output_dir": "output",
        "languages": [],
        "translation_files": {
            'name': 'name.lang.yml',
            'dept': 'dept.lang.yml',
            'role': 'role.lang.yml',
            'digit': 'digit.lang.yml',
            'misc': 'misc.lang.yml',
        },
    },
)
class WebsiteLanguageGenerator:
    def __init__(
        self, conf_dir, conf_stub, officer_info_files, ministry_file, output_dir, languages, translation_files
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.officer_info_files = officer_info_files
        self.ministry_path = Path(ministry_file)
        self.output_dir = Path(output_dir)
        self.languages = languages
        self.translation_files = translation_files

        ### TODO CHANGE THIS
        self.languages = []

        self.officer_info_dict = self.get_officer_infos(self.officer_info_files)
        print(f"#Officer_info: {len(self.officer_info_dict)}")

        self.translations = {}
        for (trans_field, trans_file_name) in self.translation_files.items():
            self.translations[trans_field] = {}
            trans_path = self.conf_dir / trans_file_name
            self.translations[trans_field] = yaml.load(trans_path.read_text(), Loader=yaml.FullLoader)

        global DIGIT_LANG_DICT
        DIGIT_LANG_DICT = self.translations['digit']

        self.post_dict = {}
        self.order_dict = {}

        self.order_idx_dict = {}
        self.officer_idx_dict = {}
        self.order_info_dict = {}

        if self.ministry_path.exists():
            self.ministry_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)
            for m in self.ministry_dict["ministries"]:
                s, e = m["start_date"], m["end_date"]
                m["start_date"] = parser.parse(s).date()
                m["end_date"] = parser.parse(e).date() if e != "today" else datetime.date.today()
        else:
            self.ministry_dict = {}

        from jinja2 import Environment, FileSystemLoader, select_autoescape

        self.env = Environment(loader=FileSystemLoader("conf/templates"), autoescape=select_autoescape())
        self.site_info = SiteInfo('cabsec', 'Cabinet Secretariat', 'Ministers')
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

            info_dict = dict((d["officer_id"], OfficerInfo(d)) for d in info_dict["officers"])
            result_dict = {**result_dict, **info_dict}
            print(f"\t{officer_info_file} {len(info_dict)} {len(result_dict)}")
        return result_dict

    def translate_date(self, date, lang, format):
        pass

    def translate_name(self, name, lang):
        return self.translations['name'].get(name, {}).get(lang, ' ')

    def translate_misc(self, text, lang):
        return self.translations['misc'].get(text, {}).get(lang, '')

    def translate_postinfo(self, post_info, lang):
        def translate_field(field, text, lang):
            if not text:
                return ''

            if text[:2] == text[-2:] == '__':
                return text
            return self.translations[field].get(text, {}).get(lang, '')

        p, l = post_info, copy.copy(post_info)
        for field in ['dept', 'role', 'juri', 'loca', 'stat']:
            text = getattr(p, field)
            if text:
                setattr(l, field, translate_field(field, text, lang))
        return l

    def translate_tenureinfo(self, tenure_info, lang):
        t, l = tenure_info, copy.copy(tenure_info)
        l.post = self.translate_postinfo(t.post, lang)
        l.lang = lang
        return l

    def translate_tenureinfos(self, tenure_infos, lang):
        return [self.translate_tenureinfo(t, lang) for t in tenure_infos]

    def translate_officerinfo(self, officer_info, lang):
        o, l = officer_info, copy.copy(officer_info)

        l._first_tenure = None
        l._first_ministry = None        
        
        l.full_name = self.translate_name(o.full_name, lang)
        l.key_tenures = [self.translate_tenureinfo(t, lang) for t in o.key_tenures]
        l.ministries = dict(
            (self.translate_misc(m, lang), self.translate_tenureinfos(ts, lang)) for m, ts in o.ministries.items()
        )
        return l

    def translate_detailinfo(self, detail_info, lang):
        d, l = detail_info, copy.copy(detail_info)
        post_strs = []
        for (pType, posts) in d.postinfo_dict.items():
            if not posts:
                continue
            post_strs.append(f"<b>{self.translate_misc(pType, lang)}</b>")
            l_posts = [self.translate_postinfo(p, lang) for p in posts]
            post_strs += [p.dept_role_str for p in l_posts]
        l.post_str = "<br>".join(post_strs)
        l.name = self.translate_name(d.name, lang)
        return l

    def translate_orderinfo(self, order_info, lang):
        o, l = order_info, copy.copy(order_info)
        l.ministry = self.translate_misc(o.ministry, lang)

        l.details = [self.translate_detailinfo(d, lang) for d in o.details]
        l.lang = lang
        return l

    def translate_siteinfo(self, site_info, lang):
        if lang == 'en':
            return site_info
        lang_info = copy.copy(site_info)
        lang_info.org_name = self.translate_misc(site_info.org_name, lang)
        lang_info.officer_name = self.translate_misc(site_info.officer_name, lang)
        lang_info.order_name = self.translate_misc(site_info.order_name, lang)

        lang_info.overview = self.translate_misc(site_info.overview, lang)

        lang_info.order_number = self.translate_misc(site_info.order_number, lang)
        lang_info.order_date = self.translate_misc(site_info.order_date, lang)
        lang_info.order_url = self.translate_misc(site_info.order_url, lang)
        lang_info.file_name = self.translate_misc(site_info.file_name, lang)
        lang_info.internet_archive = self.translate_misc(site_info.internet_archive, lang)
        lang_info.download_date = self.translate_misc(site_info.download_date, lang)

        lang_info.order_information = self.translate_misc(site_info.order_information, lang)
        lang_info.order_details = self.translate_misc(site_info.order_details, lang)

        lang_info.num = self.translate_misc(site_info.num, lang)
        lang_info.page = self.translate_misc(site_info.page, lang)
        lang_info.posts = self.translate_misc(site_info.posts, lang)
        lang_info.id_name = self.translate_misc(site_info.id_name, lang)
        return lang_info

    def build_primeinfo(self, officer_id, ministries):
        def merge_spans(spans, span):
            if not spans:
                return [span]

            last_span = spans[-1]
            if last_span[1] == span[0]:
                spans[-1] = (last_span[0], span[1])
            else:
                spans.append(span)
            return spans

        officer_info = self.officer_info_dict[officer_id]
        ministry_spans = [(m["start_date"].year, m["end_date"].year) for m in ministries]
        merged_spans = functools.reduce(merge_spans, ministry_spans, [])
        tenure_str = ", ".join(f'{s} - {e}' for (s, e) in merged_spans)
        return PrimeInfo(officer_info, tenure_str)

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

    def populate_manager_infos(self, officer_info):
        all_tenure_infos = flatten(officer_info.ministries.values())

        for (pos_idx, t) in enumerate(all_tenure_infos):
            t.manager_infos.append(ManagerInfo.build(officer_info, t.tenure))
            for manager_idx in t.tenure.manager_idxs:
                m_tenure = self.tenures[manager_idx] 
                m_officer_info = self.officer_info_dict[m_tenure.officer_id]
                t.manager_infos.append(ManagerInfo.build(m_officer_info, m_tenure))
                
            ministry = self.get_ministry(t.tenure.start_date)
            assert ministry, 'Wrong date {t.tenure.start_date}'
            pm_officer_id = ministry['pm_officer_id']
            image_url = self.officer_info_dict[pm_officer_id].image_url
            t.manager_infos.append(ManagerInfo.build_pm(ministry, image_url))
        #end

    def get_tenure_jsons(self, officer_info):
        tenure_jsons = []
        for ministry, tenures in officer_info.ministries.items():
            for tenure_info in tenures:
                t_json = {'ministry': ministry}
                t_json['deptrole'] = f'{tenure_info.dept},<br>{tenure_info.role}'
                t_json['start_date_str'] = tenure_info.start_date_str
                t_json['end_date_str'] = tenure_info.end_date_str
                t_json['start_order_id'] = tenure_info.start_order_id
                t_json['end_order_id'] = tenure_info.end_order_id

                t_json['manager_infos'] = []
                for manager_info in tenure_info.manager_infos:
                    m_info = {
                        'full_name': manager_info.full_name,
                        'image_url': manager_info.image_url,
                        'role': manager_info.role,
                        'start_date_str': manager_info.start_date_str,
                        'end_date_str': manager_info.end_date_str,
                    }
                    t_json['manager_infos'].append(m_info)
                tenure_jsons.append(t_json)
        return tenure_jsons
    
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
        ministry, ministry_start_date = self.get_ministry_name_date(order.date)

        order_info = OrderInfo(order, details, ministry, ministry_start_date, num_pages)
        return order_info

    def get_html_path(self, entity, idx, lang=None):
        if idx:
            if lang:
                return self.output_dir / lang / f"{entity}-{idx}.html"
            else:
                return self.output_dir / f"{entity}-{idx}.html"
        else:
            assert entity in ("officers", "orders", "prime"), f"{idx} is empty for {entity}"
            if lang:
                return self.output_dir / lang / f"{entity}.html"
            else:
                return self.output_dir / f"{entity}.html"

    def render_html(self, entity, obj, lang='en'):
        template = self.env.get_template(f"{entity}-lang.html.jinja")
        l_site_info = self.translate_siteinfo(self.site_info, lang)
        if entity == "officer":
            return template.render(site=l_site_info, officer=obj)
        elif entity == "officers":
            return template.render(site=l_site_info, officer_groups=obj)
        elif entity == "orders":
            return template.render(site=l_site_info, order_groups=obj)
        elif entity == "prime":
            return template.render(site=l_site_info, primes=obj)
        else:
            return template.render(site=l_site_info, order=obj)

    def gen_prime_page(self):
        prime_dict = {}
        for m in self.ministry_dict['ministries']:
            prime_dict.setdefault(m['pm_officer_id'], []).append(m)

        en_prime_infos = [self.build_primeinfo(o_id, ms) for (o_id, ms) in prime_dict.items()]

        html_path = self.get_html_path("prime", "")
        html_path.write_text(self.render_html("prime", en_prime_infos))

        # for lang in self.languages:
        #     lang_infos = [self.translate_primeinfo(p, lang) for p in en_prime_infos]
        #     html_path = self.get_html_path("prime", "", lang)
        #     html_path.write_text(self.render_html("prime", lang_infos, lang))

    def gen_order_page(self, order_idx, order):
        if not order.details:
            return

        print(f"> {order.order_id} {str(order.date)}")

        order_info = self.build_orderinfo(order)
        self.order_info_dict[order.order_id] = order_info
        html_path = self.get_html_path("order", order.order_id)
        html_path.write_text(self.render_html("order", order_info))

        for lang in self.languages:
            html_path = self.get_html_path("order", order.order_id, lang)
            lang_order_info = self.translate_orderinfo(order_info, lang)
            html_path.write_text(self.render_html("order", lang_order_info, lang))

    def get_ministry_name_date(self, date):
        if not self.ministry_dict:
            return "No Ministry", None

        for m in self.ministry_dict["ministries"]:
            if m["start_date"] <= date < m["end_date"]:
                return m["name"], m["start_date"]
        return None, None


    def get_ministry(self, date):
        if not self.ministry_dict:
            return None

        for m in self.ministry_dict["ministries"]:
            if m["start_date"] <= date < m["end_date"]:
                return m
        return None
    

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
        
        self.populate_manager_infos(officer_info)
        officer_info.tenure_json_str = json.dumps(self.get_tenure_jsons(officer_info), indent=2)

        html_path = self.get_html_path("officer", officer_idx)
        html_path.write_text(self.render_html("officer", officer_info))

        for lang in self.languages:
            html_path = self.get_html_path("officer", officer_idx, lang)
            lang_officer_info = self.translate_officerinfo(officer_info, lang)
            html_path.write_text(self.render_html("officer", lang_officer_info, lang))

    def gen_officers_page(self):
        def group_infos(officer_infos):
            infos = sorted(officer_infos, key=attrgetter("first_char"))
            return [list(g) for k, g in groupby(infos, key=attrgetter("first_char"))]

        en_officer_infos = self.officer_info_dict.values()
        officer_groups = group_infos(en_officer_infos)

        print(f"Officer groups: {len(officer_groups)}")
        html_path = self.get_html_path("officers", "")
        html_path.write_text(self.render_html("officers", officer_groups))

        for lang in self.languages:
            lang_infos = [self.translate_officerinfo(o, lang) for o in en_officer_infos]
            lang_groups = group_infos(lang_infos)
            html_path = self.get_html_path("officers", "", lang)
            html_path.write_text(self.render_html("officers", lang_groups, lang))

    def gen_orders_page(self):
        def group_infos(order_infos):
            infos = sorted(order_infos, key=attrgetter("ministry_start_date", "date"))
            return [list(g) for k, g in groupby(infos, key=attrgetter("ministry_start_date"))]

        en_order_infos = self.order_info_dict.values()
        order_groups = group_infos(en_order_infos)

        print(f"Order groups: {len(order_groups)}")
        html_path = self.get_html_path("orders", "")
        html_path.write_text(self.render_html("orders", order_groups))

        for lang in self.languages:
            lang_infos = [self.translate_orderinfo(o, lang) for o in en_order_infos]
            lang_groups = group_infos(lang_infos)
            html_path = self.get_html_path("orders", "", lang)
            html_path.write_text(self.render_html("orders", lang_groups, lang))

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

        self.tenures = list(flatten(doc.tenures for doc in docs))
        self.tenures.sort(key=attrgetter("tenure_idx"))

        assert [t.tenure_idx for t in self.tenures] == list(range(len(self.tenures)))
        
        self.lgr.info(f"Handling #tenures: {len(self.tenures)}")

        officer_key = attrgetter("officer_id")
        officer_groups = groupby(sorted(self.tenures, key=officer_key), key=officer_key)
        for (officer_idx, (officer_id, officer_tenures)) in enumerate(officer_groups):
            officer_idx += 1  # ensure officer_idx starts from 1
            self.gen_officer_page(officer_idx, officer_id, officer_tenures)
            self.officer_idx_dict[officer_id] = officer_idx
            self.officer_info_dict[officer_id].officer_idx = officer_idx

        [self.gen_order_page(idx, o) for idx, o in enumerate(orders)]

        self.gen_officers_page()
        self.gen_orders_page()
        self.gen_prime_page()
        
        self.write_search_index()

        self.lgr.info("Leaving website builder")
        self.remove_log_handler()
        return docs
