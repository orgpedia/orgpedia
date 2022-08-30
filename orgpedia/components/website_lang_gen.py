import datetime
import functools
import json
import logging
import sys
from itertools import groupby
from operator import attrgetter
from pathlib import Path
import copy
import string

import yaml
from dateutil import parser
from more_itertools import flatten, first
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
            lang_dt_str.append(str(DIGIT_LANG_DICT[c][lang]))
        else:
            lang_dt_str.append(c)
    return ''.join(lang_dt_str)

def lang_year(dt, lang):
    return ''.join(DIGIT_LANG_DICT[c][lang] for c in str(dt))

class LabelsInfo:
    def __init__(self, attr_val_dict, lang):
        self.lang = lang
        for (attr, val) in attr_val_dict.items():
            setattr(self, attr, val)



class MinistryInfo:
    def __init__(self, yml_dict):
        self.name = yml_dict['name']

        e = yml_dict['end_date']        
        self.start_date = parser.parse(yml_dict['start_date']).date()
        self.end_date = parser.parse(e).date() if e != "today" else datetime.date.today()
        self.prime_name = yml_dict['pm']
        self.pm_id = yml_dict['pm_officer_id']
        self.deputy_pms = yml_dict.get('deputy_pms', [])
        for deputy_pm in self.deputy_pms:
            deputy_pm['start_date'] = parser.parse(deputy_pm['start_date']).date()
            de = deputy_pm['end_date']
            deputy_pm['end_date'] = parser.parse(de).date() if de != "today" else datetime.date.today()

    @property
    def start_year(self):
        return self.start_date.year

    @property
    def end_year(self):
        return self.end_date.year

    def has_date(self, dt):
        return self.start_date <= dt <= self.end_date
    
    @classmethod
    def get_tenure_date_pairs(cls, ministries, is_deputy=False):
        def merge_spans(spans, span):
            if not spans:
                return [span]

            last_span = spans[-1]
            if last_span[1] == span[0]:
                spans[-1] = (last_span[0], span[1])
            else:
                spans.append(span)
            return spans
        
        if not is_deputy:
            ministry_spans = [(m.start_year, m.end_year) for m in ministries]
        else:
            ministry_spans = [(d['start_date'].year, d['end_date'].year) for d in ministries]
        merged_spans = functools.reduce(merge_spans, ministry_spans, [])
        return list(merged_spans)

class OrderGroupInfo:
    def __init__(self, order_infos, idx, all_idxs, all_ministries):
        self.order_infos = order_infos
        self.idx = idx
        self.all_idxs = all_idxs
        self.ministry = order_infos[0].ministry
        self.all_ministries = all_ministries
        self.crumbs = []


class OfficerGroupInfo:
    def __init__(self, officer_infos, idx, all_idxs):
        self.officer_infos = officer_infos
        self.idx = idx
        self.all_idxs = all_idxs
        self.crumbs = []


class OfficerInfo:
    def __init__(self, yml_dict, officer_idx):
        assert officer_idx != 0
        self.officer_id = yml_dict["officer_id"]
        self.image_url = yml_dict.get("image_url", "")
        self.full_name = yml_dict["full_name"]
        self.key_infos = []
        self.ministries = {}
        self.url = f'officer-{officer_idx}.html'
        self.officer_idx = officer_idx
        self._first_tenure = None
        self._first_ministry = None
        self.tenure_json_str = ''
        self.prime_tenure_date_pairs = []
        self.deputy_tenure_date_pairs = []
        self.crumbs = []
        self.lang = 'en'

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
    def last_ministry(self):
        return list(self.ministries.keys())[-1]

    @property
    def first_tenure(self):
        # assuming ministries and tenures is reverse sorted
        if self._first_tenure is None:
            self._first_tenure = self.ministries[self.first_ministry][0]
        return self._first_tenure

    @property    
    def last_tenure(self):
        return self.ministries[self.last_ministry][-1]

    @property
    def first_ministry_tenures(self):
        print(f'{self.ministries.keys()}, {self.first_ministry}')
        return self.ministries[self.first_ministry]

    @property
    def tenure_str(self):
        first_tenure, last_tenure = self.first_tenure, self.last_tenure
        return f'{first_tenure.start_year} - {last_tenure.end_year}'

    @property
    def prime_tenure_str(self):
        dps, l = self.prime_tenure_date_pairs, self.lang
        lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in  dps]
        return ', '.join(lang_pairs)
    
    @property
    def deputy_tenure_str(self):
        dps, l = self.deputy_tenure_date_pairs, self.lang
        lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in  dps]
        return ', '.join(lang_pairs)
        

    def get_searchdoc_dict(self):
        doc = {}
        doc["idx"] = self.officer_idx
        doc["full_name"] = self.full_name
        doc["officer_id"] = self.officer_id
        doc["image_url"] = self.image_url
        doc["url"] = self.url
        if self.ministries:
            doc["tenure_str"] = self.tenure_str            
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
        self.border = ''
        self.top_margin = ''

    @classmethod
    def build(cls, officer_info, tenure):
        o, t = officer_info, tenure
        return ManagerInfo(o.full_name, o.image_url, t.role, t.start_date, t.end_date)


    @classmethod
    def build_pm(cls, ministry, image_url):
        s, e = ministry.start_date, ministry.end_date
        return ManagerInfo(ministry.prime_name, image_url, 'Prime Minister', s, e)
    
    @property
    def start_date_str(self):
        return format_lang_date(self.start_date, self.lang, 'd MMMM YYYY')

    @property
    def end_date_str(self):
        # return self.tenure.end_date.strftime("%d %b %Y")
        return format_lang_date(self.end_date, self.lang, 'd MMMM YYYY')


class KeyInfo:
    def __init__(self, dept, role, tenure_dates):
        self.dept = dept
        self.role = role
        self.tenure_dates = tenure_dates
        self.lang = 'en'

    @property
    def tenure_str(self):
        if isinstance(self.tenure_dates, tuple):
            smy = format_lang_date(self.tenure_dates[0], self.lang, 'MMMM YYYY')
            emy = format_lang_date(self.tenure_dates[1], self.lang, 'MMMM YYYY')
            return f'{smy} - {emy}'            
        else:
            dps, l = self.tenure_dates, self.lang
            lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in  dps]
            return ','.join(lang_pairs)

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
    def start_year(self):
        return lang_year(self.tenure.start_date.year, self.lang)
    
    @property
    def end_year(self):
        return lang_year(self.tenure.end_date.year, self.lang)        


    @property
    def start_order_id(self):
        return self.tenure.start_order_id

    @property
    def end_order_id(self):
        return self.tenure.end_order_id


PAGEURL = "/Users/mukund/orgpedia/cabsec/import/html/"


class OrderInfo:
    def __init__(self, order, details, ministry, ministry_start_date, ministry_end_date, num_pages):
        def get_image_url(idx):
            idx += 1
            # return f'{PAGEURL}{order.order_id.replace(".pdf","")}/svg-{idx:03d}.svg'
            order_stub = order.order_id.replace(".pdf", "")
            return Path('order_images') / order_stub / f'orig-{idx:03d}-000.jpg'

        def get_svg_url(idx):
            idx += 1
            order_stub = order.order_id.replace(".pdf", "")
            return Path('svgs') / order_stub / f'svg-{idx:03d}.jpg'

        self.order = order
        self.ministry = ministry
        self.ministry_start_date = ministry_start_date
        self.ministry_end_date = ministry_end_date        
        self.details = details
        self.pages = [get_image_url(idx) for idx in range(num_pages)]
        self.lang = 'en'
        self.url = f"order-{order.order_id}.html"
        self.num_pages = num_pages
        self.svg_pages = [get_svg_url(idx) for idx in range(num_pages)]
        self.category = getattr(order, 'category', 'Council of Ministers')

        self.page_details_dict = {}
        [self.page_details_dict.setdefault(d.page_idx, []).append(d) for d in self.order.details]

        self.category = getattr(order, 'category', 'Council of Ministers')
        self.crumbs = []        


    @property
    def detail_page_idxs(self):
        return list(self.page_details_dict.keys())

    @property
    def grouped_details(self):
        return list(self.page_details_dict.items())

    @property
    def num_detailed_pages(self):
        return lang_year(len(self.page_details_dict), self.lang)
    

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

    def get_ministry_years_str(self, lang='en'):
        return f'{lang_year(self.ministry_start_date.year, lang)}-{lang_year(self.ministry_end_date.year, lang)}'

    @property
    def num_details(self):
        return len(self.order.details)


class DetailInfo:
    def __init__(self, detail, officer_idx, officer_name, officer_image_url):
        self.officer_url = f"officer-{officer_idx}.html"
        self.name = officer_name
        self.officer_image_url = officer_image_url
        self.postinfo_dict = self.get_postinfo_dict(detail)        
        self.short_post_str, self.long_post_str = self.get_all_html_post_str()        
        self.idx = detail.detail_idx

    def get_postinfo_dict(self, detail, lang='en'):
        postinfo_dict = {}
        for pType in ["continues", "relinquishes", "assumes"]:
            posts = getattr(detail, pType)
            postinfo_dict[pType] = [ PostInfo(p) for p in posts ]
        return postinfo_dict

    def get_all_html_post_str(self):
        def get_post_str(post):
            dept, role = post.dept, post.role
            pStr = dept if not role else f"{dept}[{role}]"
            pStr = "" if pStr is None else pStr
            pStr = f'<p class="text-sm font-normal leading-4">{pStr}</p>'
            return pStr
        
        post_lines = []
        for (pType, posts) in self.postinfo_dict.items():        
            post_lines.append(f'<h4 class="text-base font-semibold leading-5"> {pType.capitalize()}:</h4>')
            post_lines.extend(get_post_str(p) for p in posts)

        short_str = '\n'.join(post_lines[:3])
        long_str = '\n'.join(post_lines[3:])
        return short_str, long_str


@Vision.factory(
    "website_language_generator",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "website_generator",
        "officer_info_files": ["conf/wiki_officer.yml"],
        "ministry_file": "conf/ministries.yml",
        "output_dir": "output",
        "languages": [],
        "translation_file": "conf/trans.yml",
    },
)
class WebsiteLanguageGenerator:
    def __init__(
        self, conf_dir, conf_stub, officer_info_files, ministry_file, output_dir, languages, translation_file
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.officer_info_files = officer_info_files
        self.ministry_path = Path(ministry_file)
        self.output_dir = Path(output_dir)
        self.languages = languages
        self.translation_file = Path(translation_file)

        ### TODO CHANGE THIS
        self.languages = [ 'hi', 'en', 'ur' ]

        self.officer_info_dict = self.get_officer_infos(self.officer_info_files)
        print(f"#Officer_info: {len(self.officer_info_dict)}")

        self.translations = yaml.load(self.translation_file.read_text(), Loader=yaml.FullLoader)

        global DIGIT_LANG_DICT
        DIGIT_LANG_DICT = self.translations['digits']

        self.lang_label_info_dict = self.build_lang_label_infos(self.translations['labels'])

        self.post_dict = {}
        self.order_dict = {}

        self.order_idx_dict = {}
        self.officer_idx_dict = {}
        self.order_info_dict = {}

        if self.ministry_path.exists():
            yml_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)            
            self.ministry_infos = self.build_ministryinfos(yml_dict)
        else:
            self.ministry_infos = []

        from jinja2 import Environment, FileSystemLoader, select_autoescape

        self.env = Environment(loader=FileSystemLoader("conf/templates"), autoescape=select_autoescape())
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

            info_dict = dict((d["officer_id"], OfficerInfo(d, idx+1)) for idx, d in enumerate(info_dict["officers"]))
            result_dict = {**result_dict, **info_dict}
            print(f"\t{officer_info_file} {len(info_dict)} {len(result_dict)}")
        return result_dict

    def translate_ministry(self, ministry, lang):
        return self.translations['ministry'][ministry][lang]

    def translate_label(self, label, lang):
        label = label.lower().replace(',', '').replace(' ', '_')
        return getattr(self.lang_label_info_dict[lang], label)
    
    def translate_date(self, date, lang, format):
        pass

    def translate_name(self, name, lang):
        if name not in self.translations['names']:
            sys.stderr.write(f'Unable to find name: {name}')
            return ''
        else:
            return self.translations['names'][name][lang]

    def translate_misc(self, text, lang):
        return self.translations['misc'].get(text, {}).get(lang, '')

    def translate_post_field(self, field, text, lang):
        if not text:
            return ''
        
        if text[:2] == text[-2:] == '__':
            return text
        return self.translations[field][text][lang]

    def translate_postinfo(self, post_info, lang):
        p, l = post_info, copy.copy(post_info)
        for field in ['dept', 'role', 'juri', 'loca', 'stat']:
            text = getattr(p, field)
            if text:
                setattr(l, field, self.translate_post_field(field, text, lang))
        return l

    def translate_digits(self, digts_str, lang):
        l_digits = []
        for c in digts_str:
            if c.isdigit():
                l_digits.append(self.translations['digits'][c][lang])
            else:
                l_digits.append(c)
        return ''.join(l_digits)

    def translate_keyinfo(self, key_info, lang):
        k, l = key_info, copy.copy(key_info)
        l.dept = self.translate_post_field('dept', k.dept, lang)
        l.role = self.translate_post_field('role', k.role, lang)
        l.lang = lang
        return l

    def translate_managerinfo(self, manager_info, lang):
        m, l = manager_info,copy.copy(manager_info)
        l.full_name = self.translate_name(m.full_name, lang)
        l.role = self.translate_post_field('role', m.role, lang)
        l.lang = lang
        return l
        
    def translate_tenureinfo(self, tenure_info, lang):
        t, l = tenure_info, copy.copy(tenure_info)
        l.post = self.translate_postinfo(t.post, lang)
        l.manager_infos = [self.translate_managerinfo(m, lang) for m in t.manager_infos]
        l.lang = lang
        return l

    def translate_tenureinfos(self, tenure_infos, lang):
        return [self.translate_tenureinfo(t, lang) for t in tenure_infos]

    def translate_officerinfo(self, officer_info, lang):
        o, l = officer_info, copy.copy(officer_info)

        l._first_tenure = None
        l._first_ministry = None
        l.lang = lang
        
        l.full_name = self.translate_name(o.full_name, lang)        
        l.key_infos = [self.translate_keyinfo(t, lang) for t in o.key_infos]
        l.ministries = dict(
            (self.translate_ministry(m, lang), self.translate_tenureinfos(ts, lang)) for m, ts in o.ministries.items()
        )
        return l

    def translate_detailinfo(self, detail_info, lang):
        d, l = detail_info, copy.copy(detail_info)
        l.name = self.translate_name(d.name, lang)

        l.postinfo_dict = {}
        for (pType, posts) in d.postinfo_dict.items():
            l_posts = [ self.translate_postinfo(p, lang) for p in posts ]
            l_pType = self.translate_label(pType, lang)
            l.postinfo_dict[l_pType] = l_posts

        l.short_post_str, l.long_post_str = l.get_all_html_post_str()        
        return l

    def translate_orderinfo(self, order_info, lang):
        o, l = order_info, copy.copy(order_info)
        l.ministry = self.translate_ministry(o.ministry, lang)
        l.category = self.translate_label(o.category, lang)

        l.details = [self.translate_detailinfo(d, lang) for d in o.details]
        l.lang = lang
        return l


    def build_lang_label_infos(self, labels_trans_dict):
        labels = [ l for  l in labels_trans_dict.keys()]
        labels = [ l.replace(',', '').replace(' ', '_').lower() for l in labels]

        languages = self.languages + ['en'] if 'en' not in self.languages else self.languages

        init_dicts = dict((lang, {}) for lang in self.languages)

        for (attr, label) in zip(labels, labels_trans_dict.keys()):
            for lang in self.languages:
                init_dicts[lang][attr] = labels_trans_dict[label][lang]

        label_info_dict = {}
        for (lang, init_dict) in init_dicts.items():
            label_info_dict[lang] = LabelsInfo(init_dict, lang)

        return label_info_dict
        

    def build_ministryinfos(self, ministry_yml):
        return [ MinistryInfo(m) for m in ministry_yml["ministries"]]

    def build_keyinfo(self, tenure):
        post = self.post_dict[tenure.post_id]
        tenure_dates = (tenure.start_date, tenure.end_date)
        return KeyInfo(post.dept, post.role, tenure_dates)

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
        leaf_roles = ('Minister of State', 'Deputy Minister')

        borders = ["border-t", "", "border-b"]
        top_margins = ["relative -mt-11", "", "-mb-11"]        

        for (pos_idx, t) in enumerate(all_tenure_infos):
            t.tenure_pos = pos_idx
            t.manager_infos.append(ManagerInfo.build(officer_info, t.tenure))

            if t.tenure.role and t.tenure.role in leaf_roles:
                for manager_idx in t.tenure.manager_idxs[:1]:
                    m_tenure = self.tenures[manager_idx] 
                    m_officer_info = self.officer_info_dict[m_tenure.officer_id]
                    t.manager_infos.append(ManagerInfo.build(m_officer_info, m_tenure))
                
            ministry = self.get_ministry(t.tenure.start_date)
            assert ministry, 'Wrong date {t.tenure.start_date}'
            image_url = self.officer_info_dict[ministry.pm_id].image_url
            t.manager_infos.append(ManagerInfo.build_pm(ministry, image_url))

            for idx, manager_info in enumerate(t.manager_infos):
                if len(t.manager_infos) == 2 and idx == 1:
                    # middle one needs to be skipped
                    idx += 1
                manager_info.border = borders[idx]
                manager_info.top_margin = top_margins[idx]                
        #end
        

    def get_tenure_jsons(self, officer_info):
        tenure_jsons = []
        for ministry, tenures in officer_info.ministries.items():
            for tenure_info in tenures:
                t_json = {'ministry': ministry}
                t_json['dept'] = f'{tenure_info.dept}'
                t_json['role'] = f'{tenure_info.role}'                
                t_json['date_str'] = f'{tenure_info.start_date_str} - {tenure_info.end_date_str}'
                t_json['start_order_id'] = tenure_info.start_order_id
                t_json['end_order_id'] = tenure_info.end_order_id
                t_json['start_order_url'] = tenure_info.start_order_url
                t_json['end_order_url'] = tenure_info.end_order_url
                

                t_json['manager_infos'] = []
                for manager_info in tenure_info.manager_infos:
                    m_info = {
                        'full_name': manager_info.full_name,
                        'image_url': manager_info.image_url,
                        'role': manager_info.role,
                        'date_str': f'{manager_info.start_date_str} - {manager_info.end_date_str}'
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
            officer_info = self.officer_info_dict[officer_id]
            officer_name = officer_info.full_name
            officer_image_url = officer_info.image_url
            details.append(DetailInfo(d, officer_idx, officer_name, officer_image_url))

        num_pages = order.details[-1].page_idx + 1
        m = self.get_ministry(order.date)
        order_info = OrderInfo(order, details, m.name, m.start_date, m.end_date, num_pages)
        return order_info

    def get_html_path(self, entity, idx, lang=None):
        if idx:
            if lang:
                return self.output_dir / lang / f"{entity}-{idx}.html"
            else:
                return self.output_dir / f"{entity}-{idx}.html"
        else:
            assert entity in ("orders", "prime", "deputy"), f"{idx} is empty for {entity}"
            if lang:
                return self.output_dir / lang / f"{entity}.html"
            else:
                return self.output_dir / f"{entity}.html"

    def render_html(self, entity, obj, lang='en'):
        template = self.env.get_template(f"{entity}.html")
        #l_site_info = self.translate_siteinfo(self.site_info, lang)
        l_site_info = self.lang_label_info_dict[lang]
        
        if entity == "officer":
            l_site_info.page_url = f'officer-{obj.officer_idx}.html'
            l_site_info.title =  f'{l_site_info.ministers}: {obj.full_name}'
            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.ministers, f'officers-{obj.first_char.upper()}.html'),
                          (obj.full_name, f'officer-{obj.officer_idx}.html')]
            return template.render(site=l_site_info, officer=obj)
        elif entity == "officers":
            l_site_info.page_url = f'officers-{obj.idx}.html'
            l_site_info.title =  f'{l_site_info.ministers} ({obj.idx})'
            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.ministers, f'officers-{obj.idx.upper()}.html')]
            return template.render(site=l_site_info, officer_group=obj)
        elif entity == "order":
            l_site_info.page_url = f'order-{obj.order_id}.html'
            l_site_info.title =  f'{l_site_info.order}: ({obj.order_id})'

            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.orders, f'orders-{obj.get_ministry_years_str()}.html'),
                          (obj.order_id, f'order-{obj.order_id}.html')]

            return template.render(site=l_site_info, order=obj)
        elif entity == "orders":
            l_site_info.page_url = f'orders-{obj.idx}.html'
            l_site_info.title =  f'{l_site_info.orders} ({obj.idx})'
            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.orders, 'orders.html')]
            
            return template.render(site=l_site_info, order_group=obj)
        elif entity == "prime":
            l_site_info.page_url = f'prime.html'
            l_site_info.title =  f'{l_site_info.prime_ministers}'
            return template.render(site=l_site_info, primes=obj)
        elif entity == "deputy":
            l_site_info.page_url = f'deputy.html'
            l_site_info.title =  f'{l_site_info.deputy_prime_ministers}'
            return template.render(site=l_site_info, primes=obj)
        else:
            raise NotImplementedError(f'Not implemented for entity: {entity}')
        
    def gen_prime_page(self):
        prime_dict = {}

        assert len(self.officer_info_dict) > 0
        [prime_dict.setdefault(m.pm_id,[]).append(m) for m in self.ministry_infos]

        en_prime_infos = []
        for pm_id, ministries in prime_dict.items():
            tenure_date_pairs = MinistryInfo.get_tenure_date_pairs(ministries)
            prime_officer_info = self.officer_info_dict[pm_id]
            
            prime_officer_info.prime_tenure_date_pairs = tenure_date_pairs
            en_prime_infos.append(prime_officer_info)

        html_path = self.get_html_path("prime", "")
        html_path.write_text(self.render_html("prime", en_prime_infos))

        for lang in self.languages:
            lang_infos = [self.translate_officerinfo(o, lang) for o in en_prime_infos]
            html_path = self.get_html_path("prime", "", lang)
            html_path.write_text(self.render_html("prime", lang_infos, lang))

    def gen_deputy_prime_page(self):
        deputy_dict = {}
        iter_deputy = [d for m in self.ministry_infos for d in m.deputy_pms]
        for deputy_pm in iter_deputy:
            deputy_dict.setdefault(deputy_pm['officer_id'], []).append(deputy_pm)

        en_deputy_infos = []
        for (deputy_id, deputy_pms) in  deputy_dict.items():
            tenure_date_pairs = MinistryInfo.get_tenure_date_pairs(deputy_pms, is_deputy=True)
            deputy_officer_info = self.officer_info_dict[deputy_id]
            deputy_officer_info.deputy_tenure_date_pairs = tenure_date_pairs
            en_deputy_infos.append(deputy_officer_info)
            
        html_path = self.get_html_path("deputy", "")
        html_path.write_text(self.render_html("deputy", en_deputy_infos))

        for lang in self.languages:
            lang_infos = [self.translate_officerinfo(o, lang) for o in en_deputy_infos]            
            html_path = self.get_html_path("deputy", "", lang)
            html_path.write_text(self.render_html("deputy", lang_infos, lang))
        
        

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


    def get_ministry(self, dt):
        return first([m for m in self.ministry_infos if m.has_date(dt)], None)
    

    def gen_officer_page(self, officer_idx, officer_id, tenures):
        def seniority(tenure):
            # add dept seniority as well
            post = self.post_dict[tenure.post_id]
            return (len(post.role_hpath), -tenure.duration_days)

        def tenure_ministry(tenure):
            if not self.ministry_infos:
                return "No Ministry"
            ministry = self.get_ministry(tenure.start_date)
            return ministry.name

        tenures = sorted(tenures, key=attrgetter("start_date"))
        self.lgr.info(f"Generating officer page: {officer_id} {len(tenures)}")
        
        if len(tenures) == 0:
            return

        ministry_tenures = groupby(tenures, key=lambda t: tenure_ministry(t))

        ministries = {}
        for (ministry, m_tenures) in ministry_tenures:
            tenure_infos = [self.build_tenureinfo(t) for t in m_tenures]
            ministries[ministry] = tenure_infos
            

        officer_info = self.officer_info_dict[officer_id]
        
        key_infos = []
        if officer_info.prime_tenure_date_pairs:
            key_infos.append(KeyInfo('', 'Prime Minister', officer_info.prime_tenure_date_pairs))

        if officer_info.deputy_tenure_date_pairs:
            key_infos.append(KeyInfo('', 'Deputy Prime Minister', officer_info.deputy_tenure_date_pairs))            

        key_tenures = sorted(tenures, key=seniority)[:3]        
        key_infos += [self.build_keyinfo(t) for t in key_tenures]

        officer_info.ministries = ministries
        officer_info.key_infos = key_infos[:3]
        officer_info.url = f"officer-{officer_info.officer_idx}.html"
        #officer_info.officer_idx = officer_idx
        
        self.populate_manager_infos(officer_info)
        officer_info.tenure_json_str = json.dumps(self.get_tenure_jsons(officer_info), indent=2)

        html_path = self.get_html_path("officer", officer_info.officer_idx)
        html_path.write_text(self.render_html("officer", officer_info))

        for lang in self.languages:
            html_path = self.get_html_path("officer", officer_info.officer_idx, lang)
            lang_officer_info = self.translate_officerinfo(officer_info, lang)
            lang_officer_info.tenure_json_str = json.dumps(self.get_tenure_jsons(lang_officer_info), indent=2)
            html_path.write_text(self.render_html("officer", lang_officer_info, lang))

    def gen_officers_page(self):
        def group_infos(officer_infos):
            infos = sorted(officer_infos, key=attrgetter("first_char"))
            return [list(g) for k, g in groupby(infos, key=attrgetter("first_char"))]

        en_officer_infos = self.officer_info_dict.values()
        print(f'Before: {len(en_officer_infos)}')
        en_officer_infos = [o for o in en_officer_infos if o.officer_idx != -1 and len(o.ministries) > 0]
        print(f'After: {len(en_officer_infos)}')
        
        officer_groups = group_infos(en_officer_infos)
        first_chars = [g[0].first_char for g in officer_groups]

        officer_group_infos = []
        for first_char, o_group in zip(first_chars, officer_groups):
            og_info = OfficerGroupInfo(o_group, first_char, first_chars)
            html_path = self.get_html_path("officers", first_char)
            html_path.write_text(self.render_html("officers", og_info))
            officer_group_infos.append(og_info)
        #end for

        for lang in self.languages:
            for og in officer_group_infos:
                lang_infos = [self.translate_officerinfo(o, lang) for o in og.officer_infos ]
                lang_og_info = OfficerGroupInfo(lang_infos, og.idx, og.all_idxs)
                html_path = self.get_html_path("officers", og.idx, lang)
                html_path.write_text(self.render_html("officers", lang_og_info, lang))


    def gen_orders_page(self):
        def group_infos(order_infos):
            infos = sorted(order_infos, key=attrgetter("ministry_start_date", "date"))
            return [list(g) for k, g in groupby(infos, key=attrgetter("ministry_start_date"))]

        en_order_infos = self.order_info_dict.values()
        order_groups = group_infos(en_order_infos)

        all_idxs = [g[0].get_ministry_years_str() for g in order_groups]
        all_ministries = [g[0].ministry for g in order_groups]

        order_group_infos = []
        for order_group in order_groups:
            idx = order_group[0].get_ministry_years_str()
            or_info = OrderGroupInfo(order_group, idx, all_idxs, all_ministries)
            html_path = self.get_html_path("orders", idx)
            html_path.write_text(self.render_html("orders", or_info))
            order_group_infos.append(or_info)
        
        print(f"Order groups: {len(order_groups)}")
        
        for lang in self.languages:
            all_lang_idxs = [ g[0].get_ministry_years_str(lang) for g in order_groups ]
            for og in order_group_infos:
                lang_infos = [self.translate_orderinfo(o, lang) for o in og.order_infos]
                idx = lang_infos[0].get_ministry_years_str(lang)
                lang_og_info = OrderGroupInfo(lang_infos, idx, all_lang_idxs, all_ministries)
                en_idx = og.order_infos[0].get_ministry_years_str('en')
                html_path = self.get_html_path("orders", en_idx, lang)
                html_path.write_text(self.render_html("orders", lang_og_info, lang))

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

        self.gen_prime_page()        
        self.gen_deputy_prime_page()

        officer_key = attrgetter("officer_id")
        officer_groups = groupby(sorted(self.tenures, key=officer_key), key=officer_key)
        for (officer_idx, (officer_id, officer_tenures)) in enumerate(officer_groups):
            
            #officer_idx += 1  # ensure officer_idx starts from 1
            self.gen_officer_page(officer_idx, officer_id, officer_tenures)
            self.officer_idx_dict[officer_id] = self.officer_info_dict[officer_id].officer_idx
            #self.officer_info_dict[officer_id].officer_idx = officer_idx

        [self.gen_order_page(idx, o) for idx, o in enumerate(orders)]

        self.gen_officers_page()
        self.gen_orders_page()
        
        self.write_search_index()

        self.lgr.info("Leaving website builder")
        self.remove_log_handler()
        return docs