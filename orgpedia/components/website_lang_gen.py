import copy
import datetime
import functools
import json
import logging
import sys
import calendar
import string
from collections import Counter
from itertools import groupby
from operator import attrgetter, itemgetter
from pathlib import Path

import yaml
from babel.dates import format_date
from dateutil import parser
from docint.util import read_config_from_disk
from docint.vision import Vision
from more_itertools import first, flatten

# from jinja2 import Environment, FileSystemLoader, select_autoescape

# TODO
# 1. Should take post hierarchies role/dept as both are needed, then need to be exported
# 2. English should work without any translation tables, currently it does not ! check
# 3. Support two modes 1) Minister 2) Officers
# 4. Ability to use compressed html files.


# b /Users/mukund/Software/docInt/docint/pipeline/website_gen.py:164

# ROLE_SENIORITY = [
#     'Director General of Police',
#     'Additional Director General of Police',
#     'Inspector General of Police',
#     'Additional Inspector General of Police',
#     'Assistant Inspector General of Police',
#     'Deputy Inspector General of Police',
#     'Commissioner',
#     'Joint Commissioner of Police',
#     'Additional Commissioner',
#     'Deputy Commissioner of Police',
#     'Additional Deputy Commissioner of Police',
#     'Assistant Commissioner of Police',
#     'Director',
#     'Deputy Director',
#     'Assistant Director',
#     'Principal',
#     'Vice Principal',
#     'Chief Complaints and Enquiry Officer',
#     'CHIEF SECURITY OFFICER',
#     'Police Inspector',
#     'Vice Chancellor',
#     'Pro Vice Chancellor',
#     'Personal Security Officer',
#     'Commandant',
#     'Deputy Commandant',
#     'Assistant Commandant (Adjudant)',
#     'Assistant Commandant (Quarter Master)',
#     'Assistant Commandant',
#     'Superintendent of Police',
#     'Additional Superintendent of Police',
#     'Assistant Superintendent of Police',
#     'Deputy Superintendent of Police',
#     'Circle Officer',
#     'Station House Officer',
# ]

ROLE_SENIORITY = [
    'Prime Minister',
    'Deputy Prime Minister',
    'Cabinet Minister',
    'Minister of State (Independent Charge)',
    'Minister of State',
    'Deputy Minister',
]

KEY_DEPTS = [
    'Ministry of Home Affairs',
    'Ministry of External Affairs',
    'Ministry of Defence',
    'Ministry of Finance',
    'Ministry of Railways',
]
DIGIT_LANG_DICT = {}
TODATE_DICT = {}  # Ugliness

RUN_START_DATE = datetime.date(year=1947, month=8, day=15)
RUN_END_DATE = datetime.date(year=2022, month=12, day=2)


def format_lang_date(dt, lang, pattern_str):
    if dt >= RUN_END_DATE:
        return TODATE_DICT[lang]

    loc_lang = 'kok' if lang == 'gom' else ('hi' if lang == 'sd' else lang)

    dt_str = format_date(dt, format=pattern_str, locale=loc_lang)
    # digits are not translated by babel

    lang_dt_str = []
    for c in dt_str:
        if c.isdigit():
            lang_dt_str.append(str(DIGIT_LANG_DICT[c][lang]))
        else:
            lang_dt_str.append(c)
    return ''.join(lang_dt_str)


def lang_year(dt, lang):
    if dt >= RUN_END_DATE.year:
        return TODATE_DICT[lang]
    else:
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
        self.lang = 'en'
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

    @property
    def period_str(self):
        s_date_str = format_lang_date(self.start_date, self.lang, 'd MMMM YYYY')
        e_date_str = format_lang_date(self.end_date, self.lang, 'd MMMM YYYY')
        return f'{s_date_str} - {e_date_str}'

    def has_date(self, dt):
        return self.start_date <= dt < self.end_date

    def get_deputy_pm_ids(self, dt):
        deputy_pm_ids = []
        for d in self.deputy_pms:
            if d['start_date'] <= dt < d['end_date']:
                deputy_pm_ids.append(d['officer_id'])
        return deputy_pm_ids

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
    def __init__(self, order_infos, idx, all_en_idxs, all_idxs, all_ministries):
        self.order_infos = order_infos
        self.idx = idx
        self.all_en_idxs = all_en_idxs
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
        self.image_url = yml_dict.get("image_url", "").replace('loksabhaph.nic.in', 'loksabha.nic.in')
        self.full_name = yml_dict["full_name"]
        self.abbr_name = yml_dict.get("abbr_name", self.full_name)
        self._first_char = self.full_name[0] if self.full_name else 'E'
        self.key_infos = []
        self.ministries = {}

        self.officer_idx = officer_idx
        self._first_tenure = None
        self._first_ministry = None
        self.tenure_json_str = ''
        self.prime_tenure_date_pairs = []
        self.deputy_tenure_date_pairs = []
        self.crumbs = []
        self.lang = 'en'
        self.url_name = self.abbr_name.replace(' ', '_')
        self.url = f'o-{self.url_name}.html'

    @property
    def first_char(self):
        return self._first_char

    @property
    def first_ministry(self):
        # assuming ministries is reverse sorted
        if self._first_ministry is None:
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
        lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in dps]
        return ', '.join(lang_pairs)

    @property
    def deputy_tenure_str(self):
        dps, l = self.deputy_tenure_date_pairs, self.lang
        lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in dps]
        return ', '.join(lang_pairs)

    @property
    def slo(self):
        return (len(self.ministries) * 2) - 1

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
    def __init__(self, full_name, url, image_url, role, start_date, end_date):
        self.full_name = full_name
        self.url = url
        self.image_url = image_url
        self.role = role
        self.start_date = start_date
        self.end_date = end_date
        self.lang = 'en'
        self.border = ''
        self.top_margin = ''

    @classmethod
    def build(cls, officer_info, tenure):
        o, t = officer_info, tenure
        return ManagerInfo(o.full_name, o.url, o.image_url, t.role, t.start_date, t.end_date)

    @classmethod
    def build_pm(cls, ministry, url, image_url):
        s, e = ministry.start_date, ministry.end_date
        return ManagerInfo(ministry.prime_name, url, image_url, 'Prime Minister', s, e)

    @property
    def start_date_str(self):
        return format_lang_date(self.start_date, self.lang, 'd MMMM YYYY')

    @property
    def end_date_str(self):
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
            lang_pairs = [f'{lang_year(s, l)} - {lang_year(e, l)}' for (s, e) in dps]
            return ', '.join(lang_pairs)


class TenureInfo:
    def __init__(self, tenure, post, start_order_url, end_order_url, all_orderid_detailidxs):
        self.tenure = tenure
        self._dept = post.dept
        self._role = tenure.role if tenure.role else "Cabinet Minister"
        self.start_order_url = start_order_url
        self.end_order_url = end_order_url
        self.manager_infos_count = -1
        self.manager_infos = []  # always will have 3, check manager_infos_count
        self.tenure_pos = -1
        self.all_orderid_detailidxs = all_orderid_detailidxs
        self.lang = 'en'

    @property
    def tenure_idx(self):
        return self.tenure.tenure_idx

    @property
    def tenure_start_date_idx(self):
        ds = str(self.tenure.start_date).replace('-', '')
        return f'{ds}-{self.tenure.officer_start_date_idx}'

    @property
    def dept(self):
        return self._dept

    @property
    def role(self):
        return self._role

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

            # file_url = Path('p') / order_stub / f'or-{idx:03d}.jpg'
            cloud_url = f'https://res.cloudinary.com/dvltlchj4/moi/{order_stub}/or-{idx:03d}.jpg'
            return cloud_url

        def get_svg_url(idx):
            idx += 1
            order_stub = order.order_id.replace(".pdf", "")
            file_url = Path('p') / order_stub / f'd-{idx:03d}.svg'
            file_url = f'../{str(file_url)}'
            cloud_url = f'https://res.cloudinary.com/dvltlchj4/fl_sanitize/moi/{order_stub}/d-{idx:03d}.svg'
            return cloud_url

        self.order = order
        self.details = details
        self.ministry = ministry
        self.ministry_start_date = ministry_start_date
        self.ministry_end_date = ministry_end_date
        self.num_pages = num_pages
        self.lang = 'en'
        self.url = f"order-{order.order_id}.html"

        self.images = [get_image_url(idx) for idx in range(num_pages)]
        self.svgs = [get_svg_url(idx) for idx in range(num_pages)]

        self.category = getattr(order, 'category', 'Council of Ministers')
        self._num_details = len(self.order.details)
        self.d_page_idxs = [d.page_idx for d in order.details]
        self.first_page_num = self.d_page_idxs[0] + 1

        self.first_image_url = self.images[self.first_page_num - 1]
        self.first_svg_url = self.svgs[self.first_page_num - 1]

        self.page_details_dict = {}
        [self.page_details_dict.setdefault(d.page_idx, []).append(d) for d in self.order.details]

        # self.category = getattr(order, 'category', 'Council of Ministers')
        self.crumbs = []
        self.ppln_crumbs = []

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

    @property
    def short_date_str(self):
        return format_lang_date(self.order.date, self.lang, 'd MMM yyyy')

    # @property
    # def url(self):
    #     return f"file:///Users/mukund/orgpedia/cabsec/import/html/{self.order.order_id}.html"

    @property
    def url_name(self):
        return f"http://cabsec.gov.in/{self.order.order_id}"

    def get_ministry_years_str(self, lang='en'):
        if self.ministry_start_date:
            return f'{lang_year(self.ministry_start_date.year, lang)}-{lang_year(self.ministry_end_date.year, lang)}'
        else:
            return ''

    @property
    def num_details(self):
        return self._num_details


# VERB_CLASS= "text-base font-semibold leading-5" # a-a
# DEPT_CLASS= "text-sm font-normal leading-4" # a-b


class DetailInfo:
    def __init__(self, detail, officer_url, officer_name, officer_image_url, order_id):
        self.officer_url = officer_url
        self.details_url = f"details-{order_id}.html?detail_num={detail.detail_idx +1}"
        self.name = officer_name
        self.officer_image_url = officer_image_url
        self.postinfo_dict = self.get_postinfo_dict(detail)
        self.short_post_str, self.long_post_str = self.get_all_html_post_str()
        self.idx = detail.detail_idx
        self.page_idx = detail.page_idx

    def get_postinfo_dict(self, detail, lang='en'):
        postinfo_dict = {}
        for pType in ["continues", "relinquishes", "assumes"]:
            posts = getattr(detail, pType)
            postinfo_dict[pType] = [PostInfo(p) for p in posts]
        return postinfo_dict

    def get_all_html_post_str(self):
        def get_post_str(post):
            dept, role = post.dept, post.role
            pStr = dept if not role else f"{dept}[{role}]"
            pStr = "" if pStr is None else pStr
            pStr = f'<p class=a-b>{pStr}</p>'
            return pStr

        post_lines = []
        for (pType, posts) in self.postinfo_dict.items():
            if posts:
                post_lines.append(f'<h4 class=a-a> {pType.capitalize()}:</h4>')
                post_lines.extend(get_post_str(p) for p in posts)

        short_str = ''.join(post_lines[:3])
        long_str = ''.join(post_lines[3:])
        return short_str, long_str

    def to_json(self):
        return [
            self.name,
            self.officer_image_url,
            self.short_post_str,
            self.long_post_str,
            self.officer_url,
            self.page_idx,
        ]


class DetailPipeInfo:
    def __init__(self, pipe, pipe_idx, detail, doc):
        def get_path(extract_object):
            return getattr(extract_object, 'path', None)

        def svg_info(extract_object):
            if hasattr(extract_object, 'get_svg_info'):
                return extract_object.get_svg_info()
            else:
                return None

        def get_html_json(extract_objects):
            if not extract_objects:
                return 'Empty'

            if hasattr(extract_objects[0], 'get_html_json'):
                return f'[{", ".join(e.get_html_json() for e in extract_objects)}]'
            elif type(extract_objects[0]) in (int, float, str):
                return f'[{",".join(f"{e}" for e in extract_objects)}]'
            else:
                return f'[{", ".join(type(e).__name__ for e in extract_objects)}]'

        detail_path = f'pa{detail.page_idx}.od{detail.detail_idx}'
        relevant_extracts = doc.get_relevant_extracts(pipe, detail_path, detail.shape)

        self.object_name = first(relevant_extracts.keys(), default='No Extract')
        first_objects = first(relevant_extracts.values(), default=[])

        self.object_count = "" if len(first_objects) <= 1 else len(first_objects)
        self.object_html_json = get_html_json(first_objects)
        self.object_sub_name = "" if len(first_objects) == 0 else type(first_objects[0]).__name__

        all_objects = [o for objs in relevant_extracts.values() for o in objs]
        self.object_paths = [get_path(o) for o in all_objects if get_path(o)]

        self.svg_infos = [svg_info(o) for o in all_objects]
        self.svg_infos = [i for i in self.svg_infos if i]

        self.pipe_name = pipe
        self.pipe_idx = pipe_idx
        self.pipe_config_url = 'http://TBD'
        self.pipe_log_url = 'http://TBD'

        self.pipe_error_count = -1
        self.pipe_error_details = ''

        self.pipe_edit_count = -1
        self.pipe_edit_details = ''

    def add_errors(self, detail_errors, all_errors):
        def count_str(error_count):
            e = error_count
            return f'{e[0][:-6]}: {e[1]}/{e[2]}'

        self.pipe_error_count = f"{len(detail_errors)}/{len(all_errors)}"

        detail_counts = Counter(e.name for e in detail_errors)
        all_counts = Counter(e.name for e in all_errors)

        error_counts = [(n, detail_counts[n], c) for (n, c) in all_counts.items()]

        error_counts.sort(key=itemgetter(1), reverse=True)
        if error_counts:
            self.pipe_error_details = str(count_str(e) for e in error_counts)
        else:
            self.pipe_error_details = ''

    def add_edits(self, detail_edits, all_edits):
        def count_str(edit_count):
            e = edit_count
            return f'{e[0][:-6]}: {e[1]}/{e[2]}'

        self.pipe_edit_count = f"{len(detail_edits)}/{len(all_edits)}"

        detail_counts = Counter(e.name for e in detail_edits)
        all_counts = Counter(e.name for e in all_edits)

        edit_counts = [(n, detail_counts[n], c) for (n, c) in all_counts.items()]

        edit_counts.sort(key=itemgetter(1), reverse=True)
        self.pipe_edit_details = str(count_str(e) for e in edit_counts)

    def to_json(self):
        return [
            self.object_name,  # 0
            self.object_count,  # 1
            self.object_html_json,  # 2
            self.svg_infos,  # 3
            self.pipe_name,  # 4
            self.pipe_error_count,  # 5
            self.pipe_error_details,  # 6
            self.pipe_edit_count,  # 7
            self.pipe_edit_details,  # 8
            self.object_sub_name,
        ]


class CabinetInfo:
    def __init__(
        self, date, ministry_idx, ministry_date_idxs, deputy_pm_idxs, composition_idxs, key_info_idxs, ministers_idxs
    ):
        self.date = date
        self.ministry_idx = ministry_idx
        self.ministry_date_idxs = ministry_date_idxs
        self.deputy_pm_idxs = deputy_pm_idxs
        self.composition_idxs = composition_idxs
        self.key_info_idxs = key_info_idxs
        self.ministers_idxs = ministers_idxs

    def trans_num(self, num):
        return ''.join(self.idx2str['digits'][int(c)] for c in str(num))

    @property
    def pm_image_url(self):
        pm_idx = self.ministers_idxs[0][0]
        offi_list = self.idx2str['offi']
        return offi_list[pm_idx][2]

    @property
    def pm_url(self):
        pm_idx = self.ministers_idxs[0][0]
        offi_list = self.idx2str['offi']
        return offi_list[pm_idx][1]

    @property
    def pm_name(self):
        pm_idx = self.ministers_idxs[0][0]
        offi_list = self.idx2str['offi']
        return offi_list[pm_idx][0]

    @property
    def deputy_pms(self):
        offi_list = self.idx2str['offi']
        return [offi_list[idx][0] for idx in self.deputy_pm_idxs]

    @property
    def composition(self):
        role_list = self.idx2str['role']
        return [(role_list[idx], self.trans_num(cnt)) for (idx, cnt) in self.composition_idxs]

    @property
    def key_info(self):
        offi_list = self.idx2str['offi']
        dept_list = self.idx2str['dept']
        return [(offi_list[o][0], dept_list[d]) for (o, d) in self.key_info_idxs]

    @property
    def ministry(self):
        mini_list = self.idx2str['mini']
        return mini_list[self.ministry_idx]

    @property
    def name(self):
        return self.ministry

    @property
    def period_str(self):
        s_y, s_m, s_d = self.ministry_date_idxs[0]
        e_y, e_m, e_d = self.ministry_date_idxs[1]

        ss_y = ''.join(self.idx2str['digits'][int(c)] for c in str(s_y))
        ss_d = ''.join(self.idx2str['digits'][int(c)] for c in str(s_d))
        ss_m = self.idx2str['months'][s_m]

        se_y = ''.join(self.idx2str['digits'][int(c)] for c in str(e_y))
        se_d = ''.join(self.idx2str['digits'][int(c)] for c in str(e_d))
        se_m = self.idx2str['months'][e_m]
        return f'{ss_d} {ss_m} {ss_y} - {se_d} {se_m} {se_y}'

    @property
    def ministers(self):
        offi_list = self.idx2str['offi']
        dept_list = self.idx2str['dept']
        role_list = self.idx2str['role']

        def get_offi_info(idx):
            o = offi_list[idx]
            return o[0], o[1], o[2]

        def get_post_str(post_idxs):
            post_strs = [f'{dept_list[d]}[{role_list[r]}]' for (d, r) in post_idxs]
            return '<br>'.join(post_strs[:3]), '<br>'.join(post_strs[3:])

        ministers_strs = []
        for [offi_idx, post_idxs] in self.ministers_idxs:
            name, url, image_url = get_offi_info(offi_idx)
            short_post_str, long_post_str = get_post_str(post_idxs)
            ministers_strs.append(
                {
                    'name': name,
                    'url': url,
                    'image_url': image_url,
                    'short_post_str': short_post_str,
                    'long_post_str': long_post_str,
                }
            )
        return ministers_strs

    @classmethod
    def get_json_idxs(cls, cabinet_infos):
        cabinet_idxs, date_idxs = [], []
        for c in cabinet_infos:
            date_idxs.append((c.date - RUN_START_DATE).days)
            cabinet_idxs.append(
                [
                    c.ministry_idx,
                    c.ministry_date_idxs,
                    c.deputy_pm_idxs,
                    c.composition_idxs,
                    c.key_info_idxs,
                    c.ministers_idxs,
                ]
            )
        date_idxs.append((RUN_END_DATE - RUN_START_DATE).days)
        return cabinet_idxs, date_idxs

    # @property
    # def period_str(self):
    #     s_date_str = format_lang_date(self.ministry_info.start_date, self.lang, 'd MMMM YYYY')
    #     e_date_str = format_lang_date(self.ministry_info.end_date, self.lang, 'd MMMM YYYY')
    #     return f'{s_date_str} - {e_date_str}'

    # def get_ministers_idxs(self, offi_dict, dept_dict, role_dict, mini_dict, officer_info_dict):
    #     def get_min_role(m):
    #         # returns the role and the its index
    #         return min([(role_dict[r], r) for _, r in m['posts']], key=itemgetter(0))

    #     def get_min_dept(m):
    #         # returns the role and the its index
    #         return min([(dept_dict[d], d) for d, _ in m['posts']], key=itemgetter(0))

    #     def get_min_idx(m):
    #         min_role_idx, _ = get_min_role(m)
    #         min_dept_idx, _ = get_min_dept(m)
    #         return (min_role_idx, min_dept_idx)

    #     if len(self.ministers) == 0:
    #         return []

    #     self.sorted_ministers = sorted(self.ministers, key=get_min_idx)
    #     minister_idxs = []
    #     for minister in self.sorted_ministers:
    #         post_idxs = [[dept_dict[d], role_dict[r]] for (d, r) in minister['posts']]
    #         offi_idx = offi_dict[minister['officer_id']]
    #         minister_idxs.append([offi_idx, post_idxs])

    #     # calculate composition
    #     role_counter = Counter(get_min_role(m)[1] for m in self.sorted_ministers)
    #     self.composition = [(r, role_counter[r]) for r in ROLE_SENIORITY[1:] if role_counter[r] > 0]

    #     first_role, first_idx = get_min_role(self.sorted_ministers[0])
    #     is_first_pm = True if first_idx == 0 else False
    #     deputy_pm_ids = self.ministry_info.get_deputy_pm_ids(self.date)
    #     deputy_pm_idxs = [offi_dict[d_id] for d_id in deputy_pm_ids]

    #     key_ministers = self.sorted_ministers[1:6] if first_pm else self.sorted_ministers[0:5]

    #     self.key_info = [(officer_info_dict[m['officer_id']].full_name, get_min_dept(m)[1]) for m in key_ministers]
    #     self.key_idxs = [(offi_dict[m['officer_id']], get_min_dept(m)[0]) for m in key_ministers]
    #     # add prime minister please
    #     return [mini_dict[self.name], deputy_pm_idxs, minister_idxs]

    # def populate_officer_info(self, officer_info_dict):
    #     def get_post_str(dept, role):
    #         pStr = dept if not role else f"{dept}[{role}]"
    #         pStr = "" if pStr is None else pStr
    #         pStr = f'<p class="text-sm font-normal leading-4">{pStr}</p>'
    #         return pStr

    #     for minister in self.sorted_ministers:
    #         offi_info = officer_info_dict[minister['officer_id']]
    #         minister['name'] = offi_info.full_name
    #         minister['url'] = offi_info.url
    #         minister['image_url'] = offi_info.image_url
    #         post_strs = [get_post_str(d, r) for (d, r) in minister['posts']]
    #         minister['short_post_str'] = '\n'.join(post_strs[:3])
    #         minister['long_post_str'] = '\n'.join(post_strs[3:])


LANG_CODES = [
    'as',
    'bn',
    'brx',
    'doi',
    'gu',
    'hi',
    'kn',
    'ks',
    'gom',
    'mai',
    'mni',
    'ml',
    'mr',
    'ne',
    'or',
    'pa',
    'sa',
    'sat',
    'sd',
    'ta',
    'te',
    'ur',
    'en',
]
# LANG_CODES = [ 'en', 'hi']


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
        "dept_hierarchy_file": "conf/dept.yml",
        "template_stub": "miniHTML",
    },
)
class WebsiteLanguageGenerator:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        officer_info_files,
        ministry_file,
        output_dir,
        languages,
        translation_file,
        dept_hierarchy_file,
        template_stub,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.officer_info_files = officer_info_files
        self.ministry_path = Path(ministry_file)
        self.output_dir = Path(output_dir)
        self.languages = languages
        self.translation_file = Path(translation_file)
        self.dept_hier_path = Path(dept_hierarchy_file)
        if self.dept_hier_path.exists():
            self.dept_hier = read_config_from_disk(self.dept_hier_path)
            self.depts = [d['name'] for d in self.dept_hier['ministries']]
            self.depts.append('')
        else:
            self.depts = []

        ### TODO CHANGE THIS, to read from input file
        self.languages = LANG_CODES
        # self.languages = ['en', 'hi']

        self.officer_info_dict = self.get_officer_infos(self.officer_info_files)
        print(f"#Officer_info: {len(self.officer_info_dict)}")

        # self.officer_idx_dict = dict((o.officer_idx, o.officer_id) for o in self.officer_info_dict.values())

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
            yml_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)
            self.ministry_infos = self.build_ministryinfos(yml_dict)
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
        return True if self.ministry_infos else False

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

            info_dict = dict((d["officer_id"], OfficerInfo(d, idx + 1)) for idx, d in enumerate(info_dict["officers"]))
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
            print(text)
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
        m, l = manager_info, copy.copy(manager_info)
        l.full_name = self.translate_name(m.full_name, lang)
        l.role = self.translate_post_field('role', m.role, lang)
        l.lang = lang
        return l

    def translate_tenureinfo(self, tenure_info, lang):
        t, l = tenure_info, copy.copy(tenure_info)
        l._dept = self.translate_post_field('dept', t.dept, lang)
        l._role = self.translate_post_field('role', t.role, lang)
        # l.post = self.translate_postinfo(t.post, lang)
        l.manager_infos = [self.translate_managerinfo(m, lang) for m in t.manager_infos]

        l.all_orderid_detailidxs = []
        for (o_id, category, d_idx, short_date_str) in t.all_orderid_detailidxs:
            o_info = self.order_info_dict[o_id]
            l_category = self.translate_label(category, lang)
            l_short_date_str = format_lang_date(o_info.order.date, lang, 'd MMM yyyy')
            l.all_orderid_detailidxs.append((o_id, l_category, d_idx, l_short_date_str))
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
            l_posts = [self.translate_postinfo(p, lang) for p in posts]
            l_pType = self.translate_label(pType, lang)
            l.postinfo_dict[l_pType] = l_posts

        l.short_post_str, l.long_post_str = l.get_all_html_post_str()
        return l

    def translate_orderinfo(self, order_info, lang):
        o, l = order_info, copy.copy(order_info)
        l.ministry = self.translate_ministry(o.ministry, lang)
        l.category = self.translate_label(o.category, lang)

        l.details = [self.translate_detailinfo(d, lang) for d in o.details]
        l._num_details = self.translate_digits(str(o._num_details), lang)
        l.lang = lang
        return l

    def translate_cabinetinfo(self, cabinet_info, lang):
        def t_dept(dept):
            return self.translate_post_field('dept', dept, lang)

        def t_role(role):
            return self.translate_post_field('role', role, lang)

        def get_post_str(dept, role):
            dept, role = t_dept(dept), t_role(role)
            pStr = dept if not role else f"{dept}[{role}]"
            pStr = "" if pStr is None else pStr
            pStr = f'<p class=a-b>{pStr}</p>'
            return pStr

        c, l = cabinet_info, copy.copy(cabinet_info)
        l.lang = lang

        l.name = self.translate_ministry(c.name, lang)
        l_sorted_ministers = []
        for m in c.sorted_ministers:
            l_m = {}
            l_m['name'] = self.translate_name(m['name'], lang)
            l_m['url'] = m['url']
            l_m['image_url'] = m['image_url']
            posts = [get_post_str(d, r) for (d, r) in m['posts']]
            l_m['short_post_str'] = ''.join(posts[:3])
            l_m['long_post_str'] = ''.join(posts[3:])
            l_sorted_ministers.append(l_m)
        l.sorted_ministers = l_sorted_ministers
        l.key_info = [(self.translate_name(n, lang), t_dept(d)) for (n, d) in c.key_info]
        l.composition = [(t_role(r), self.translate_digits(str(c), lang)) for (r, c) in c.composition if c > 0]
        return l

    def translate_months(self, lang):
        def get_month(month_idx):
            dt = datetime.date(year=2022, month=month_idx, day=1)
            dt_str = format_date(dt, format='d MMMM YYYY', locale=loc_lang)
            return dt_str.split()[1]

        loc_lang = 'kok' if lang == 'gom' else ('hi' if lang == 'sd' else lang)
        return [''] + [get_month(m) for m in range(1, 13)]

    def translate_idx2str(self, idx2str, lang):
        def t_dept(dept):
            return self.translate_post_field('dept', dept, lang)

        def t_role(role):
            return self.translate_post_field('role', role, lang)

        l_idx2str = {}
        l_idx2str['offi'] = [[self.translate_name(n, lang), u, i] for (n, u, i) in idx2str['offi']]
        l_idx2str['dept'] = [t_dept(d) for d in idx2str['dept']]
        l_idx2str['role'] = [t_role(r) for r in idx2str['role']]
        l_idx2str['mini'] = [self.translate_ministry(m, lang) for m in idx2str['mini']]
        l_idx2str['digits'] = [self.translations['digits'][c][lang] for c in idx2str['digits']]
        l_idx2str['months'] = self.translate_months(lang)
        return l_idx2str

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

    def build_keyinfo(self, tenure):
        post = self.post_dict[tenure.post_id]
        tenure_dates = (tenure.start_date, tenure.end_date)
        role = tenure.role if tenure.role else 'Cabinet Minister'
        return KeyInfo(post.dept, role, tenure_dates)

    def build_tenureinfo(self, tenure):
        post = self.post_dict[tenure.post_id]
        start_order_id = tenure.start_order_id
        start_url = f"order-{start_order_id}.html?detail_num={tenure.start_detail_idx+1}"

        if tenure.end_order_id:
            end_order_id = tenure.end_order_id
            end_url = f"order-{end_order_id}.html?detail_num={tenure.end_detail_idx+1}"
        else:
            end_url = ""

        o_dict = self.order_info_dict
        oid_didxs = [
            [o_id, o_dict[o_id].category, d_idx, o_dict[o_id].short_date_str]
            for (o_id, d_idx) in tenure.all_order_infos
        ]
        return TenureInfo(tenure, post, start_url, end_url, oid_didxs)

    def populate_manager_infos(self, officer_info):
        all_tenure_infos = flatten(officer_info.ministries.values())
        leaf_roles = ('Minister of State', 'Deputy Minister')

        borders = [" border-t", "", " border-b"]
        top_margins = [" relative -mt-11", "", " -mb-11"]

        for (pos_idx, t) in enumerate(all_tenure_infos):
            t.tenure_pos = pos_idx
            t.manager_infos.append(ManagerInfo.build(officer_info, t.tenure))

            if t.tenure.role and t.tenure.role in leaf_roles:
                for manager_id in t.tenure.manager_ids[:1]:
                    m_tenure = self.tenure_dict[manager_id]
                    m_officer_info = self.officer_info_dict[m_tenure.officer_id]
                    t.manager_infos.append(ManagerInfo.build(m_officer_info, m_tenure))

            if not self.has_ministry():
                continue

            ministry = self.get_ministry(t.tenure.start_date)
            assert ministry, 'Wrong date {t.tenure.start_date}'

            url = self.officer_info_dict[ministry.pm_id].url
            image_url = self.officer_info_dict[ministry.pm_id].image_url

            t.manager_infos.append(ManagerInfo.build_pm(ministry, url, image_url))
            t.manager_infos_count = len(t.manager_infos)

            # Always keep manager_infos to be of 3 and manage hidden elsewhere
            pm_info = t.manager_infos[-1]
            for idx in range(t.manager_infos_count, 3):
                t.manager_infos.append(copy.copy(pm_info))

            t.manager_infos.reverse()
            for idx, manager_info in enumerate(t.manager_infos):
                # if len(t.manager_infos) == 2 and idx == 1:
                #     # middle one needs to be skipped
                #     idx += 1
                manager_info.border = borders[idx]
                manager_info.top_margin = top_margins[idx]

        # end

    def get_tenure_jsons(self, officer_info):
        tenure_jsons = []
        for (ministry_idx, (ministry, tenures)) in enumerate(officer_info.ministries.items()):
            for tenure_info in tenures:
                t_json = {'ministry': ministry}
                t_json['tenure_start_date_idx'] = f'{tenure_info.tenure_start_date_idx}'
                t_json['tenure_idx'] = f'{tenure_info.tenure_idx}'
                t_json['ministry_idx'] = ministry_idx
                t_json['dept'] = f'{tenure_info.dept}'
                t_json['role'] = f'{tenure_info.role}'
                t_json['date_str'] = f'{tenure_info.start_date_str} - {tenure_info.end_date_str}'
                t_json['start_order_id'] = tenure_info.start_order_id
                t_json['end_order_id'] = tenure_info.end_order_id
                t_json['start_order_url'] = tenure_info.start_order_url
                t_json['end_order_url'] = tenure_info.end_order_url
                t_json['manager_infos_count'] = tenure_info.manager_infos_count
                t_json['all_orderid_detailidxs'] = tenure_info.all_orderid_detailidxs

                t_json['manager_infos'] = []
                for manager_info in tenure_info.manager_infos:
                    m_info = {
                        'full_name': manager_info.full_name,
                        'url': manager_info.url,
                        'image_url': manager_info.image_url,
                        'role': manager_info.role,
                        'date_str': f'{manager_info.start_date_str} - {manager_info.end_date_str}',
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

    def build_cabinet_info(self, date, date_tenures, str2idx):
        def get_min_role(m):
            # returns the role and the its index
            return min([(role_dict[r], r) for _, r in m['posts']], key=itemgetter(0))

        def get_min_dept(m):
            # returns the dept and the its index
            return min([(dept_dict[d], d) for d, _ in m['posts']], key=itemgetter(0))

        def get_min_idx(m):
            min_role_idx, _ = get_min_role(m)
            min_dept_idx, _ = get_min_dept(m)
            return (min_role_idx, min_dept_idx)

        def get_dept_id(post_id):
            return post_id.split('>')[1] if '>' in post_id else ''

        def has_dept(m, dept):
            return any(d for d, _ in m['posts'] if d == dept)

        ministry = self.get_ministry(date)
        ministry_idx = [idx for (idx, m) in enumerate(self.ministry_infos) if m.name == ministry.name][0]
        m_s, m_e = (ministry.start_date, ministry.end_date)
        ministry_date_idxs = [[m_s.year, m_s.month, m_s.day], [m_e.year, m_e.month, m_e.day]]

        [offi_dict, dept_dict, role_dict, mini_dict] = str2idx

        # group tenures by minister
        tenures = sorted(date_tenures, key=attrgetter('officer_id'))
        ministers = []
        for officer_id, offi_tenures in groupby(tenures, key=attrgetter('officer_id')):
            posts = [(get_dept_id(t.post_id), t.role) for t in offi_tenures]
            ministers.append({'officer_id': officer_id, 'posts': posts})

        sorted_ministers = sorted(ministers, key=get_min_idx)

        if sorted_ministers:
            first_idx, _ = get_min_role(sorted_ministers[0])
            is_first_pm = True if first_idx == 0 else False
            pm_idx = [offi_dict[ministry.pm_id], [(dept_dict[''], role_dict['Prime Minister'])]]
            ministers_idxs = [] if is_first_pm else [pm_idx]
        else:
            ministers_idxs = []

        for m in sorted_ministers:
            name_idx = offi_dict[m['officer_id']]
            post_idxs = [(dept_dict[d], role_dict[r]) for (d, r) in m['posts']]
            ministers_idxs.append([name_idx, post_idxs])

        # calculate composition
        role_counter = Counter(get_min_role(m)[1] for m in sorted_ministers)
        composition_idxs = [(role_dict[r], role_counter[r]) for r in ROLE_SENIORITY[1:] if role_counter[r] > 0]

        deputy_pm_ids = ministry.get_deputy_pm_ids(date)
        deputy_pm_idxs = [offi_dict[d_id] for d_id in deputy_pm_ids]

        if sorted_ministers:
            key_ministers = []

            for dept in KEY_DEPTS:
                for minister in sorted_ministers:
                    if has_dept(minister, dept):
                        key_ministers.append(minister)
                        break

            assert key_ministers

            # key_ministers = sorted_ministers[1:6] if is_first_pm else sorted_ministers[0:5]
            key_info_idxs = [(offi_dict[m['officer_id']], get_min_dept(m)[0]) for m in key_ministers[:4]]
        else:
            key_info_idxs = []

        return CabinetInfo(
            date, ministry_idx, ministry_date_idxs, deputy_pm_idxs, composition_idxs, key_info_idxs, ministers_idxs
        )

    def build_cabinet_infos(self, tenures):
        def get_overlapping_tenures(sorted_tenures, dt, start_idx):
            print(f'Overlapping: {dt}')
            s_idx, o_tenures = -1, []
            for (idx, tenure) in enumerate(sorted_tenures[start_idx:]):
                if dt in tenure:
                    if s_idx == -1:
                        s_idx = idx + start_idx
                    o_tenures.append(tenure)
            return s_idx, o_tenures

        # build str2idx
        offi_dict = dict((oid, idx) for (idx, oid) in enumerate(self.officer_info_dict))
        dept_dict = dict((d, idx) for (idx, d) in enumerate(self.depts))
        role_dict = dict((r, idx) for (idx, r) in enumerate(ROLE_SENIORITY))
        mini_dict = dict((m.name, idx) for (idx, m) in enumerate(self.ministry_infos))
        role_dict[None] = 2  # cabinet minister
        str2idx = [offi_dict, dept_dict, role_dict, mini_dict]

        # build idx2str
        idx2str = {}
        offi_infos = self.officer_info_dict.values()
        idx2str['offi'] = [[o.full_name, o.url, o.image_url] for o in offi_infos]
        idx2str['dept'] = self.depts
        idx2str['role'] = ROLE_SENIORITY
        idx2str['mini'] = [m.name for m in self.ministry_infos]
        idx2str['months'] = list(calendar.month_name)
        idx2str['digits'] = list(string.digits)

        start_dates = [t.start_date for t in tenures]
        end_dates = [t.end_date for t in tenures if t.end_date]

        all_dates = sorted(set(start_dates + end_dates))
        sorted_tenures = sorted(tenures, key=attrgetter('start_date'))

        start_idx, cabinet_infos = 0, []
        for dt in all_dates:
            if dt > RUN_END_DATE:
                continue

            new_sidx, o_tenures = get_overlapping_tenures(sorted_tenures, dt, start_idx)
            cabinet_infos.append(self.build_cabinet_info(dt, o_tenures, str2idx))
            start_idx = new_sidx if new_sidx != -1 else start_idx
        return cabinet_infos, idx2str

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

    def render_html(self, entity, obj, lang='en'):
        template = self.env.get_template(f"{entity}.html")
        # l_site_info = self.translate_siteinfo(self.site_info, lang)
        l_site_info = self.lang_label_info_dict[lang]
        setattr(l_site_info, lang, ' selected')

        if entity == "officer":
            l_site_info.page_url = obj.url  # f'officer-{obj.officer_idx}.html' ## TODO change this to URL
            l_site_info.title = f'{l_site_info.ministers}: {obj.full_name}'
            obj.crumbs = [
                (l_site_info.home, 'prime.html'),
                (l_site_info.ministers, f'officers-{obj.first_char.upper()}.html'),
                (obj.full_name, obj.url),  # f'officer-{obj.officer_idx}.html'),
            ]
            return template.render(site=l_site_info, officer=obj)
        elif entity == "officers":
            l_site_info.page_url = f'officers-{obj.idx}.html'
            l_site_info.title = f'{l_site_info.ministers} ({obj.idx})'
            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.ministers, f'officers-{obj.idx.upper()}.html')]
            return template.render(site=l_site_info, officer_group=obj)
        elif entity == "order":
            l_site_info.page_url = f'order-{obj.order_id}.html'
            l_site_info.title = f'{l_site_info.order}: ({obj.order_id})'

            obj.crumbs = [
                (l_site_info.home, 'prime.html'),
                (l_site_info.orders, f'orders-{obj.get_ministry_years_str()}.html'),
                (obj.order_id, f'order-{obj.order_id}.html'),
            ]

            return template.render(site=l_site_info, order=obj)
        elif entity == "orders":
            obj_idx = obj.idx.replace(' ', '_')
            l_site_info.page_url = f'orders-{obj_idx}.html'
            l_site_info.title = f'{l_site_info.orders} ({obj.idx})'
            obj.crumbs = [(l_site_info.home, 'prime.html'), (l_site_info.orders, 'orders.html')]

            return template.render(site=l_site_info, order_group=obj)
        elif entity == "prime":
            l_site_info.page_url = 'prime.html'
            l_site_info.title = f'{l_site_info.prime_ministers}'
            return template.render(site=l_site_info, primes=obj)
        elif entity == "deputy":
            l_site_info.page_url = 'deputy.html'
            l_site_info.title = f'{l_site_info.deputy_prime_ministers}'
            return template.render(site=l_site_info, primes=obj)
        elif entity == "ministry":
            l_site_info.page_url = 'ministry.html'
            l_site_info.title = f'{l_site_info.council_of_ministers}'
            return template.render(site=l_site_info, cabinet=obj)
        else:
            raise NotImplementedError(f'Not implemented for entity: {entity}')

    def gen_prime_page(self):
        prime_dict = {}

        assert len(self.officer_info_dict) > 0
        [prime_dict.setdefault(m.pm_id, []).append(m) for m in self.ministry_infos]

        en_prime_infos = []
        for pm_id, ministries in prime_dict.items():
            tenure_date_pairs = MinistryInfo.get_tenure_date_pairs(ministries)
            prime_officer_info = self.officer_info_dict[pm_id]

            prime_officer_info.prime_tenure_date_pairs = tenure_date_pairs
            en_prime_infos.append(prime_officer_info)

        html_path = self.get_html_path("prime", "")
        # html_path.write_text(self.render_html("prime", en_prime_infos))

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
        for (deputy_id, deputy_pms) in deputy_dict.items():
            tenure_date_pairs = MinistryInfo.get_tenure_date_pairs(deputy_pms, is_deputy=True)
            deputy_officer_info = self.officer_info_dict[deputy_id]
            deputy_officer_info.deputy_tenure_date_pairs = tenure_date_pairs
            en_deputy_infos.append(deputy_officer_info)

        html_path = self.get_html_path("deputy", "")
        # html_path.write_text(self.render_html("deputy", en_deputy_infos))

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
        if not self.has_ministry():
            html_path.write_text(self.render_html("order", order_info))

        for lang in self.languages:
            html_path = self.get_html_path("order", order.order_id, lang)
            lang_order_info = self.translate_orderinfo(order_info, lang)
            html_path.write_text(self.render_html("order", lang_order_info, lang))

    def get_ministry(self, dt):
        ministry = first([m for m in self.ministry_infos if m.has_date(dt)], None)
        return ministry

    def gen_officer_page(self, officer_idx, officer_id, tenures):
        def seniority(tenure):
            role_s = ROLE_SENIORITY.index(tenure.role if tenure.role else 'Cabinet Minister')
            if '>' in tenure.post_id:
                dept = tenure.post_id.split('>')[1]
                dept_idx = self.depts.index(dept) if dept in self.depts else 1000
            else:
                dept_idx = 1000

            return (role_s, dept_idx, -tenure.duration_days)

        def role_dept(tenure):
            role_s = ROLE_SENIORITY.index(tenure.role if tenure.role else 'Cabinet Minister')
            if '>' in tenure.post_id:
                dept = tenure.post_id.split('>')[1]
                dept_idx = self.depts.index(dept) if dept in self.depts else 1000
            else:
                dept_idx = 1000
            return (role_s, dept_idx)

        def tenure_ministry(tenure):
            if not self.ministry_infos:
                return "No Ministry"
            ministry = self.get_ministry(tenure.start_date)
            return ministry.name

        def merge_pairs(spans, span):
            if not spans:
                return [span]

            last_span = spans[-1]
            if last_span[1] == span[0]:
                spans[-1] = (last_span[0], span[1])
            else:
                spans.append(span)
            return spans

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

        # key_tenures = sorted(tenures, key=seniority)[:3]
        # key_infos += [self.build_keyinfo(t) for t in key_tenures]

        tenures = sorted(tenures, key=role_dept)
        for (group_key, group_tenures) in groupby(tenures, key=role_dept):
            group_tenures = list(group_tenures)
            tenure = group_tenures[0]
            if '>' not in tenure.post_id:
                continue
            role = tenure.role if tenure.role else 'Cabinet Minister'
            dept = tenure.post_id.split('>')[1]
            date_pairs = [(t.start_date.year, t.end_date.year) for t in group_tenures]
            date_pairs = functools.reduce(merge_pairs, date_pairs, [])

            key_infos.append(KeyInfo(dept, role, date_pairs))

        officer_info.ministries = ministries
        officer_info.key_infos = key_infos[:3]
        # officer_info.url = f"officer-{officer_info.officer_idx}.html" # TODO why is this needed ?
        # officer_info.officer_idx = officer_idx

        self.populate_manager_infos(officer_info)
        officer_info.tenure_json_str = json.dumps(
            self.get_tenure_jsons(officer_info), separators=(',', ':'), ensure_ascii=False
        )  # , indent=2)

        html_path = self.get_html_path("o", officer_info.url_name)
        if not self.has_ministry():
            html_path.write_text(self.render_html("officer", officer_info))

        for lang in self.languages:
            html_path = self.get_html_path("o", officer_info.url_name, lang)
            lang_officer_info = self.translate_officerinfo(officer_info, lang)
            lang_officer_info.tenure_json_str = json.dumps(
                self.get_tenure_jsons(lang_officer_info), separators=(',', ':'), ensure_ascii=False
            )  # , indent=2)
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
            if not self.has_ministry():
                html_path.write_text(self.render_html("officers", og_info))
            officer_group_infos.append(og_info)
        # end for

        for lang in self.languages:
            for og in officer_group_infos:
                lang_infos = [self.translate_officerinfo(o, lang) for o in og.officer_infos]
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
            or_info = OrderGroupInfo(order_group, idx, all_idxs, all_idxs, all_ministries)
            html_path = self.get_html_path("orders", idx)
            if not self.has_ministry():
                html_path.write_text(self.render_html("orders", or_info))
            order_group_infos.append(or_info)

        print(f"Order groups: {len(order_groups)}")

        for lang in self.languages:
            all_lang_idxs = [g[0].get_ministry_years_str(lang) for g in order_groups]
            all_lang_ministries = [self.translate_ministry(m, lang) for m in all_ministries]
            for og in order_group_infos:
                lang_infos = [self.translate_orderinfo(o, lang) for o in og.order_infos]
                idx = lang_infos[0].get_ministry_years_str(lang)
                en_idx = og.order_infos[0].get_ministry_years_str('en')
                lang_og_info = OrderGroupInfo(lang_infos, idx, all_idxs, all_lang_idxs, all_lang_ministries)
                html_path = self.get_html_path("orders", en_idx, lang)
                html_path.write_text(self.render_html("orders", lang_og_info, lang))

    def gen_details_page(self, doc):
        order = doc.order
        if not order.details:
            return

        order_info = self.order_info_dict[order.order_id]
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

    def gen_cabinet_page(self, tenures):
        cabinet_infos, idx2str = self.build_cabinet_infos(tenures)
        last_cabinet_info = cabinet_infos[-1]

        cabinet_idxs, date_idxs = CabinetInfo.get_json_idxs(cabinet_infos)

        for lang in self.languages:
            lang_idx2str = self.translate_idx2str(idx2str, lang)
            last_cabinet_info.idx2str = lang_idx2str

            html_path = self.get_html_path("ministry", '', lang)
            html_path.write_text(self.render_html("ministry", last_cabinet_info, lang))

            lang_idx2str['ministry_idxs'] = cabinet_idxs
            lang_idx2str['date_idxs'] = date_idxs

            idx_file = self.output_dir / lang / Path("ministry_idx.json")
            idx_file.write_text(json.dumps(lang_idx2str, separators=(',', ':'), ensure_ascii=False))

    def write_top_pages(self):
        for top_page in ['index.html', 'disclaimer.html', 'languages.html']:
            top_file = self.output_dir / top_page
            top_file.write_text((self.template_dir / top_page).read_text())

    def write_search_index(self):
        from lunr import lunr

        docs = [o.get_searchdoc_dict() for o in self.officer_info_dict.values()]

        lunrIdx = lunr(ref="idx", fields=["full_name", "officer_id"], documents=docs)

        search_index_file = self.output_dir / "lunr.idx.json"
        search_index_file.write_text(json.dumps(lunrIdx.serialize(), separators=(',', ':')))

        docs_file = self.output_dir / "docs.json"
        docs_file.write_text(json.dumps(docs, separators=(',', ':')))

    def pipe(self, docs, **kwargs):
        self.add_log_handler()
        docs = list(docs)
        print("Entering website builder")
        self.lgr.info("Entering website builder")

        self.lgr.info(f"Handling #docs: {len(docs)}")

        self.write_top_pages()

        orders = [doc.order for doc in docs if doc.order.date]
        orders.sort(key=attrgetter("date"))
        self.order_dict = dict((o.order_id, o) for o in orders)
        self.order_idx_dict = dict((o.order_id, i) for (i, o) in enumerate(orders))

        self.lgr.info(f"Handling #orders: {len(orders)}")

        self.post_dict = dict((p.post_id, p) for o in orders for p in o.get_posts())

        self.tenures = list(flatten(doc.tenures for doc in docs))
        self.tenures.sort(key=attrgetter("tenure_id"))
        self.tenure_dict = dict((t.tenure_id, t) for t in self.tenures)

        # assert [t.tenure_idx for t in self.tenures] == list(range(len(self.tenures)))

        self.lgr.info(f"Handling #tenures: {len(self.tenures)}")

        if self.has_ministry():
            self.gen_cabinet_page(self.tenures)
            self.gen_prime_page()
            self.gen_deputy_prime_page()

        [self.gen_order_page(idx, o) for idx, o in enumerate(orders)]

        officer_key = attrgetter("officer_id")
        officer_groups = groupby(sorted(self.tenures, key=officer_key), key=officer_key)
        for (officer_idx, (officer_id, officer_tenures)) in enumerate(officer_groups):
            self.gen_officer_page(officer_idx, officer_id, officer_tenures)

        self.gen_officers_page()
        self.gen_orders_page()

        print('Generating Details')
        if self.has_ministry():
            [self.gen_details_page(doc) for doc in docs]

        print('Writing Search Index')
        self.write_search_index()

        self.lgr.info("Leaving website builder")
        self.remove_log_handler()
        return docs
