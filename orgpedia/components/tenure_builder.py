import datetime
import json
import logging
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import groupby
from operator import attrgetter
from pathlib import Path
from typing import List

import yaml
from dateutil import parser
from docint.data_error import DataError
from docint.vision import Vision
from more_itertools import flatten, pairwise

from ..extracts.orgpedia import Tenure

# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34


class TenureManagerWithLeafRole(DataError):
    @classmethod
    def build(cls, tenure, manager_tenures):
        mgrs_id = ", ".join(t.officer_id for t in manager_tenures)
        msg = f"t_id: {tenure.officer_id} mgrs_id: {mgrs_id} post_id: {tenure.post_id}"
        path = f"te.{tenure.tenure_id}"
        return TenureManagerWithLeafRole(path=path, msg=msg, name='TenureManagerWithLeafRole')


class TenureWithNoManager(DataError):
    @classmethod
    def build(cls, tenure):
        msg = f"t_id: {tenure.officer_id} post_id: {tenure.post_id}"
        path = f"te.{tenure.tenure_id}"
        return TenureWithNoManager(path=path, msg=msg, name='TenureWithNoManager')


class TenureMissingAssumeError(DataError):
    
    @classmethod
    def build(cls, info):
        path = f"order.details{info.detail_idx}"
        order_str = f"{info.order_id}[{info.detail_idx}]"
        post_str = f"Post: {info.post_id}"
        officer_str = f"Officer: {info.officer_id}"
        msg = f"Missing assume for {info.verb}: {order_str} {post_str} {officer_str}"
        return TenureMissingAssumeError(path=path, msg=msg, name='TenureMissingAssume')


class TenureMultipleRolesError(DataError):
    roles: List[str]

    @classmethod
    def build(cls, info):
        def role_info(i):
            return f"{i.order_id}:{i.detail_idx}-{i.role}"

        path = f"order.details{info.detail_idx}"
        roles = [i.role for i in info.all_infos if i.role]
        role_str = "|".join(role_info(i) for i in info.all_infos)
        officer_str = f"Officer: {info.officer_id}"
        msg = f"Multiple roles {officer_str} {role_str}"
        return TenureMultipleRolesError(path=path, msg=msg, roles=roles, name='TenureMultipleRoles')


class TenureLongError(DataError):
    duration_days: int

    @classmethod
    def build(cls, tenure):
        msg = "Long Tenure " + str(tenure)
        print(msg)
        return TenureLongError(path='', msg=msg, duration_days=tenure.duration_days, name='TenureLong')


class TenureGapError(DataError):
    gap_years: int

    @classmethod
    def build(cls, tenure, gap_years):
        msg = f"Long Tenure Gap: {gap_years} years " + str(tenure)
        print(msg)
        return TenureGapError(path='', msg=msg, gap_years=gap_years, name='TenureGap')


# This should be called DetailPostInfo
@dataclass
class DetailInfo:
    order_id: str
    order_date: datetime.date
    detail_idx: int
    officer_id: str
    verb: str
    post_id: str
    role: str
    order_category: str
    all_infos: List = field(default_factory=[])

    def __str__(self):
        return f"{self.order_id} {self.officer_id} {self.post_id}"

    @property
    def verb_code(self):
        return 0 if self.verb == 'relinquishes' else 1 if self.verb == 'assumes' else 0


@Vision.factory(
    "tenure_builder",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "tenure_builder",
        "ministry_file": "conf/ministries.yml",
        "default_role": "Cabinet Minister",
    },
)
class TenureBuilder:
    def __init__(self, conf_dir, conf_stub, ministry_file, default_role):
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.ministry_path = Path(ministry_file)

        self.first_orders_dict = {}
        if self.ministry_path.exists():
            self.ministry_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)
            for m in self.ministry_dict["ministries"]:
                s, e = m["start_date"], m["end_date"]
                m["start_date"] = parser.parse(s).date()
                m["end_date"] = parser.parse(e).date() if e != "today" else datetime.date.today()
                self.first_orders_dict[m["first_order_id"]] = m["start_date"]
        else:
            self.ministry_dict = {}

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.curr_tenure_idx = -1
        self.default_role = default_role
        self.officer_start_date_dict = defaultdict(list)

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

    def build_detail_infos(self, order):
        def valid_date(order):
            if not order.date:
                return False

            y = order.date.year
            return False if (y < 1947 or y > 2033) else True

        def iter_posts(detail):
            if not detail.officer.officer_id:
                return

            for verb in ["continues", "relinquishes", "assumes"]:
                for post in getattr(detail, verb):
                    yield verb, post

        def get_duplicates(iter):
            seen = set()
            return set(i for i in iter if i in seen or seen.add(i))

        def build_info(detail, verb, post):
            role = post.role_hpath[-1] if post.role_hpath else None

            order_date = order.date
            # if order.order_id in self.first_orders_dict:
            #     order_date = self.first_orders_dict[order.order_id]
            #     order.date = order_date
            #     print(f'Changing {order.date} -> {order_date}')

            return DetailInfo(
                order.order_id,
                order_date,
                detail.detail_idx,
                detail.officer.officer_id,
                verb,
                post.post_id,
                role,
                order.category,
                [],
            )

        if order.order_id == "1_Upload_3296.pdf":
            pass

        if not valid_date(order):
            return []

        # remove details with identical officer_ids
        dup_o_ids = get_duplicates(d.officer.officer_id for d in order.details)

        if dup_o_ids:
            print(f'Duplicate order_ids: {order.order_id} {dup_o_ids}')

        v_details = [d for d in order.details if d.officer.officer_id not in dup_o_ids]

        return [build_info(d, v, p) for d in v_details for (v, p) in iter_posts(d)]

    def ministry_end_date(self, date):
        assert self.ministry_dict
        for m in self.ministry_dict["ministries"]:
            if m["start_date"] <= date < m["end_date"]:
                return m["end_date"]
        return None

    def get_ministry(self, date):
        assert self.ministry_dict
        for m in self.ministry_dict["ministries"]:
            if m["start_date"] <= date < m["end_date"]:
                return m["name"]
        return None

    def build_officer_tenures(self, officer_id, detail_infos):  # noqa C901
        officer_tenure_idx = -1

        def build_tenure(start_info, end_order_id, end_date, end_detail_idx):
            nonlocal officer_tenure_idx
            # self.curr_tenure_idx += 1
            officer_tenure_idx += 1

            roles = [start_info.role] + [i.role for i in start_info.all_infos]
            roles_counter = Counter(roles)

            if len(roles_counter) > 1:
                roleError = TenureMultipleRolesError.build(start_info)
                self.lgr.debug(roleError.msg)
                errors.append(roleError)

            role = max(roles_counter, key=roles_counter.get, default=self.default_role)
            all_order_infos = [(i.order_id, i.detail_idx) for i in start_info.all_infos]

            osd_key = f'{start_info.officer_id}-{start_info.order_date}'
            officer_start_date_idx = len(self.officer_start_date_dict[osd_key])
            self.lgr.info(f'\t\tNEW T: [{start_info.officer_id},{start_info.order_date}->{end_order_id},{end_date}]')
            return Tenure(
                tenure_id=f'{start_info.officer_id}-{officer_tenure_idx}',
                tenure_idx=officer_tenure_idx,
                officer_id=start_info.officer_id,
                post_id=start_info.post_id,
                start_date=start_info.order_date,
                end_date=end_date,
                start_order_id=start_info.order_id,
                start_detail_idx=start_info.detail_idx,
                end_order_id=end_order_id,
                end_detail_idx=end_detail_idx,
                role=role,
                all_order_infos=all_order_infos,
                officer_start_date_idx=officer_start_date_idx,
            )

        def get_postids(infos):
            infos = list(infos)
            if not infos:
                return "[0]"

            if isinstance(infos[0], DetailInfo):
                post_ids = [i.post_id for i in infos]
            else:
                post_ids = list(infos)
            return f'[{len(post_ids)}]: {", ".join(post_ids)}'

        errors = []

        def handle_order_infos(order_infos):
            first = order_infos[0]
            o_id, o_date, d_idx = first.order_id, first.order_date, first.detail_idx
            o_tenures = []
            active_posts = set(postid_info_dict.keys())
            self.lgr.info(f"\tActive{get_postids(active_posts)} Order{get_postids(order_infos)}")
            if first.order_category == "Council of Ministers":
                ### TODO COUNCIL OF MINISTERS CAN HAVE RELINQUISHED POST AS WELL, THIS handles CONTINUES
                order_posts = set(i.post_id for i in order_infos)
                ignored_posts = list(active_posts - order_posts)
                if ignored_posts:
                    ignored_posts.sort()
                    for post_id in ignored_posts:
                        ignored_info = postid_info_dict[post_id]
                        o_tenures.append(build_tenure(ignored_info, o_id, o_date, d_idx))
                        del postid_info_dict[post_id]
                    self.lgr.info(f"\t\tClosing Actives*{get_postids(ignored_posts)} {o_id} {d_idx}")

            for info in order_infos:
                if info.verb in ("assumes", "continues"):
                    if info.post_id not in postid_info_dict:
                        postid_info_dict.setdefault(info.post_id, info)
                        info.all_infos.append(info)
                        # if info.verb == "continues":
                        #     errors.append(TenureMissingAssumeError.build(info))
                    else:
                        postid_info_dict[info.post_id].all_infos.append(info)
                elif info.verb == "relinquishes":
                    start_info = postid_info_dict.get(info.post_id, None)
                    if not start_info:
                        self.lgr.warning(f"***Missing Assume post_id: {info.post_id} not found in {str(info)}")
                        errors.append(TenureMissingAssumeError.build(info))
                        continue
                    start_info.all_infos.append(info)
                    assert (
                        o_id == info.order_id and d_idx == info.detail_idx
                    ), f'{o_id} == {info.order_id}, {d_idx} == {info.detail_idx}'
                    o_tenures.append(build_tenure(start_info, o_id, o_date, d_idx))
                    self.lgr.info(f"\t\tClosing Active: {info.post_id} {o_id} {d_idx}")
                    del postid_info_dict[info.post_id]
                else:
                    raise NotImplementedError(f"Unknown verb: {info.verb}")
            return o_tenures

        def close_order_infos():
            # close out all orders that have crossed ministry boundary
            for post_id, info in postid_info_dict.items():
                end_date = self.ministry_end_date(info.order_date)
                officer_tenures.append(build_tenure(info, "", end_date, -1))
            postid_info_dict.clear()

        detail_infos = sorted(detail_infos, key=lambda i: (i.order_date, i.verb_code, i.order_id, i.post_id))
        self.lgr.info(f"\n## Processing Officer: {officer_id} #detailpost_infos: {len(detail_infos)}")
        self.lgr.info(f"\n\tOrders: {set(d.order_id for d in detail_infos)}")

        postid_info_dict, officer_tenures, prev_ministry = {}, [], None
        for order_id, order_infos in groupby(detail_infos, key=attrgetter("order_id")):
            order_infos = list(order_infos)
            if self.ministry_dict:
                curr_ministry = self.get_ministry(order_infos[0].order_date)
                if prev_ministry and prev_ministry != curr_ministry:
                    self.lgr.info("\tNew ministry Clearing older posts.")
                    close_order_infos()
                prev_ministry = curr_ministry

            self.lgr.info(f"Order: {order_id} #detailpost_infos: {len(order_infos)}")
            officer_tenures += handle_order_infos(order_infos)

        if postid_info_dict:
            self.lgr.warning(f"***No Closing Orders{get_postids(postid_info_dict.keys())}")
            if self.ministry_dict:
                for post_id, info in postid_info_dict.items():
                    end_date = self.ministry_end_date(info.order_date)
                    officer_tenures.append(build_tenure(info, "", end_date, -1))

        errors += [TenureLongError.build(t) for t in officer_tenures if t.duration_days > 365 * 6]
        for (t1, t2) in pairwise(officer_tenures):
            gap_years = (t2.end_date - t1.start_date).days / 365.0
            if gap_years > 10:
                print('**** ERROR FOUND ***')
                errors.append(TenureGapError.build(t2, gap_years))
        return officer_tenures, errors

    def compute_manager(self, tenures):
        leaf_roles = ('Minister of State', 'Deputy Minister')
        leaf_tenures = [t for t in tenures if t.role in leaf_roles]

        postid_dict, errors = {}, []
        [postid_dict.setdefault(t.post_id, []).append(t) for t in tenures]

        for tenure in leaf_tenures:
            postid_ts = postid_dict.get(tenure.post_id, [])
            manager_ts = [t for t in postid_ts if t.tenure_id != tenure.tenure_id and tenure.overlaps(t)]

            leaf_ts = [t for t in manager_ts if t.role in leaf_roles]
            if leaf_ts:
                e = TenureManagerWithLeafRole.build(tenure, leaf_ts)
                self.lgr.debug(f'{e.name} {e.msg} {str(tenure)}')
                errors.append(e)

            manager_ts = [t for t in manager_ts if t.role not in leaf_roles]
            if not manager_ts:
                errors.append(TenureWithNoManager.build(tenure))
            tenure.manager_ids = [mt.tenure_id for mt in manager_ts]

            for manager_tenure in manager_ts:
                manager_tenure.reportee_ids.append(tenure.tenure_id)

            self.lgr.debug(f"T: {str(tenure)} M: {'|'.join(str(t) for t in manager_ts)}")
        return errors

    def write_tenures(self, tenures):
        tenure_dicts = []
        for t in tenures:
            td = t.dict()
            td["start_date"] = str(td["start_date"])
            td["end_date"] = str(td["end_date"])
            tenure_dicts.append(td)
        tenure_output_path = Path("output/tenures.json")
        tenure_output_path.write_text(json.dumps({"tenures": tenure_dicts}, indent=2))

    def pipe(self, docs, **kwargs):
        self.add_log_handler()
        print("Inside tenure_builder")
        self.lgr.info("Entering tenure builder")
        docs = list(docs)

        for doc in docs:
            doc.add_pipe("tenure_builder")
            doc.add_extra_field("tenures", ("list", "orgpedia.extracts.orgpedia", "Tenure"))

        orders = [doc.order for doc in docs]
        detail_infos = list(flatten(self.build_detail_infos(o) for o in orders))

        detail_infos.sort(key=attrgetter("officer_id"))
        officer_groupby = groupby(detail_infos, key=attrgetter("officer_id"))

        tenures, errors = [], []
        for officer_id, officer_infos in officer_groupby:
            o_tenures, o_errors = self.build_officer_tenures(officer_id, officer_infos)
            tenures += o_tenures
            errors += o_errors

        errors += self.compute_manager(tenures)

        self.lgr.info(f"#Tenures: {len(tenures)}")
        order_id_doc_dict = {}
        for doc in docs:
            order_id_doc_dict[doc.order.order_id] = doc
            doc.tenures = []

        for tenure in tenures:
            doc = order_id_doc_dict[tenure.start_order_id]
            doc.tenures.append(tenure)

        # self.write_tenures(tenures)
        self.lgr.info(f"=={doc.pdf_name}.tenure_builder {len(tenures)} {DataError.error_counts(errors)}")
        self.lgr.info("Leaving tenure_builder")
        self.remove_log_handler()
        return docs
