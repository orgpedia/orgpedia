from collections import Counter
import datetime
import json
import logging
import sys
from dataclasses import dataclass, field
from itertools import groupby
from operator import attrgetter
from pathlib import Path
from typing import List

import yaml
from dateutil import parser
from more_itertools import flatten


from docint.region import DataError
from docint.vision import Vision

from ..extracts.orgpedia import Tenure

# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34


class TenureManagerWithLeafRole(DataError):
    @classmethod
    def build(cls, tenure, manager_tenures):
        mgrs_id = ", ".join(t.officer_id for t in manager_tenures)
        msg = f"t_id: {tenure.officer_id} mgrs_id: {mgrs_id} post_id: {tenure.post_id}"
        path = f"te.{tenure.tenure_idx}"
        return TenureManagerWithLeafRole(path=path, msg=msg)


class TenureWithNoManager(DataError):
    @classmethod
    def build(cls, tenure):
        msg = f"t_id: {tenure.officer_id} post_id: {tenure.post_id}"
        path = f"te.{tenure.tenure_idx}"
        return TenureWithNoManager(path=path, msg=msg)


class TenureMissingAssumeError(DataError):
    @classmethod
    def build(cls, info):
        path = f"order.details{info.detail_idx}"
        order_str = f"{info.order_id}[{info.detail_idx}]"
        post_str = f"Post: {info.post_id}"
        officer_str = f"Officer: {info.officer_id}"
        msg = f"Missing assume for {info.verb}: {order_str} {post_str} {officer_str}"
        return TenureMissingAssumeError(path=path, msg=msg)


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
        return TenureMultipleRolesError(path=path, msg=msg, roles=roles)


class TenureLongTenure(DataError):
    duration_days: int

    @classmethod
    def build(cls, tenure):
        msg = "Long Tenure " + str(tenure)
        print(msg)
        return TenureLongTenure(path='', msg=msg, duration_days=tenure.duration_days)


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


@Vision.factory(
    "tenure_builder",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "tenure_builder",
        "ministry_file": "conf/ministries.yml",
    },
)
class TenureBuilder:
    def __init__(self, conf_dir, conf_stub, ministry_file):
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.ministry_path = Path(ministry_file)

        if self.ministry_path.exists():
            self.ministry_dict = yaml.load(self.ministry_path.read_text(), Loader=yaml.FullLoader)
            for m in self.ministry_dict["ministries"]:
                s, e = m["start_date"], m["end_date"]
                m["start_date"] = parser.parse(s).date()
                m["end_date"] = parser.parse(e).date() if e != "today" else datetime.date.today()
        else:
            self.ministry_dict = {}

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.curr_tenure_idx = -1

    def build_detail_infos(self, order):
        def valid_date(order):
            if not order.date:
                return False

            y = order.date.year
            return False if (y < 1947 or y > 2021) else True

        def iter_posts(detail):
            if not detail.officer.officer_id:
                return

            for verb in ["continues", "relinquishes", "assumes"]:
                for post in getattr(detail, verb):
                    yield verb, post

        def build_info(detail, verb, post):
            role = post.role_hpath[-1] if post.role_hpath else None
            return DetailInfo(
                order.order_id,
                order.date,
                detail.detail_idx,
                detail.officer.officer_id,
                verb,
                post.post_id,
                role,
                order.category,
                [],
            )

        if order.order_id == "1_Upload_2830.pdf":
            print("Found It")

        if not valid_date(order):
            return []

        return [build_info(d, v, p) for d in order.details for (v, p) in iter_posts(d)]

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

    def build_officer_tenures(self, officer_id, detail_infos):
        def build_tenure(start_info, end_order_id, end_date, end_detail_idx):
            self.curr_tenure_idx += 1

            roles = [start_info.role] + [i.role for i in start_info.all_infos]
            roles_counter = Counter(roles)

            if len(roles_counter) > 1:
                roleError = TenureMultipleRolesError.build(start_info)
                self.lgr.debug(roleError.msg)
                errors.append(roleError)

            role = max(roles_counter, key=roles_counter.get, default='Cabinet Minister')
            all_order_infos = [(i.order_id, i.detail_idx) for i in start_info.all_infos]
            return Tenure(
                tenure_idx=self.curr_tenure_idx,
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
                order_posts = set(i.post_id for i in order_infos)
                ignored_posts = list(active_posts - order_posts)
                if ignored_posts:
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
                    assert o_id == info.order_id and d_idx == info.detail_idx
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

        detail_infos = sorted(detail_infos, key=lambda i: (i.order_date, i.order_id))
        self.lgr.info(f"\n## Processing Officer: {officer_id} #detailpost_infos: {len(detail_infos)}")

        postid_info_dict, officer_tenures, prev_ministry = {}, [], None
        for order_id, order_infos in groupby(detail_infos, key=attrgetter("order_id")):
            order_infos = list(order_infos)
            curr_ministry = self.get_ministry(order_infos[0].order_date)
            if prev_ministry and prev_ministry != curr_ministry:
                self.lgr.info("\tNew ministry Clearing older posts.")
                close_order_infos()

            self.lgr.info(f"Order: {order_id} #detailpost_infos: {len(order_infos)}")
            officer_tenures += handle_order_infos(order_infos)
            prev_ministry = curr_ministry

        if postid_info_dict:
            self.lgr.warning(f"***No Closing Orders{get_postids(postid_info_dict.keys())}")
            if self.ministry_dict:
                for post_id, info in postid_info_dict.items():
                    end_date = self.ministry_end_date(info.order_date)
                    officer_tenures.append(build_tenure(info, "", end_date, -1))

        errors += [TenureLongTenure.build(t) for t in officer_tenures if t.duration_days > 365 * 6]
        return officer_tenures, errors

    def compute_manager(self, tenures):
        leaf_roles = ('Minister of State', 'Deputy Minister')
        leaf_tenures = [t for t in tenures if t.role in leaf_roles]

        postid_dict, errors = {}, []
        [postid_dict.setdefault(t.post_id, []).append(t) for t in tenures]

        for tenure in leaf_tenures:
            postid_ts = postid_dict.get(tenure.post_id, [])
            manager_ts = [t for t in postid_ts if t.tenure_idx != tenure.tenure_idx and tenure.overlaps(t)]

            leaf_ts = [t for t in manager_ts if t.role in leaf_roles]
            if leaf_ts:
                e = TenureManagerWithLeafRole.build(tenure, leaf_ts)
                self.lgr.debug(f'{e.name} {e.msg} {str(tenure)}')
                errors.append(e)

            manager_ts = [t for t in manager_ts if t.role not in leaf_roles]
            if not manager_ts:
                errors.append(TenureWithNoManager.build(tenure))
            tenure.manager_idxs = [mt.tenure_idx for mt in manager_ts]

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
        print("Inside tenure_builder")
        self.lgr.info("Entering tenure builder")
        docs = list(docs)

        for doc in docs:
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

        self.write_tenures(tenures)
        self.lgr.info(f"=={doc.pdf_name}.tenure_builder {len(tenures)} {DataError.error_counts(errors)}")
        self.lgr.info("Leaving tenure_builder")
        return docs