import datetime
import json
import logging
import sys
from dataclasses import dataclass, field
from itertools import groupby, product
from operator import attrgetter
from pathlib import Path
from statistics import mean, median

from more_itertools import first, flatten, partition
from polyleven import levenshtein

from ..extracts.orgpedia import OfficerID
from docint.util import read_config_from_disk
from docint.vision import Vision

# b /Users/mukund/Software/docInt/docint/pipeline/details_merger.py:34


def align_name_match(short, long):
    if len(long) < len(short):
        short, long = long, short

    if short == long:
        return True

    if not short or not long:
        return False

    if short[0] != long[0]:
        return False

    s_idx = 0
    for l_idx in range(len(long)):
        if short[s_idx] == long[l_idx]:
            s_idx, l_idx = s_idx + 1, l_idx + 1
        else:
            l_idx += 1

        if len(short) == s_idx:
            break

    return True if s_idx == len(short) else False


def fix_name(name):
    return (
        name.lower()
        .replace("kr.", "kumar")
        .replace("pd.", "prasad")
        .replace(" s.", "singh")
        .replace(" ", "")
    )


def leven_equal(d1, d2, date_cutoff=1):
    return levenshtein(str(d1), str(d2), date_cutoff) <= date_cutoff


def inv_equal(d1, d2):
    return d1.year == d2.year and d1.month == d2.day and d1.day == d2.month


def fuzzy_date_match(d1, d2, date_cutoff=1):
    return d1 == d2 or inv_equal(d1, d2) or leven_equal(d1, d2, date_cutoff)


def get_duplicates(iter):
    seen = set()
    return [i for i in iter if i in seen or seen.add(i)]


@dataclass
class OfficerHistory:
    details: []
    orders: []
    order_idxs: []
    officer_id: str
    names: set = field(default_factory=set)
    birth_dates: set = field(default_factory=set)

    def __post_init__(self):
        self.names.add(fix_name(self.details[0].officer.name))
        if self.details[0].officer.birth_date:
            self.birth_dates.add(self.details[0].officer.birth_date)

    @property
    def name(self):
        return self.details[0].officer.name

    def has_birth_date(self):
        return True if self.birth_dates else None

    @property
    def birth_date(self):
        return self.details[0].officer.birth_date

    def __len__(self):
        return len(self.details)

    def __str__(self):
        return f"{self.details[0].officer.cadre}:{self.officer_id}: {self.names_str} [{len(self)}] {self.birth_date_str}"

    @property
    def names_str(self):
        return "|".join(self.names)

    @property
    def birth_date_str(self):
        return "|".join(str(d) for d in self.birth_dates)

    def get_last_post(self):
        posts = self.details[-1].get_after_posts()
        assert len(posts) in (0, 1)
        if posts and not posts[0].errors:
            return posts[0]
        else:
            return None

    def name_date_pairs(self):
        return product(self.names, self.birth_dates)

    def exact_date_match(self, d_birth_date):
        for birth_date in self.birth_dates:
            if birth_date == d_birth_date:
                return True
        return False

    # def alias_name_match(self, d_name):
    #     for name in self.names:
    #         if align_match(name, d_name):
    #             return True
    #     return False

    def exact_name_date_match(self, d_name, d_birth_date):
        d_name = fix_name(d_name)
        for name, birth_date in product(self.names, self.birth_dates):
            if d_name == name and birth_date == d_birth_date:
                return True
        return False

    def exact_name_fuzzy_date_match(self, d_name, d_birth_date, date_cutoff=1):
        for name, birth_date in product(self.names, self.birth_dates):
            if name == d_name and fuzzy_date_match(
                d_birth_date, birth_date, date_cutoff
            ):
                return True
        return False

    def fuzzy_name_exact_date_match(self, d_name, d_birth_date, name_cutoff=1):
        for name, birth_date in product(self.names, self.birth_dates):
            if (
                levenshtein(d_name, name, name_cutoff) <= name_cutoff
                and d_birth_date == birth_date  # noqa: W503
            ):
                return True
        return False

    def fuzzy_name_date_match(self, d_name, d_birth_date, name_cutoff=1, date_cutoff=1):
        d_name = fix_name(d_name)
        for name, birth_date in product(self.names, self.birth_dates):
            if levenshtein(
                d_name, name, name_cutoff
            ) <= name_cutoff and fuzzy_date_match(d_birth_date, birth_date):
                return True
        return False

    def exact_name_match(self, d_name):
        d_name = fix_name(d_name)
        for name in self.names:
            if name == d_name:
                return True
        return False

    def fuzzy_name_match(self, d_name, name_cutoff=1):
        d_name = fix_name(d_name)
        for name in self.names:
            if levenshtein(d_name, name, name_cutoff) <= name_cutoff:
                return True
        return False

    def align_name_match(self, d_name):
        d_name = fix_name(d_name)
        for name in self.names:
            if align_name_match(d_name, name):
                return True
        return False

    def add_detail(self, order, detail, order_idx):
        self.names.add(fix_name(detail.officer.name))
        if detail.officer.birth_date:
            self.birth_dates.add(detail.officer.birth_date)

        self.details.append(detail)
        self.orders.append(order)
        self.order_idxs.append(order_idx)

        if not self.officer_id and detail.officer.officer_id:
            self.officer_id = detail.officer.officer_id
            for d in self.details:
                if not d.officer.officer_id:
                    d.officer.officer_id = self.officer_id
                else:
                    assert (
                        self.officer_id == d.officer.officer_id
                    ), f"mismatch in officer_id: {self.officer_id} != {d.officer.officer_id}"
        elif self.officer_id and detail.officer.officer_id:
            if self.officer_id != detail.officer.officer_id:
                assert (
                    False
                ), f"HUGE ERROR: {self.officer_id} != {detail.officer.officer_id} {self.name} <-> {detail.officer.name}"

    def get_last_order_date(self):
        return self.orders[-1].date

    def assign_officer_id(self, officer_id):
        for d in self.details:
            d.officer.officer_id = officer_id

    def build_OfficerID(self):
        def name_str(o):
            return f"{o.name}|{o.salut}|{o.full_name}|str(o.birth_date)"

        def get_alias(ns):
            nsl = ns.split("|")
            return {"name": nsl[0], "salut": nsl[1], "full_name": nsl[2]}

        o = self.details[0].officer

        name_strs = sorted(
            (name_str(d.officer) for d in self.details), key=lambda s: s.lower()
        )
        name_str_counts = [
            (len(list(g)), n) for n, g in groupby(name_strs, key=lambda s: s.lower())
        ]
        name_str_counts.sort(reverse=True)
        name_strs = [n for c, n in name_str_counts]

        ns, alias_ns = name_strs[0], name_strs[1:]
        name, salut, full_name, birth_date = ns.split("|")
        aliases = [get_alias(ns) for ns in alias_ns[1:]]

        invalid_dt = datetime.date(year=1900, month=1, day=1)
        dt = first((o.birth_date for d in self.details if o.birth_date), invalid_dt)

        oid = OfficerID(
            officer_id=o.officer_id,
            salut=o.salut,
            name=o.name,
            full_name=o.full_name,
            cadre=o.cadre,
            aliases=aliases,
            birth_date=dt,
            home_location=o.home_district,
        )
        oid_dict = oid.dict()
        oid_dict["details"] = [
            f"{o_idx}>{d.detail_idx}"
            for (o_idx, d) in zip(self.order_idxs, self.details)
        ]
        return oid_dict


@Vision.factory(
    "details_merger",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "details_merger",
        "pre_edit": True,
        "cadre_file_dict": {},
        "post_id_fields": [],
        "officer_match_fields": [],
        "mismatch_names_file": "mismatch_names.yml",
    },
)
class DetailsMerger:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        pre_edit,
        cadre_file_dict,
        post_id_fields,
        officer_match_fields,
        mismatch_names_file,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = Path(conf_stub)
        self.pre_edit = pre_edit
        self.post_id_fields = post_id_fields
        self.added_details = set()

        self.duplicate_nows_names = set()
        self.order_officerid_dict = {}
        self.order_name_birthdate_dict = {}

        for cadre, cadre_file in cadre_file_dict.items():
            print(f"{cadre}: {cadre_file}")
            officers = OfficerID.from_disk(cadre_file)
            names_dict = {}
            for o in officers:
                names = [o.name] + [a["name"] for a in o.aliases]
                names_nows = set(fix_name(n) for n in names)

                already_present = [n for n in names_nows if n in names_dict]
                if already_present:
                    self.duplicate_nows_names.update(already_present)
                else:
                    [names_dict.setdefault(n.lower(), o.officer_id) for n in names_nows]

        mismatch_names_path = self.conf_dir / mismatch_names_file
        mis_dict = read_config_from_disk(mismatch_names_path)
        self.mismatch_nows_names = self.build_mismatch(mis_dict)

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None

        self.lgr.info(f"#Duplicates: {len(self.duplicate_nows_names)}")
        self.lgr.info(f"#Duplicates: {'|'.join(self.duplicate_nows_names)}")
        self.lgr.info(f"#Mismatch Names: {len(self.mismatch_nows_names)}")

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

    def build_mismatch(self, mis_dict):
        tuples = []
        for lb in mis_dict["mis_matches"]:
            tuples.append(tuple(sorted([fix_name(lb[0]), fix_name(lb[1])])))
        return set(tuples)

    def detail_added(self, order, detail):
        return (order.order_id, detail.detail_idx) in self.added_details

    def add_detail(self, order, detail):
        detail_tup = (order.order_id, detail.detail_idx)
        if detail_tup in self.added_details:
            self.lgr.info(f"HUGE DETAIL ERROR {order.order_id}.{detail.detail_idx}")
        self.added_details.add(detail_tup)

    def is_duplicate(self, officer_history):
        ns = [fix_name(n) for n in officer_history.names]
        return any(n for n in ns if n in self.duplicate_nows_names)

    def find_matching_detail(self, officer_history, order, o_idx):  # noqa: C901
        def iter_valid_details(order, cadre, order_officer_id):
            for detail in order.details:
                if not detail.officer:
                    continue
                if self.detail_added(order, detail):
                    continue
                if cadre != detail.officer.cadre:
                    continue

                if order_officer_id and detail.officer.officer_id:
                    continue

                yield detail

        def get_before_post(detail):
            posts = detail.get_before_posts()
            assert len(posts) in (0, 1)
            if posts and not posts[0].errors:
                return posts[0]
            else:
                return None

        def officer_id_match():
            if oh_id:
                return self.order_officerid_dict.get((order.order_id, oh_id), None)
            return None

        def exact_name_exact_date_match():
            if oh.has_birth_date() and order.category == "civil_list":
                for name, dt in oh.name_date_pairs():
                    key_tuple = (order.order_id, name, dt)
                    detail = self.order_name_birthdate_dict.get(key_tuple, None)
                    if detail:
                        return detail
            return None

        def fuzzy_name_exact_date_match():
            if oh.has_birth_date() and order.category == "civil_list":
                for d_name, d in valid_details:
                    d_date = d.officer.birth_date
                    if d_date and oh.fuzzy_name_exact_date_match(d_name, d_date):
                        return d
            return None

        def exact_name_fuzzy_date_match():
            if oh.has_birth_date() and order.category == "civil_list":
                for d_name, d in valid_details:
                    d_date = d.officer.birth_date
                    if d_date and oh.exact_name_fuzzy_date_match(d_name, d_date):
                        return d
            return None

        def fuzzy_name_fuzzy_date_match():
            if oh.has_birth_date() and order.category == "civil_list":
                for d_name, d in valid_details:
                    d_date = d.officer.birth_date
                    if d_date and oh.fuzzy_name_date_match(d_name, d_date):
                        return d
            return None

        def exact_name_exact_post_match():
            if oh_last_post:
                for d_name, d in valid_details:
                    d_post = get_before_post(d)
                    if d_post and oh_last_post.post_id == d_post.post_id:
                        if oh.exact_name_match(d_name):
                            return d
                        else:
                            names = "|".join(oh.names)
                            self.lgr.info(
                                f"\t\tMissed-post: {names}<->{d_name} {oh_last_post.post_str}<->{d_post.post_str}"
                            )
            return None

        def fuzzy_name_exact_post_match():
            if oh_last_post:
                for d_name, d in valid_details:
                    d_post = get_before_post(d)
                    if (
                        d_post
                        and oh_last_post.post_id == d_post.post_id  # noqa: W503
                        and oh.fuzzy_name_match(d_name)  # noqa: W503
                    ):
                        return d
            return None

        def align_name_exact_date_match():
            if oh.has_birth_date() and order.category == "civil_list":
                for d_name, d in valid_details:
                    d_date = d.officer.birth_date
                    if oh.exact_date_match(d_date) and oh.align_name_match(d_name):
                        return d
            return None

        def exact_name_match():
            if not self.is_duplicate(officer_history):
                for d_name, d in valid_details:
                    if oh.exact_name_match(d_name) and (
                        fix_name(d_name) not in self.duplicate_nows_names
                    ):
                        return d
            return None

        def fuzzy_name_match():
            if not self.is_duplicate(officer_history):
                for d_name, d in valid_details:
                    if (
                        oh.fuzzy_name_match(d_name)
                        and allowed_name_match(oh, d_name)  # noqa: W503
                        and (fix_name(d_name) not in self.duplicate_nows_names)
                    ):
                        return d
            return None

        def allowed_name_match(oh, d_name):
            for o_name in oh.names:
                tup = tuple(sorted([o_name, fix_name(d_name)]))
                if tup in self.mismatch_nows_names:
                    return False
            return True

        oh = officer_history
        oh_id = oh.officer_id
        order_id = order.order_id

        detail = officer_id_match()
        if detail:
            if self.detail_added(order, detail):
                self.lgr.info(
                    f"HUGE DUPLICATE DETAIL ERROR officer_id_match {order.order_id}.{detail.detail_idx}"
                )
            else:
                self.lgr.info(
                    f"\tFound: {order_id}:{o_idx} officer_id_match {oh.names_str}<->{detail.officer.name} {oh.birth_date_str}<->{detail.officer.birth_date}"
                )
                return detail

        detail = exact_name_exact_date_match()
        if detail:
            if self.detail_added(order, detail):
                self.lgr.info(
                    f"HUGE DUPLICATE DETAIL ERROR exact_name_exact_date_match {order.order_id}.{detail.detail_idx}"
                )
            else:
                self.lgr.info(
                    f"\tFound: {order_id}:{o_idx} exact_name_exact_date_match"
                )
                return detail

        o_cadre = oh.details[0].officer.cadre
        valid_details = [
            (fix_name(d.officer.name), d)
            for d in list(iter_valid_details(order, o_cadre, oh_id))
        ]

        detail = exact_name_fuzzy_date_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} exact_name_fuzzy_date_match {oh.birth_date_str}<->{detail.officer.birth_date}"
            )
            return detail

        detail = fuzzy_name_exact_date_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} fuzzy_name_exact_date_match {oh.names_str}<->{detail.officer.name}"
            )
            return detail

        detail = fuzzy_name_fuzzy_date_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} fuzzy_name_fuzzy_date_match {oh.names_str}<->{detail.officer.name} {oh.birth_date_str}<->{detail.officer.birth_date}"
            )
            return detail

        oh_last_post = oh.get_last_post()
        detail = exact_name_exact_post_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} exact_name_exact_post_match {oh.names_str}<->{detail.officer.name}"
            )
            return detail

        detail = fuzzy_name_exact_post_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} fuzzy_name_exact_post_match {oh.names_str}<->{detail.officer.name}"
            )
            return detail

        detail = align_name_exact_date_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} align_name_exact_date_match {oh.names_str}<->{detail.officer.name} {oh.birth_date_str}<->{detail.officer.birth_date}"
            )
            return detail

        detail = exact_name_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} exact_name_match {oh.names_str}<->{detail.officer.name}"
            )
            return detail

        detail = fuzzy_name_match()
        if detail:
            self.lgr.info(
                f"\tFound: {order_id}:{o_idx} fuzzy_name_match {oh.names_str}<->{detail.officer.name}"
            )
            return detail

        return None

    def get_officer_histories(self, orders):
        def iter_valid_details(orders):
            for o_idx, order in enumerate(orders):
                for detail in order.details:
                    if not detail.officer or self.detail_added(order, detail):
                        continue

                    yield o_idx, order, detail.detail_idx, detail

        def get_duplicate_nows_names(orders):
            def duplicate_names(order):
                ns = [fix_name(d.officer.name) for d in order.details]
                seen = set()
                return [n for n in ns if n in seen or seen.add(n)]

            return flatten(duplicate_names(o) for o in orders)

        orders = sorted(orders, key=attrgetter("date"))
        officer_histories = []

        order_duplicate_nows_names = get_duplicate_nows_names(orders)
        self.duplicate_nows_names.update(order_duplicate_nows_names)
        self.lgr.info(f"#Duplicates: {len(self.duplicate_nows_names)}")

        for (o_idx, order, d_idx, detail) in iter_valid_details(orders):
            officer_id = detail.officer.officer_id
            self.lgr.info(
                f"{order.order_id}:{o_idx}[{d_idx}] officer_id:{officer_id} name: {detail.officer.name} dob: {detail.officer.birth_date}"
            )

            o_history = OfficerHistory(
                orders=[order],
                details=[detail],
                order_idxs=[o_idx],
                officer_id=officer_id,
            )
            self.add_detail(order, detail)
            self.lgr.info(f"Processing {order.order_id} {o_idx}>{detail.detail_idx}")

            for (child_oidx, child_order) in enumerate(orders[o_idx + 1 :], o_idx + 1):
                o_detail = self.find_matching_detail(o_history, child_order, child_oidx)

                if o_detail:
                    o_history.add_detail(child_order, o_detail, child_oidx)
                    self.add_detail(child_order, o_detail)
                    self.lgr.info(
                        f"Adding {child_order.order_id} {child_oidx}>{o_detail.detail_idx}"
                    )
                else:
                    pass
                    # self.lgr.info(f'\tNot Found: {child_order.order_id}:{child_oidx} {child_order.category} officer_id: {officer_id}')

            officer_histories.append(o_history)
        # end for
        return officer_histories

    def assign_id(self, officer_histories):
        cadre_id_dict, officer_ids = {}, []
        for oh in officer_histories:
            for d in oh.details:
                assert (
                    not d.officer.officer_id
                ), f"{str(oh)} detail.officer_id :{d.officer.officer_id}:"

            cadre = oh.details[0].officer.cadre
            officer_idx = cadre_id_dict.setdefault(cadre, 1)
            officer_id = f'{cadre.replace(".","")}-allot-{officer_idx}'
            oh.assign_officer_id(officer_id)
            cadre_id_dict[cadre] += 1
            officer_ids.append(oh.build_OfficerID())

        officer_ids.sort(key=lambda oid: (oid["cadre"], oid["name"].lower()))
        return officer_ids

    def write_officerids(self, officerids, officerid_path):
        for oid in officerids:
            bd = oid["birth_date"]
            oid["birth_date"] = f"{bd.year}-{bd.month}-{bd.day}"
        officerid_path.write_text(json.dumps({"officers": officerids}, indent=2))

    def pipe(self, docs, **kwargs):
        def stats(nums):
            nums = list(nums)
            if not nums:
                return "empty sequence"
            else:
                return f"min: {min(nums)} max: {max(nums)} avg: {mean(nums):.2f} median: {median(nums)}"

        def is_old_history(order_history):
            return order_history.get_last_order_date() < order_cutoff_date

        def has_error_posts(detail):
            return any(p for p in detail.get_posts() if p.has_error())

        self.lgr.info("Entering details_merger.pipe")

        now = datetime.datetime.now()
        sys.stderr.write(f"Starting   {now.hour:02}:{now.minute:02}:{now.second:02}\n")
        sys.stderr.flush()

        order_cutoff_date = datetime.date(year=2021, month=1, day=1)

        orders = [doc.order for doc in docs]
        civil_orders = [o for o in orders if o.category == "civil_list"]
        non_civil_orders = [o for o in orders if o.category != "civil_list"]

        # officer_id_dict
        officerid_details = [
            (o, d)
            for o in orders
            for d in o.details
            if d.officer and d.officer.officer_id
        ]
        order_officerids = [
            ((o.order_id, d.officer.officer_id), d) for o, d in officerid_details
        ]
        dup_order_officerids = get_duplicates(t[0] for t in order_officerids)
        self.lgr.info(
            f"#order_officerids duplicates: {len(dup_order_officerids)} {dup_order_officerids}"
        )
        self.order_officerid_dict = dict(order_officerids)

        self.order_name_birthdate_dict = dict(
            (
                ((o.order_id, fix_name(d.officer.name), d.officer.birth_date), d)
                for o in civil_orders
                for d in o.details
                if d.officer and d.officer.birth_date
            )
        )

        total_details = sum(len(o.details) for o in orders)
        withid_details = len(self.order_officerid_dict)

        non_civil_noid_details = len(
            [
                d
                for o in non_civil_orders
                for d in o.details
                if (not d.officer.officer_id)
            ]
        )
        noid_post_error_details = len(
            [
                d
                for o in non_civil_orders
                for d in o.details
                if (not d.officer.officer_id) and has_error_posts(d)
            ]
        )
        print(
            f"#non_civil_NOID_details: {non_civil_noid_details} #errors: {noid_post_error_details}"
        )

        sys.stderr.write(f"Built dict {now.hour:02}:{now.minute:02}:{now.second:02}\n")
        sys.stderr.flush()

        officer_histories = self.get_officer_histories(orders)
        noid_OHs = [oh for oh in officer_histories if not oh.officer_id]

        nodup_noid_OHs, dup_noid_OHs = partition(self.is_duplicate, noid_OHs)
        nodup_noid_OHs, dup_noid_OHs = list(nodup_noid_OHs), list(dup_noid_OHs)

        new_noid_OHs, old_noid_OHs = partition(is_old_history, nodup_noid_OHs)

        print(
            f"#details: {total_details} #withid_details: {withid_details} #OHs: {len(officer_histories)} #noid_OHs: {len(noid_OHs)} #noid_dup_OHs: {len(dup_noid_OHs)}"
        )

        [self.lgr.info(oh) for oh in nodup_noid_OHs]
        print(f"stats: {stats(len(oh) for oh in noid_OHs)}")

        nodup_noid_officerids = self.assign_id(nodup_noid_OHs)
        officerid_path = Path("output/officerid.json")

        self.write_officerids(nodup_noid_officerids, officerid_path)
        return docs
