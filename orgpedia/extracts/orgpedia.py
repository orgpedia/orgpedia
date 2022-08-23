import datetime
import json
from pathlib import Path
from typing import Dict, List, Union

from more_itertools import flatten
from pydantic import BaseModel

from docint.region import DataError, Region, Span


class IncorrectOfficerNameError(DataError):
    pass


class EnglishWordsInNameError(DataError):
    pass


class OfficerIDNotFoundError(DataError):
    pass


class Officer(Region):
    salut: str
    name: str
    full_name: str
    birth_date: datetime.date = None
    relative_name: str = ""
    home_district: str = ""
    posting_date: datetime.date = None
    cadre: str = ""

    officer_idx: int = -1
    officer_id: str = ""

    orig_lang: str = "en"
    orig_salut: str = ""
    orig_name: str = ""
    orig_full_name: str = ""

    @classmethod
    def build(cls, words, salut, name, cadre=""):
        salut, name = salut.strip(), name.strip()
        full_name = salut + " " + name if salut else name

        word_idxs = [w.word_idx for w in words]
        page_idx = words[0].page_idx if words else None
        return Officer(
            words=words,
            word_lines=[words],
            salut=salut,
            name=name,
            full_name=full_name,
            cadre=cadre,
            word_idxs=word_idxs,
            page_idx_=page_idx,
            word_lines_idxs=[word_idxs],
        )


class Post(Region):
    post_str: str

    dept_hpath: List[str]
    role_hpath: List[str]
    juri_hpath: List[str]
    loca_hpath: List[str]
    stat_hpath: List[str]

    dept_spans: List[Span]
    role_spans: List[Span]
    juri_spans: List[Span]
    loca_spans: List[Span]
    stat_spans: List[Span]

    post_id: str = ""
    post_idx: int = -1

    @property
    def dept(self):
        return self.dept_hpath[-1] if self.dept_hpath else None

    @property
    def role(self):
        return self.role_hpath[-1] if self.role_hpath else None

    @property
    def juri(self):
        return self.juri_hpath[-1] if self.juri_hpath else None

    @property
    def loca(self):
        return self.loca_hpath[-1] if self.loca_hpath else None

    @property
    def stat(self):
        return self.stat_hpath[-1] if self.stat_hpath else None

    @property
    def spans(self):
        return (
            self.dept_spans
            + self.role_spans
            + self.juri_spans
            + self.loca_spans
            + self.stat_spans
        )

    @property
    def spans_dict(self):
        s_dict = {
            "dept": self.dept_spans,
            "role": self.role_spans,
            "juri": self.juri_spans,
            "loca": self.loca_spans,
            "stat": self.stat_spans,
        }
        return dict((field, spans) for field, spans in s_dict.items() if spans)

    @property
    def fields(self):
        return ["dept", "role", "juri", "loca", "stat"]

    def has_error(self):
        return True if self.errors else False

    def __str__(self):
        def p_s(hpath):
            return "->".join(hpath) if hpath else ""

        pLines = []
        pLines.append(f"role: {p_s(self.role_hpath)}")
        pLines.append(f"dept: {p_s(self.dept_hpath)}")
        for field in ["juri", "loca", "stat"]:
            field_hpath = getattr(self, f"{field}_hpath")
            if field_hpath:
                pLines.append(f"{field}: {p_s(field_hpath)}")
        return "\n".join(pLines)

    def to_str2(self, indent=""):
        def p_s(hpath):
            return "->".join(hpath) if hpath else ""

        pLines = []
        pLines.append(f"{indent}role: {p_s(self.role_hpath)}")
        pLines.append(f"{indent}dept: {p_s(self.dept_hpath)}")
        for field in ["juri", "loca", "stat"]:
            field_hpath = getattr(self, f"{field}_hpath")
            if field_hpath:
                pLines.append(f"{indent}{field}: {p_s(field_hpath)}")
        return "\n".join(pLines)

    @classmethod
    def to_str(self, posts, post_str):
        p = post_str
        posts_to_strs = []
        for post in posts:
            strs = [
                f"{f[0].upper()}:{Span.to_str(p, s)}<"
                for (f, s) in post.spans_dict.items()
            ]
            posts_to_strs.append(" ".join(strs))
        return "-".join(posts_to_strs)

    @classmethod
    def build(
        cls, words, post_str, dept=None, role=None, juri=None, loca=None, stat=None
    ):
        def build_spans(label, spans):
            return [Span(start=span.start, end=span.end, label=label) for span in spans]

        dept_spans = build_spans("dept", dept.spans) if dept else []
        role_spans = build_spans("role", role.spans) if role else []
        juri_spans = build_spans("juri", juri.spans) if juri else []
        loca_spans = build_spans("loca", loca.spans) if loca else []
        stat_spans = build_spans("stat", stat.spans) if stat else []

        dept_hpath = dept.hierarchy_path if dept else []
        role_hpath = role.hierarchy_path if role else []
        juri_hpath = juri.hierarchy_path if juri else []
        loca_hpath = loca.hierarchy_path if loca else []
        stat_hpath = stat.hierarchy_path if stat else []

        word_idxs = [w.word_idx for w in words]
        page_idx = words[0].page_idx if words else None

        return Post(
            words=words,
            word_lines=[words],
            post_str=post_str,
            word_idxs=word_idxs,
            page_idx_=page_idx,
            word_lines_idxs=[word_idxs],
            dept_hpath=dept_hpath,
            role_hpath=role_hpath,
            juri_hpath=juri_hpath,
            loca_hpath=loca_hpath,
            stat_hpath=stat_hpath,
            dept_spans=dept_spans,
            role_spans=role_spans,
            juri_spans=juri_spans,
            loca_spans=loca_spans,
            stat_spans=stat_spans,
        )

    @classmethod
    def build_no_spans(
        cls, words, post_str, dept=[], role=[], juri=[], loca=[], stat=[]
    ):
        word_idxs = [w.word_idx for w in words]
        page_idx = words[0].page_idx if words else None

        return Post(
            words=words,
            word_lines=[words],
            post_str=post_str,
            word_idxs=word_idxs,
            page_idx_=page_idx,
            word_lines_idxs=[word_idxs],
            dept_hpath=dept,
            role_hpath=role,
            juri_hpath=juri,
            loca_hpath=loca,
            stat_hpath=stat,
            dept_spans=[],
            role_spans=[],
            juri_spans=[],
            loca_spans=[],
            stat_spans=[],
        )


class OrderDetail(Region):
    officer: Officer
    continues: List[Post] = []
    relinquishes: List[Post] = []
    assumes: List[Post] = []
    detail_idx: int

    is_valid: bool = True
    path: str = ""

    @property
    def page_idx(self):
        return self.words[0].page_idx

    @classmethod
    def build(
        cls,
        words,
        word_lines,
        officer,
        detail_idx,
        continues=[],
        relinquishes=[],
        assumes=[],
    ):
        word_idxs = [w.word_idx for w in words]
        page_idx = words[0].page_idx if words else None
        word_lines_idxs = [[w.word_idx for w in wl] for wl in word_lines]

        return OrderDetail(
            words=words,
            word_lines=word_lines,
            word_idxs=word_idxs,
            page_idx_=page_idx,
            word_lines_idxs=word_lines_idxs,
            officer=officer,
            continues=continues,
            relinquishes=relinquishes,
            assumes=assumes,
            detail_idx=detail_idx,
        )

    def to_str(self, print_color=False):
        d_lines = [self.raw_text()]

        d_lines.append(f"O: {self.officer.salut}|{self.officer.name}")
        for verb in ["continues", "relinquishes", "assumes"]:
            posts = getattr(self, verb)
            if posts:
                d_lines.append(f"{verb}:")
                d_lines.extend([f'{p.to_str2("  ")}' for p in posts])
        # end
        if self.errors:
            d_lines += ["Errors:"]
            d_lines += [f"  {str(e)}" for e in self.errors]
        return "\n".join(d_lines)

    def to_id_str(self):
        o_id = self.officer.officer_id if self.officer else ""
        d_lines = [
            f"[{self.detail_idx}] O: {self.officer.salut}|{self.officer.name} > {o_id}"
        ]
        for verb in ["continues", "relinquishes", "assumes"]:
            posts = getattr(self, verb)
            if posts:
                d_lines.append(f"{verb}:")
                d_lines.extend([f"{p.post_id}" for p in posts])
        # end
        return "\n".join(d_lines)

    def get_posts(self, verb="all"):
        if verb == "all":
            return self.continues + self.relinquishes + self.assumes
        else:
            return getattr(self, verb)

    def get_after_posts(self):
        return self.continues + self.assumes

    def get_before_posts(self):
        return self.continues + self.relinquishes

    def get_regions(self):
        return [self.officer] + self.get_posts()


    


class IncorrectOrderDateError(DataError):
    pass


class OrderDateNotFoundErrror(DataError):
    pass


class Order(Region):
    order_id: str
    date: Union[datetime.date, None]
    order_idx: int = -1
    number: str = ""
    path: Path
    details: List[OrderDetail]
    category: str = ""

    def get_regions(self):
        return (
            [self] + self.details + list(flatten(d.get_regions() for d in self.details))
        )

    @classmethod
    def build(cls, order_id, order_date, path, details):
        return Order(
            words=[],
            word_lines=[],
            word_idxs=[],
            page_idx_=None,
            order_id=order_id,
            date=order_date,
            path=path,
            details=details,
        )

    def get_posts(self):
        return [p for d in self.details for p in d.get_posts()]

    def get_posts_page_idx(self, page_idx):
        page_details = [d for d in self.details if d.page_idx == page_idx]
        return [p for d in page_details for p in d.get_posts()]

    def get_officers(self, page_idx):
        return [d.officer for d in self.details if d.page_idx == page_idx]

# If it is not extending Region should it still be there, yes as it will be moved to Orgpeida


class Tenure(BaseModel):
    tenure_idx: int
    officer_id: str
    post_id: str

    start_date: datetime.date
    end_date: datetime.date

    start_order_id: str
    start_detail_idx: int

    end_order_id: str = ""
    end_detail_idx: int = -1

    role: str = None
    manager_idxs: List[int] = []

    @property
    def duration(self):
        return self.end_date - self.start_date

    @property
    def duration_days(self):
        return (self.end_date - self.start_date).days

    def get_start_page_idx(self, start_order):
        return start_order.details[self.start_detail_idx].page_idx

    def get_end_page_idx(self, end_order):
        return end_order.details[self.end_detail_idx].page_idx

    def __str__(self):
        s =  f'O: {self.officer_id} '
        s += f'{self.start_order_id}:{self.start_detail_idx} <-> '
        s += f'{self.end_order_id}:{self.end_detail_idx}'
        s += f' D: {self.duration_days}'
        return s

    def overlap_days(self, tenure):
        min_end = min(tenure.end_date, self.end_date)
        max_start = max(tenure.start_date, self.start_date)
        return max(0, (min_end - max_start).days)


    def overlaps(self, tenure):
        return self.overlap_days(tenure) > 0


class OfficerID(BaseModel):
    officer_idx: int = -1
    officer_id: str = ""
    id_code: str = ""

    salut: str = ""
    name: str
    full_name: str = ""
    cadre: str = ""

    aliases: List[Dict[str, str]] = []
    tenures: List[Tenure] = []

    birth_date: datetime.date = None
    batch_year: int = -1
    home_location: str = ""
    education: str = ""

    # currently not keeping language as that should be a separate process

    @classmethod
    def from_disk(self, json_file):
        json_file = Path(json_file)
        if json_file.suffix.lower() in (".json", ".jsn"):
            officer_jsons = json.loads(json_file.read_text())

        officers = [OfficerID(**d) for d in officer_jsons["officers"]]
        return officers


class PostID(BaseModel):
    post_idx: int = -1
    post_id: str = ""
    dept_path: List[str] = []
    role_path: List[str] = []
    juri_path: List[str] = []
    stat_path: List[str] = []
    loca_path: List[str] = []

    tenures: List[Tenure] = []
