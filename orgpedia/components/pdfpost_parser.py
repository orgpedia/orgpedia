import logging
import re
import sys
from pathlib import Path
from typing import List

from more_itertools import first

from ..extracts.orgpedia import Post
from docint.hierarchy import Hierarchy, HierarchySpanGroup, MatchOptions
from docint.region import DataError, TextConfig
from docint.span import Span
from docint.util import read_config_from_disk
from docint.vision import Vision


class PostEmptyDeptAndJuriError(DataError):
    pass


class PostEmptyRoleError(DataError):
    pass


class PostUnmatchedTextsError(DataError):
    texts: List[str]


@Vision.factory(
    "pdfpost_parser",
    default_config={
        "doc_confdir": "conf",
        "hierarchy_files": {
            "dept": "dept.yml",
            "role": "role.yml",
            "juri": "juri.yml",
            "loca": "loca.yml",
            "stat": "stat.yml",
        },
        "noparse_file": "post.noparse.yml",
        "ignore_labels": ["ignore"],
        "conf_stub": "postparser",
    },
)
class PostParser:
    def __init__(
        self, doc_confdir, hierarchy_files, noparse_file, ignore_labels, conf_stub
    ):
        print(noparse_file)
        self.doc_confdir = Path(doc_confdir)
        self.hierarchy_files = hierarchy_files
        self.ignore_labels = ignore_labels
        self.noparse_file = self.doc_confdir / noparse_file
        self.conf_stub = conf_stub

        self.hierarchy_dict = {}
        for field, file_name in self.hierarchy_files.items():
            hierarchy_path = self.doc_confdir / file_name
            hierarchy = Hierarchy(hierarchy_path)
            self.hierarchy_dict[field] = hierarchy

        self.noparse_dict = self.load_noparse(self.noparse_file)

        self.text_config = TextConfig(rm_labels=self.ignore_labels)
        self.match_options = MatchOptions(ignore_case=True)
        self.lgr = logging.getLogger(__name__ + ".")
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None

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

    def _enable_hierarchy_logger(self):
        logging.getLogger("docint.hierarchy").addHandler(
            logging.StreamHandler(sys.stdout)
        )
        logging.getLogger("docint.hierarchy").setLevel(logging.DEBUG)

    def _disable_hierarchy_logger(self):
        logging.getLogger("docint.hierarchy").setLevel(logging.ERROR)

    def load_noparse(self, noparse_file):
        def rev(info_dict):
            return dict((k, list(reversed(v))) for k, v in info_dict.items())

        noparse_file_dict = read_config_from_disk(noparse_file)
        return dict((p["post"], rev(p["info"])) for p in noparse_file_dict["posts"])

    def handle_juri(self, post_str, dept_sg, role_sg, juri_sgs):  # noqa: C901
        def is_commissioner_role(role_sg):
            return "Commissioner" in role_sg.hierarchy_path if role_sg else False

        def is_dept_with_juri_label(dept_sg):
            return dept_sg.get_label_val("_juri") is not None if dept_sg else None

        def get_commisionerate(post_str):
            post_str = post_str.lower()
            assert (
                "jaipur" in post_str or "jodhpur" in post_str
            ), f"No commissionerate in {post_str}"
            comm_city = "jaipur" if "jaipur" in post_str else "jodhpur"
            comm_name = f"{comm_city} commissionerate"
            return self.hierarchy_dict["juri"].find_match(comm_name, self.match_options)

        def get_role_level(role_sg):
            role_levels_dict = {
                "Additional Superintendent of Police": "districts",
                "Circle Officer": "circles",
                "Deputy Superintendent of Police": "circles",
            }
            return role_levels_dict.get(role_sg.leaf, None) if role_sg else None

        def get_juri_level(post_str):
            markers = [
                ("DSITT", "districts"),
                ("DISTT", "districts"),
                ("DISST", "districts"),
                ("Range", "ranges"),
            ]
            for marker, category in markers:
                if marker.lower() in post_str.lower():
                    return category
            return None

        is_dept_with_juri = is_dept_with_juri_label(dept_sg)
        is_comm_role = is_commissioner_role(role_sg)

        if dept_sg and (not is_dept_with_juri) and (not is_comm_role):
            self.lgr.debug("\tEmpty Juri")
            return []

        if is_comm_role:
            sub_path = ["jurisdiction", "policing"]
            sel_sgs = HierarchySpanGroup.select_path_match(juri_sgs, sub_path)

            if len(sel_sgs) == 0:
                sel_sgs = get_commisionerate(post_str)
            else:
                sel_sgs = HierarchySpanGroup.select_sum_matching_len(sel_sgs)
                if len(sel_sgs) > 1:
                    print(f"## {post_str} {len(sel_sgs)}")
                    for sg in sel_sgs:
                        print(
                            f"\t{sg.new_str()} {sg.sum_match_len} {sg.sum_span_len} {sg.sum_span_len_start}"
                        )

            self.lgr.debug(f"\tJuri: is_comm_role: {Hierarchy.to_str(sel_sgs)}")
            return sel_sgs
        elif is_dept_with_juri:
            label = dept_sg.get_label_val("_juri")
            sub_path = ["jurisdiction", label]
            sel_sgs = HierarchySpanGroup.select_path_match(juri_sgs, sub_path)

            if len(sel_sgs) > 1:
                sel_sgs = HierarchySpanGroup.select_deeper(sel_sgs)
                sel_sgs = HierarchySpanGroup.select_sum_matching_len(sel_sgs)
                if len(sel_sgs) > 1:
                    sel_sgs = HierarchySpanGroup.select_sum_inv_span_gap(sel_sgs)
                    if len(sel_sgs) > 1:
                        sel_sgs = HierarchySpanGroup.select_unique(sel_sgs)

            self.lgr.debug(f"\tJuri: dept_has_juri_label: {Hierarchy.to_str(sel_sgs)}")
            assert (
                len(sel_sgs) <= 1
            ), f"label: {label} {len(sel_sgs)}  span_groups found, {post_str} {Hierarchy.to_str(sel_sgs)}"
            return sel_sgs
        else:
            sub_path = ["jurisdiction", "policing"]
            sel_sgs = HierarchySpanGroup.select_path_match(juri_sgs, sub_path)
            if len(sel_sgs) == 1:
                return sel_sgs

            juri_level = get_juri_level(post_str)
            role_level = get_role_level(role_sg)

            sel_sgs = HierarchySpanGroup.select_level_match(sel_sgs, juri_level)
            sel_sgs = HierarchySpanGroup.select_sum_matching_len(sel_sgs)

            if len(sel_sgs) > 1:
                sel_sgs = HierarchySpanGroup.select_level_match(sel_sgs, role_level)
                sel_sgs = HierarchySpanGroup.select_unique(sel_sgs)
                if len(sel_sgs) > 1:
                    print(f"== Multiple {post_str} {len(sel_sgs)}")
                    for sg in sel_sgs:
                        print(
                            f"\t{sg.new_str()} {sg.sum_match_len} {sg.sum_span_len} {sg.sum_span_len_start} {sg.hierarchy_path}"
                        )

                    sel_sgs = sel_sgs[:1]

            self.lgr.debug(f"\tJuri: sum_matching_len: {Hierarchy.to_str(sel_sgs)}")
            assert (
                len(sel_sgs) <= 1
            ), f"{len(sel_sgs)}  span_groups found, {post_str} {Hierarchy.to_str(sel_sgs)}"
            return sel_sgs

    def test(self, post, post_path):
        errors = []

        def isUnderTraining(post):
            isRPA = (post.dept == "Rajasthan Police Academy") and (not post.role)
            return isRPA or post.stat == "Under Training"

        def isSuspendedOrDeputed(post):
            return post.stat == "Under Suspension" or post.stat == "On Deputation"

        emptyDeptAndJuri = (not post.dept) and (not post.juri)
        awaitingPosting = post.stat == "Awaiting Posting Order"
        underTraining = isUnderTraining(post)
        underTrainSusDep = underTraining or isSuspendedOrDeputed(post)
        promoted = post.stat == "on Promotion"

        if not post.role and not underTraining:
            msg = f"Empty role >{post.post_str}<"
            errors.append(PostEmptyRoleError(msg=msg, path=post_path))

        if (
            emptyDeptAndJuri  # noqa: W503 todo
            and (not awaitingPosting)  # noqa: W503 todo
            and (not underTrainSusDep)  # noqa: W503 todo
            and (not promoted)  # noqa: W503 todo
        ):
            msg = f"Both department and jurisdiction fields are empty >{post.post_str}<"
            errors.append(PostEmptyDeptAndJuriError(msg=msg, path=post_path))

        non_overlap_spans = Span.accumulate(post.spans, post.post_str)
        u_texts = Span.unmatched_texts(non_overlap_spans, post.post_str)
        u_texts = [t for t in u_texts if t.isalnum()]
        if u_texts:
            msg = f'unmatched texts: >{"<, >".join(u_texts)}< >{post.post_str}<'
            errors.append(
                PostUnmatchedTextsError(msg=msg, path=post_path, texts=u_texts)
            )
        return errors

    def parse(self, post_words, post_str, post_path, rank=None):
        self.lgr.info(f">{post_str}")

        if post_str in self.noparse_dict:
            path_dict = self.noparse_dict[post_str]
            post = Post.build_no_spans(post_words, post_str, **path_dict)
            return post

        field_dict = {}
        post_str = post_str.replace("‐", "-")
        select_strategy_dict = {
            "dept": "connected_sum_span_len",
            # "role": "at_start",
            "role": "left_most",
            "juri": "none",
            "loca": "sum_span_len",
            "stat": "first",
        }
        for (field, hierarchy) in self.hierarchy_dict.items():
            match_options = MatchOptions(
                ignore_case=True,
                merge_strategy="child_span",
                select_strategy=select_strategy_dict[field],
                match_on_word_boundary=True,
            )
            # TODO this can be removed by editing role.yml and removing punct
            if field == "role":
                match_options.match_on_word_boundary = False

            try:
                span_groups = hierarchy.find_match(post_str, match_options)
            except AssertionError as e:  # noqa: F841
                _, _, vtb = sys.exc_info()
                field_dict[field] = span_groups = []
                self.lgr.exception(f"\t{field}: PARSE FAILED {post_str}")
            else:
                field_dict[field] = span_groups

            if (
                field == "role"
                and not span_groups  # noqa: W503
                and post_str.lower().startswith("circle")  # noqa: W503
            ):
                field_dict["role"] = hierarchy.find_match(
                    "CIRCLE OFFICER", match_options
                )

            if field == "role" and not span_groups and rank is not None:
                field_dict["role"] = hierarchy.find_match(rank, match_options)

            if field == "juri" and span_groups:
                dept_sg = first(field_dict["dept"], None)
                role_sg = first(field_dict["role"], None)
                sgs = self.handle_juri(post_str, dept_sg, role_sg, span_groups)
                field_dict["juri"] = sgs

            h_paths = [sg.hierarchy_path for sg in field_dict[field]]
            self.lgr.info(
                f"{field}: {Hierarchy.to_str(field_dict[field])} {h_paths[:1]}"
            )

        field_dict = dict((k, first(v, None)) for k, v in field_dict.items())

        if post_path == "p0.t0.r14.c4":
            # b /Users/mukund/Software/docInt/docint/pipeline/pdfpost_parser.py:282
            print("found it")

        post = Post.build(post_words, post_str, **field_dict)
        return post

    def get_rank(self, doc):
        first_page = doc.pages[0]

        if not hasattr(first_page, "heading"):
            return None

        heading_str = " ".join([w.text for w in first_page.heading.words])
        reg_dict = {
            "Additional Superintendent of Police": "addl[\. ]*s[\.]?p[\.]?",  # noqa: W605
            "Deputy Superintendent of Police": "dy[\. ]*s[\.]?p[\.]?",  # noqa: W605
        }

        for rank, reg_str in reg_dict.items():
            r = re.compile(reg_str, re.I)
            m = r.search(heading_str)
            if m:
                return rank
        return None

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"post_parser: {doc.pdf_name}")

        doc.add_extra_page_field("posts", ("list", "orgpedia.extracts.orgpedia", "Post"))
        header_info = doc.header_info
        stub_idx, loca_idx = header_info.index("post_stub"), header_info.index("loca")

        rank = self.get_rank(doc)
        print(rank)

        total_posts, errors = 0, []
        for page_idx, page in enumerate(doc.pages):
            page.posts = []
            if not page.tables:
                continue
            assert len(page.tables) == 1, f"Multiple tables {doc.pdf_name} {page_idx}"
            for (row_idx, row) in enumerate(page.tables[0].body_rows):
                post_path = f"p{page_idx}.t0.r{row_idx}.c{stub_idx}"

                if post_path == "p9.t0.r21.c4":
                    # b /Users/mukund/Software/docInt/docint/pipeline/pdfpost_parser.py:326
                    print("found it")

                stub_c, loca_c = row.cells[stub_idx], row.cells[loca_idx]
                stub, loca = stub_c.arranged_text(10), loca_c.arranged_text()
                stubL, locaL = stub.lower(), loca.lower()

                post_str = stub if stubL.endswith(locaL) else f"{stub} {loca}"
                post_words = stub_c.words + loca_c.words

                post = self.parse(post_words, post_str, post_path, rank)
                page.posts.append(post)
                post.errors = self.test(post, post_path)
                errors.extend(post.errors)
            total_posts += len(page.posts)

        self.lgr.info(f"==Total:{total_posts} {DataError.error_counts(errors)}")
        [self.lgr.info(str(e)) for e in errors]

        self.remove_log_handler(doc)
        return doc

    def __del__(self):
        pass
        # print("DELETING")
        # record_dir = Path("/tmp/record")
        # for field, file_name in self.hierarchy_files.items():
        # hierarchy_path = record_dir / file_name
        # self.hierarchy_dict[field].write_record(hierarchy_path)


if __name__ == "__main__":
    hierarchy_files = {
        "dept": "dept.yml",
        "role": "role.yml",
        "juri": "juri.yml",
        "loca": "loca.yml",
        "stat": "stat.yml",
    }

    post_parser = PostParser("conf", hierarchy_files, ["ignore"], "postparser")

    post_parser.parser([], sys.argv[1], 0)

"""
    def parse(self, post_words, post_str, detail_idx):
        match_paths_dict = {}
        self.lgr.info(f">{post_str}")
        for (field, hierarchy) in self.hierarchy_dict.items():
            try:
                match_paths = hierarchy.find_match(post_str, self.match_options)
            except Exception as e:
                self.lgr.info(f"\t{field}: PARSE FAILED {post_str}")
                match_paths = []
            else:
                match_paths_dict[field] = match_paths
                n = len(match_paths)
                self.lgr.info(f"\t{field}[{n}]: {Hierarchy.to_str(match_paths)}")
        # end for

        post = self.build_post(match_paths_dict, post_words, post_str, detail_idx)
        return post


    def build_post(self, hier_sg_dict, post_words, post_str, detail_idx):
        def get_role(role_sgs):
            sgs = [sg for sg in role_sgs if sg.full_span.start == 0]
            return sgs[0] if len(sgs) == 1 else None

        def get_dept(dept_sgs):
            dept_sgs.sort(key=attrgetter("sum_span_len"), reverse=True)
            return dept_sgs[0] if dept_sgs else None

        def get_juri(juri_sgs):
            juri_sgs.sort(key=attrgetter("sum_span_len"), reverse=True)
            return juri_sgs[0] if juri_sgs else None

        def get_loca(loca_sgs):
            txts_sgs = [(Span.to_str(post_str, sg), sg) for sg in loca_sgs]
            txts_sgs.sort(key=lambda tup: len(tup[0]), reverse=True)
            return txts_sgs[0][1] if (txts_sgs and txts_sgs[0]) else None

        def get_stat(stat_sgs):
            return stat_sgs[0] if stat_sgs else None

        post_dict = {}
        for (field, span_groups) in hier_sg_dict.items():
            # if field != 'juri':
            proc = f"get_{field}"
            field_sg = locals()[proc](span_groups)
            post_dict[field] = field_sg
        post = Post.build(post_words, post_str, **post_dict)
        return post



"""
