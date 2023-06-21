import logging
import re
import sys
from pathlib import Path
from typing import List

from docint.data_error import DataError
from docint.hierarchy import Hierarchy, HierarchySpanGroup, MatchOptions
from docint.span import Span
from docint.util import read_config_from_disk
from more_itertools import first

from ..extracts.orgpedia import Post


class PostEmptyDeptAndJuriError(DataError):
    pass


class PostEmptyRoleError(DataError):
    pass


class PostUnmatchedTextsError(DataError):
    texts: List[str]


class PostParser:
    def __init__(self, hierarchy_files, noparse_file=None):
        self.hierarchy_dict = {}
        for field, file_name in hierarchy_files.items():
            hierarchy = Hierarchy(Path(file_name))
            self.hierarchy_dict[field] = hierarchy

        self.noparse_dict = self.load_noparse(noparse_file)
        self.match_options = MatchOptions(ignore_case=True)
        self.lgr = logging.getLogger(__name__ + ".")
        self.lgr.setLevel(logging.DEBUG)

    def _enable_hierarchy_logger(self):
        logging.getLogger("docint.hierarchy").addHandler(logging.StreamHandler(sys.stdout))
        logging.getLogger("docint.hierarchy").setLevel(logging.DEBUG)

    def _disable_hierarchy_logger(self):
        logging.getLogger("docint.hierarchy").setLevel(logging.ERROR)

    def load_noparse(self, noparse_file):
        def rev(info_dict):
            return dict((k, list(reversed(v))) for k, v in info_dict.items())

        if not noparse_file:
            return {}

        noparse_file_dict = read_config_from_disk(noparse_file)
        return dict((p["post"], rev(p["info"])) for p in noparse_file_dict["posts"])

    def handle_juri(self, post_str, dept_sg, role_sg, juri_sgs, post_path):  # noqa: C901
        def is_commissioner_role(role_sg):
            return "Commissioner" in role_sg.hierarchy_path if role_sg else False

        def is_dept_with_juri_label(dept_sg):
            return dept_sg.get_label_val("_juri") is not None if dept_sg else None

        def get_commisionerate(post_str):
            post_str = post_str.lower()
            assert "jaipur" in post_str or "jodhpur" in post_str, f"{post_path} No commissionerate: {post_str}"
            # print("** ERROR No commissionerate: {post_str}")
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
                        print(f"\t{sg.new_str()} {sg.sum_match_len} {sg.sum_span_len}", end="")
                        print(" {sg.sum_span_len_start}")

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
            ), f"{post_path} label: {label} {len(sel_sgs)} span_groups: {post_str} {Hierarchy.to_str(sel_sgs)}"
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
                        print(f"\t{sg.new_str()} {sg.sum_match_len} {sg.sum_span_len}", end="")
                        print(f"{sg.sum_span_len_start} {sg.hierarchy_path}")

                    sel_sgs = sel_sgs[:1]

            self.lgr.debug(f"\tJuri: sum_matching_len: {Hierarchy.to_str(sel_sgs)}")
            assert len(sel_sgs) <= 1, f"{len(sel_sgs)} span_groups:, {post_str} {Hierarchy.to_str(sel_sgs)}"
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
            errors.append(PostEmptyRoleError(msg=msg, path=post_path, name='PostEmptyRole'))

        if (
            emptyDeptAndJuri  # noqa: W503 todo
            and (not awaitingPosting)  # noqa: W503 todo
            and (not underTrainSusDep)  # noqa: W503 todo
            and (not promoted)  # noqa: W503 todo
        ):
            msg = f"Both department and jurisdiction fields are empty >{post.post_str}<"
            e = PostEmptyDeptAndJuriError(msg=msg, path=post_path, name='PostEmptyDeptAndJuri')
            errors.append(e)

        non_overlap_spans = Span.accumulate(post.spans, post.post_str)
        u_texts = Span.unmatched_texts(non_overlap_spans, post.post_str)
        u_texts = [t for t in u_texts if t.isalnum()]
        if u_texts:
            msg = f'unmatched texts: >{"<, >".join(u_texts)}< >{post.post_str}<'
            e = PostUnmatchedTextsError(msg=msg, path=post_path, texts=u_texts, name='PostUnmatchedTexts')
            errors.append(e)
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
                field_dict["role"] = hierarchy.find_match("CIRCLE OFFICER", match_options)

            if field == "role" and not span_groups and rank is not None:
                field_dict["role"] = hierarchy.find_match(rank, match_options)

            if field == "juri" and span_groups:
                dept_sg = first(field_dict["dept"], None)
                role_sg = first(field_dict["role"], None)
                sgs = self.handle_juri(post_str, dept_sg, role_sg, span_groups, post_path)
                field_dict["juri"] = sgs

            h_paths = [sg.hierarchy_path for sg in field_dict[field]]
            self.lgr.info(f"{field}: {Hierarchy.to_str(field_dict[field])} {h_paths[:1]}")

        field_dict = dict((k, first(v, None)) for k, v in field_dict.items())

        # if post_path == "p0.t0.r14.c4":
        #     # b /Users/mukund/Software/docInt/docint/pipeline/pdfpost_parser.py:282
        #     print("found it")

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
