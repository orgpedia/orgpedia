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
    def __init__(
        self,
        hierarchy_files,
        noparse_file=None,
        trans_dict={},
        ignore_case=True,
        merge_strategy="child_span",
        select_strategy="connected_sum_span_len",
        match_on_word_boundary=True,
    ):

        self.hierarchy_dict = {}
        for field, file_name in hierarchy_files.items():
            hierarchy = Hierarchy(Path(file_name))
            self.hierarchy_dict[field] = hierarchy

        self.noparse_dict = self.load_noparse(noparse_file)
        self.trans_dict = trans_dict

        self.ignore_case = ignore_case
        self.merge_strategy = merge_strategy
        self.select_strategy = select_strategy
        self.match_on_word_boundary = match_on_word_boundary

        self.match_options = None

        # self._enable_hierarchy_logger()
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

        noparse_file_dict = read_config_from_disk(noparse_file)
        return dict((p["post"], rev(p["info"])) for p in noparse_file_dict.get("posts", []))

    def fix_post_str(self, post_str):
        return post_str

    def translate_post_str(self, post_str):
        return self.trans_dict[post_str] if self.trans_dict else post_str

    def get_match_options(self, post_field):
        def get_val(match_var):
            if isinstance(match_var, dict):
                return match_var.get(post_field, match_var.get('default', None))
            else:
                return match_var

        match_options = MatchOptions(
            ignore_case=get_val(self.ignore_case),
            merge_strategy=get_val(self.merge_strategy),
            select_strategy=get_val(self.select_strategy),
            match_on_word_boundary=get_val(self.match_on_word_boundary),
        )
        return match_options

    def print_details(self, post, errors, doc_path):
        he = "*" if errors else " "  # has_error
        ed = ",".join(e.name for e in errors)  # error_details
        ed = f"E: {ed}" if errors else ""

        if post.orig_str:
            to_trans = '#' if 'UNK' in post.post_str else ''
            print(f'P:{doc_path}{he}:{post.orig_str}{to_trans}')
            print(f'P:{doc_path}{he}:\t{post.post_str}')
        else:
            print(f'P:{doc_path}{he}:{post.post_str}')
        print(f'P:{doc_path}{he}:\t{post.to_short_str()} {ed}')
        print(f'P:{doc_path}{he}:')

    def parse(self, post_words, post_str, doc_path="", role_str=""):
        self.lgr.info(f">{post_str}")

        orig_str = post_str
        post_str = self.fix_post_str(post_str).strip()
        if not post_str.isascii():
            fixed_str = post_str  # noqa useful for printing
            post_str = self.translate_post_str(post_str)
        else:
            orig_str = None

        if post_str in self.noparse_dict:
            path_dict = self.noparse_dict[post_str]
            post = Post.build_no_spans(post_words, post_str, **path_dict)
            return post, []

        field_dict = {}
        for (field, hierarchy) in self.hierarchy_dict.items():
            self.match_options = self.get_match_options(field)

            try:
                span_groups = hierarchy.find_match(post_str, self.match_options)
            except AssertionError as e:  # noqa: F841
                _, _, vtb = sys.exc_info()
                span_groups = []
                self.lgr.exception(f"\t{field}: PARSE FAILED {post_str}")

            if field == "role" and not span_groups and role_str:
                span_groups = hierarchy.find_match(role_str, self.match_options)

            handle_field = getattr(self, f'handle_{field}')
            span_groups = handle_field(post_str, field_dict, span_groups, doc_path)

            field_dict[field] = first(span_groups, None)
        # end for

        post = Post.build(post_words, post_str, **field_dict)
        post.orig_str = orig_str
        errors = self.test(post, doc_path)

        self.print_details(post, errors, doc_path)
        return post, errors

    def handle_dept(self, post_str, field_dict, span_groups, doc_path):
        return span_groups

    def handle_role(self, post_str, field_dict, span_groups, doc_path):
        return span_groups

    def handle_juri(self, post_str, field_dict, span_groups, doc_path):
        return span_groups

    def handle_loca(self, post_str, field_dict, span_groups, doc_path):
        return span_groups

    def handle_stat(self, post_str, field_dict, span_groups, doc_path):
        return span_groups

    def test(self, post, doc_path):
        pass
