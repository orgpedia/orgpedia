import logging
import sys
from pathlib import Path
from textwrap import wrap
from typing import List

from ..extracts.orgpedia import Post
from docint.hierarchy import Hierarchy, MatchOptions
from docint.region import DataError, Region, TextConfig
from docint.vision import Vision


class PostEmptyError(DataError):
    pass


class PostEmptyVerbError(DataError):
    pass


class PostMismatchError(DataError):
    pass


class PostInfo(Region):
    post_str: str
    continues: List[Post] = []
    relinquishes: List[Post] = []
    assumes: List[Post] = []
    detail_idx: int  # TODO please remove this, this is not inc correctly
    is_valid: bool = True

    @classmethod
    def build(
        cls, words, word_lines, post_str, detail_idx, continues, relinquishes, assumes
    ):
        word_idxs = [w.word_idx for w in words]
        page_idx = words[0].page_idx if words else None
        word_lines_idxs = [[w.word_idx for w in wl] for wl in word_lines]

        return PostInfo(
            words=words,
            word_lines=word_lines,
            word_idxs=word_idxs,
            word_lines_idxs=word_lines_idxs,
            page_idx_=page_idx,
            post_str=post_str,
            continues=continues,
            relinquishes=relinquishes,
            assumes=assumes,
            detail_idx=detail_idx,
        )

    @property
    def posts_dict(self):
        return {
            "continues": self.continues,
            "relinquishes": self.relinquishes,
            "assumes": self.assumes,
        }

    def to_str(self, region_str, region_idx_str, err_str, ident_str):
        post_str = f"PostInfo:[{ident_str}]\n----------\n"
        post_str += region_idx_str + "\n"
        for post_type, posts in self.posts_dict.items():
            post_str += f"{post_type:13}: {Post.to_str(posts, region_str)}\n"
        post_str += f'{"error":13}: {err_str}' if err_str else ""
        post_str += "\n"
        return post_str

    def __str__(self):
        v_strs = []
        for verb, posts in self.posts_dict.items():
            v_strs.append(f"{verb:13}: {Post.to_str(posts, self.post_str)}")
        return "\n".join(v_strs)


@Vision.factory(
    "post_parser_onsentence",
    default_config={
        "doc_confdir": "conf",
        "hierarchy_files": {
            "dept": "dept.yml",
            "role": "role.yml",
            "verb": "verb.yml",
        },
        "ignore_labels": ["ignore"],
        "conf_stub": "postparser",
    },
)
class PostParserOnSentence:
    def __init__(self, doc_confdir, hierarchy_files, ignore_labels, conf_stub):
        self.doc_confdir = Path(doc_confdir)
        self.hierarchy_files = hierarchy_files
        self.ignore_labels = ignore_labels
        self.conf_stub = conf_stub

        self.hierarchy_dict = {}
        for field, file_name in self.hierarchy_files.items():
            hierarchy_path = self.doc_confdir / file_name
            hierarchy = Hierarchy(hierarchy_path)
            self.hierarchy_dict[field] = hierarchy

        self.match_options = MatchOptions(ignore_case=True)
        self.text_config = TextConfig(rm_labels=self.ignore_labels)

        self.lgr = logging.getLogger(__name__ + ".")
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
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

    def check_span_groups(self, posts_groups_dict, path):
        errors = []
        if not posts_groups_dict:
            errors.append(PostEmptyError(path=path, msg="postinfo is empty"))
            return errors

        if not all(len(post_groups) for post_groups in posts_groups_dict.values()):
            verbs = [v for v, pg in posts_groups_dict.items() if len(pg) == 0]
            msg = f"empty verbs: {','.join(verbs)}"
            errors.append(PostEmptyVerbError(path=path, msg=msg))
            return errors

        for (verb, post_groups) in posts_groups_dict.items():
            num_depts = len(
                [p for p in post_groups if p.root.endswith("__department__")]
            )
            num_roles = len([p for p in post_groups if p.root.endswith("__role__")])

            if num_roles > num_depts:
                msg = f"verb: {verb} num_roles:{num_roles} num_depts:{num_depts}"
                errors.append(PostMismatchError(path=path, msg=msg))
        return errors

    def edit_span_groups(posts_groups_dict):
        continues = posts_groups_dict.get("continues", [])
        relinquishes = posts_groups_dict.get("relinquishes", [])

        if not (continues and relinquishes):
            return

        # TODO endswith
        c_depts = [p.leaf for p in continues if p.root == "__department__"]
        r_depts = [p.leaf for p in relinquishes if p.root == "__department__"]

        del_idxs = [idx for (idx, dept) in enumerate(c_depts) if dept in r_depts]

        new_continues = [p for (idx, p) in enumerate(continues) if idx in del_idxs]
        posts_groups_dict["continues"] = new_continues

    def build_post_info(self, post_region, hier_span_groups, detail_idx):
        field_span_groups = [
            (field, span_group)
            for (field, sgs) in hier_span_groups.items()
            for span_group in sgs
        ]
        field_span_groups.sort(key=lambda tup: tup[1].min_start)

        verb, dept_role_groups_dict = "continues", {}
        for field, hier_span_group in field_span_groups:
            if hier_span_group.root == "verb":  # TODO move this to __verb__
                verb = hier_span_group.leaf
                verb_span = hier_span_group.spans[0]
                post_region.add_span(
                    verb_span.start, verb_span.end, "verb", self.text_config
                )
            else:
                dept_role_groups_dict.setdefault(verb, []).append(hier_span_group)

        # is_valid = True
        post_region_str = post_region.line_text(self.text_config)
        idx_region_str = post_region.word_idxs_line_text(self.text_config)
        assert len(post_region_str) == len(idx_region_str)

        posts_dict = {"continues": [], "relinquishes": [], "assumes": []}
        for post_type, hier_span_groups in dept_role_groups_dict.items():
            dept_sg, role_sg, spans = None, None, []
            for hier_span_group in hier_span_groups:
                if hier_span_group.root == "__role__":
                    role_sg = hier_span_group
                    [
                        post_region.add_span(
                            span.start, span.end, "role", self.text_config
                        )
                        for span in hier_span_group
                    ]
                else:
                    dept_sg = hier_span_group
                    [
                        post_region.add_span(
                            span.start, span.end, "dept", self.text_config
                        )
                        for span in hier_span_group
                    ]
                    spans += [span for span in hier_span_group]
            post_words = post_region.get_words_in_spans(spans)
            posts_dict[post_type].append(
                Post.build(post_words, post_region_str, dept_sg, role_sg)
            )

        post_info = PostInfo.build(
            post_region.words,
            post_region.word_lines,
            post_region_str,
            detail_idx,
            **posts_dict,
        )
        # ident_str = (
        #    f"{post_region.doc.pdf_name}:{post_region.page.page_idx}>{detail_idx}"
        # )
        path = f"pa{post_region.page.page_idx}.or.de{detail_idx}"
        post_info.errors += self.check_span_groups(dept_role_groups_dict, path)

        log_texts = wrap(post_region_str, width=90)
        idx_texts, start_idx = [], 0
        for t in log_texts:
            idx_texts.append(
                idx_region_str[start_idx : start_idx + len(t)]  # noqa: E203
            )
            start_idx += len(t) + 1

        # err_str = post_info.error_counts_str
        # log_str = "\n\n".join(f"{t}\n{i}" for (t, i) in zip(log_texts, idx_texts))
        # self.lgr.debug(post_info.to_str(post_region_str, log_str, err_str, path))
        return post_info

    def build_post_info2(self, post_region, hier_span_groups, detail_idx):
        def build_post(post_fields_dict, post_spans):
            dept_sg = post_fields_dict.get("department", None)
            role_sg = post_fields_dict.get("role", None)
            post_words = post_region.get_words_in_spans(post_spans)
            return Post.build(post_words, post_region_str, dept_sg, role_sg)

        field_span_groups = [
            (field, span_group)
            for (field, sgs) in hier_span_groups.items()
            for span_group in sgs
        ]
        field_span_groups.sort(key=lambda tup: tup[1].min_start)

        verb, dept_role_groups_dict = "continues", {}
        for field, hier_span_group in field_span_groups:
            if hier_span_group.root == "verb":  # TODO move this to __verb__
                verb = hier_span_group.leaf
                verb_span = hier_span_group.spans[0]
                post_region.add_span(
                    verb_span.start, verb_span.end, "verb", self.text_config
                )
            else:
                dept_role_groups_dict.setdefault(verb, []).append(hier_span_group)

        # is_valid = True
        post_region_str = post_region.line_text(self.text_config)
        idx_region_str = post_region.word_idxs_line_text(self.text_config)
        assert len(post_region_str) == len(idx_region_str)

        posts_dict = {"continues": [], "relinquishes": [], "assumes": []}
        for verb, hier_span_groups in dept_role_groups_dict.items():
            post_field_dict, post_spans = {}, []
            for hier_span_group in hier_span_groups:
                field = hier_span_group.root.replace("_", "")
                if field in post_field_dict:
                    posts_dict[verb].append(build_post(post_field_dict, post_spans))
                    post_field_dict, post_spans = {}, []
                post_field_dict[field] = hier_span_group
                post_spans += [span for span in hier_span_group]
                [
                    post_region.add_span(s.start, s.end, field, self.text_config)
                    for s in hier_span_group
                ]
            posts_dict[verb].append(build_post(post_field_dict, post_spans))

        post_info = PostInfo.build(
            post_region.words,
            post_region.word_lines,
            post_region_str,
            detail_idx,
            **posts_dict,
        )
        # ident_str = (
        #    f"{post_region.doc.pdf_name}:{post_region.page.page_idx}>{detail_idx}"
        # )
        path = f"pa{post_region.page.page_idx}.or.de{detail_idx}"

        post_info.errors += self.check_span_groups(dept_role_groups_dict, path)

        log_texts = wrap(post_region_str, width=90)
        idx_texts, start_idx = [], 0
        for t in log_texts:
            idx_texts.append(
                idx_region_str[start_idx : start_idx + len(t)]  # noqa: E203
            )
            start_idx += len(t) + 1

        # err_str = post_info.error_counts_str
        # log_str = "\n\n".join(f"{t}\n{i}" for (t, i) in zip(log_texts, idx_texts))
        # self.lgr.debug(post_info.to_str(post_region_str, log_str, err_str, path))
        return post_info

    def parse(self, post_region, post_str, detail_idx):
        match_paths_dict = {}
        post_str.replace(".", " ")

        self.lgr.debug("SpanGroups:\n----------")
        for (field, hierarchy) in self.hierarchy_dict.items():
            match_paths = hierarchy.find_match(post_str, self.match_options)
            match_paths_dict[field] = match_paths
            self.lgr.debug(f"{field}: {Hierarchy.to_str(match_paths)}")
            # [self.lgr.debug(f"\t{str(mp)}") for mp in match_paths]
        # end for
        post_info = self.build_post_info2(post_region, match_paths_dict, detail_idx)
        return post_info

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"post_parser: {doc.pdf_name}")

        doc.add_extra_page_field("post_infos", ("list", __name__, "PostInfo"))
        for page in doc.pages:
            page.post_infos = []
            list_items = getattr(page, "list_items", [])

            for postinfo_idx, list_item in enumerate(list_items):

                # TODO Should we remove excess space and normalize it ? worthwhile...
                post_str = list_item.line_text(self.text_config)
                self.lgr.debug(f"{post_str}\nSpans:\n----------")
                self.lgr.debug(list_item.str_spans(indent="\t"))
                post_info = self.parse(list_item, post_str, postinfo_idx)
                page.post_infos.append(post_info)

        self.remove_log_handler(doc)
        return doc


"""

    def build_post_info(
        self, post_region, post_str, post_words, match_paths_dict, detail_idx
    ):
        annots, dept_role_spans = [], []
        post_types = ["continues", "relinquishes", "assumes"]
        for field, match_paths in match_paths_dict.items():
            match_paths.sort(key=lambda m: m.min_start)

            span_pairs = [m.full_span for m in match_paths]
            esp, ps = list(enumerate(span_pairs)), post_str
            if field == "verb":
                annots += [(s, f"V{ps[s:e][0].lower()}{i}") for (i, (s, e)) in esp]
                [
                    post_region.add_span(s, e, "verb", self.text_config)
                    for (i, (s, e)) in esp
                ]
            else:
                annots += [(s, f"{field[0].upper()}{i}") for (i, (s, e)) in esp]
                dept_role_spans += [(field, (s, e)) for (i, (s, e)) in esp]
        annots.sort()
        ordered_annot_str = "-".join(a for (s, a) in annots)

        post_idxs_types, err_str = get_posts_typse(ordered_annot_str, match_paths_dict)

        if err_str:
            self.lgr.debug(f"*** {ordered_annot_str} {post_str} {err_str}")
            [
                post_region.add_span(s, e, f"post-{field}-continues", self.text_config)
                for (field, (s, e)) in dept_role_spans
            ]
            return PostInfo.build_invalid(post_words, detail_idx, err_str)
        else:
            self.lgr.debug(f"{ordered_annot_str} {post_idxs_types} {post_str}")

        types_posts = []
        for post_type, post_idxs in zip(post_types, post_idxs_types):
            type_posts = []
            for idx in post_idxs:
                dept_match_path = match_paths_dict["dept"][idx]
                if len(match_paths_dict["role"]) > idx:
                    role_match_path = match_paths_dict["role"][idx]
                else:
                    role_match_path = None

                post = Post.build(
                    post_words, post_str, dept=dept_match_path, role=role_match_path
                )
                self.mark_in_region(
                    post_region, post_type, dept_match_path, role_match_path
                )
                type_posts.append(post)
            types_posts.append(type_posts)

        post_info = PostInfo.build(
            post_words, detail_idx, types_posts[0], types_posts[1], types_posts[2],
        )
        return post_info


def get_posts_types(annot_str, match_paths_dict):
    verb = "continues"

    verb_dict = {"continues": [], "relinquishes": [], "assumes": []}
    verb_found = {"continues": False, "relinquishes": False, "assumes": False}

    rm, dm = -1, -1
    for annot in annot_str.split("-"):
        if not annot:
            print(f"AnnotEmptyError: {annot_str}")
            return [], "AnnotEmptyError"

        if annot[0] == "V":
            a1 = annot[1]
            verb = (
                "continues" if a1 == "c" else "relinquishes" if a1 == "r" else "assumes"
            )
            verb_found[verb] = True
        else:
            verb_dict[verb].append(annot)
            idx = int(annot[1])
            (rm, dm) = (max(rm, idx), dm) if annot[0] == "R" else (rm, max(dm, idx))
    # end

    if annot_str[0] != "V":
        verb_found["continues"] = True

    if not all(bool(a) == f for (a, f) in zip(verb_dict.values(), verb_found.values())):
        # print(f'AnnotMissingError: {annot_str}')
        return [], "AnnotMissingError"

    n_roles, n_depts = len(match_paths_dict["role"]), len(match_paths_dict["dept"])

    # print(f'roles: rm {rm} # {n_roles} depts dm {dm} # {n_depts}')

    if rm + 1 != n_roles:
        # print(f'RoleIndexError: {annot_str} idx:{rm} #roles: {n_roles}')
        return [], "RoleIndexError"

    if dm + 1 != n_depts or (rm + 1 > n_depts):
        # print(f'DeptIndexError: {annot_str} idx:{dm} #depts: {n_depts}')
        return [], f"DeptIndexError {annot_str}"

    actual = [
        sorted(list(set(int(a[1]) for a in annots))) for annots in verb_dict.values()
    ]
    return actual, ""


    def mark_in_region(self, post_region, post_type, dept_match_path, role_match_path):
        for span in dept_match_path.spans:
            label = f"post-dept-{post_type}"
            post_region.add_span(span[0], span[1], label, self.text_config)

        if not role_match_path:
            return

        for span in role_match_path.spans:
            label = f"post-role-{post_type}"
            post_region.add_span(span[0], span[1], label, self.text_config)
"""
