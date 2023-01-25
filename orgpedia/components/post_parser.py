import logging
import sys
from pathlib import Path
from textwrap import wrap
from typing import List

from docint.data_error import DataError
from docint.hierarchy import Hierarchy, MatchOptions
from docint.para import TextConfig
from docint.region import Region
from docint.vision import Vision

from ..extracts.orgpedia import Post


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
    errors: List[DataError] = []

    @classmethod
    def build(cls, words, word_lines, post_str, detail_idx, continues, relinquishes, assumes):
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
        "ignore_labels": ["ignore", "puncts"],
        "conf_stub": "postparser",
    },
)
class PostParserOnSentence:
    def __init__(self, doc_confdir, hierarchy_files, ignore_labels, conf_stub):
        self.doc_confdir = Path(doc_confdir)
        self.hierarchy_files = hierarchy_files
        self.ignore_labels = ignore_labels + ['puncts']
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
            num_depts = len([p for p in post_groups if p.root.endswith("__department__")])
            num_roles = len([p for p in post_groups if p.root.endswith("__role__")])

            if num_roles > num_depts:
                msg = f"verb: {verb} num_roles:{num_roles} num_depts:{num_depts}"
                errors.append(PostMismatchError(path=path, msg=msg))
        return errors


    def build_post_info(self, post_region, hier_span_groups, detail_idx):
        def build_post(post_fields_dict, post_spans):
            dept_sg = post_fields_dict.get("department", None)
            role_sg = post_fields_dict.get("role", None)

            if dept_sg and not role_sg:
                role_sg = default_role_sg

            post_words = post_region.get_words_for_spans(post_spans, self.text_config)
            return Post.build(post_words, post_region_str, dept_sg, role_sg)

        field_span_groups = [(field, span_group) for (field, sgs) in hier_span_groups.items() for span_group in sgs]
        field_span_groups.sort(key=lambda tup: tup[1].min_start)

        field_spans = {}
        verb, dept_role_groups_dict, default_role_sg = "continues", {}, None
        for field, hier_span_group in field_span_groups:
            if hier_span_group.root == "verb":  # TODO move this to __verb__
                verb = hier_span_group.leaf
                verb_span = hier_span_group.spans[0]
                # post_region.add_label(verb_span, "verb", self.text_config) CHANGED
                field_spans.setdefault("verb", []).append(verb_span)
            else:
                dept_role_groups_dict.setdefault(verb, []).append(hier_span_group)
                if hier_span_group.root == "__role__":
                    default_role_sg = hier_span_group
        # end for

        if not default_role_sg:
            default_role_sg = self.hierarchy_dict["role"].find_match('Cabinet Minister', self.match_options)[0]

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
                # [post_region.add_label(Span(start=s.start, end=s.end), field, self.text_config) for s in hier_span_group]
                field_spans.setdefault(field, []).extend(hier_span_group)

            posts_dict[verb].append(build_post(post_field_dict, post_spans))

        for field, spans in field_spans.items():
            post_region.add_label(spans, field, self.text_config)

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
            idx_texts.append(idx_region_str[start_idx : start_idx + len(t)])  # noqa: E203
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
        post_info = self.build_post_info(post_region, match_paths_dict, detail_idx)
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
                doc.add_errors(post_info.errors)
                page.post_infos.append(post_info)

        self.remove_log_handler(doc)
        return doc
