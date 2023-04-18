import logging
import sys
from pathlib import Path

from docint.hierarchy import Hierarchy, MatchOptions
from docint.region import Region
from docint.span import SpanGroup
from docint.util import find_date, load_config
from docint.vision import Vision
from docint.word_line import words_in_lines
from more_itertools import first

from ..extracts.orgpedia import (
    IncorrectOrderDateError,
    Officer,
    Order,
    OrderDateNotFoundError,
    OrderDetail,
    Post,
)


@Vision.factory(
    "order_tagger",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "ordertagger",
        "hierarchy_files": {
            "dept": "dept.yml",
            "role": "role.yml",
        },
    },
)
class OrderTagger:
    def __init__(self, conf_dir, conf_stub, hierarchy_files):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.hierarchy_files = hierarchy_files

        self.hierarchy_dict = {}
        for field, file_name in self.hierarchy_files.items():
            hierarchy_path = self.conf_dir / file_name
            hierarchy = Hierarchy(hierarchy_path)
            self.hierarchy_dict[field] = hierarchy
        self.match_options = MatchOptions(ignore_case=True)

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.fixes_dict = {}

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

    def get_order_date(self, doc):
        od_labels = doc.pages[0].word_labels.get("ORDERDATEPLACE", [])

        page_idxs = od_labels['page_idx_']
        page_idx = page_idxs[0] if isinstance(page_idxs, list) else page_idxs
        od_words = [doc[page_idx][w_idx] for w_idx in od_labels['word_idxs']]
        word_lines = words_in_lines(Region.from_words(words=od_words), para_indent=False)

        result_dt, errors, date_text = None, [], ""

        err_details = []
        for word_line in word_lines:
            date_line = " ".join(f"{w.text}" for w in word_line)
            err_line = " ".join(f"{w.word_idx}->{w.text}" for w in word_line)
            err_details.append(f"DL: {doc.pdf_name} {date_line} {err_line}")
            if len(date_line) < 10:
                date_text += date_line + "\n"
                continue

            dt, err_msg = find_date(date_line)
            if dt and (not err_msg):
                result_dt = dt
                date_text = date_line  # overwrite it
                break
            date_text += date_line + "\n"

        if result_dt and (result_dt.year < 1947 or result_dt.year > 2021):
            path = "pa0.layoutlm.ORDERDATEPLACE"
            msg = f"{doc.pdf_name} Incorrect date: {result_dt} in {date_text}"
            errors.append(IncorrectOrderDateError(path=path, msg=msg, name='IncorrectOrderDate'))
        elif result_dt is None:
            path = "pa0.layoutlm.ORDERDATEPLACE"
            msg = f"{doc.pdf_name} text: >{date_text}<"
            errors.append(OrderDateNotFoundError(path=path, msg=msg, name='OrderDateNotFoundError'))

        if errors:
            print("\n".join(err_details))

        print(f"Order Date: {result_dt}")
        return result_dt, errors

    def get_salut(self, name):
        short = "capt-col-dr.(smt.)-dr. (smt.)-dr. (shrimati)-dr-general ( retd . )-general (retd.)-general-km-kum-kumari-maj. gen. (retd.)-maj-miss-ms-prof. (dr.)-prof-sadhvi-sardar-shri-shrimati-shrinati-shrl-shrt-shr-smt-sushree-sushri"

        saluts = []
        for s in short.split("-"):
            p = f"{s} .-{s} -{s}. -({s}) -({s}.) -({s}.)-{s}."
            saluts.extend(p.split("-"))

        name_lower = name.lower()
        found_salut = first([s for s in saluts if name_lower.startswith(s)], "")
        result = name[: len(found_salut)]
        return result

    def build_officer(self, conf_officer, page):
        words = [page.words[idx] for idx in conf_officer["idxs"]]
        officer_text = " ".join(w.text for w in words)
        officer_text = officer_text.strip(".|,-*@():%/1234567890$ '")

        self.lgr.info(f"Building Officer on: >{officer_text}<")

        salut = self.get_salut(officer_text)
        name = officer_text[len(salut) :].strip()  # noqa: E203
        officer = Officer.build(words, salut, name, cadre="goi_minister")
        return officer

    def build_post(self, conf_post, page):
        words = [page.words[idx] for idx in conf_post["idxs"]]
        post_text = " ".join(w.text for w in words)

        self.lgr.info(f"Building Post on: >{post_text}<")

        dept_sgs = self.hierarchy_dict["dept"].find_match(post_text, self.match_options)
        self.lgr.debug(f"dept: {Hierarchy.to_str(dept_sgs)}")

        b_post_text = SpanGroup.blank_text(dept_sgs, post_text)
        role_sgs = self.hierarchy_dict["role"].find_match(b_post_text, self.match_options)
        self.lgr.debug(f"role: {Hierarchy.to_str(role_sgs)}")

        assert len(dept_sgs) == 1 and len(role_sgs) in (0, 1)
        role_sg = role_sgs[0] if role_sgs else None
        post = Post.build(words, post_text, dept_sgs[0], role_sg)
        return post

    def build_detail(self, conf_detail, page):
        officer = self.build_officer(conf_detail["officer"], page)
        detail_words = officer.words[:]
        verb_dict = {}
        for verb in ["continues", "relinquishes", "assumes"]:
            for conf_post in conf_detail.get(verb, []):
                post = self.build_post(conf_post, page)
                detail_words.extend(post.words)
                verb_dict.setdefault(verb, []).append(post)

        detail = OrderDetail.build(
            detail_words,
            [detail_words],
            officer,
            conf_detail["detail_idx"],
            continues=verb_dict.get("continues", []),
            relinquishes=verb_dict.get("relinquishes", []),
            assumes=verb_dict.get("assumes", []),
        )
        return detail

    def merge_detail(self, conf_detail, order_detail, page):
        if "officer" in conf_detail:
            officer = self.build_officer(conf_detail["officer"], page)
        else:
            officer = order_detail.officer

        detail_words = officer.words[:]

        verb_dict = {}
        for verb in ["continues", "relinquishes", "assumes"]:
            verb_dict[verb] = getattr(order_detail, verb)
            print(f"verb: order_len: {len(verb_dict[verb])}")

            # overwrite it with conf_detail
            for conf_post in conf_detail.get(verb, []):
                post = self.build_post(conf_post, page)
                detail_words.extend(post.words)
                verb_dict.setdefault(verb, []).append(post)

        detail = OrderDetail.build(
            detail_words,
            [detail_words],
            officer,
            conf_detail["detail_idx"],
            continues=verb_dict["continues"],
            relinquishes=verb_dict["relinquishes"],
            assumes=verb_dict["assumes"],
        )
        return detail

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"manual_tagger: {doc.pdf_name}")

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)
        conf_order = doc_config.get("order", None)

        if not doc_config:
            self.remove_log_handler(doc)
            return doc

        edits = doc_config.get("edits", [])
        if edits:
            print(f"Edited document: {doc.pdf_name}")
            doc.edit(edits)

        mode = doc_config.get("mode", "build")

        if mode in ("merge_detail", "overwrite_detail", "append_detail"):
            assert hasattr(doc, "order")
        else:
            assert not hasattr(doc, "order")
            doc.add_extra_field("order", ("obj", "orgpedia.extracts.orgpedia", "Order"))

        details = []
        for conf_detail in conf_order.get("details", []):
            page = doc.pages[conf_detail["page_idx"]]

            if mode == "build":
                detail = self.build_detail(conf_detail, page)
                assert conf_detail["detail_idx"] == len(details)
                details.append(detail)

            elif mode == "merge_detail":
                detail_idx = conf_detail["detail_idx"]
                order_detail = doc.order.details[detail_idx]
                merged_detail = self.merge_detail(conf_detail, order_detail, page)
                doc.order.details[detail_idx] = merged_detail

            elif mode == "overwrite_detail":
                detail = self.build_detail(conf_detail, page)
                detail_idx = conf_detail["detail_idx"]
                if detail_idx < len(doc.order.details):
                    doc.order.details[detail_idx] = detail
                else:
                    assert detail_idx == len(doc.order.details)
                    doc.order.details.append(detail)
            if mode == "append_detail":
                detail = self.build_detail(conf_detail, page)
                assert conf_detail["detail_idx"] == len(doc.order.details)
                doc.order.details.append(detail)

        if "order_date" in conf_order:
            order_date, _ = find_date(conf_order["order_date"])
            self.lgr.info(f"order_tagger: Date: {order_date}")
        else:
            order_date, date_errors = self.get_order_date(doc)
            self.lgr.info(f"order_tagger: Date: {order_date}")

        if mode == "build":
            doc.order = Order.build(doc.pdf_name, order_date, doc.pdffile_path, details)
            doc.order.category = "Change of Portfolio"
        else:
            doc.order.date = order_date

        doc.add_errors([])

        self.lgr.info(f"=={doc.pdf_name}.order_tagger {len(doc.order.details)}")
        self.remove_log_handler(doc)
        return doc


# b /Users/mukund/Software/docInt/docint/pipeline/order_tagger.py:154
