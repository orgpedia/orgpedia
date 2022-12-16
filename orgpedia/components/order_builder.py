import calendar
import logging
import string
import sys
from collections import Counter
from pathlib import Path

from docint.data_error import DataError, UnmatchedTextsError
from docint.region import Region
from docint.util import find_date, load_config
from docint.vision import Vision
from docint.word_line import words_in_lines
from more_itertools import first

from ..extracts.orgpedia import IncorrectOrderDateError, Officer, Order, OrderDateNotFoundErrror, OrderDetail

"""
# TODO:
1. Can we create a common list of words to be ignored"
2. Do we need to colors here, or that can be separate component that just prints the values ? (Thinking alound)
3. Need a name parser and name finder
4. merge the post_info finder with this, by moving
5. consolidate order date finding in a single class
6. need a clear way of finalizing what goes in info and what goes in debug
7. need a clear way of testing objects and adding errors, handling ignores, writing log


"""


@Vision.factory(
    "order_builder",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "orderbuilder",  # TODO pleae change this.
        "pre_edit": True,
        "ignore_texts": "",
        "ignore_texts_file": "",
    },
)
class OrderBuilder:
    def __init__(self, conf_dir, conf_stub, pre_edit, ignore_texts, ignore_texts_file):
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.pre_edit = pre_edit
        self.ignore_texts = ignore_texts.split("-")
        self.ignore_texts_file = Path(ignore_texts_file) if ignore_texts_file else None

        self.color_config = {
            "person": "white on yellow",
            "post-dept-continues": "white on green",
            "post-dept-relinquishes": "white on spring_green1",
            "post-dept-assumes": "white on dark_slate_gray1",
            "dept": "white on spring_green1",
            "department": "white on spring_green1",
            "post-role-continues": "white on red",
            "post-role-relinquishes": "white on magenta",
            "post-role-assumes": "white on purple",
            "role": "white on red",
            "verb": "white on black",
        }
        if self.ignore_texts_file:
            self.ignore_texts_from_file = self.ignore_texts_file.read_text().split("\n")
            self.ignore_texts_from_file = [t.lower() for t in self.ignore_texts_from_file]
            print(f"Read {len(self.ignore_texts_from_file)} texts")
        else:
            self.ignore_texts_from_file = []

        self.ignore_texts = [t.lower() for t in self.ignore_texts]

        self.ignore_unmatched = set(
            [
                "the",
                "of",
                "office",
                "charge",
                "and",
                "will",
                "he",
                "additional",
                "&",
                "has",
                "offices",
                "also",
                "to",
                "be",
                "uf",
                "continue",
                "addition",
                "she",
                "other",
                "hold",
                "temporarily",
                "assist",
                "held",
                "his",
                "in",
                "that",
                "(a)",
                "(b)",
                "temporary",
                "as",
                "or",
                "with",
                "effect",
                "holding",
                "allocated",
                "duties",
                "been",
                "after",
                "under",
                "of(a)",
                "and(b)",
                "and(c)",
                "him",
                "till",
                "recovers",
                "fully",
                "look",
                "work",
                "from",
                "th",
                "june",
                "1980",
                "for",
                "time",
                "being",
                ")",
                "(",
                "/",
                "by",
                "portfolio",
                "discharge",
                "assisting",
                "hereafter",
                "designated",
            ]
            + self.ignore_texts  # noqa: W503
            + self.ignore_texts_from_file  # noqa: W503
        )

        self.unmatched_ctr = Counter()

        ignore_puncts = string.punctuation
        self.punct_tbl = str.maketrans(ignore_puncts, " " * len(ignore_puncts))
        self.month_names = list(calendar.month_name) + list(calendar.month_abbr)
        self.month_names = [m.lower() for m in self.month_names]

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

    def get_salut(self, name):
        short = "capt-col-dr.(smt.)-dr. (smt.)-dr. (shrimati)-dr-general (retd.)-general-km-kum-kumari-maj. gen. (retd.)-maj-miss-ms-prof. (dr.)-prof-sadhvi-sardar-shri-shrimati-shrinati-shrl-shrt-shr-smt-sushree-sushri"

        saluts = []
        for s in short.split("-"):
            p = f"{s} .-{s} -{s}. -({s}) -({s}.) -({s}.)-{s}."
            saluts.extend(p.split("-"))

        name_lower = name.lower()
        found_salut = first([s for s in saluts if name_lower.startswith(s)], "")
        result = name[: len(found_salut)]
        return result

    def build_detail(self, list_item, post_info, detail_idx):
        person_spans = list_item.get_spans("person")

        is_valid = False if len(person_spans) != 1 else True

        if person_spans:
            person_span = person_spans[0]
            officer_words = list_item.get_words_for_spans([person_span])

            full_name = list_item.get_text_for_spans([person_span])
            full_name = full_name.strip(".|,-*():%/1234567890$ '")
            salut = self.get_salut(full_name)
            name = full_name[len(salut) :]  # noqa: E203

            if "," in name:
                print(f"Replacing comma {name}")
                name = name.replace(",", "")

            officer = Officer.build(officer_words, salut, name, cadre="goi_minister")
            # order_detail = OrderDetail.build(
            #     list_item.words,
            #     list_item.word_lines,
            #     officer,
            #     detail_idx,
            #     continues=post_info.continues,
            #     relinquishes=post_info.relinquishes,
            #     assumes=post_info.assumes,
            # )

            order_detail = OrderDetail.build(
                list_item.words,
                list_item.word_lines,
                officer,
                detail_idx,
                assumes=post_info.assumes,
                continues=post_info.continues,
                relinquishes=post_info.relinquishes,
            )

            # order_detail.extra_spans = person_spans[1:]
            order_detail.is_valid = is_valid
            return order_detail
        else:
            return None

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

        if result_dt and (result_dt.year < 1947 or result_dt.year > 2022):
            path = "pa0.word_labels.ORDERDATEPLACE"
            msg = f"{doc.pdf_name} Incorrect date: {result_dt} in {date_text}"
            errors.append(IncorrectOrderDateError(path=path, msg=msg))
        elif result_dt is None:
            path = "pa0.word_labels.ORDERDATEPLACE"
            msg = f"{doc.pdf_name} text: >{date_text}<"
            errors.append(OrderDateNotFoundErrror(path=path, msg=msg))

        if errors:
            print("\n".join(err_details))

        print(f"Order Date: {result_dt}")
        return result_dt, errors

    def get_order_number(self, doc):
        on_labels = doc.pages[0].word_labels.get("HEADER", [])
        page_idxs = on_labels['page_idx_']
        page_idx = page_idxs[0] if isinstance(page_idxs, list) else page_idxs
        on_words = [doc[page_idx][w_idx] for w_idx in on_labels['word_idxs']]

        word_lines = words_in_lines(Region.from_words(words=on_words), para_indent=False)
        for word_line in word_lines:
            if word_line:
                first_line = " ".join(w.text for w in word_line)
                return first_line
        return ""

    def process_unmatched(self, unmatched_texts):
        def is_date_or_digit(text):
            if text.isdigit():
                return True

            if text.lower() in self.month_names:
                return True

            for date_ext in ["rd", "st", "nd", "th"]:
                if text.lower().rstrip(date_ext + " ").strip().isdigit():
                    return True

            if text.isdigit():
                return True

            return False

        def is_all_punct(text):
            text = text.translate(self.punct_tbl).strip()
            return False if text else True

        u_texts = [t for t in unmatched_texts if t.lower() not in self.ignore_unmatched]
        u_texts = [t for t in u_texts if not is_date_or_digit(t)]
        u_texts = [t for t in u_texts if not is_all_punct(t)]
        return u_texts

    def test(self, list_item, order_detail, post_info, path):
        list_item_text = list_item.line_text()

        # ident_str = f'{list_item.doc.pdf_name}:{path}'
        # edit_str = "|".join([f"{e}" for e in list_item.edits])
        edit_str = "|"

        person_spans = list_item.get_spans("person")
        person_str = person_spans[0].span_str(list_item_text) if person_spans else ""

        # if type(list_item).__name__ == "ListItem":
        #     errors = list_item.list_errors + post_info.errors
        # else:
        #     errors = list_item.errors + post_info.errors

        # errors += order_detail.errors if order_detail is not None else []
        errors = []

        # u_texts = [ t.lower() for t in list_item.get_unlabeled_texts() if t.lower() not in self.ignore_unmatched ]

        u_texts, u_idxs = list_item.get_unlabeled_texts_idxs()
        # u_texts = self.process_unmatched(list_item.get_unlabeled_texts())
        u_texts = self.process_unmatched(u_texts)
        if u_texts:
            u_idxs = [i for t in u_texts for i in u_idxs if i.startswith(t)]
            errors.append(UnmatchedTextsError.build(path, u_texts, word_idxs=u_idxs))

        # u_texts_str = ' '.join(u_texts)
        # if not ('minis' in u_texts_str or 'depa' in u_texts_str):
        #     return []

        self.lgr.debug(list_item.orig_text())
        self.lgr.debug(f'{"edits":13}: {edit_str}')
        self.lgr.debug(f'{"person":13}: {person_str}')
        self.lgr.debug(str(post_info))
        if errors:
            self.lgr.debug("Error")
            # list_item.print_color_idx(self.color_config, width=150)
            for e in errors:
                self.lgr.debug(f"\t{str(e)}")
        self.lgr.debug("------------------------")
        return errors

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"order_builder: {doc.pdf_name}")
        doc.add_extra_field("order", ("obj", "orgpedia.extracts.orgpedia", "Order"))

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)
        edits = doc_config.get("edits", [])
        if edits:
            print(f"Edited document: {doc.pdf_name}")
            doc.edit(edits)

        ignore_dict = doc_config.get("ignores", {})
        if ignore_dict:
            print(f"Ignoring {ignore_dict.keys()}")

        # TODO these need to be regions so that lineage exists
        order_details, detail_idx, errors = [], 0, []

        order_date, date_errors = self.get_order_date(doc)
        # order_number = self.get_order_number(doc)
        errors.extend(date_errors)

        self.lgr.debug(f"*** order_date:{order_date}")
        # self.lgr.debug(f'*** order_number:{doc.order_number}')

        for page in doc.pages:
            list_items = getattr(page, "list_items", [])
            assert len(list_items) == len(page.post_infos)
            en_list_post = enumerate(zip(list_items, page.post_infos))
            for (idx, (list_item, post_info)) in en_list_post:
                list_item_path = f"pa{page.page_idx}.li{idx}"
                if post_info.is_valid:
                    order_detail = self.build_detail(list_item, post_info, detail_idx)
                    if order_detail:
                        self.lgr.debug(order_detail.to_str())
                        print(order_detail.to_str())
                        order_details.append(order_detail)
                        detail_idx += 1

                    errors += self.test(list_item, order_detail, post_info, list_item_path)
                else:
                    errors += self.test(list_item, None, post_info, list_item_path)

        doc.order = Order.build(doc.pdf_name, order_date, doc.pdffile_path, order_details)
        doc.order.category = "Change of Portfolio"

        errors = [e for e in errors if not DataError.ignore_error(e, ignore_dict)]
        doc.add_errors(errors)

        # self.write_fixes(doc, errors)

        self.lgr.info(f"=={doc.pdf_name}.order_builder {len(doc.order.details)} {DataError.error_counts(errors)}")
        [self.lgr.info(str(e)) for e in errors]
        self.remove_log_handler(doc)
        return doc

    def write_fixes(self, doc, errors):
        # b /Users/mukund/Software/docInt/docint/pipeline/table_order_builder.py:361
        unmatched_errors = [e for e in errors if isinstance(e, UnmatchedTextsError)]

        for u_error in unmatched_errors:
            cell = doc.get_region(u_error.path)
            cell_img_str = cell.page.get_base64_image(cell.shape, height=200)
            cell_word_str = " ".join(f"{w.text}-{w.word_idx}" for w in cell.words)
            cell_unmat_str = ", ".join(u_error.texts)

            page_idx = cell.page.page_idx
            unmatched_idxs = [w.word_idx for t in u_error.texts for w in cell.words if t.lower() in w.text.lower()]
            unmatched_paths = [f"pa{page_idx}.wo{idx}" for idx in unmatched_idxs]
            row = [
                u_error.path,
                cell_word_str,
                cell_unmat_str,
                cell_img_str,
                u_error.texts,
                unmatched_paths,
            ]
            self.fixes_dict.setdefault(doc.pdf_name, []).append(row)

    def __del__(self):
        # u_word_counts = self.unmatched_ctr.most_common(None)
        # self.lgr.info(f'++{"|".join(f"{u} {c}" for (u,c) in u_word_counts)}')
        # Path('/tmp/missing.yml').write_text(yaml.dump(self.missing_unicode_dict), encoding="utf-8")

        def get_html_row(row):
            row[-1] = f'<img src="{row[-1]}">'
            return "<tr><td>" + "</td><td>".join(row) + "</td></tr>"

        def get_html_rows(pdf_name, rows):
            pdf_url = f"file:///Users/mukund/orgpedia/govcabsec/pipeline/doOCR_/output/.html/{pdf_name}.html"
            pdf_html_name = f'<a href="{pdf_url}">{pdf_name}</a>'
            html_hdr = f'<tr><td colspan="{len(rows[0])}" style="text-align:left;">'
            html = html_hdr + pdf_html_name + "</td></tr>"
            html += "\n".join(get_html_row(r[0:4]) for r in rows)
            return html

        def get_yml_row(row):
            unmatched_texts, unmatched_paths = row[4], row[5]
            yml_str = f'\n# unmatched {",".join(unmatched_texts)}\n'
            for idx, u_text in enumerate(unmatched_texts):
                u_path = unmatched_paths[idx] if idx < len(unmatched_paths) else row[0]
                yml_str += f"  - replaceStr {u_path} <all> {u_text}\n"
            return yml_str

        def get_yml_rows(pdf_name, rows):
            yml_str = f"#F conf/{pdf_name}.order_builder.yml\n"
            yml_str += "edits:\n"
            yml_str += "\n".join(get_yml_row(r) for r in rows)
            return yml_str + "\n"

        if not self.fixes_dict:
            return

        headers = "Path-Sentence-Unmatched-Image".split("-")
        html_fixes_path = Path("output") / "fixes.html"
        html_str = "<html>\n<body>\n<table border=1>\n"
        html_str += "<tr><th>" + "</th><th>".join(headers) + "</th></tr>"

        fixes_items = sorted(self.fixes_dict.items(), key=lambda tup: -len(tup[1]))

        html_str += "\n".join(get_html_rows(k, v) for k, v in fixes_items)
        html_str += "\n</table>"
        html_fixes_path.write_text(html_str, encoding="utf-8")

        yml_fixes_path = Path("output") / "fixes.yml"

        yml_str = "\n".join(get_yml_rows(k, v) for k, v in fixes_items)
        yml_fixes_path.write_text(yml_str, encoding="utf-8")
