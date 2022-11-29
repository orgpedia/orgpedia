import logging
import operator as op
import re
import sys
from collections import Counter
from itertools import chain
from pathlib import Path
from string import punctuation

from docint.data_error import DataError, UnmatchedTextsError
from docint.hierarchy import Hierarchy, MatchOptions
from docint.para import TextConfig
from docint.region import Region
from docint.span import Span, SpanGroup
from docint.table import TableEmptyBodyCellError, TableMismatchColsError
from docint.util import find_date, load_config, read_config_from_disk
from docint.vision import Vision
from docint.vocab import Vocab
from docint.word_line import words_in_lines
from enchant import request_pwl_dict
from more_itertools import first

from ..extracts.orgpedia import (
    EnglishWordsInNameError,
    IncorrectOfficerNameError,
    IncorrectOrderDateError,
    Officer,
    Order,
    OrderDateNotFoundErrror,
    OrderDetail,
    Post,
)


@Vision.factory(
    "table_order_builder",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "tableorder",
        "hierarchy_files": {
            "dept": "dept.yml",
            "role": "role.yml",
        },
        "dict_file": "output/pwl_words.txt",
        "unicode_file": "conf/unicode.yml",
        "verb_pages": {},
    },
)
class TableOrderBuidler:
    def __init__(self, conf_dir, conf_stub, hierarchy_files, dict_file, unicode_file, verb_pages):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.hierarchy_files = hierarchy_files
        self.dict_file = Path(dict_file)
        self.unicode_file = Path(unicode_file)
        self.verb_pages = {}

        self.hierarchy_dict = {}
        for field, file_name in self.hierarchy_files.items():
            hierarchy_path = self.conf_dir / file_name
            hierarchy = Hierarchy(hierarchy_path)
            self.hierarchy_dict[field] = hierarchy
        self.match_options = MatchOptions(ignore_case=True)

        yml = read_config_from_disk(self.unicode_file)
        self.unicode_dict = dict((u, a if a != "<ignore>" else "") for u, a in yml.items())

        self.ignore_paren_strs = [
            "harg",
            "depart",
            "defence",
            "banking",
            "indep",
            "state",
            "indapendent",
            "smt .",
            "deptt",
            "shrimati",
            "indap",
            "indop",
        ]

        # ADDING DEPARTMENTS
        i = "of-the-and-to-hold-temporary-charge-in-also-not-any-additional-incharge-with-departments-for"
        self.ignore_unmatched = set(i.split("-"))

        self.vocab = Vocab(self.dict_file.read_text().split('\n'))
        # self.dictionary = request_pwl_dict(str(self.dict_file))

        self.unmatched_ctr = Counter()
        self.punct_tbl = str.maketrans(punctuation, " " * len(punctuation))

        self.missing_unicode_dict = {}

        self.lgr = logging.getLogger(f"docint.pipeline.{self.conf_stub}")
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
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

    def add_missing_unicodes(self, missing):
        [self.missing_unicode_dict.setdefault(k, "missing") for k in missing]

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

    def get_officer(self, officer_cell, path):
        def make_ascii(officer_cell, unicode_dict):
            assert not officer_cell.label_spans
            not_found = []
            for text, word in officer_cell.iter_word_text():
                if not word.text.isascii():
                    u_text = word.text
                    if u_text in unicode_dict:
                        a_text = unicode_dict[u_text]
                        # self.lgr.debug(f'UnicodeFixed: {u_text}->{a_text}')
                        assert a_text is not None, f"incorrect text >{u_text}<"
                        officer_cell.replace_word_text(word, "<all>", a_text)
                    else:
                        sys.stderr.write(f"Unicode: >{u_text}<\n")
                        not_found.append(word.text)
                        pass
                    # self.lgr.info(f'unicode text not found: {u_text}\n')
            return not_found

        errors = []
        if not officer_cell:
            msg = "empty cell"
            errors.append(TableEmptyBodyCellError(path=path, msg=msg, is_none=True))

        missing_unicodes = make_ascii(officer_cell, self.unicode_dict)
        self.add_missing_unicodes(missing_unicodes)  # save the missing unicodes

        officer_text = officer_cell.line_text()
        officer_text = officer_text.strip(".|,-*@():%/1234567890$ '")

        if missing_unicodes:
            print(f"Unicode: {officer_text}: {missing_unicodes}")

        # check if there are english words
        englist_texts = []
        for text in officer_text.split():
            text = text.strip("()")
            if text and self.vocab.has_text(text):
                if text.isupper():
                    officer_text = officer_text.replace(text, "")
                else:
                    englist_texts.append(text)

        if englist_texts:
            if "Deputy Prime Minister" in officer_text:
                officer_text = officer_text.replace("Deputy Prime Minister", "")

            eng_str = ",".join(englist_texts)
            msg = f"English words in officer name: >{eng_str}<"
            errors.append(EnglishWordsInNameError(msg=msg, path=path))

        if len(officer_text) < 10 or len(officer_text) > 45:
            msg = f"Short officer name: >{officer_text}<"
            errors.append(IncorrectOfficerNameError(msg=msg, path=path))

        if "," in officer_text:
            print(f"Replacing comma {officer_text}")
            officer_text = officer_text.replace(",", ".")

        salut = self.get_salut(officer_text)
        name = officer_text[len(salut) :].strip()  # noqa: E203

        officer = Officer.build(officer_cell.words, salut, name, cadre="goi_minister")
        return officer, errors

    def get_paren_spans(
        self,
        post_str,
        ignore_paren_len=5,
    ):
        paren_spans = []
        for m in re.finditer(r"\((.*?)\)", post_str):
            mat_str = m.group(1).lower()
            if len(mat_str) < ignore_paren_len:
                continue
            elif any([sl in mat_str for sl in self.ignore_paren_strs]):
                continue
            else:
                s, e = m.span()
                self.lgr.debug(f"BLANKPAREN: {m.group(0)} ->[{s}: {e}]")
                paren_spans.append(Span(start=s, end=e))
        return paren_spans

    def get_allcaps_spans(self, post_str):
        allcaps_spans = []
        for m in re.finditer(r"\S+", post_str):
            mat_str = m.group(0).strip("()")
            if mat_str.isupper() and len(mat_str) > 1 and "." not in mat_str:
                s, e = m.span()
                allcaps_spans.append(Span(start=s, end=e))

        allcaps_spans = Span.accumulate(allcaps_spans, post_str)
        return allcaps_spans

    def get_posts(self, post_cell, path, ignore_dict, table_role):
        def make_ascii(officer_cell, unicode_dict):
            assert not officer_cell.label_spans
            not_found = []
            for text, word in officer_cell.iter_word_text():
                if not word.text.isascii():
                    u_text = word.text
                    if u_text in unicode_dict:
                        a_text = unicode_dict[u_text]
                        # self.lgr.debug(f'UnicodeFixed: {u_text}->{a_text}')
                        assert a_text is not None, f"incorrect text >{u_text}<"
                        officer_cell.replace_word_text(word, "<all>", a_text)
                    else:
                        sys.stderr.write(f"Unicode: >{u_text}<\n")
                        not_found.append(word.text)
                        pass
                    # self.lgr.info(f'unicode text not found: {u_text}\n')
            return not_found

        posts, errors = [], []
        if not post_cell:
            msg = "empty cell"
            errors.append(TableEmptyBodyCellError(path=path, msg=msg, is_none=True))

        missing_unicodes = make_ascii(post_cell, self.unicode_dict)
        self.add_missing_unicodes(missing_unicodes)  # save the missing unicodes

        ignore_config = TextConfig(rm_labels=["ignore"])
        post_str = post_cell.line_text(ignore_config)

        print(post_str)
        paren_spans = self.get_paren_spans(post_str)
        print(paren_spans)
        post_cell.add_label(paren_spans, "ignore", ignore_config)

        post_str = post_cell.line_text(ignore_config)
        allcaps_spans = self.get_allcaps_spans(post_str)
        post_cell.add_label(allcaps_spans, "ignore", ignore_config)
        if allcaps_spans:
            print(f"Removed ALL Caps Before >{post_str}<")
            print(f"Removed After  >{post_cell.line_text(ignore_config)}")

        post_cell.merge_words(self.vocab, ignore_config)
        post_cell.correct_words(self.vocab, ignore_config)
        post_cell.label_regex([r"[.,;\']", "in the"], "ignore", ignore_config)

        post_str, hier_span_groups = post_cell.line_text(ignore_config), []
        # replacing double space
        post_str = post_str.replace("  ", " ")
        self.lgr.debug(f"{post_str}")

        dept_sgs = self.hierarchy_dict["dept"].find_match(post_str, self.match_options)
        self.lgr.debug(f"dept: {Hierarchy.to_str(dept_sgs)}")

        b_post_str = SpanGroup.blank_text(dept_sgs, post_str)
        role_sgs = self.hierarchy_dict["role"].find_match(b_post_str, self.match_options)

        self.lgr.debug(f"role: {Hierarchy.to_str(role_sgs)}")

        hier_span_groups = dept_sgs + role_sgs
        hier_span_groups = sorted(hier_span_groups, key=op.attrgetter("min_start"))

        table_role_sgs = self.hierarchy_dict["role"].find_match(table_role, self.match_options)

        role_sg = None
        for span_group in hier_span_groups:
            if span_group.root == "__department__":
                dept_sg = span_group
                if not role_sg:
                    self.lgr.debug(f'*SETTING {path} table:{table_role}|{post_str}')
                    role_sg = table_role_sgs[0]
                elif role_sg.leaf != table_role_sgs[0].leaf and 'Prime Minister' not in role_sg.leaf:
                    print(f'*MISMATCH {post_str} table: {table_role_sgs[0].leaf} role: {role_sg.leaf}')

                posts.append(Post.build(post_cell.words, post_str, dept_sg, role_sg))
            else:
                role_sg = span_group

        all_spans = list(chain(*[sg.spans for sg in hier_span_groups]))

        u_texts = Span.unmatched_texts(all_spans, post_str)

        u_texts = [t.lower() for ts in u_texts for t in ts.strip().split()]
        u_texts = [t.translate(self.punct_tbl).strip() for t in u_texts]
        u_texts = [t for t in u_texts if t]
        u_texts = [t for t in u_texts if t not in self.ignore_unmatched]
        u_texts = [t for t in u_texts if not t.isdigit()]

        if u_texts and path not in ignore_dict.get("UnmatchedTextsError", []):
            print(path, u_texts, ignore_dict.get("UnmatchedTextsError", []))
            self.unmatched_ctr.update(u_texts)
            post_idx_str = ' '.join(f'{w.word_idx}:{w.text}' for w in post_cell.words)
            errors.append(UnmatchedTextsError.build(path, u_texts, post_idx_str))
        return posts, errors

    def build_detail(self, row, path, doc_verb, detail_idx, ignore_dict, table_role):
        errors = []
        if len(row.cells) != 3:
            msg = "Expected: 3 columns Actual: {len(row.cells)}"
            errors.append(TableMismatchColsError(path, msg))

        officer_cell = row.cells[1] if len(row.cells) > 1 else None
        officer, officer_errors = self.get_officer(officer_cell, f"{path}.ce1")

        post_cell = row.cells[2] if len(row.cells) > 2 else ""
        posts, post_errors = self.get_posts(post_cell, f"{path}.ce2", ignore_dict, table_role)

        c, r, a = [], [], []
        if doc_verb == "continues":
            c = posts
        elif doc_verb == "relinquishes":
            r = posts
        elif doc_verb == "assumes":
            a = posts

        d = OrderDetail.build(
            row.words,
            [row.words],
            officer,
            detail_idx,
            continues=c,
            relinquishes=r,
            assumes=a,
        )
        all_errors = officer_errors + post_errors
        print("--------")
        print(d.to_str())
        if all_errors:
            print("Errors:")
            print(f"  {[str(e) for e in all_errors]}")

        self.lgr.debug("--------")
        self.lgr.debug(d.to_str())

        return d, all_errors

    def get_order_date(self, doc):
        od_labels = doc.pages[0].word_labels.get("ORDERDATEPLACE", {})
        # import pdb
        # pdb.set_trace()

        if not od_labels:
            errors = []
            path = "pa0.word_labels.ORDERDATEPLACE"
            msg = f"{doc.pdf_name} text: EMPTY"
            errors.append(OrderDateNotFoundErrror(path=path, msg=msg))
            return None, errors

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

        if not result_dt and date_text:
            dt, err_msg = find_date(date_text)
            if dt and (not err_msg):
                result_dt = dt

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

    def iter_rows(self, doc):
        detail_idx = 0
        for (page_idx, page) in enumerate(doc.pages):
            for (table_idx, table) in enumerate(page.tables):
                for (row_idx, row) in enumerate(table.body_rows):
                    yield page_idx, table_idx, row_idx, row, detail_idx
                    detail_idx += 1

    def get_verb(self, doc, page_idx):
        def has_any_words(queries, page):
            for query in queries:
                if any(w for w in page.words if query in w.text):
                    return True
            return False

        if self.verb_pages:
            vps = self.verb_pages.items()
            page_verb = first((v for v, p_idxs in vps if page_idx in p_idxs), 'continues')
            print('*** page_idx: {page_idx} verb:{page_verb}')
            return page_verb
        else:
            page = doc[0]
            if len(doc.pages) > 1 and has_any_words(('relinquish', 'resigned'), page):
                print('*** FOUND RELINQUISHED ***')
                return 'relinquishes'
            elif has_any_words(['assume'], page):
                print(f'*** FOUND ASSUMED *** {doc.pdf_name}')
                return 'assumes'
            else:
                return 'continues'

    def __call__(self, doc):
        def get_title_role(line):
            line = line.replace(' ', '').replace('()', '').strip().lower()
            if not line:
                self.lgr.debug(f"{doc.pdf_name}: Line is empty ")
                return 'Cabinet Minister'
            elif 'cabin' in line or 'inet' in line or 'cablnet' in line:
                return 'Cabinet Minister'
            elif 'depu' in line or 'uty' in line:
                return 'Deputy Minister'
            elif (
                'indep' in line
                or 'indefenlen' in line
                or 'indbpendentcharge' in line
                or 'ineenjentchage' in line
                or 'indefendentcharge' in line
                or 'indeeerdentcharge' in line
            ):
                return 'Minister of State (Independent Charge)'
            else:
                self.lgr.debug(f"MINISTER OF STATE** {line}")
                return 'Minister of State'

        self.add_log_handler(doc)
        self.lgr.info(f"table_order_builder: {doc.pdf_name}")

        doc.add_extra_field("order", ("obj", "orgpedia.extracts.orgpedia", "Order"))

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)

        old_verb_pages = self.verb_pages
        self.verb_pages = doc_config.get("verb_pages", {})

        edits = doc_config.get("edits", [])
        if edits:
            print(f"Edited document: {doc.pdf_name}")
            doc.edit(edits)

        ignore_dict = doc_config.get("ignores", {})
        if ignore_dict:
            print(f"Ignoring {ignore_dict.keys()}")

        order_details, errors = [], []
        order_date, date_errors = self.get_order_date(doc)
        errors.extend(date_errors)

        table_role = 'Cabinet Minister'

        for page_idx, table_idx, row_idx, row, detail_idx in self.iter_rows(doc):
            doc_verb = self.get_verb(doc, page_idx)  ## TODO THIS IS VERY SLOW, do this one for page
            table_title = doc.pages[page_idx].tables[table_idx].title
            if table_title:
                table_title = table_title.raw_text().strip()
                table_role = get_title_role(table_title)

            path = f"pa{page_idx}.ta{table_idx}.ro{row_idx}"
            detail, d_errors = self.build_detail(
                row,
                path,
                doc_verb,
                detail_idx,
                ignore_dict,
                table_role,
            )
            # detail.errors = d_errors
            order_details.append(detail)
            errors.extend(d_errors)

        doc.order = Order.build(doc.pdf_name, order_date, doc.pdffile_path, order_details)
        doc.order.category = "Council of Ministers"

        # self.write_fixes(doc, errors)

        errors = [e for e in errors if not DataError.ignore_error(e, ignore_dict)]

        doc.add_errors(errors)

        self.lgr.info(f"=={doc.pdf_name}.table_order_builder {len(doc.order.details)} {DataError.error_counts(errors)}")
        [self.lgr.info(str(e)) for e in errors]

        self.verb_pages = old_verb_pages
        self.remove_log_handler(doc)
        return doc

    def write_fixes(self, doc, errors):
        # b /Users/mukund/Software/docInt/docint/pipeline/table_order_builder.py:361
        unmatched_errors = [e for e in errors if isinstance(e, UnmatchedTextsError)]

        for u_error in unmatched_errors:
            cell = doc.get_region(u_error.path)
            cell_img_str = cell.page.get_base64_image(cell.shape, height=100)
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
            html_hdr = f'<tr><td colspan="{len(rows[0])}" style="text-align:center;">'
            html = html_hdr + pdf_name + "</td></tr>"
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
        html_str += "\n".join(get_html_rows(k, v) for k, v in self.fixes_dict.items())
        html_str += "\n</table>"
        html_fixes_path.write_text(html_str, encoding="utf-8")

        yml_fixes_path = Path("output") / "fixes.yml"
        yml_str = "\n".join(get_yml_rows(k, v) for k, v in self.fixes_dict.items())
        yml_fixes_path.write_text(yml_str, encoding="utf-8")
