import logging
import sys
from pathlib import Path

from docint.data_error import DataError
from docint.util import find_date
from docint.vision import Vision

from ..extracts.orgpedia import Officer, Order, OrderDetail


@Vision.factory(
    "infer_headers",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "mergetables",
        "pre_edit": True,
        "header_dict": {
            "s_no": "num",
            "sl": "num",
            "sno": "num",
            "sh/_smt": "salut",
            "sh/smt": "salut",
            "sh_/_smt": "salut",
            "sh": "salut",
            "": "salut",
            "name_of_officers": "name",
            "name": "name",
            "father_name": "relative_name",
            "father’s_name": "relative_name",
            "fnam": "relative_name",
            "date_of_birth": "birth_date",
            "date_of_brith": "birth_date",
            "dob": "birth_date",
            "home_distt": "home_district",
            "home_district": "home_district",
            "home-distt": "home_district",
            "hdst": "home_district",
            "present_posting": "post_stub",
            "ppst1": "post_stub",
            "place": "loca",
            "ppst2": "loca",
            "place_/_unit": "loca",
            "place/_units": "loca",
            "palce": "loca",
            "date_of_posting": "posting_date",
            "date_of_order": "posting_date",
            "dt_of_posting": "posting_date",
            "dop": "posting_date",
            "caste": "caste",
        },
    },
)
class InferHeaders:
    def __init__(self, conf_dir, conf_stub, pre_edit, header_dict):
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.pre_edit = pre_edit
        self.header_dict = header_dict

        self.lgr = logging.getLogger(f"docint.pipeline.{self.conf_stub}")
        self.lgr.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        self.lgr.addHandler(stream_handler)

    def __call__(self, doc):
        self.lgr.info(f"Processing {doc.pdf_name}")
        try:
            header_row = doc.pages[0].tables[0].header_rows[0]
            cells = header_row.cells
        except Exception as e:  # noqa: F841
            self.lgr.info(f"\t{doc.pdf_name} Empty Header Row")
            assert False
            cells = []

        header_info = []
        for cell in cells:
            cell_text = cell.arranged_text()
            header_text = (
                cell_text.lower()
                .replace(".", "")
                .replace(" ", "_")
                .replace(",", "")
                .replace("'", "")
                .replace("`", "")
                .replace("*", "")
            )
            if header_text not in self.header_dict:
                self.lgr.info(f"\tNot Found: {doc.pdf_name} {cell_text}->{header_text}")
                continue
            header_info.append(self.header_dict[header_text])
        doc.header_info = header_info
        return doc


@Vision.factory(
    "pdforder_builder",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "pdforder",
        "pre_edit": True,
    },
)
class PDFOrderBuilder:
    def __init__(self, conf_dir, conf_stub, pre_edit):
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.pre_edit = pre_edit

        self.lgr = logging.getLogger(f"docint.pipeline.{self.conf_stub}")
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

    def test_officer(self, officer):
        return []

    def test_post(self, post):
        return []

    def build_detail(self, row, officer, post, path, row_idx):
        o_errors = self.test_officer(officer)
        p_errors = self.test_post(post)

        d = OrderDetail.build(row.words, [row.words], officer, row_idx, continues=[post])

        print("--------")
        print(d.to_str())
        return d, o_errors + p_errors

    def build_officer(self, row, header_info, row_idx):
        def extract_in_name(name):
            if name.endswith("(ADHOC)"):
                name = name.replace("(ADHOC)", "").strip()

            if name.endswith("(SCRB)"):
                name = name.replace("(SCRB)", "").strip()

            if name.endswith(" IPS"):
                name, cadre = name[:-3].strip(), "I.P.S."
            else:
                name, cadre = name, "R.P.S."

            return name, cadre

        def clean_text(v):
            return v if not v else v.replace("\n", " ").replace("\xa0", " ").replace("*", "").replace("`", "")

        o_fields = [
            "salut",
            "name",
            "relative_name",
            "birth_date",
            "home_district",
            "posting_date",
        ]
        o_vals = [row.cells[header_info.index(f)].arranged_text() for f in o_fields]
        o_vals = [clean_text(v) for v in o_vals]

        officer_dict = dict(zip(o_fields, o_vals))

        name, cadre = extract_in_name(officer_dict["name"])

        officer_dict["full_name"] = officer_dict["name"] = name
        officer_dict["words"] = row.words
        officer_dict["cadre"] = cadre
        officer_dict["word_idxs"] = [w.word_idx for w in row.words]
        officer_dict["page_idx_"] = row.words[0].page_idx if row.words else None

        # print(officer_dict)

        for date_field in ["birth_date", "posting_date"]:
            if not officer_dict[date_field]:
                del officer_dict[date_field]
            else:
                dt, err = find_date(officer_dict[date_field])
                if err:
                    del officer_dict[date_field]
                else:
                    officer_dict[date_field] = dt
        return Officer(**officer_dict)

    def get_order_date(self, doc):
        first_page = doc.pages[0]

        if not hasattr(first_page, "heading"):
            return None

        heading_str = " ".join([w.text for w in first_page.heading.words])
        order_date, err = find_date(heading_str)
        if err:
            print(f"OrderDateError: {doc.pdf_name} {err} >{heading_str}<")

        print(f"OrderDate: {doc.pdf_name} {order_date} >{heading_str}<")
        return order_date

    def iter_rows(self, doc):
        for page_idx, page in enumerate(doc.pages):
            if len(page.tables) == 0:
                continue
            assert len(page.tables) == 1
            for (row_idx, row) in enumerate(page.tables[0].body_rows):
                yield page, row, row_idx

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"pdf_order_builder: {doc.pdf_name}")
        # doc.add_extra_field("order_details", ("list", __name__, "OrderDetails"))
        doc.add_extra_field("order", ("obj", "orgpedia.extracts.orgpedia", "Order"))

        order_date = self.get_order_date(doc)
        details, errors, detail_idx = [], [], 0
        for page, row, row_idx in self.iter_rows(doc):
            officer = self.build_officer(row, doc.header_info, row_idx)

            path = f"p{page.page_idx}.t0.r{row_idx}"
            post = page.posts[row_idx]

            detail, d_errors = self.build_detail(row, officer, post, path, detail_idx)
            #detail.errors = d_errors
            details.append(detail)
            errors.extend(d_errors)
            detail_idx += 1

        doc.order = Order.build(doc.pdf_name, order_date, doc.pdffile_path, details)
        doc.order.category = "civil_list"
        self.lgr.info(f"==Total:{len(errors)} {DataError.error_counts(errors)}")
        self.remove_log_handler(doc)
        return doc


# b /Users/mukund/Software/docInt/docint/pipeline/pdforder_builder.py:164
