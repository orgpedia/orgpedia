import json
from pathlib import Path

from docint.vision import Vision
from more_itertools import flatten

MarathiNums = "१२३४५६७८९०.() "


def is_number(cell):
    cell = cell.strip(".) ")
    return all(c in MarathiNums for c in cell)


EnglishNums = "1234567890.() "
MarthiEnglishNumDict = dict((m, e) for (m, e) in zip(MarathiNums, EnglishNums))


def trans_number(cell):
    return "".join(MarthiEnglishNumDict[c] for c in cell)


def get_row_texts(row):
    return [c.text_with_break() for c in row.cells]


BatchSize = 100


@Vision.factory(
    "doc_translator",
    default_config={
        "stub": "doctranslator",
        "translations_file": "doc_translations.json",
        "write_output": True,
        "output_dir": "output",
    },
)
class DocTranslator:
    def __init__(self, stub, translations_file, write_output, output_dir):
        from google.cloud import translate_v2 as translate

        self.conf_dir = Path("conf")
        self.translations_file = self.conf_dir / translations_file
        self.stub = stub
        self.write_output = write_output
        self.output_dir = Path(output_dir)

        self.indic2en_trans = self.load_translations()
        self.gcp_client = translate.Client()

    def load_translations(self):
        indic2en_trans = {}
        if self.translations_file.exists():
            json_list = json.loads(self.translations_file.read_text())
            for trans_dict in json_list:
                m, e = trans_dict["mr"], trans_dict["en"]
                indic2en_trans[m] = e
        return indic2en_trans

    def save_translations(self):
        save_trans = sorted(
            [{"mr": k, "en": v} for (k, v) in self.indic2en_trans.items()],
            key=lambda d: d["mr"],
        )
        self.translations_file.write_text(json.dumps(save_trans, indent=2, ensure_ascii=False))

    def gcp_translate(self, texts_set):
        if not texts_set:
            return []

        texts = [t for t in texts_set if t not in self.indic2en_trans]
        for start in range(0, len(texts), BatchSize):
            texts_batch = texts[start : start + BatchSize]

            trans_dicts = self.gcp_client.translate(texts_batch, source_language="mr", target_language="en")

            trans = [t["translatedText"] for t in trans_dicts]

            for (txt, trn) in zip(texts_batch, trans):
                self.indic2en_trans[txt] = trn

            self.save_translations()

    def get_text_trans(self, text):
        return None if text.isascii() else self.indic2en_trans[text]

    def get_table_trans(self, table):
        table_trans = []
        rows_texts = [get_row_texts(row) for row in table.all_rows]
        for row_texts in rows_texts:
            table_trans.append([trans_number(c) if is_number(c) else self.get_text_trans(c) for c in row_texts])
        return table_trans

    def __call__(self, doc):
        doc.add_extra_page_field("para_trans", ("noparse", "", ""))
        doc.add_extra_page_field("table_trans", ("noparse", "", ""))

        para_texts, cell_texts = [], []
        for page in doc.pages:
            pts = [p.text_with_break().strip() for p in page.paras]
            para_texts += [pt for pt in pts if not pt.isascii()]

            for row in [r for t in page.tables for r in t.all_rows]:
                cell_texts += [c for c in get_row_texts(row) if not c.isascii() and not is_number(c)]

        para_texts, cell_texts = set(para_texts), set(cell_texts)
        self.gcp_translate(para_texts)
        self.gcp_translate(cell_texts)

        for page in doc.pages:
            page.para_trans = [self.get_text_trans(p.text_with_break().strip()) for p in page.paras]
            page.table_trans = [self.get_table_trans(t) for t in page.tables]

        if self.write_output:
            json_path = self.output_dir / f"{doc.pdf_name}.{self.stub}.json"

            def get_para_infos(page):
                para_infos = []
                for para_idx, (para, trans) in enumerate(zip(page.paras, page.para_trans)):
                    para_infos.append(
                        {
                            "page_idx": page.page_idx,
                            "para_idx": para_idx,
                            "mr": para.text_with_break().strip(),
                            "en": trans,
                        }
                    )
                return para_infos

            def get_table_infos(page):
                table_infos = []
                for table_idx, table in enumerate(page.tables):
                    row_trans = page.table_trans[table_idx]
                    for row_idx, (row, row_texts) in enumerate(zip(table.all_rows, row_trans)):
                        cell_texts = [{"mr": c.text_with_break(), "en": ct} for (c, ct) in zip(row, row_texts)]
                    table_infos.append(
                        {
                            "page_idx": page.page_idx,
                            "table_idx": table_idx,
                            "row_idx": row_idx,
                            "cells": cell_texts,
                        }
                    )
                return table_infos

            para_infos = list(flatten(get_para_infos(pg) for pg in doc.pages))
            table_infos = list(flatten(get_table_infos(pg) for pg in doc.pages))
            json_path.write_text(
                json.dumps(
                    {"para_infos": para_infos, "table_infos": table_infos},
                    indent=2,
                    ensure_ascii=False,
                )
            )

        return doc
