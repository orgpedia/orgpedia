import logging
import string
import sys
from pathlib import Path

from ..data_error import DataError
from ..para import TextConfig
from ..span import Span
from ..util import get_full_path, load_config
from ..vision import Vision

# b ../docint/pipeline/sents_fixer.py:87


class OfficerMisalignedError(DataError):
    pass


class OfficerMultipleError(DataError):
    num_officers: int


@Vision.factory(
    "para_name_finder",
    default_config={
        "item_name": "list_items",
        "conf_dir": "conf",
        "conf_stub": "wordfix",
        "pre_edit": True,
        "rm_labels": ["ignore"],
        "model_dir": "/import/models",
        "ner_model_name": "huggingface:dslim/bert-base-NER",
        "default_names": ["prime minister", "p.m"],
    },
)
class ParaNameFinder:
    def __init__(
        self,
        item_name,
        conf_dir,
        conf_stub,
        pre_edit,
        rm_labels,
        model_dir,
        ner_model_name,
    ):

        self.item_name = item_name
        self.conf_dir = conf_dir
        self.conf_stub = conf_stub
        self.pre_edit = pre_edit
        self.rm_labels = rm_labels
        self.model_dir = get_full_path(model_dir)
        self.ner_model_name = ner_model_name

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

    def mark_manual_words(self, list_item):
        if list_item.get_spans("officer"):
            return 0

        ignore_config = TextConfig(rm_labels=["ignore"], rm_nl=True)
        line_text = list_item.line_text(ignore_config)
        d_names = [n for n in self.default_names if n in line_text.lower()]
        if d_names:
            d_name = pm_word[0]  # noqa
            start = line_text.lower().index(d_name)
            end = start + len(d_name)
            list_item.add_label(Span(start=start, end=end), "officer", ignore_config)
            self.lgr.debug(f"OFFICER {line_text[start:end]}")
            return 1
        else:
            return 0

    def mark_names(self, list_item):
        def expand_span(s):
            last_char = line_text[s.end].strip(string.punctuation).strip()
            if last_char:
                old_name = s.span_str(line_text)
                s.end = line_text.index(" ", s.end)
                new_name = s.span_str(line_text)
                self.lgr.debug(f"NameExpansion: {old_name}->{new_name}")
            return s

        def to_span(ner_result):
            r = ner_result
            start = 0 if r["start"] < 25 else r["start"]  # gobble up all chars
            end = r["end"]
            return Span(start=start, end=end)

        ignore_config = TextConfig(rm_labels=self.rm_labels, rm_nl=True)
        line_text = list_item.line_text(ignore_config)

        ner_results = self.nlp(line_text)

        officer_spans = [to_span(r) for r in ner_results if r["entity"].endswith("-PER")]
        officer_spans = Span.accumulate(officer_spans, text=line_text, ignore_chars=" .,")
        if not officer_spans:
            return
        officer_spans = [expand_span(s) for s in officer_spans]
        officer_spans = Span.accumulate(officer_spans, text=line_text, ignore_chars=" .,")

        print('Officer: {len(officer_spans)}')

        list_item.add_label(officer_spans, "officer", ignore_config)

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"word_fixer: {doc.pdf_name}")

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)
        if doc_config.edits:
            print(f"Edited document: {doc.pdf_name}")
            doc.edit(doc_config.edits)

        for page_idx, page in enumerate(doc.pages):
            # access what to fix through path
            items = getattr(page, self.item_name, [])
            for (list_idx, list_item) in enumerate(items):

                self.mark_names(list_item)

        self.remove_log_handler(doc)
        return doc
