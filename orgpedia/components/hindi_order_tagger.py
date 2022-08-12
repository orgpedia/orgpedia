import logging
import re
import sys
import unicodedata
from pathlib import Path

from more_itertools import first
from polyleven import levenshtein

from ..extracts.orgpedia import Officer, Order, OrderDetail

# from docint.region Region, UnmatchedTextsError
from docint.span import Span

# from docint.hierarchy import Hierarchy, MatchOptions
from docint.util import find_date, load_config, read_config_from_disk
from docint.vision import Vision
from orgpedia.components.pdfpost_parser import PostParser

from orgpedia.components.hindi_order_builder import (
    BadCharsInNameError,
    IncorrectNameError,
    UntranslatableTextsInPostError,
)

# from docint.extracts.orgpedia import IncorrectOrderDateError, OrderDateNotFoundErrror


PassthroughStr = ".,()-/123456789:"
PasssthroughList = [
    "11",
    "10",
    "12",
    "13",
    "14",
    "5th",
    "4th",
    "2nd",
    "7th",
    "10th",
    "3rd",
    "1st",
    "2 nd",
    "SSW",
    "th",
]
DotStr = "०0o.."


@Vision.factory(
    "hindi_order_tagger",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "order_tagger",
        "names_file": "names.yml",
        "posts_file": "posts.yml",
        "cadre": "I.P.S.",
    },
)
class HindiOrderTagger:
    def __init__(self, conf_dir, conf_stub, names_file, posts_file, cadre):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        self.names_file = self.conf_dir / names_file
        self.posts_file = self.conf_dir / posts_file
        self.cadre = cadre
        self.has_relative_name = False

        self.names_dict = read_config_from_disk(self.names_file)["hindi_names"]
        self.salut_dict = {
            "श्रीमती": "Smt",
            "श्री": "Shri",
            "सुश्री": "Miss",
            "डॉ": "Dr.",
            "": "",
        }
        self.name_split_strs = ""

        post_yml_dict = read_config_from_disk(self.posts_file)
        self.post_stubs_dict = post_yml_dict["stubs_translation"]
        self.post_dict = post_yml_dict["translation"]
        self.post_dict.update(post_yml_dict["juri_translation"])
        self.post_leven_cache = {}

        self.post_parser = self.init_post_parser()

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
        self.lgr.addHandler(stream_handler)
        self.file_handler = None
        self.fixes_dict = {}

    def init_post_parser(self):
        hierarchy_files = {
            "dept": "dept.yml",
            "role": "role.yml",
            "juri": "juri.yml",
            "loca": "loca.yml",
            "stat": "stat.yml",
        }
        return PostParser(
            self.conf_dir,
            hierarchy_files,
            "post.noparse.short.yml",
            ["ignore"],
            "postparser",
        )

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

    def fix_hi_name(self, hi_text):
        hi_name = (
            hi_text.strip("()|।-:, 0123456789.$I[")
            .replace("0", ".")
            .replace("०", ".")
            .replace("o", ".")
            .replace(",", ".")
            .replace("-", " ")
        )
        return hi_name

    def test_hi_name(self, hi_name, path, name_words):
        allowed_chars = ".|("
        uname = unicodedata.name
        hi_name = hi_name.replace(" ", "").replace("\n", "")
        if not all([uname(c).startswith("DEVANAGARI") for c in hi_name]):
            all_name_split_strs = "".join(self.name_split_strs)
            bad = [c for c in hi_name if not uname(c).startswith("DEVANAGARI")]
            bad = [c for c in bad if c not in all_name_split_strs]
            bad = [c for c in bad if c not in allowed_chars]
            if bad:
                bad_msgs = []
                for bad_char in bad:
                    bad_word = first([w for w in name_words if bad_char in w.text])
                    bad_msgs.append(
                        f"{bad_char}< >{bad_word.text}[{bad_word.word_idx}]"
                    )
                msg = f'Has these bad chars: >{"< >".join(bad_msgs)}< in >{hi_name}<'
                return [
                    BadCharsInNameError(
                        bad_chars=bad, hi_name=hi_name, path=path, msg=msg
                    )
                ]
        return []

    def get_salut(self, name, merged_saluts=True):
        short = "mr-mrs-dr-smt-shrimati-shri-sh-ms-श्रीमती-श्री-सुश्री-डॉ"

        print(f"SALUT >{name}<")

        saluts = []
        for s in short.split("-"):
            p = f"{s} -{s}. -({s}) -({s}.) -({s}.)-{s}."
            saluts.extend(p.split("-"))

        found_salut = first([s for s in saluts if name.lower().startswith(s)], "")

        if merged_saluts and (not found_salut):
            print("Merged Saluts Found")
            found_salut = "श्री" if name.startswith("श्री") else found_salut

        result = name[: len(found_salut)]
        return result

    def split_hi_name(self, hi_text):
        def has_name_split_str(hi_text):
            return any(nss for nss in self.name_split_strs if nss in hi_text)

        def split_on_name_split_str(hi_text):
            for nss in self.name_split_strs:
                if nss in hi_text:
                    return hi_text.split(nss, 1)
            return hi_text, ""

        hi_full, hi_relative = hi_text.strip(), ""

        if self.has_relative_name and has_name_split_str(hi_text):
            hi_full, hi_relative = split_on_name_split_str(hi_text)
            hi_full, hi_relative = hi_full.strip(), hi_relative.strip()

        hi_salut = self.get_salut(hi_full).strip()
        hi_name = hi_full[len(hi_salut) :].strip()
        return hi_salut, hi_name, hi_relative

    def translate_name(self, hi_salut, hi_name, path):
        def transliterate_name(name_word):
            allowed_chars = "|/।(॥"
            if name_word == ".":
                return name_word
            elif name_word in allowed_chars:
                return ""
            else:
                msg = f"Missing >{name_word}< in >{hi_name}<"
                print(f"= Officer Error: {msg} {path}")
                errors.append(
                    IncorrectNameError(
                        path=path, msg=msg, sub_str=name_word, full_name=hi_name
                    )
                )
                return ""

        if path == "pa0.ta0.ro10.ce1":
            print("Found It")

        salut = self.salut_dict[hi_salut.strip(" .")]
        name_words, errors = [], []  # noqa: F841
        hi_name = hi_result = hi_name.strip(". ")
        for hi_word in re.split("[ .]+", hi_name):
            name_word = self.names_dict.get(hi_word, None)
            if name_word:
                hi_result = hi_result.replace(hi_word, name_word, 1)
            else:
                hi_trans = transliterate_name(hi_word)
                hi_result = hi_result.replace(hi_word, hi_trans, 1)

        print(f"Name: {path} {hi_salut}->{salut} {hi_name}->{hi_result}")
        return salut, hi_result, errors

    def build_officer(self, officer_words, path):
        hi_text = " ".join(w.text for w in officer_words)

        # clean the text befor translation
        hi_text = self.fix_hi_name(hi_text)

        char_errors = self.test_hi_name(hi_text, path, officer_words)
        if char_errors:
            print("= Officer Char Errors")
            print("\n".join(str(e) for e in char_errors))
            return (None, char_errors)

        # split the name in salut, name and relative_name
        hi_salut, hi_name, hi_relative = self.split_hi_name(hi_text)

        # translate salut, name and relative_name
        salut, name, name_errors = self.translate_name(hi_salut, hi_name, path)
        relative_name, rel_errors = "", []
        if hi_relative:
            _, relative_name, rel_errors = self.translate_name("", hi_relative, path)

        full_name = f"{salut} {name}"

        word_idxs = [w.word_idx for w in officer_words]
        page_idx = officer_words[0].page_idx
        officer = Officer(
            words=officer_words,
            word_line=[officer_words],
            word_idxs=word_idxs,
            page_idx_=page_idx,
            word_lines_idxs=[word_idxs],
            salut=salut,
            name=name,
            relative_name=relative_name,
            full_name=full_name,
            orig_lang="hi",
            orig_salut=hi_salut,
            orig_name=hi_name,
            orig_full_name=hi_text,
            cadre=self.cadre,
        )
        if name_errors:
            print("= Officer Name Errors")
            print("\n".join(str(e) for e in char_errors))

        officer.errors = char_errors + name_errors + rel_errors
        return officer, officer.errors

    def find_post_leven_match(self, hi_text, cutoff=1):
        if hi_text in self.post_leven_cache:
            return self.post_leven_cache[hi_text]

        for hi_word, en_word in self.post_dict.items():
            if levenshtein(hi_word, hi_text, cutoff) <= cutoff:
                self.post_leven_cache[hi_text] = en_word
                print(f">{hi_text}< Matches >{hi_word}<")
                return en_word
        return None

    def translate_post(self, hi_post):
        def translate(hi_text):
            en_words, un_words = ([], [])
            for hi_word in hi_text.split():
                hi_word = hi_word.strip(",।();॥\"/'")

                if (hi_word in PasssthroughList) or (hi_word in PassthroughStr):
                    en_words.append(hi_word)
                    continue

                en_word = self.post_dict.get(hi_word, "")
                if en_word:
                    en_words.append(en_word)
                    continue

                hi_word_strip = hi_word.strip(").")
                en_word = self.post_dict.get(hi_word_strip, "")
                if en_word:
                    en_words.append(hi_word.replace(hi_word_strip, en_word))
                    continue

                en_word = self.find_post_leven_match(hi_word_strip)
                if en_word:
                    en_words.append(hi_word.replace(hi_word_strip, en_word))
                else:
                    un_words.append(hi_word)

            return en_words, un_words

        orig_hi_post = hi_post
        matched_span_stubs = []
        # replace stubs
        for (hi_stub, en_stub) in self.post_stubs_dict.items():
            if hi_stub in hi_post:
                stub_span = Span.find_first(hi_post, hi_stub)
                hi_post = Span.blank_text([stub_span], hi_post)
                matched_span_stubs.append((stub_span, en_stub))
                if hi_stub in hi_post:
                    self.lgr.warning(f"Multiple matches of {hi_stub} in {orig_hi_post}")

        matched_spans = [tup[0] for tup in matched_span_stubs]

        untrans_texts = []
        un_texts, un_spans = Span.unmatched_texts_spans(matched_spans, hi_post)
        for un_text, un_span in zip(un_texts, un_spans):
            en_words, untrans_words = translate(un_text)
            if en_words:
                en_text = " ".join(en_words)
                matched_span_stubs.append((un_span, en_text))
            untrans_texts.extend(untrans_words)
        matched_span_stubs.sort(key=lambda tup: tup[0].start)
        post_str = " ".join(tup[1] for tup in matched_span_stubs)
        return post_str, untrans_texts

    def build_post(self, post_words, path):
        hi_text = " ".join(w.text for w in post_words)
        hi_text = hi_text.replace("\n", " ").replace("|", "").strip("|। ()")
        hi_text = hi_text.replace("०", ".").replace("0", ".").replace("o", ".")

        post_str, untrans_texts = self.translate_post(hi_text)
        if untrans_texts:
            print(
                f"hi:>{hi_text}< en:>UntranslatableTextsInPostError< {path} {','.join(untrans_texts)}"
            )
            msg = f'Untranslatable texts: >{"<, >".join(untrans_texts)}< >{hi_text}<'
            trans_err = UntranslatableTextsInPostError(
                msg=msg, path=path, texts=untrans_texts, post_text=hi_text
            )
            return None, [trans_err]

        print(f"hi:>{hi_text}< en:>{post_str}< {path}")
        post = self.post_parser.parse(post_words, post_str, path)
        post_errors = self.post_parser.test(post, path)
        if post_errors:
            print("= Post Error")
            print("\n".join(str(e) for e in post_errors))

        post.errors = post_errors
        return post, post_errors

    def build_detail(self, conf_detail, page):
        officer_words = [page.words[idx] for idx in conf_detail["officer"]["idxs"]]
        officer_path = f"pa{page.page_idx}.wo{officer_words[0].word_idx}"
        officer, officer_errors = self.build_officer(officer_words, officer_path)
        detail_words = officer.words[:]

        verb_dict = {}
        for verb in ["continues", "relinquishes", "assumes"]:
            for conf_post in conf_detail.get(verb, []):
                post_words = [page.words[idx] for idx in conf_post["idxs"]]
                post_path = f"pa{page.page_idx}.wo{post_words[0].word_idx}"

                post, post_errors = self.build_post(post_words, post_path)
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

        update_order = doc_config.get("update", False)

        if not update_order:
            assert not hasattr(doc, "order")
            doc.add_extra_field("order", ("obj", "orgpedia.extracts.orgpedia", "Order"))
            order_date, date_errors = self.get_order_date(doc)
        else:
            assert hasattr(doc, "order")

        details = []
        for conf_detail in conf_order.get("details", []):
            page = doc.pages[conf_detail["page_idx"]]

            if update_order:
                detail_idx = conf_detail["detail_idx"]
                if doc.order.details:
                    order_detail = doc.order.details[detail_idx]
                    updated_detail = self.update_detail(conf_detail, order_detail, page)
                    doc.order.details[detail_idx] = updated_detail
                else:
                    new_detail = self.build_detail(conf_detail, page)
                    details.append(new_detail)
            else:
                detail = self.build_detail(conf_detail, page)
                details.append(detail)

        if not update_order:
            doc.order = Order.build(doc.pdf_name, order_date, doc.pdffile_path, details)

        if update_order and not doc.order.details:
            doc.order.details = details

        if "order_date" in conf_order:
            order_date, _ = find_date(conf_order["order_date"])
            self.lgr.info(f"order_tagger: Date: {order_date}")
            doc.order.date = order_date

        self.lgr.info(f"=={doc.pdf_name}.order_tagger {len(doc.order.details)}")
        self.remove_log_handler(doc)
        return doc


# b /Users/mukund/Software/docInt/docint/pipeline/order_tagger.py:154
