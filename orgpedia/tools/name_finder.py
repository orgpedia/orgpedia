import string

from docint.span import Span
from docint.util import get_model_path


class NameFinder:
    def __init__(self, model_name, model_dir):
        from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline

        ner_model_dir = get_model_path(model_name, model_dir)
        tokenizer = AutoTokenizer.from_pretrained(ner_model_dir)
        model = AutoModelForTokenClassification.from_pretrained(ner_model_dir)

        self.nlp = pipeline("ner", model=model, tokenizer=tokenizer)

    def find(self, text):
        def expand_span(s):
            last_char = text[s.end].strip(string.punctuation).strip()
            if last_char:
                old_name = s.span_str(text)
                s.end = text.index(" ", s.end)
                new_name = s.span_str(text)
                print(f"NameExpansion: {old_name}->{new_name}")
            return s

        def to_span(ner_result):
            r = ner_result
            start = 0 if r["start"] < 25 else r["start"]  # gobble up all chars
            end = r["end"]
            return Span(start=start, end=end)

        ner_results = self.nlp(text)
        name_spans = [to_span(r) for r in ner_results if r["entity"].endswith("-PER")]
        name_spans = Span.accumulate(name_spans, text=text, ignore_chars=" .,")
        # print(f'{text}')
        # print("\t" + ",".join(f'>{text[s.slice()]}<' for s in name_spans))

        return name_spans
