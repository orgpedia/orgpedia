from docint.span import Span
from docint.util import get_model_path


class EntityFinder:
    def __init__(self, model_name, model_dir):
        from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline

        self.ner_model_dir = get_model_path(model_name, model_dir)
        tokenizer = AutoTokenizer.from_pretrained(self.ner_model_dir)
        model = AutoModelForTokenClassification.from_pretrained(self.ner_model_dir)

        self.nlp = pipeline("ner", model=model, tokenizer=tokenizer)

    def find(self, text, entity, ignore_chars=", ."):
        def to_span(ner_result):
            r = ner_result
            start = 0 if r["start"] < 25 else r["start"]  # gobble up all chars
            end = r["end"]
            return Span(start=start, end=end)

        if entity.lower() not in ('person', 'org', 'officer', 'dept'):
            raise ValueError('Unknown entities requested: {",".join(unk_entities)}')

        ner_results = self.nlp(text)

        end_str = f'-{entity.upper()[:3]}'
        entity_spans = [to_span(r) for r in ner_results if r["entity"].endswith(end_str)]
        entity_spans = Span.accumulate(entity_spans, text=text, ignore_chars=ignore_chars)

        # print(f'{text}')
        # print("\t" + ",".join(f'>{text[s.slice()]}<' for s in entity_spans))
        return entity_spans
