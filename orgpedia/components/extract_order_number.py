import datetime
import json
import os
from pathlib import Path
from typing import List, Union

from docint.region import Region

# from ..util import get_full_path, get_model_path, is_readable_nonempty, is_repo_path
from docint.vision import Vision
from more_itertools import flatten

MarathiNums = "१२३४५६७८९०.() "


class OrderNumber(Region):
    orig_str: str
    order_type: str
    order_number: str
    order_date: Union[datetime.date, None]
    line_number: int


class OrderNumberParser:
    def __init__(self):
        self.skipped_lines = []
        pass

    def split_line(self, order_number_str):
        replacements = [
            ('क्रमांकः', 'क्रमांक :'),
            (' क्र. ', ' क्र. :'),
            ('क्रमांक- ', 'क्रमांक- :'),
            ('क्रमांक ', 'क्रमांक :'),
            ('क्रं.', 'क्रं.:'),
            ('क्रमांक-', 'क्रमांक-:'),
            ('क्र.- ', 'क्र.- :'),
            ('क्र- ', 'क्र- :'),
            ('क्र.', 'क्र.:'),
        ]

        date_variations = [', दिनांक', ',दिनांक', ' दिनांक', ', दि.', ',दि.', ' दि.']
        order_line, date_line = order_number_str, ''
        for d in date_variations:
            if d in order_number_str:
                order_line, date_line = order_number_str.split(d, 1)
                break

        if order_line.count(':') >= 1:
            return order_line, None

        for old, new in replacements:
            order_line = order_line.replace(old, new, 1)
            if order_line.count(':') == 1:
                break
        return order_line, date_line

    def clean_order_number(self, order_number):
        order_number = order_number.strip('- , ')
        order_number = order_number.replace(' -', '-').replace('- ', '-')
        order_number = order_number.replace(' /', '/').replace('/ ', '/')
        return order_number

    def parse(self, order_number_str):
        if '/' not in order_number_str:
            self.skipped_count += 1
            return None, None, None

        order_line, date_line = self.split_line(order_number_str)

        # todo parse date_line

        if order_line.index(':') > order_line.index('/'):
            self.skipped_count += 1
            return None, None, None

        if len(order_line) > 120:
            self.skipped_count += 1
            return None, None, None

        order_type, order_number = order_line.split(':', 1)
        order_number = self.clean_order_number(order_number)
        return order_type, order_number, None




@Vision.factory(
    "extract_order_number",
    default_config={
        "stub": "extract_order_number",
    },
)
class ExtractOrderNumber:
    def __init__(self, stub):
        self.stub = stub

    def __call__(self, doc):
        def is_center_aligned(line):
            padding = 0.1
            if not line:
                return False
            return (l_xmin + padding) < line.xmin < line.xmax < (l_xmax - padding)

        doc.add_extra_field("order_number", ("obj", __name__, "OrderNumber"))
        # doc.add_extra_field("order_number_referenced", ("list",__name__, OrderNumber))

        parser = OrderNumberParser()

        doc.order_number, line = None, None
        line_str, order_type, order_number, order_date = None, None, None, None
        order_line_number = None

        if len(doc.pages) > 1 and len(doc.pages[1].paras) > 0:
            line = doc.pages[1].lines[0]
            line_str = line.raw_text().strip()
            order_type, order_number, order_date = parser.parse(line_str)
            order_line_number = 1
        else:
            page = doc.pages[0]
            l_xmin = min((w.xmin for w in page.words), default=0.0)
            l_xmax = max((w.xmax for w in page.words), default=1.0)

            for (idx, line) in ((i, l) for i, l in enumerate(page.lines) if is_center_aligned(l)):
                #print('\t' + line.raw_text())
                if line.raw_text().count('/') >= 1:
                    line_str = line.raw_text().strip()
                    order_type, order_number, order_date = parser.parse(line_str)
                    print(f'**FirstPage: {idx}: {order_number}')
                    order_line_number = idx + 1
                    break

        if order_number is not None:
            print(f'extract_order_number:{doc.pdf_name}: {order_number}')
            order_number = OrderNumber(
                words=line.words,
                word_idxs=line.word_idxs,
                page_idx=line.page_idx,
                line_number=order_line_number,
                orig_str=line_str,
                order_type=order_type,
                order_number=order_number,
                order_date=order_date)
        else:
            print(f'extract_order_number:{doc.pdf_name}: Unable to find order')


        o_pages = [p for p in doc.pages if len(p.paras) > 0]
        for (page, line, idx) in [(p, ln, i) for p in o_pages for (i, ln) in enumerate(p.lines)]:
            if line.raw_text().count('/') >= 1:
                line_str = line.raw_text().strip()
                print(line_str)
                order_type, order_number, order_date = parser.parse(line_str)
                print(f'Referred: {line_str} -> {order_number}')
        return doc
