import datetime
import json
import os
import time

from pathlib import Path
from typing import List, Union

from docint.region import Region

# from ..util import get_full_path, get_model_path, is_readable_nonempty, is_repo_path
from docint.vision import Vision
from more_itertools import flatten, pairwise


# def pairwise(iterable):
#     # pairwise('ABCDEFG') --> AB BC CD DE EF FG
#     a, b = tee(iterable)
#     next(b, None)
#     return zip(a, b)

class OrderNumber(Region):
    orig_str: str
    order_type: str
    order_number: str
    line_number: int


class OrderNumberParser:
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
            (' क्र . ', ' क्र. : '),
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
            return None, None, None

        #print(f'\t>>{order_number_str}<<')
        order_line, date_line = self.split_line(order_number_str)

        # todo parse date_line
        if (':' not in order_line) or (order_line.index(':') > order_line.index('/')):
            return None, None, None

        if len(order_line) > 120:
            return None, None, None

        #print(f'\t>>{order_line}<<')
        order_type, order_number = order_line.split(':', 1)
        order_type, order_number = order_type.strip(), self.clean_order_number(order_number)
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
            print(f'\t\t\t{page_xmin} {line.xmin} -- {page_xmax} {line.xmax} {line.raw_text()}')
            return (page_xmin + padding) < line.xmin < line.xmax < (page_xmax - padding)

        def is_center_aligned2(line):
            if not line:
                return False
            left_gap, right_gap = (line.xmin - page_xmin), (page_xmax - line.xmax)
            return abs(left_gap - right_gap) < 0.1 and left_gap > 0.1

        def has_gap(line, gap):
            return any((w2.xmin - w1.xmax) > gap for (w1, w2) in pairwise(line.words))

        def is_left_aligned(line):
            return line and abs(page_xmin - line.xmin) < 0.1

        def is_right_aligned(line):
            return line and abs(page_xmax - line.xmax) < 0.1

        def is_center_gapped(line):
            return has_gap(line, 0.25) and is_left_aligned(line) and is_right_aligned(line)

        start_time = time.time()        
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
            page_xmin = min((w.xmin for w in page.words), default=0.0)
            page_xmax = max((w.xmax for w in page.words), default=1.0)

            to_parse = [(idx, ln) for (idx, ln) in enumerate(page.lines) if ln and (is_center_aligned2(ln) or is_center_gapped(ln) or is_right_aligned(ln))]

            #print(f'\tFound {len(to_parse)} lines')
            for (idx, line) in to_parse:
                #print(f'\t=={idx} {line.raw_text()}')                
                if line.raw_text().count('/') >= 1:
                    line_str = line.raw_text().strip()
                    order_type, order_number, order_date = parser.parse(line_str)
                    if order_number:
                        print(f'\t**FirstPage: {idx}: {order_number} =={line_str}')
                        order_line_number = idx + 1
                        break

        if order_number is not None:
            print(f'\textract_order_number:{doc.pdf_name}: >{order_type}< >{order_number}<')
            doc.order_number = OrderNumber(
                words=line.words,
                word_idxs=line.word_idxs,
                page_idx_=line.page_idx,
                line_number=order_line_number,
                orig_str=line_str,
                order_type=order_type,
                order_number=order_number)

        else:
            print(f'\textract_order_number:{doc.pdf_name}: Unable to find order')


        # o_pages = [p for p in doc.pages if len(p.paras) > 0]
        # for (page, line, idx) in [(p, ln, i) for p in o_pages for (i, ln) in enumerate(p.lines)]:
        #     if line.raw_text().count('/') >= 1:
        #         line_str = line.raw_text().strip()
        #         #print(line_str)
        #         order_type, order_number, order_date = parser.parse(line_str)
        #         #print(f'Referred: {line_str} -> {order_number}')

        end_time = time.time()
        print(f'\telapsed_time:{doc.pdf_name}: {(end_time - start_time):.6f}')
        return doc
