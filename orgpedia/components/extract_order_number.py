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

class OrderTypeParser:
    TypeDict = {'Order': ['आदेश', 'कार्यालयीन आदेश'],
                'Resolution': ['निर्णय', 'निर्णयः', 'निर्णय्र', 'निर्णयःग्रापापु', 'निणर्य', 'निर्णया', 'निर्णय़'
                               'नर्णय', 'नर्णय', 'GR'],
                'Corrigendum': ['शुध्दीपत्रक', 'शुद्धीपत्रक', 'शुध्दिपत्रक', 'शुध्दीपत्र', 'शुध्दीप्रतक', 'शुध्दिपत्र',
                                'शुद्धिपत्र', 'शुद्धिपत्रक', 'शुध्दीत्रक', 'शुध्दीपत्रक्र', 'शुध्दीपत्रकक्र', 'शुध्दीपत्रकः',
                                'शुध्दीत्रक', 'शुद्विपत्रक', 'शुद्धीपत्रकः', 'शुद्धीपत्र', 'शुद्धिपत्रक्र', 'शध्दीपत्रक'],
                'Notification': ['अधिसूचना'],
                'Circular': ['परिपत्रक', 'परपित्रक', 'परीपत्रक', 'परिपत्रकः', 'परित्रक'],
                'Memorandum': ['ज्ञापन', 'ज्ञापनः', 'ज्ञापन_'],
                'Addendum': [],
                'Pagination': ['पृष्ठांकन'],

                'Supplementary Order': ['पूरकनिर्णय', 'निर्णय पुरकपत्र'],
                'Letter': ['पत्र'],

                'Supplementary Letter': ['पूरकपत्र', 'पुरकपत्रक', 'पुरकपत्र', 'परिपत्रक्र', 'पूरकपत्रक',
                                         'पूरकपत्र्र', 'पूरक पत्रक', 'पूरक पत्र', 'पुरक पत्र'],
                'Brief Circular': ['परिपत्रकः संकिर्ण', 'परिपत्रक संकिर्ण', 'परिपत्रक संकीर्ण'],
                'Certificate': ['प्रमाणपत्र'],

                'Memorandum Correction': ['ज्ञापन शुध्दीपत्रक', 'ज्ञापन शुध्दीपत्र'],
                'Resolution Correction': ['निर्णय शुध्दीपत्र', 'निर्णयः दुरूस्ती', 'निर्णय शुध्दिपत्रक', 'निर्णय शुध्दीपत्रक',
                                          'शुद्धीपत्रक निर्णय', 'शुद्धीपत्रक निर्णय', 'शुध्दीपत्रक निर्णय', 'निर्णय शुद्धीपत्रक',
                                          'निर्णय शुद्धिपत्रक', 'निर्णय दुरूस्ती', 'निर्णय ( शुध्दीपत्रक )',
                                          'निर्णय ( शुध्दीपत्र )'],
                'Order Correction': ['आदेश शुध्दीपत्रक'],
                'Circular Correction': ['परिपत्रक शुध्दीपत्रक', 'परिपत्रक शुद्धीपत्रक'],

                'Reference': ['संदर्भ'],
                'Petition': ['याचिका', 'रिट याचिका'],
                'Permenant Certificate': ['स्थायित्व प्रमाणपत्र'],
                }




    Depts = ['महसूल व वन आपत्ती व्यवस्थापन मदत व पुनर्वसन विभाग',
             'वैद्यकीय शिक्षण व औषधी द्रव्ये विभाग', 'सामाजिक न्याय व विशेष सहाय्य विभाग', 'पाणी पुरवठा व स्वच्छता विभाग',
             'उद्योग ऊर्जा व कामगार विभाग', 'उद्योग ऊर्जा व कामगार विभाग', 'उच्च व तंत्र शिक्षण विभाग',
             'उच्च व तंत्रशिक्षण विभाग', 'सार्वजनिक बांधकाम विभाग', 'सार्वजनिक आरोग्य विभाग', 'मृद व जलसंधारण विभाग',
             'विधि व न्याय विभाग', 'महसूल व वन विभाग', 'सामान्य प्रविभाग', 'गृहनिर्माण विभाग', 'अनापुग्रासंविभाग',
             'अनापुग्रासंविभाग', 'गृहनिर्माण विभाग', 'प्रशासकीय विभाग', 'वित्त विभागाचा', 'जलसंपदा विभाग', 'वित्त विभाग',
             'गृह विभाग', 'प्रविभाग', 'सामान्य प्रशासन विभाग']

    def __init__(self):
        self.order_types_set={}
        for (k, vals) in OrderTypeParser.TypeDict.items():
            for v in vals:
                self.order_types_set[v] = k
            self.order_types_set[k.lower()] = k

    def parse(self, order_line):
        order_type = order_line
        order_type = order_type.replace('-', '').replace('.', '').replace(' , ', ' ').replace(' (','')
        order_type = order_type.replace('  ', ' ')
        order_type = order_type.strip(' ,– ')

        for dept in OrderTypeParser.Depts:
            order_type = order_type.replace(dept, '')

        order_type = order_type.replace('क्रमांकः ', '')
        order_type = order_type.replace(' शासन ', '').replace(' क्रमांक', '').replace(' क्र', '')

        order_type = order_type.replace('निर्णयः', 'निर्णय')
        order_type = order_type.replace('Government ', '').replace('No', '')
        if order_type.startswith('शासन'):
            order_type = order_type.replace('शासन', '', 1)

        if order_type.endswith('क्रमांक'):
            order_type = order_type.replace('क्रमांक', '')

        order_type = order_type.replace('No.', '')

        order_type = order_type.strip(' ,– ')
        order_type = order_type.strip('१२३४() ')
        order_type_en = None
        if order_type.lower() in self.order_types_set:
            order_type_en = self.order_types_set[order_type.lower()]

        return order_type_en


class OrderNumber(Region):
    orig_str: str
    order_type: str
    order_number: str
    line_number: int

class OrderNumberParser:
    def split_line(self, order_number_str):
        colon_replacements = [
            ('क्रमांकः', 'क्रमांक :'),
            ('परिपत्रकः', 'परिपत्रक :'),
            ('शुद्धिपत्रकः', 'शुद्धिपत्रक :'),
            (' क्रः ', 'क्र: '),
        ]

        replacements = [
            ('क्रमांक ', 'क्रमांक :'),
            ('क्रमांक- ', 'क्रमांक- :'),
            ('क्रमांक-', 'क्रमांक-:'),
            ('क्रं.', 'क्रं.:'),
            (' क्र. ', ' क्र. :'),
            ('क्र.- ', 'क्र.- :'),
            ('क्र- ', 'क्र- :'),
            ('क्र.', 'क्र.:'),
            (' क्र . ', ' क्र. : '),

        ]

        if order_number_str.isascii():
            return self.split_line_en(order_number_str)

        date_variations = [', दिनांक', ',दिनांक', ' दिनांक', ', दि.', ',दि.', ' दि.']
        order_line, date_line = order_number_str, ''
        for d in date_variations:
            if d in order_number_str:
                order_line, date_line = order_number_str.split(d, 1)
                break

        for old_c, new_c in colon_replacements:
            order_line = order_line.replace(old_c, new_c, 1)
            if order_line.count(':') == 1:
                break

        if order_line.count(':') >= 1:
            return order_line, None

        for old, new in replacements:
            order_line = order_line.replace(old, new, 1)
            if order_line.count(':') == 1:
                break
        return order_line, date_line

    def split_line_en(self, order_number_str):
        order_line = order_number_str
        if order_line.count(':') > 1:
            return order_line, None

        order_line = order_line.replace('No.', 'No. :')
        return order_line, None





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
        self.order_number_parser = OrderNumberParser()
        self.order_type_parser = OrderTypeParser()


    def __call__(self, doc):
        def is_center_aligned(line):
            padding = 0.1
            if not line:
                return False
            #print(f'\t\t\t{page_xmin} {line.xmin} -- {page_xmax} {line.xmax} {line.raw_text()}')
            return (page_xmin + padding) < line.xmin < line.xmax < (page_xmax - padding)

        def is_center_aligned2(line):
            if not line:
                return False
            left_gap, right_gap = (line.xmin - page_xmin), (page_xmax - line.xmax)

            #print(f'\t\t\t{page_xmin} {line.xmin} -- {page_xmax} {line.xmax} {line.raw_text()}')
            return abs(left_gap - right_gap) < 0.1 and left_gap > 0.1

        def has_gap(line, gap):
            return any((w2.xmin - w1.xmax) > gap for (w1, w2) in pairwise(line.words))

        def is_left_aligned(line):
            return line and abs(page_xmin - line.xmin) < 0.1

        def is_right_aligned(line):
            return line and abs(page_xmax - line.xmax) < 0.1

        def is_center_gapped(line):
            return has_gap(line, 0.25) and is_left_aligned(line) and is_right_aligned(line)

        def is_full_line(line, page1):
            page1_xmin = min((w.xmin for w in page1.words), default=0.0)
            page1_xmax = max((w.xmax for w in page1.words), default=1.0)

            return abs(page1_xmin - line.xmin) < 0.05 and abs(page1_xmax - line.xmax) < 0.05

        start_time = time.time()
        doc.add_extra_field("order_number", ("obj", __name__, "OrderNumber"))
        # doc.add_extra_field("order_number_referenced", ("list",__name__, OrderNumber))



        doc.order_number, line = None, None
        line_str, order_type, order_number, order_date = None, None, None, None
        order_line_number = None
        tried_first_page = False

        if len(doc.pages) > 1 and not is_full_line(doc.pages[1].lines[0], doc.pages[1]): # and len(doc.pages[1].paras) > 0:
            line = doc.pages[1].lines[0]
            line_str = line.raw_text().strip()
            order_type, order_number, order_date = self.order_number_parser.parse(line_str)
            order_line_number = 1

        if order_number is None:
            page = doc.pages[0]
            page_xmin = min((w.xmin for w in page.words), default=0.0)
            page_xmax = max((w.xmax for w in page.words), default=1.0)

            to_parse = [(idx, ln) for (idx, ln) in enumerate(page.lines) if ln and (is_center_aligned2(ln) or is_center_gapped(ln) or is_right_aligned(ln) or is_center_aligned(ln))]

            tried_first_page = True
            #print(f'\tFound {len(to_parse)} lines')
            for (idx, line) in to_parse:
                #print(f'\t=={idx} {line.raw_text()}')
                if line.raw_text().count('/') > 1:
                    line_str = line.raw_text().strip()
                    order_type, order_number, order_date = self.order_number_parser.parse(line_str)
                    if order_number:
                        print(f'\t**FirstPage: {idx}: {order_number} =={line_str}')
                        order_line_number = idx + 1
                        break


        if order_number is not None:
            order_type_en = self.order_type_parser.parse(order_type)
            print(f'\textract_order_number:{doc.pdf_name}: >{order_type}< >{order_type_en}< >{order_number}< ')
            doc.order_number = OrderNumber(
                words=line.words,
                word_idxs=line.word_idxs,
                page_idx_=line.page_idx,
                line_number=order_line_number,
                orig_str=line_str,
                order_type=order_type,
                order_number=order_number,
                order_type_en=order_type_en
            )

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

"""
cat order_number2.txt| grep Found | awk -F'>' '{print $NF}' | sed 's/शासन //' | sed 's/ क्रमांक//' | sed 's/क्र.//' | tr -d '<' | tr -d ',' | tr -d '-' | tr -d ' .' | sort | uniq -c | sort -nr | less
"""
