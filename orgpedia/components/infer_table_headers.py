from operator import attrgetter
from pathlib import Path
from typing import List

from docint.hierarchy import Hierarchy, MatchOptions
from docint.util import load_config
from docint.vision import Vision
from pydantic import BaseModel


class TableHeaderInfo(BaseModel):
    header_types: List[str]
    src_page_idx: int
    src_table_idx: int

    @classmethod
    def build(cls, header_row, header_types, page_idx, table_idx):
        return TableHeaderInfo(header_types=header_types, src_page_idx=page_idx, src_table_idx=table_idx)

    @property
    def num_columns(self):
        return len(self.header_types)

    def has_type(self, table_type):
        return any(table_type in t for t in self.header_types)

    def get_index(self, header_type):
        idxs = [i for i, t in enumerate(self.header_types) if header_type in t]
        if len(idxs) > 1:
            raise ValueError(f'Multiple types found for {header_type}')
        return idxs[0] if idxs else None

    def get_type(self, idx):
        return self.header_types[idx]


@Vision.factory(
    "infer_table_headers",
    default_config={"conf_dir": "conf", "conf_stub": "infertableheader", "type_file": "type.yml"},
)
class InferTableHeaders:
    def __init__(self, conf_dir, conf_stub, type_file):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = conf_stub
        type_file_path = self.conf_dir / type_file
        self.type_hierarchy = Hierarchy(type_file_path)
        self.match_options = MatchOptions(ignore_case=True, allow_overlap=True)

    def get_header_info(self, header_rows, page_idx, table_idx):
        assert len(header_rows) == 1
        header_types = []
        for cell in header_rows[0].cells:
            type_span_groups = self.type_hierarchy.find_match(cell.raw_text(), self.match_options)
            t_paths = ['->'.join(sg.hierarchy_path) for sg in type_span_groups]
            if len(type_span_groups) > 2:
                type_span_groups.sort(reverse=True, key=attrgetter('sum_span_len'))
                type_span_groups = type_span_groups[:1]

                print(f'*** MULTIPLE {t_paths} ->', end='')
                t_paths = ['->'.join(sg.hierarchy_path) for sg in type_span_groups]
                print(f'{t_paths}')

            if len(set(t_paths)) != 1:
                assert len(type_span_groups) == 1, f'Incorrect Span Groups >{cell.raw_text()}<'
                print(f'***** FIX THIS ** Incorrect Span Groups >{cell.raw_text()} {t_paths} <')
                t_path = ""
            else:
                t_path = t_paths[0]
                print(f'\t{t_path}')
            header_types.append(t_path)

        return TableHeaderInfo.build(header_rows[0], header_types, page_idx, table_idx)

    def __call__(self, doc):
        doc.add_extra_page_field("table_header_infos", ("list", __name__, 'TableHeaderInfo'))
        last_table_header_info = None

        doc_config = load_config(self.conf_dir, doc.pdf_name, self.conf_stub)
        edits = doc_config.get("edits", [])
        if edits:
            print(f"Edited document: {doc.pdf_name}")
            doc.edit(edits)

        for page in doc.pages:
            page.table_header_infos = []
            tables = getattr(page, 'tables', [])
            for (table_idx, table) in enumerate(tables):
                if not table.header_rows:
                    assert last_table_header_info, f'Header not found {page.page_idx} -> {table_idx}'
                    assert (
                        table.num_columns == last_table_header_info.num_columns
                    ), f'Cols page:{page.page_idx}[{table_idx}] {table.num_columns} != {last_table_header_info.num_columns}'
                    page.table_header_infos.append(last_table_header_info)
                else:
                    header_info = self.get_header_info(table.header_rows, page.page_idx, table_idx)
                    page.table_header_infos.append(header_info)
                    last_table_header_info = header_info
            assert len(page.table_header_infos) == len(page.tables)
        return doc
