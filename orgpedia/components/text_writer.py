from itertools import groupby
from operator import itemgetter
from pathlib import Path

from docint.vision import Vision


@Vision.factory(
    "text_writer",
    default_config={"stub": "textwriter", "output_dir": "output/", "languages": ["en"]},
)
class TextWriter:
    def __init__(self, stub, output_dir, languages):
        self.conf_dir = Path("conf")
        self.stub = stub
        self.output_dir = Path(output_dir)
        self.languages = languages

    def write_table(self, table_info, lang, lines):
        if lang == "mr":
            total_width = max(len(r["mr"]) for r in table_info["rows"])
            lines.append("-" * total_width)
            lines.extend(r["mr"].replace("|", " | ").strip() for r in table_info["rows"])
            lines.append("-" * total_width)
            return

        if not table_info:
            return

        rows = []
        for row in [r[lang] for r in table_info["rows"]]:
            rows.append(row.strip("|").split("|"))

        # full_rows = [r for r in rows if len(rows) > 1]
        num_cols = max(len(r) for r in rows)
        col_widths = [max((len(r[idx]) if idx < len(r) else 0) for r in rows) for idx in range(num_cols)]

        total_width = sum(col_widths) + (3 * (num_cols - 1)) + 2 + 2
        lines.append("-" * total_width)
        for row in rows:
            txt_row = [f"{c:{max(w, 1)}s}" for (c, w) in zip(row, col_widths)]
            lines.append(f"| {' | '.join(txt_row)} |")
        lines.append("-" * total_width)

    def build_table_info(self, table, table_trans):
        def get_val(c, row, idx):
            if c is not None:
                return c
            else:
                cell = row.cells[idx]
                return cell.text_with_break()

        row_infos = []
        for row, row_trans in zip(table.all_rows, table_trans):
            mr_txt = "|".join(c.text_with_break() for c in row.cells)
            en_txt = "|".join(get_val(c, row, idx) for (idx, c) in enumerate(row_trans))
            row_infos.append({"mr": f"|{mr_txt}|", "en": f"|{en_txt}|"})
        return {"rows": row_infos}

    def __call__(self, doc):
        lang_lines_dict = dict((lang, []) for lang in self.languages)
        for page_idx, page in enumerate(doc.pages):

            for (lang, lines) in lang_lines_dict.items():
                lines.append(f"# Page {page_idx+1}")

            para_table_idx_dict = {}
            for (para_idx, table_idxs) in groupby(page.table_para_idxs, key=itemgetter(1)):
                para_table_idx_dict[para_idx] = list(t for (t, p) in table_idxs)

            # add dummy entry, if table is last item
            for para_idx, para in enumerate(page.paras + [None]):
                if para_idx in para_table_idx_dict:
                    for table_idx in para_table_idx_dict[para_idx]:
                        t = table_idx
                        if not page.table_trans:
                            continue

                        table_info = self.build_table_info(page.tables[t], page.table_trans[t])
                        for (lang, lines) in lang_lines_dict.items():
                            self.write_table(table_info, lang, lines)

                if not para:  # dummy entry
                    continue

                for (lang, lines) in lang_lines_dict.items():
                    if lang == "en":
                        if page.para_trans:
                            t = page.para_trans[para_idx]
                            t = para.text_with_break().strip() if t is None else t
                            lines.append(t)
                    else:
                        lines.append(para.text_with_break().strip())
        # end
        for (lang, lines) in lang_lines_dict.items():
            lang_file = self.output_dir / f"{doc.pdf_name}.{lang}.txt"
            lang_file.write_text("\n".join(lines))
        return doc
