import json
from pathlib import Path
from operator import attrgetter

from docint.region import Region
from docint.para import Para
from docint.vision import Vision
from more_itertools import first


def build_para(page, para_lines):
    word_lines = [[w for w in ln.words] for ln in para_lines if ln]

    words = [w for ws in word_lines for w in ws]

    word_idxs = [w.word_idx for w in words]
    word_lines_idxs = [[w.word_idx for w in ws] for ws in word_lines]

    # para_text = "".join(ln.text_with_break() for ln in para_lines)
    # print(f">{para_text.strip()}<")

    return Para(
        words=words,
        page_idx_=page.page_idx,
        word_lines=word_lines,
        word_idxs=word_idxs,
        word_lines_idxs=word_lines_idxs,
    )


@Vision.factory(
    "para_finder",
    default_config={
        "stub": "parafinder",
        "write_output": True,
        "output_dir": "output/",
    },
)
class ParaFinder:
    def __init__(self, stub, write_output, output_dir):
        self.conf_dir = Path("conf")
        self.stub = stub
        self.write_output = write_output
        self.output_dir = Path(output_dir)

    def __call__(self, doc):
        def has_period(line):
            return (not line) or line.raw_text().strip().endswith(".")

        def in_table(page, line):
            if not page.tables:
                return False

            # if line.words[0].text == '|':
            #     new_line = Region.from_words(line.words[1:])
            # else:
            new_line = line

            return any(t.box.y_subsumes(new_line) for t in page.tables)

        def in_table_idx(page, line):
            # if line.words[0].text == '|':
            #     new_line = Region.from_words(line.words[1:])
            # else:
            new_line = line

            return first((i for (i, t) in enumerate(page.tables) if t.box.y_subsumes(new_line)), None)

        def is_center_aligned(line):
            padding = 0.1
            return (l_xmin + padding) < line.xmin < line.xmax < (l_xmax - padding)

        def is_last_line(line):
            line_str = line.raw_text().strip()
            return "www.maharashtra.gov.in" in line_str

        doc.add_extra_page_field("paras", ("list", "docint.para", "Para"))
        doc.add_extra_page_field("table_para_idxs", ("noparse", "", ""))

        last_line_seen, para_lines = False, []
        for page in doc.pages:
            page.paras = []
            page.tables = [t for t in page.tables if t.words]

            if last_line_seen:
                assert not para_lines
                page.table_para_idxs = []
                continue

            table_para_idxs_set = set()
            l_xmin = min((w.xmin for w in page.words), default=0.0)
            l_xmax = max((w.xmax for w in page.words), default=1.0)

            last_line_ymax = 1.2 # keeping some bufffer

            #page_lines = sorted(page.lines, key=attrgetter('ymin'))

            for (line_idx, line) in enumerate(page.lines):
                #print(f'{line_idx}:', end=" ")
                if (not line) or (not line.arranged_text().strip()) or (line.arranged_text().strip() == "|"):
                    #print("empty line", end=" ")
                    if para_lines:
                        #print("build_para", end=" ")
                        page.paras.append(build_para(page, para_lines))
                        para_lines.clear()

                elif in_table(page, line):
                    #print("in table", end=" ")
                    if para_lines:
                        page.paras.append(build_para(page, para_lines))
                        para_lines.clear()

                    table_idx = in_table_idx(page, line)
                    #print(f'[{line_idx}] ADDING: table_idx: {table_idx}, {len(page.paras)} >{line.arranged_text()[:10]}...<', end=" ")
                    table_para_idxs_set.add((table_idx, len(page.paras)))
                elif has_period(line) or is_center_aligned(line):
                    #print(f"has period|center_aligned >{line.arranged_text()[:15]}...<", end=" ")
                    para_lines.append(line)
                    page.paras.append(build_para(page, para_lines))
                    para_lines.clear()

                elif is_last_line(line):
                    #print(" LAST LINE", end=" ")
                    last_line_seen = True
                    last_line_ymax = line.ymax
                    para_lines.append(line)
                    page.paras.append(build_para(page, para_lines))
                    para_lines.clear()
                    # IMP leave the loop
                    break
                else:
                    #print(f"adding to Para[{len(page.paras)}] >{line.arranged_text()}<", end=" ")
                    para_lines.append(line)

                #print()

            if para_lines:
                page.paras.append(build_para(page, para_lines))
                para_lines.clear()

            # Eliminate tables below the last line
            page.tables = [t for t in page.tables if t.ymax < last_line_ymax]

            page.table_para_idxs = sorted(table_para_idxs_set)
            assert len(page.tables) == len(
                page.table_para_idxs
            ), f"para {doc.pdf_name}:Page_idx: {page.page_idx} {len(page.tables)} <-> {page.table_para_idxs}"

        if self.write_output:
            json_path = self.output_dir / f"{doc.pdf_name}.{self.stub}.json"

            def get_para_info(para, idx):
                return {
                    "page_idx": para.page_idx,
                    "para_idx": idx,
                    "text": para.text_with_break().strip(),
                }

            def get_table_info(table, idx):
                return {
                    "page_idx": table.page_idx,
                    "table_idx": idx,
                    "rows": [r.get_markdown() for r in table.all_rows],
                }

            para_infos = [get_para_info(p, i) for pg in doc.pages for (i, p) in enumerate(pg.paras)]
            table_infos = [get_table_info(t, i) for pg in doc.pages for (i, t) in enumerate(pg.tables)]
            table_para_idxs_infos = [pg.table_para_idxs for pg in doc.pages]

            json_path.write_text(
                json.dumps(
                    {
                        "para_infos": para_infos,
                        "table_infos": table_infos,
                        "table_para_idxs_infos": table_para_idxs_infos,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
        return doc
