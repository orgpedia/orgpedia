import logging
import sys
from pathlib import Path
import json

from docint.data_error import DataError
from docint.vision import Vision


# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34


class OfficerIDDiffError(DataError):
    pass


class PostIDDiffError(DataError):
    pass


@Vision.factory(
    "details_differ",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "orderdetails",
        "mode": "diff",
        "output_dir": "output",
    },
)
class DetailsDiffer:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        mode,
        output_dir,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = Path(conf_stub)

        assert mode in ('overwrite', 'diff')
        self.mode = mode
        self.output_dir = Path(output_dir)

        self.lgr = logging.getLogger(__name__)
        self.lgr.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.DEBUG)
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

    def get_detail_line(self, d):
        def get_post(post):
            dept = post["dept_hpath"][1] if len(post["dept_hpath"]) > 1 else ""
            role = post["role_hpath"][-1] if len(post["role_hpath"]) else ""
            return [dept, role]

        new_d = {"detail_idx": d["detail_idx"]}
        new_d['officer_id'] = d['officer'].get('officer_id', '')
        for v in ("continues", "relinquishes", "assumes"):
            new_d[v] = [get_post(p) for p in d.get(v, [])]
        return new_d

    def diff_posts(self, verb, ref_posts, new_posts, path):
        diffs, errors = [], []
        for idx, (ref, new) in enumerate(zip(ref_posts, new_posts)):
            if ref == new:
                continue
            d = f'{ref[0]}->{new[0]}' if ref[0] != new[0] else ''
            r = f'{ref[1]}->{new[1]}' if ref[1] != new[1] else ''
            msg = f'[{idx}]{d}{r}'
            diffs.append(msg)
            errors.append(PostIDDiffError(path=path, msg=f'{verb}: {msg}'))
        return diffs, errors

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"details_differ: {doc.pdf_name}")

        details_path = self.output_dir / f'{doc.pdf_name}.orderdetails.json'

        new_dt = str(doc.order.date)
        new_lines = [self.get_detail_line(d.dict()) for d in doc.order.details]

        errors = []
        if self.mode == "overwrite" or not details_path.exists():
            first_line = f'{{"date": "{new_dt}", "details": ['
            details_lines = [json.dumps(new_d) for new_d in new_lines]
            details_str = ",\n".join(details_lines)
            details_path.write_text(first_line + "\n" + details_str + "\n]}")
        else:
            j_order = json.loads(details_path.read_text())

            ref_lines = j_order['details']  # [self.get_detail_line(d) for d in j_order['details']]
            for detail_idx, (ref, new) in enumerate(zip(ref_lines, new_lines)):
                diffs = []
                path = f'de{detail_idx}'
                if ref['officer_id'] != new['officer_id']:
                    msg = f'{ref["officer_id"]}->{new["officer_id"]}'
                    diffs.append(f'O: {msg}')
                    errors.append(OfficerIDDiffError(path=path, msg=msg))

                for v in ("continues", "relinquishes", "assumes"):
                    verb_diffs, post_errors = self.diff_posts(v, ref[v], new[v], path)
                    diffs.extend(verb_diffs)
                    errors.extend(post_errors)

                if diffs:
                    print(f'{doc.pdf_name}[{detail_idx}]: {"|".join(diffs)}')

        doc.add_errors(errors)
        self.lgr.info(f"=={doc.pdf_name}.detail_differ {len(doc.order.details)} {DataError.error_counts(errors)}")
        [self.lgr.info(str(e)) for e in errors]
        self.remove_log_handler(doc)
        return doc
