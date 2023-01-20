import datetime
import hashlib
import json
import logging
from pathlib import Path
import urllib
import sys


from pydantic import BaseModel

from docint.util import read_config_from_disk
from docint.vision import Vision


# b /Users/mukund/Software/docInt/docint/pipeline/id_assigner.py:34

BLOCKSIZE = 2**10


class DocMeta(BaseModel):
    url: str
    download_time: datetime.datetime
    archive_url: str
    archive_time: datetime.datetime
    sha: str
    sha_matched: bool

    @classmethod
    def get_relevant_objects(cls, officerIDInfos, path, shape):
        _, path_detail_idx = path.split(".", 1)
        path_detail_idx = int(path_detail_idx[2:])

        officerIDInfo = officerIDInfos[path_detail_idx]
        return [officerIDInfo]

    def get_html_json(self):
        return f'{{ID: {self.officer_id}, name: {self.name}, method: {self.method} }}'

    def get_svg_info(self):
        return {'idxs': {'person': [w.word_idx for w in self.words]}}


def get_sha(file):
    md5 = hashlib.md5()
    while True:
        buf = file.read(BLOCKSIZE)
        if not buf:
            break
        md5.update(buf)
    return md5.hexdigest()


@Vision.factory(
    "archive_checker",
    default_config={
        "conf_dir": "conf",
        "conf_stub": "archive_checker",
        "output_dir": "output",
        "urls_file": "urls.yml",
    },
)
class ArchiveChecker:
    def __init__(
        self,
        conf_dir,
        conf_stub,
        output_dir,
        urls_file,
    ):
        self.conf_dir = Path(conf_dir)
        self.conf_stub = Path(conf_stub)
        self.output_dir = Path(output_dir)
        self.urls_file = self.conf_dir / Path(urls_file)

        yml_dict = read_config_from_disk(self.urls_file)
        self.urls_dict = {}
        for (pdf_path, pdf_info) in yml_dict.items():
            pdf_path = Path(pdf_path)
            pdf_name = pdf_info.get('name', pdf_path.name)
            self.urls_dict[pdf_name] = (pdf_info['url'], pdf_info['download_time'], pdf_path)

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

    def __call__(self, doc):
        self.add_log_handler(doc)
        self.lgr.info(f"archive_checker: {doc.pdf_name}")

        from waybackpy import WaybackMachineCDXServerAPI

        doc.add_extra_field("archive_info", ("obj", __name__, "DocMeta"))

        archive_info_path = self.output_dir / f'{doc.pdf_name}.archive_info.json'
        if archive_info_path.exists():
            archive_info_dict = json.loads(archive_info_path.read_text())
            doc.archive_info = DocMeta(**archive_info_dict)
            return doc

        (url, downloaded_time, pdf_path) = self.urls_dict[doc.pdf_name]
        cdx_api = WaybackMachineCDXServerAPI(url, 'orgpedia user agent')
        newest = cdx_api.newest()

        # URL: https://web.archive.org/web/20220709041837id_/https://cabsec...'
        # note the id_
        full_url = newest.archive_url
        id_pos = full_url.index(url) - 1
        content_url = full_url[:id_pos] + 'id_' + full_url[id_pos:]

        with urllib.request.urlopen(content_url) as f:
            url_sha = get_sha(f)

        with open(doc.pdf_path, 'rb') as f:
            pdf_sha = get_sha(f)

        sha_matched = url_sha == pdf_sha
        doc.archive_info = DocMeta(
            url=url,
            download_time=downloaded_time,
            archive_url=newest.archive_url,
            archive_time=newest.datetime_timestamp,
            sha=pdf_sha,
            sha_matched=sha_matched,
        )
        archive_info_path.write_text(doc.archive_info.json(indent=2))
        return doc
