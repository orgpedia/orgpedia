from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Optional

import pytest
from pydantic import BaseModel

from ledger import (
    DuplicateRecordKeyError,
    Ledger,
    PartitionMismatchError,
    QuerySyntaxError,
    RecordNotFoundError,
    ValidationError,
)


class UrlInfo(BaseModel):
    record_key: str
    doc_date: dt.date
    department_code: Optional[str] = None


class UploadInfo(BaseModel):
    record_key: str
    doc_date: dt.date
    path: Optional[str] = None


class PdfInfo(BaseModel):
    record_key: str
    doc_date: dt.date
    language: Optional[str] = None


@pytest.fixture
def source_map(tmp_path: Path):
    return {
        "urlinfo": {"directory": tmp_path / "urlinfos", "model": UrlInfo},
        "uploadinfo": {"directory": tmp_path / "uploadinfos", "model": UploadInfo},
        "pdfinfo": {"directory": tmp_path / "pdfinfos", "model": PdfInfo},
    }


def make_record(ledger: Ledger, key: str, iso_date: str, dept: str = "FIN", lang: str = "en"):
    doc_date = dt.date.fromisoformat(iso_date)
    partition = str(doc_date.year)
    return ledger.RecordModel(
        record_key=key,
        doc_date=doc_date,
        partition=partition,
        urlinfo=UrlInfo(record_key=key, doc_date=doc_date, department_code=dept),
        uploadinfo=UploadInfo(record_key=key, doc_date=doc_date, path=f"/tmp/{key}.pdf"),
        pdfinfo=PdfInfo(record_key=key, doc_date=doc_date, language=lang),
    )


def test_json_backend_crud_and_filters(source_map) -> None:
    ledger = Ledger(sources=source_map, backend="json", auto_load=True)

    ledger.insert(make_record(ledger, "rk-1", "2025-01-10", dept="FIN", lang="en"))
    ledger.insert(make_record(ledger, "rk-2", "2025-01-20", dept="HR", lang="en"))
    ledger.insert(make_record(ledger, "rk-3", "2024-12-31", dept="FIN", lang="hi"))

    assert ledger.exists("rk-1") is True
    got = ledger.get("rk-1")
    assert got is not None
    assert got.partition == "2025"
    assert got.urlinfo.department_code == "FIN"

    ledger.update({"record_key": "rk-1", "urlinfo": {"department_code": "LEGAL"}})
    assert ledger.get("rk-1").urlinfo.department_code == "LEGAL"

    rows = list(
        ledger.records(
            year=2025,
            doc_date_between=("2025-01-01", "2025-12-31"),
            where=[("urlinfo.department_code", "eq", "HR")],
            order_by=["doc_date:asc", "record_key:asc"],
        )
    )
    assert [r.record_key for r in rows] == ["rk-2"]

    # "sources." prefix remains supported for compatibility.
    rows2 = list(ledger.records(where=[("sources.pdfinfo.language", "eq", "en")], order_by="record_key:asc"))
    assert [r.record_key for r in rows2] == ["rk-1", "rk-2"]

    assert ledger.count(where=[("pdfinfo.language", "eq", "en")]) == 2
    first = ledger.first(order_by="doc_date:asc", year=2025)
    assert first is not None and first.record_key == "rk-1"

    with pytest.raises(QuerySyntaxError):
        list(ledger.records(where=[("urlinfo.department_code", "invalid_op", "FIN")]))

    with pytest.raises(RecordNotFoundError):
        ledger.update({"record_key": "missing", "urlinfo": {"department_code": "OPS"}})

    with pytest.raises(DuplicateRecordKeyError):
        ledger.insert(make_record(ledger, "rk-1", "2025-01-10"))


def test_partition_doc_date_invariant_enforced(source_map) -> None:
    ledger = Ledger(sources=source_map, backend="json")
    ledger.insert(make_record(ledger, "rk-1", "2025-01-10"))

    with pytest.raises(PartitionMismatchError):
        ledger.update({"record_key": "rk-1", "doc_date": "2024-01-01"})

    bad = {
        "record_key": "rk-2",
        "doc_date": "2025-02-01",
        "partition": "2024",
        "urlinfo": {"record_key": "rk-2", "doc_date": "2025-02-01", "department_code": "FIN"},
        "uploadinfo": {"record_key": "rk-2", "doc_date": "2025-02-01", "path": "/tmp/rk-2.pdf"},
        "pdfinfo": {"record_key": "rk-2", "doc_date": "2025-02-01", "language": "en"},
    }
    with pytest.raises(PartitionMismatchError):
        ledger.insert(bad)


def test_upsert_many_atomic_and_non_atomic(source_map) -> None:
    ledger = Ledger(sources=source_map, backend="json")
    rec1 = make_record(ledger, "rk-1", "2025-01-10")
    rec2 = make_record(ledger, "rk-2", "2025-01-11")
    bad = {"record_key": "bad", "partition": "2025"}  # missing doc_date/sources

    stats_atomic = ledger.upsert_many([rec1, bad], atomic=True)
    assert stats_atomic["failed"] == 1
    assert stats_atomic["inserted"] == 0
    assert ledger.exists("rk-1") is False

    stats_non_atomic = ledger.upsert_many([rec1, bad, rec2], atomic=False)
    assert stats_non_atomic["failed"] == 1
    assert stats_non_atomic["inserted"] == 2
    assert ledger.exists("rk-1") is True
    assert ledger.exists("rk-2") is True


def test_json_save_and_reload(source_map) -> None:
    ledger = Ledger(sources=source_map, backend="json", auto_load=True)
    ledger.insert(make_record(ledger, "rk-1", "2025-01-10"))
    ledger.insert(make_record(ledger, "rk-2", "2024-06-01"))

    stats = ledger.save()
    assert stats["years_saved"] == ["2024", "2025"]
    assert stats["files_written"] == 6
    assert stats["rows_written"] == 6
    assert isinstance(stats["duration_ms"], int)

    # no-op when clean
    assert ledger.save(year=2025)["years_saved"] == []

    # index rebuilt from json on new instance
    ledger2 = Ledger(sources=source_map, backend="json", auto_load=True)
    assert ledger2.exists("rk-1") is True
    assert ledger2.get("rk-1") is not None


def test_sqlite_backend_with_index_fields(source_map, tmp_path: Path) -> None:
    # Seed canonical JSON first.
    json_ledger = Ledger(sources=source_map, backend="json")
    json_ledger.insert(make_record(json_ledger, "rk-1", "2025-01-10", dept="FIN", lang="en"))
    json_ledger.insert(make_record(json_ledger, "rk-2", "2025-01-11", dept="HR", lang="en"))
    json_ledger.save()

    sqlite_path = tmp_path / "ledger.sqlite3"
    ledger = Ledger(
        sources=source_map,
        backend="sqlite",
        sqlite_path=sqlite_path,
        index_fields=["urlinfo.department_code", "pdfinfo.language"],
    )

    assert ledger.exists("rk-1") is True
    assert ledger.get("rk-1") is not None

    rows = list(ledger.records(where=[("urlinfo.department_code", "eq", "FIN")]))
    assert [r.record_key for r in rows] == ["rk-1"]

    # SQLite index columns should be present and indexed.
    conn = sqlite3.connect(sqlite_path)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(records)").fetchall()}
        assert {"idx_0", "idx_1"}.issubset(cols)

        index_names = {row[1] for row in conn.execute("PRAGMA index_list(records)").fetchall()}
        assert "idx_records_idx_0" in index_names
        assert "idx_records_idx_1" in index_names
    finally:
        conn.close()

    # mutate through sqlite backend, then persist back to json
    ledger.update({"record_key": "rk-1", "urlinfo": {"department_code": "LEGAL"}})
    ledger.insert(make_record(ledger, "rk-3", "2025-01-12", dept="OPS", lang="hi"))
    save_stats = ledger.save(year=2025)
    assert save_stats["years_saved"] == ["2025"]

    # json backend should observe persisted changes
    check = Ledger(sources=source_map, backend="json")
    assert check.get("rk-1").urlinfo.department_code == "LEGAL"
    assert check.exists("rk-3") is True


def test_refresh_index_removed_from_public_api(source_map) -> None:
    ledger = Ledger(sources=source_map, backend="json")
    assert not hasattr(ledger, "refresh_index")


def test_init_rejects_source_model_without_doc_date(tmp_path: Path) -> None:
    class BadUrlInfo(BaseModel):
        record_key: str

    bad_sources = {
        "urlinfo": {"directory": tmp_path / "urlinfos", "model": BadUrlInfo},
        "uploadinfo": {"directory": tmp_path / "uploadinfos", "model": UploadInfo},
        "pdfinfo": {"directory": tmp_path / "pdfinfos", "model": PdfInfo},
    }

    with pytest.raises(ValidationError):
        Ledger(sources=bad_sources)
