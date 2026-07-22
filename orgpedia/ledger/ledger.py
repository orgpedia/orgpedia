from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional, Sequence, Set, Tuple, TypedDict

from pydantic import BaseModel, create_model

Partition = str
RecordKey = str
SourceName = str
FilterOp = Literal[
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "notin",
    "contains",
    "startswith",
    "endswith",
    "isnull",
    "isnotnull",
    "between",
]
WhereClause = Tuple[str, FilterOp, object]
Where = List[WhereClause]

SUPPORTED_OPS: Set[str] = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "notin",
    "contains",
    "startswith",
    "endswith",
    "isnull",
    "isnotnull",
    "between",
}
CORE_RECORD_FIELDS: Set[str] = {"record_key", "doc_date", "partition"}


class LedgerError(Exception):
    pass


class RecordNotFoundError(LedgerError):
    pass


class DuplicateRecordKeyError(LedgerError):
    pass


class ValidationError(LedgerError):
    pass


class PartitionMismatchError(LedgerError):
    pass


class PersistenceError(LedgerError):
    pass


class QuerySyntaxError(LedgerError):
    pass


class SourceConfig(TypedDict):
    directory: str | Path
    model: type[BaseModel]


SourceMap = Dict[SourceName, SourceConfig]


class Record(BaseModel):
    record_key: RecordKey
    doc_date: dt.date
    partition: Partition


def _is_ascii(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def _normalize_record_key(value: object) -> str:
    key = str(value or "").strip()
    if not key:
        raise ValidationError("record_key is required")
    if not _is_ascii(key):
        raise ValidationError(f"record_key must be ASCII: {key!r}")
    return key


def _normalize_doc_date(value: object) -> dt.date:
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ValidationError(f"invalid ISO date: {value!r}") from exc
    raise ValidationError(f"invalid doc_date type: {type(value).__name__}")


def _normalize_partition(value: object) -> str:
    token = str(value or "").strip()
    if len(token) != 4 or not token.isdigit():
        raise ValidationError(f"invalid partition token: {value!r}")
    return token


def _partition_from_doc_date(doc_date: dt.date) -> str:
    return str(doc_date.year)


def _to_sortable(value: object) -> Tuple[bool, object]:
    if value is None:
        return (True, "")
    if isinstance(value, dt.date):
        return (False, value.isoformat())
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return (False, json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))
        except TypeError:
            return (False, str(value))
    return (False, value)


def _resolve_field(record: Record, field_path: str) -> object:
    parts = [p for p in field_path.split(".") if p]
    if not parts:
        return None

    # Backward compatibility for old paths prefixed with "sources."
    if parts[0] == "sources":
        parts = parts[1:]
        if not parts:
            return None

    current: object = record
    for part in parts:
        if isinstance(current, BaseModel):
            if part in current.__class__.model_fields:
                current = getattr(current, part)
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _coerce_compare_value(actual: object, expected: object) -> object:
    if isinstance(actual, dt.date) and isinstance(expected, str):
        return _normalize_doc_date(expected)
    return expected


def _matches_where(record: Record, where: Optional[Where]) -> bool:
    if where is None:
        return True
    if not isinstance(where, list):
        raise QuerySyntaxError("where must be a list of tuples")

    for idx, clause in enumerate(where):
        if not isinstance(clause, (tuple, list)) or len(clause) != 3:
            raise QuerySyntaxError(f"invalid where tuple at index {idx}: {clause!r}")

        field, op, value = clause
        if not isinstance(field, str) or not field:
            raise QuerySyntaxError(f"invalid field at where index {idx}: {field!r}")
        if not isinstance(op, str) or op not in SUPPORTED_OPS:
            raise QuerySyntaxError(f"invalid operator at where index {idx}: {op!r}")

        actual = _resolve_field(record, field)
        expected = _coerce_compare_value(actual, value)

        if op == "eq":
            ok = actual == expected
        elif op == "ne":
            ok = actual != expected
        elif op == "gt":
            ok = actual is not None and expected is not None and actual > expected
        elif op == "gte":
            ok = actual is not None and expected is not None and actual >= expected
        elif op == "lt":
            ok = actual is not None and expected is not None and actual < expected
        elif op == "lte":
            ok = actual is not None and expected is not None and actual <= expected
        elif op == "in":
            if not isinstance(expected, (list, tuple, set)) or len(expected) == 0:
                raise QuerySyntaxError(
                    f"invalid where tuple at index {idx}: operator 'in' needs non-empty collection"
                )
            ok = actual in expected
        elif op == "notin":
            if not isinstance(expected, (list, tuple, set)) or len(expected) == 0:
                raise QuerySyntaxError(
                    f"invalid where tuple at index {idx}: operator 'notin' needs non-empty collection"
                )
            ok = actual not in expected
        elif op == "contains":
            if actual is None:
                ok = False
            elif isinstance(actual, str):
                ok = str(expected) in actual
            else:
                try:
                    ok = expected in actual  # type: ignore[operator]
                except Exception:
                    ok = False
        elif op == "startswith":
            ok = isinstance(actual, str) and isinstance(expected, str) and actual.startswith(expected)
        elif op == "endswith":
            ok = isinstance(actual, str) and isinstance(expected, str) and actual.endswith(expected)
        elif op == "isnull":
            ok = actual is None
        elif op == "isnotnull":
            ok = actual is not None
        else:  # between
            if not isinstance(expected, (list, tuple)) or len(expected) != 2:
                raise QuerySyntaxError(
                    f"invalid where tuple at index {idx}: operator 'between' needs 2-item collection"
                )
            lo = _coerce_compare_value(actual, expected[0])
            hi = _coerce_compare_value(actual, expected[1])
            ok = actual is not None and lo <= actual <= hi

        if not ok:
            return False

    return True


def _filter_records(
    records: Sequence[Record],
    *,
    record_keys: Optional[List[str]] = None,
    year: Optional[int | str] = None,
    doc_date_between: Optional[Tuple[str, str]] = None,
    where: Optional[Where] = None,
) -> List[Record]:
    rows = list(records)

    if record_keys is not None:
        wanted = {_normalize_record_key(k) for k in record_keys}
        rows = [row for row in rows if row.record_key in wanted]

    if year is not None:
        part = _normalize_partition(year)
        rows = [row for row in rows if row.partition == part]

    if doc_date_between is not None:
        if not isinstance(doc_date_between, (tuple, list)) or len(doc_date_between) != 2:
            raise QuerySyntaxError("doc_date_between expects a 2-item tuple/list")
        start = _normalize_doc_date(doc_date_between[0])
        end = _normalize_doc_date(doc_date_between[1])
        rows = [row for row in rows if start <= row.doc_date <= end]

    rows = [row for row in rows if _matches_where(row, where)]
    return rows


def _sort_records(records: Sequence[Record], order_by: Optional[str | List[str]]) -> List[Record]:
    terms = [order_by] if isinstance(order_by, str) else list(order_by or ["record_key:asc"])
    rows = list(records)

    for term in reversed(terms):
        if not isinstance(term, str) or not term:
            raise QuerySyntaxError(f"invalid order_by term: {term!r}")
        if ":" in term:
            field, direction = term.split(":", 1)
            direction = direction.lower().strip()
        else:
            field, direction = term.strip(), "asc"

        if direction not in ("asc", "desc"):
            raise QuerySyntaxError(f"invalid sort direction in order_by: {term!r}")

        reverse = direction == "desc"
        rows.sort(key=lambda row: _to_sortable(_resolve_field(row, field)), reverse=reverse)

    return rows


def _atomic_write_jsonl(path: Path, rows: Sequence[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")

    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, path)

        # Best effort parent fsync.
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except OSError as exc:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise PersistenceError(f"failed writing {path}: {exc}") from exc


def _list_partitions(sources: SourceMap) -> List[str]:
    partitions: Set[str] = set()
    for source_config in sources.values():
        directory = source_config["directory"]
        if not directory.exists() or not directory.is_dir():
            continue
        for file_path in directory.glob("*.jsonl"):
            partitions.add(_normalize_partition(file_path.stem))
    return sorted(partitions)


def build_record_model(
    sources: SourceMap,
    *,
    name: str = "RecordModel",
) -> type[Record]:
    dynamic_fields: Dict[str, Tuple[Any, Any]] = {}
    for source_name, source_config in sources.items():
        model_cls = source_config["model"]
        if "record_key" not in model_cls.model_fields or "doc_date" not in model_cls.model_fields:
            raise ValidationError(
                f"source '{source_name}' model must define required fields: record_key, doc_date"
            )
        if not model_cls.model_fields["record_key"].is_required():
            raise ValidationError(f"source '{source_name}' field 'record_key' must be required")
        if not model_cls.model_fields["doc_date"].is_required():
            raise ValidationError(f"source '{source_name}' field 'doc_date' must be required")
        dynamic_fields[source_name] = (model_cls, ...)

    record_model = create_model(name, __base__=Record, **dynamic_fields)
    return record_model  # type: ignore[return-value]


class _RecordCodec:
    def __init__(self, sources: SourceMap, record_model: type[Record]) -> None:
        self.sources = sources
        self.RecordModel = record_model

    def _prepare_record_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        record_key = _normalize_record_key(data.get("record_key"))
        doc_date = _normalize_doc_date(data.get("doc_date"))
        partition = _partition_from_doc_date(doc_date)

        provided_partition = data.get("partition")
        if provided_partition is not None and _normalize_partition(provided_partition) != partition:
            raise PartitionMismatchError(
                f"partition mismatch for {record_key}: expected {partition}, got {provided_partition}"
            )

        unknown_top = [
            key
            for key in data.keys()
            if key not in CORE_RECORD_FIELDS and key not in self.sources and key != "sources"
        ]
        if unknown_top:
            raise ValidationError(f"unknown top-level fields: {unknown_top}")

        source_payloads: Dict[str, Any] = {}
        if "sources" in data:
            sources_container = data["sources"]
            if not isinstance(sources_container, dict):
                raise ValidationError("sources must be a dict when provided")
            source_payloads.update(sources_container)

        for source_name in self.sources:
            if source_name in data:
                source_payloads[source_name] = data[source_name]

        unknown_sources = [key for key in source_payloads if key not in self.sources]
        if unknown_sources:
            raise ValidationError(f"unknown source names: {unknown_sources}")

        prepared: Dict[str, Any] = {
            "record_key": record_key,
            "doc_date": doc_date,
            "partition": partition,
        }

        for source_name, source_config in self.sources.items():
            model_cls = source_config["model"]
            source_value = source_payloads.get(source_name)

            if source_value is None:
                payload = {"record_key": record_key, "doc_date": doc_date}
            elif isinstance(source_value, BaseModel):
                payload = source_value.model_dump(mode="python")
            elif isinstance(source_value, dict):
                payload = dict(source_value)
            else:
                raise ValidationError(
                    f"invalid source payload for '{source_name}': {type(source_value).__name__}"
                )

            payload.setdefault("record_key", record_key)
            payload.setdefault("doc_date", doc_date)

            try:
                source_model = model_cls.model_validate(payload)
            except Exception as exc:
                raise ValidationError(f"invalid source '{source_name}' payload: {exc}") from exc

            if _normalize_record_key(getattr(source_model, "record_key")) != record_key:
                raise ValidationError(f"source '{source_name}' record_key mismatch for {record_key}")

            source_doc_date = _normalize_doc_date(getattr(source_model, "doc_date"))
            if source_doc_date != doc_date:
                raise ValidationError(f"source '{source_name}' doc_date mismatch for {record_key}")

            prepared[source_name] = source_model

        return prepared

    def coerce_record(self, record: Record | Dict[str, Any]) -> Record:
        if isinstance(record, BaseModel):
            raw = record.model_dump(mode="python")
        elif isinstance(record, dict):
            raw = dict(record)
        else:
            raise ValidationError(f"invalid record type: {type(record).__name__}")

        prepared = self._prepare_record_data(raw)
        try:
            return self.RecordModel.model_validate(prepared)
        except Exception as exc:
            raise ValidationError(f"invalid record payload: {exc}") from exc

    def apply_patch(self, current: Record, patch: Dict[str, object]) -> Record:
        if not isinstance(patch, dict):
            raise ValidationError("patch must be a dict")

        record_key = patch.get("record_key")
        if _normalize_record_key(record_key) != current.record_key:
            raise ValidationError("patch record_key mismatch")

        if "partition" in patch and _normalize_partition(patch["partition"]) != current.partition:
            raise PartitionMismatchError(f"partition change is forbidden for {current.record_key}")

        merged: Dict[str, Any] = current.model_dump(mode="python")

        if "doc_date" in patch:
            next_doc_date = _normalize_doc_date(patch["doc_date"])
            if _partition_from_doc_date(next_doc_date) != current.partition:
                raise PartitionMismatchError(
                    f"doc_date patch would change partition for {current.record_key}"
                )
            merged["doc_date"] = next_doc_date

        nested_sources = patch.get("sources")
        if nested_sources is not None:
            if not isinstance(nested_sources, dict):
                raise ValidationError("sources patch must be a dict")
            for source_name, source_patch in nested_sources.items():
                if source_name not in self.sources:
                    raise ValidationError(f"unknown source in patch: {source_name}")
                existing_payload = merged[source_name]
                if isinstance(existing_payload, BaseModel):
                    existing_payload = existing_payload.model_dump(mode="python")

                if isinstance(source_patch, BaseModel):
                    merged[source_name] = source_patch.model_dump(mode="python")
                elif isinstance(source_patch, dict):
                    new_payload = dict(existing_payload)
                    new_payload.update(source_patch)
                    merged[source_name] = new_payload
                else:
                    raise ValidationError(
                        f"invalid source patch for '{source_name}': {type(source_patch).__name__}"
                    )

        for source_name in self.sources:
            if source_name not in patch:
                continue
            source_patch = patch[source_name]
            existing_payload = merged[source_name]
            if isinstance(existing_payload, BaseModel):
                existing_payload = existing_payload.model_dump(mode="python")

            if isinstance(source_patch, BaseModel):
                merged[source_name] = source_patch.model_dump(mode="python")
            elif isinstance(source_patch, dict):
                new_payload = dict(existing_payload)
                new_payload.update(source_patch)
                merged[source_name] = new_payload
            else:
                raise ValidationError(
                    f"invalid source patch for '{source_name}': {type(source_patch).__name__}"
                )

        merged["record_key"] = current.record_key
        merged["partition"] = current.partition
        return self.coerce_record(merged)


def _load_partition_records_from_disk(
    sources: SourceMap,
    codec: _RecordCodec,
    partition: str,
) -> Dict[str, Record]:
    part = _normalize_partition(partition)

    source_rows: Dict[str, Dict[str, BaseModel]] = {}
    all_keys: Set[str] = set()

    for source_name, source_config in sources.items():
        file_path = source_config["directory"] / f"{part}.jsonl"
        model_cls = source_config["model"]
        rows: Dict[str, BaseModel] = {}

        if file_path.exists():
            try:
                with file_path.open("r", encoding="utf-8") as handle:
                    for line_no, raw in enumerate(handle, start=1):
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                        except json.JSONDecodeError as exc:
                            raise PersistenceError(
                                f"invalid JSON in {file_path}:{line_no}: {exc}"
                            ) from exc

                        try:
                            model = model_cls.model_validate(payload)
                        except Exception as exc:
                            raise ValidationError(
                                f"invalid row in {file_path}:{line_no}: {exc}"
                            ) from exc

                        key = _normalize_record_key(getattr(model, "record_key"))
                        doc_date = _normalize_doc_date(getattr(model, "doc_date"))
                        if _partition_from_doc_date(doc_date) != part:
                            raise PartitionMismatchError(
                                f"partition mismatch in {file_path}:{line_no} for {key}"
                            )

                        if key in rows:
                            raise DuplicateRecordKeyError(f"duplicate record_key '{key}' in {file_path}")

                        rows[key] = model
            except OSError as exc:
                raise PersistenceError(f"unable to read {file_path}: {exc}") from exc

        source_rows[source_name] = rows
        all_keys.update(rows.keys())

    records: Dict[str, Record] = {}

    for key in sorted(all_keys):
        doc_date: Optional[dt.date] = None
        for source_name in sources:
            source_model = source_rows[source_name].get(key)
            if source_model is None:
                continue
            model_doc_date = _normalize_doc_date(getattr(source_model, "doc_date"))
            if doc_date is None:
                doc_date = model_doc_date
            elif doc_date != model_doc_date:
                raise ValidationError(
                    f"doc_date mismatch across sources for {key} in partition {part}"
                )

        if doc_date is None:
            continue

        payload: Dict[str, Any] = {
            "record_key": key,
            "doc_date": doc_date,
            "partition": part,
        }

        for source_name, source_config in sources.items():
            row = source_rows[source_name].get(key)
            if row is None:
                try:
                    row = source_config["model"](record_key=key, doc_date=doc_date)
                except Exception as exc:
                    raise ValidationError(
                        f"unable to default-materialize '{source_name}' for key={key}: {exc}"
                    ) from exc
            payload[source_name] = row

        records[key] = codec.coerce_record(payload)

    return records


def _write_partition_to_json(
    sources: SourceMap,
    partition: str,
    records: Sequence[Record],
) -> Tuple[int, int]:
    part = _normalize_partition(partition)
    ordered = sorted(records, key=lambda rec: rec.record_key)
    files_written = 0
    rows_written = 0

    for source_name, source_config in sources.items():
        rows: List[Dict[str, object]] = []
        for record in ordered:
            source_model = getattr(record, source_name)
            if isinstance(source_model, BaseModel):
                rows.append(source_model.model_dump(mode="json"))
            elif isinstance(source_model, dict):
                rows.append(dict(source_model))
            else:
                raise ValidationError(
                    f"invalid source '{source_name}' value for record {record.record_key}"
                )

        path = source_config["directory"] / f"{part}.jsonl"
        _atomic_write_jsonl(path, rows)
        files_written += 1
        rows_written += len(rows)

    return files_written, rows_written


class Backend(ABC):
    @abstractmethod
    def load_partition(self, partition: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def unload_partition(self, partition: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def loaded_partitions(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def get(self, record_key: str) -> Optional[Record]:
        raise NotImplementedError

    @abstractmethod
    def exists(self, record_key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def insert(self, record: Record | Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def update(self, patch: Dict[str, object]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert(self, record: Record | Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_many(self, records: List[Record | Dict[str, Any]], atomic: bool) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def records(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
        order_by: Optional[str | List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page_size: int = 1000,
    ) -> Iterator[Record]:
        raise NotImplementedError

    @abstractmethod
    def count(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def save(self, partition: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class JsonBackend(Backend):
    def __init__(
        self,
        sources: SourceMap,
        record_model: type[Record],
        *,
        auto_load: bool = True,
        cache_years: int = 2,
    ) -> None:
        self.sources = sources
        self.RecordModel = record_model
        self.codec = _RecordCodec(sources, record_model)
        self.auto_load = auto_load
        self.cache_years = cache_years

        for source_config in self.sources.values():
            source_config["directory"].mkdir(parents=True, exist_ok=True)

        self._key_to_partition: Dict[str, str] = {}
        self._loaded: Dict[str, Dict[str, Record]] = {}
        self._load_order: List[str] = []
        self._dirty_partitions: Set[str] = set()

        self._build_index_from_disk()

    def _build_index_from_disk(self) -> None:
        self._key_to_partition = {}
        for partition in _list_partitions(self.sources):
            part_rows = _load_partition_records_from_disk(self.sources, self.codec, partition)
            for key in part_rows:
                existing = self._key_to_partition.get(key)
                if existing is not None and existing != partition:
                    raise DuplicateRecordKeyError(
                        f"record_key={key} appears in partitions {existing} and {partition}"
                    )
                self._key_to_partition[key] = partition

    def _touch_partition(self, partition: str) -> None:
        if partition in self._load_order:
            self._load_order.remove(partition)
        self._load_order.append(partition)

        if self.cache_years > 0 and len(self._load_order) > self.cache_years:
            candidate = self._load_order[0]
            if candidate not in self._dirty_partitions:
                self._load_order.pop(0)
                self._loaded.pop(candidate, None)

    def _ensure_loaded(self, partition: str) -> None:
        if partition in self._loaded:
            self._touch_partition(partition)
            return
        if not self.auto_load:
            return
        self.load_partition(partition)

    def _ensure_loaded_for_key(self, record_key: str) -> None:
        partition = self._key_to_partition.get(record_key)
        if partition is None:
            return
        self._ensure_loaded(partition)

    def load_partition(self, partition: str) -> int:
        part = _normalize_partition(partition)
        if part in self._loaded:
            self._touch_partition(part)
            return len(self._loaded[part])

        records = _load_partition_records_from_disk(self.sources, self.codec, part)
        self._loaded[part] = records

        for key in records:
            existing = self._key_to_partition.get(key)
            if existing is not None and existing != part:
                raise DuplicateRecordKeyError(
                    f"record_key={key} belongs to {existing}, cannot also load from {part}"
                )
            self._key_to_partition[key] = part

        self._touch_partition(part)
        return len(records)

    def unload_partition(self, partition: str) -> None:
        part = _normalize_partition(partition)
        if part in self._dirty_partitions:
            raise PersistenceError(f"cannot unload dirty partition {part}; call save() first")
        self._loaded.pop(part, None)
        if part in self._load_order:
            self._load_order.remove(part)

    def loaded_partitions(self) -> List[str]:
        return sorted(self._loaded.keys())

    def get(self, record_key: str) -> Optional[Record]:
        key = _normalize_record_key(record_key)
        self._ensure_loaded_for_key(key)
        partition = self._key_to_partition.get(key)
        if partition is None:
            return None
        row = self._loaded.get(partition, {}).get(key)
        if row is None:
            return None
        return row.model_copy(deep=True)

    def exists(self, record_key: str) -> bool:
        try:
            key = _normalize_record_key(record_key)
        except ValidationError:
            return False
        return key in self._key_to_partition

    def insert(self, record: Record | Dict[str, Any]) -> None:
        materialized = self.codec.coerce_record(record)
        key = materialized.record_key
        if self.exists(key):
            raise DuplicateRecordKeyError(f"record already exists: {key}")

        part = materialized.partition
        if part not in self._loaded:
            self.load_partition(part)
        self._loaded.setdefault(part, {})[key] = materialized
        self._key_to_partition[key] = part
        self._dirty_partitions.add(part)
        self._touch_partition(part)

    def update(self, patch: Dict[str, object]) -> None:
        if not isinstance(patch, dict):
            raise ValidationError("patch must be a dict")

        key = _normalize_record_key(patch.get("record_key"))
        self._ensure_loaded_for_key(key)
        part = self._key_to_partition.get(key)
        if part is None:
            raise RecordNotFoundError(f"record not found: {key}")
        if part not in self._loaded:
            self.load_partition(part)

        current = self._loaded.get(part, {}).get(key)
        if current is None:
            raise RecordNotFoundError(f"record not found: {key}")

        updated = self.codec.apply_patch(current, patch)
        if updated.partition != part:
            raise PartitionMismatchError(f"partition change is forbidden for {key}")

        self._loaded[part][key] = updated
        self._dirty_partitions.add(part)

    def upsert(self, record: Record | Dict[str, Any]) -> None:
        key = _normalize_record_key(record.get("record_key") if isinstance(record, dict) else getattr(record, "record_key", None))
        if self.exists(key):
            payload = record.model_dump(mode="python") if isinstance(record, BaseModel) else dict(record)
            payload["record_key"] = key
            self.update(payload)
        else:
            self.insert(record)

    def upsert_many(self, records: List[Record | Dict[str, Any]], atomic: bool) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "touched_partitions": [],
        }
        if not records:
            return stats

        touched: Set[str] = set()
        snapshot: Optional[Tuple[Any, Any, Any, Any]] = None
        if atomic:
            snapshot = (
                deepcopy(self._loaded),
                dict(self._key_to_partition),
                set(self._dirty_partitions),
                list(self._load_order),
            )

        for idx, record in enumerate(records):
            try:
                key = _normalize_record_key(
                    record.get("record_key") if isinstance(record, dict) else getattr(record, "record_key", None)
                )
                existed = self.exists(key)
                self.upsert(record)
                if existed:
                    stats["updated"] += 1
                else:
                    stats["inserted"] += 1
                part = self._key_to_partition.get(key)
                if part is not None:
                    touched.add(part)
            except Exception as exc:
                stats["failed"] += 1
                stats["errors"].append(f"record[{idx}]: {exc}")
                if atomic and snapshot is not None:
                    self._loaded, self._key_to_partition, self._dirty_partitions, self._load_order = snapshot
                    stats["inserted"] = 0
                    stats["updated"] = 0
                    stats["skipped"] = 0
                    touched.clear()
                    break

        stats["touched_partitions"] = sorted(touched)
        return stats

    def _candidate_partitions(
        self,
        record_keys: Optional[List[str]],
        year: Optional[int | str],
    ) -> List[str]:
        if year is not None:
            return [_normalize_partition(year)]

        if record_keys:
            parts = {
                self._key_to_partition[key]
                for key in record_keys
                if key in self._key_to_partition
            }
            return sorted(parts)

        partitions = set(_list_partitions(self.sources))
        partitions.update(self._loaded.keys())
        return sorted(partitions)

    def records(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
        order_by: Optional[str | List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page_size: int = 1000,
    ) -> Iterator[Record]:
        if limit is not None and limit < 0:
            raise ValidationError("limit must be >= 0")
        if offset is not None and offset < 0:
            raise ValidationError("offset must be >= 0")
        if page_size <= 0:
            raise ValidationError("page_size must be > 0")

        keys_filter = None
        if record_keys is not None:
            keys_filter = [_normalize_record_key(key) for key in record_keys]

        for part in self._candidate_partitions(keys_filter, year):
            self._ensure_loaded(part)

        rows: List[Record] = []
        candidate_parts = self._candidate_partitions(keys_filter, year)
        for part in candidate_parts:
            rows.extend(self._loaded.get(part, {}).values())

        filtered = _filter_records(
            rows,
            record_keys=keys_filter,
            year=year,
            doc_date_between=doc_date_between,
            where=where,
        )
        ordered = _sort_records(filtered, order_by)

        start = offset or 0
        if limit is None:
            sliced = ordered[start:]
        else:
            sliced = ordered[start : start + limit]

        for row in sliced:
            yield row.model_copy(deep=True)

    def count(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
    ) -> int:
        return sum(
            1
            for _ in self.records(
                record_keys=record_keys,
                year=year,
                doc_date_between=doc_date_between,
                where=where,
                page_size=5000,
            )
        )

    def save(self, partition: Optional[str] = None) -> Dict[str, Any]:
        started = time.perf_counter()

        if partition is None:
            partitions_to_save = sorted(self._dirty_partitions)
        else:
            part = _normalize_partition(partition)
            partitions_to_save = [part] if part in self._dirty_partitions else []

        years_saved: List[str] = []
        files_written = 0
        rows_written = 0

        for part in partitions_to_save:
            if part not in self._loaded:
                self.load_partition(part)
            part_rows = list(self._loaded.get(part, {}).values())
            part_files, part_rows_written = _write_partition_to_json(self.sources, part, part_rows)
            files_written += part_files
            rows_written += part_rows_written
            years_saved.append(part)
            self._dirty_partitions.discard(part)

        duration_ms = int((time.perf_counter() - started) * 1000)
        return {
            "years_saved": years_saved,
            "files_written": files_written,
            "rows_written": rows_written,
            "duration_ms": duration_ms,
        }

    def close(self) -> None:
        return


def _indexable_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        return str(value)


class SqliteBackend(Backend):
    def __init__(
        self,
        sources: SourceMap,
        record_model: type[Record],
        *,
        sqlite_path: Optional[str | Path] = None,
        index_fields: Optional[List[str]] = None,
    ) -> None:
        self.sources = sources
        self.RecordModel = record_model
        self.codec = _RecordCodec(sources, record_model)

        self.index_fields: List[str] = []
        if index_fields:
            for field in index_fields:
                if not isinstance(field, str) or not field.strip():
                    raise ValidationError(f"invalid index field: {field!r}")
                if field not in self.index_fields:
                    self.index_fields.append(field)

        self._index_columns: Dict[str, str] = {
            field: f"idx_{i}" for i, field in enumerate(self.index_fields)
        }

        if sqlite_path is None:
            roots = [str(cfg["directory"].parent) for cfg in self.sources.values()]
            common_parent = Path(os.path.commonpath(roots)) if roots else Path.cwd()
            sqlite_path = common_parent / ".ledger.sqlite3"

        self.sqlite_path = Path(sqlite_path)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.sqlite_path))
        self.conn.row_factory = sqlite3.Row

        self._dirty_partitions: Set[str] = set()
        self._loaded_partitions: Set[str] = set()

        self._ensure_schema()
        self._bootstrap_from_json()

    def _ensure_schema(self) -> None:
        dynamic_cols = ""
        if self._index_columns:
            dynamic_cols = ", " + ", ".join(f"{col} TEXT" for col in self._index_columns.values())

        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS records (
                record_key TEXT PRIMARY KEY,
                partition TEXT NOT NULL,
                doc_date TEXT NOT NULL,
                payload_json TEXT NOT NULL
                {dynamic_cols}
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_records_partition ON records(partition)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_records_doc_date ON records(doc_date)")
        for col in self._index_columns.values():
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_records_{col} ON records({col})")
        self.conn.commit()

    def _record_to_row_values(self, record: Record) -> Dict[str, object]:
        payload = record.model_dump(mode="json")
        values: Dict[str, object] = {
            "record_key": record.record_key,
            "partition": record.partition,
            "doc_date": record.doc_date.isoformat(),
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
        }

        for field, column in self._index_columns.items():
            values[column] = _indexable_value(_resolve_field(record, field))

        return values

    def _upsert_row(self, record: Record, cursor: sqlite3.Cursor) -> None:
        row_values = self._record_to_row_values(record)
        columns = list(row_values.keys())
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{col}=excluded.{col}" for col in columns if col != "record_key")

        sql = f"""
            INSERT INTO records ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(record_key) DO UPDATE SET {updates}
        """
        cursor.execute(sql, [row_values[col] for col in columns])

    def _insert_row(self, record: Record, cursor: sqlite3.Cursor) -> None:
        row_values = self._record_to_row_values(record)
        columns = list(row_values.keys())
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO records ({', '.join(columns)}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, [row_values[col] for col in columns])
        except sqlite3.IntegrityError as exc:
            raise DuplicateRecordKeyError(f"record already exists: {record.record_key}") from exc

    def _row_to_record(self, row: sqlite3.Row) -> Record:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError as exc:
            raise PersistenceError(f"invalid payload_json for key={row['record_key']}: {exc}") from exc

        try:
            record = self.RecordModel.model_validate(payload)
        except Exception as exc:
            raise ValidationError(f"invalid sqlite payload for key={row['record_key']}: {exc}") from exc
        return record

    def _bootstrap_from_json(self) -> None:
        self.conn.execute("DELETE FROM records")
        cursor = self.conn.cursor()

        for partition in _list_partitions(self.sources):
            part_rows = _load_partition_records_from_disk(self.sources, self.codec, partition)
            for record in part_rows.values():
                self._upsert_row(record, cursor)

        self.conn.commit()

    def load_partition(self, partition: str) -> int:
        part = _normalize_partition(partition)
        self._loaded_partitions.add(part)
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM records WHERE partition = ?",
            (part,),
        ).fetchone()
        return int(row["c"] if row is not None else 0)

    def unload_partition(self, partition: str) -> None:
        part = _normalize_partition(partition)
        self._loaded_partitions.discard(part)

    def loaded_partitions(self) -> List[str]:
        return sorted(self._loaded_partitions)

    def get(self, record_key: str) -> Optional[Record]:
        key = _normalize_record_key(record_key)
        row = self.conn.execute(
            "SELECT payload_json, record_key FROM records WHERE record_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row).model_copy(deep=True)

    def exists(self, record_key: str) -> bool:
        try:
            key = _normalize_record_key(record_key)
        except ValidationError:
            return False
        row = self.conn.execute("SELECT 1 FROM records WHERE record_key = ?", (key,)).fetchone()
        return row is not None

    def insert(self, record: Record | Dict[str, Any]) -> None:
        materialized = self.codec.coerce_record(record)
        cursor = self.conn.cursor()
        self._insert_row(materialized, cursor)
        self.conn.commit()
        self._dirty_partitions.add(materialized.partition)

    def update(self, patch: Dict[str, object]) -> None:
        if not isinstance(patch, dict):
            raise ValidationError("patch must be a dict")
        key = _normalize_record_key(patch.get("record_key"))
        current = self.get(key)
        if current is None:
            raise RecordNotFoundError(f"record not found: {key}")

        updated = self.codec.apply_patch(current, patch)
        if updated.partition != current.partition:
            raise PartitionMismatchError(f"partition change is forbidden for {key}")

        cursor = self.conn.cursor()
        self._upsert_row(updated, cursor)
        self.conn.commit()
        self._dirty_partitions.add(updated.partition)

    def upsert(self, record: Record | Dict[str, Any]) -> None:
        materialized = self.codec.coerce_record(record)
        cursor = self.conn.cursor()
        self._upsert_row(materialized, cursor)
        self.conn.commit()
        self._dirty_partitions.add(materialized.partition)

    def upsert_many(self, records: List[Record | Dict[str, Any]], atomic: bool) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "touched_partitions": [],
        }
        if not records:
            return stats

        touched: Set[str] = set()
        dirty_snapshot = set(self._dirty_partitions)
        cursor = self.conn.cursor()

        if atomic:
            self.conn.execute("BEGIN")

        try:
            for idx, record in enumerate(records):
                try:
                    materialized = self.codec.coerce_record(record)
                    exists = self.exists(materialized.record_key)
                    self._upsert_row(materialized, cursor)
                    if exists:
                        stats["updated"] += 1
                    else:
                        stats["inserted"] += 1
                    touched.add(materialized.partition)
                    self._dirty_partitions.add(materialized.partition)
                except Exception as exc:
                    stats["failed"] += 1
                    stats["errors"].append(f"record[{idx}]: {exc}")
                    if atomic:
                        raise

            self.conn.commit()
        except Exception:
            if atomic:
                self.conn.rollback()
                self._dirty_partitions = dirty_snapshot
                stats["inserted"] = 0
                stats["updated"] = 0
                stats["skipped"] = 0
                touched.clear()
            else:
                self.conn.commit()

        stats["touched_partitions"] = sorted(touched)
        return stats

    def _normalize_sql_field(self, field_path: str) -> str:
        parts = [p for p in field_path.split(".") if p]
        if not parts:
            raise QuerySyntaxError(f"invalid field path: {field_path!r}")
        if parts[0] == "sources":
            parts = parts[1:]
            if not parts:
                raise QuerySyntaxError(f"invalid field path: {field_path!r}")
        return ".".join(parts)

    def _sql_column_for_field(self, field_path: str) -> Optional[str]:
        field = self._normalize_sql_field(field_path)
        if field in {"record_key", "partition", "doc_date"}:
            return field
        return self._index_columns.get(field)

    def _sql_param_for_field(self, field_path: str, value: object) -> object:
        field = self._normalize_sql_field(field_path)
        if value is None:
            return None
        if field == "doc_date":
            return _normalize_doc_date(value).isoformat()
        if field == "partition":
            return _normalize_partition(value)
        if field == "record_key":
            return _normalize_record_key(value)
        converted = _indexable_value(value)
        return converted

    def _compile_where_to_sql(
        self, where: Optional[Where]
    ) -> Tuple[List[str], List[object], Optional[Where]]:
        if where is None:
            return [], [], None
        if not isinstance(where, list):
            raise QuerySyntaxError("where must be a list of tuples")

        sql_clauses: List[str] = []
        sql_params: List[object] = []
        residual: List[WhereClause] = []

        for idx, clause in enumerate(where):
            if not isinstance(clause, (tuple, list)) or len(clause) != 3:
                raise QuerySyntaxError(f"invalid where tuple at index {idx}: {clause!r}")

            field, op, value = clause
            if not isinstance(field, str) or not field:
                raise QuerySyntaxError(f"invalid field at where index {idx}: {field!r}")
            if not isinstance(op, str) or op not in SUPPORTED_OPS:
                raise QuerySyntaxError(f"invalid operator at where index {idx}: {op!r}")

            column = self._sql_column_for_field(field)
            if column is None:
                residual.append((field, op, value))
                continue

            if op == "eq":
                val = self._sql_param_for_field(field, value)
                if val is None:
                    sql_clauses.append(f"{column} IS NULL")
                else:
                    sql_clauses.append(f"{column} = ?")
                    sql_params.append(val)
            elif op == "ne":
                val = self._sql_param_for_field(field, value)
                if val is None:
                    sql_clauses.append(f"{column} IS NOT NULL")
                else:
                    sql_clauses.append(f"{column} != ?")
                    sql_params.append(val)
            elif op == "gt":
                sql_clauses.append(f"{column} > ?")
                sql_params.append(self._sql_param_for_field(field, value))
            elif op == "gte":
                sql_clauses.append(f"{column} >= ?")
                sql_params.append(self._sql_param_for_field(field, value))
            elif op == "lt":
                sql_clauses.append(f"{column} < ?")
                sql_params.append(self._sql_param_for_field(field, value))
            elif op == "lte":
                sql_clauses.append(f"{column} <= ?")
                sql_params.append(self._sql_param_for_field(field, value))
            elif op == "in":
                if not isinstance(value, (list, tuple, set)) or len(value) == 0:
                    raise QuerySyntaxError(
                        f"invalid where tuple at index {idx}: operator 'in' needs non-empty collection"
                    )
                vals = [self._sql_param_for_field(field, v) for v in value]
                placeholders = ", ".join("?" for _ in vals)
                sql_clauses.append(f"{column} IN ({placeholders})")
                sql_params.extend(vals)
            elif op == "notin":
                if not isinstance(value, (list, tuple, set)) or len(value) == 0:
                    raise QuerySyntaxError(
                        f"invalid where tuple at index {idx}: operator 'notin' needs non-empty collection"
                    )
                vals = [self._sql_param_for_field(field, v) for v in value]
                placeholders = ", ".join("?" for _ in vals)
                sql_clauses.append(f"{column} NOT IN ({placeholders})")
                sql_params.extend(vals)
            elif op == "contains":
                val = self._sql_param_for_field(field, value)
                sql_clauses.append(f"{column} LIKE ?")
                sql_params.append(f"%{str(val)}%")
            elif op == "startswith":
                val = self._sql_param_for_field(field, value)
                sql_clauses.append(f"{column} LIKE ?")
                sql_params.append(f"{str(val)}%")
            elif op == "endswith":
                val = self._sql_param_for_field(field, value)
                sql_clauses.append(f"{column} LIKE ?")
                sql_params.append(f"%{str(val)}")
            elif op == "isnull":
                sql_clauses.append(f"{column} IS NULL")
            elif op == "isnotnull":
                sql_clauses.append(f"{column} IS NOT NULL")
            else:  # between
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    raise QuerySyntaxError(
                        f"invalid where tuple at index {idx}: operator 'between' needs 2-item collection"
                    )
                lo = self._sql_param_for_field(field, value[0])
                hi = self._sql_param_for_field(field, value[1])
                sql_clauses.append(f"{column} BETWEEN ? AND ?")
                sql_params.extend([lo, hi])

        return sql_clauses, sql_params, (residual if residual else None)

    def _parse_order_by_terms(self, order_by: Optional[str | List[str]]) -> List[Tuple[str, str]]:
        terms = [order_by] if isinstance(order_by, str) else list(order_by or ["record_key:asc"])
        parsed: List[Tuple[str, str]] = []
        for term in terms:
            if not isinstance(term, str) or not term:
                raise QuerySyntaxError(f"invalid order_by term: {term!r}")
            if ":" in term:
                field, direction = term.split(":", 1)
            else:
                field, direction = term, "asc"
            field = field.strip()
            direction = direction.strip().lower()
            if direction not in {"asc", "desc"}:
                raise QuerySyntaxError(f"invalid sort direction in order_by: {term!r}")
            parsed.append((field, direction))
        return parsed

    def _compile_order_by_sql(self, order_terms: List[Tuple[str, str]]) -> Optional[str]:
        compiled: List[str] = []
        for field, direction in order_terms:
            column = self._sql_column_for_field(field)
            if column is None:
                return None
            compiled.append(f"{column} {direction.upper()}")
        return ", ".join(compiled)

    def records(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
        order_by: Optional[str | List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page_size: int = 1000,
    ) -> Iterator[Record]:
        if limit is not None and limit < 0:
            raise ValidationError("limit must be >= 0")
        if offset is not None and offset < 0:
            raise ValidationError("offset must be >= 0")
        if page_size <= 0:
            raise ValidationError("page_size must be > 0")

        clauses: List[str] = []
        params: List[object] = []

        keys_filter: Optional[List[str]] = None
        if record_keys is not None:
            keys_filter = [_normalize_record_key(key) for key in record_keys]
            if keys_filter:
                placeholders = ", ".join("?" for _ in keys_filter)
                clauses.append(f"record_key IN ({placeholders})")
                params.extend(keys_filter)

        year_filter: Optional[str] = None
        if year is not None:
            year_filter = _normalize_partition(year)
            clauses.append("partition = ?")
            params.append(year_filter)

        if doc_date_between is not None:
            if not isinstance(doc_date_between, (tuple, list)) or len(doc_date_between) != 2:
                raise QuerySyntaxError("doc_date_between expects a 2-item tuple/list")
            start = _normalize_doc_date(doc_date_between[0]).isoformat()
            end = _normalize_doc_date(doc_date_between[1]).isoformat()
            clauses.append("doc_date >= ? AND doc_date <= ?")
            params.extend([start, end])

        where_sql, where_params, residual_where = self._compile_where_to_sql(where)
        clauses.extend(where_sql)
        params.extend(where_params)

        order_terms = self._parse_order_by_terms(order_by)
        order_by_sql = self._compile_order_by_sql(order_terms)

        sql = "SELECT payload_json, record_key FROM records"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if order_by_sql is not None:
            sql += " ORDER BY " + order_by_sql

        # Only push LIMIT/OFFSET when no residual where filtering remains.
        can_push_window = residual_where is None and order_by_sql is not None
        if can_push_window and limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        if can_push_window and offset:
            if limit is None:
                sql += " LIMIT -1"
            sql += " OFFSET ?"
            params.append(int(offset))

        # Fully SQL-evaluated path can stream directly from cursor.
        if residual_where is None and order_by_sql is not None and can_push_window:
            cursor = self.conn.execute(sql, params)
            while True:
                batch = cursor.fetchmany(page_size)
                if not batch:
                    break
                for row in batch:
                    yield self._row_to_record(row).model_copy(deep=True)
            return

        rows = self.conn.execute(sql, params).fetchall()
        records_list = [self._row_to_record(row) for row in rows]

        # Static filters and SQL-compilable where clauses are already applied in SQL.
        filtered = records_list
        if residual_where is not None:
            filtered = [row for row in filtered if _matches_where(row, residual_where)]

        if order_by_sql is None:
            ordered = _sort_records(filtered, order_by)
        else:
            ordered = filtered

        start_idx = offset or 0
        if can_push_window:
            sliced = ordered
        elif limit is None:
            sliced = ordered[start_idx:]
        else:
            sliced = ordered[start_idx : start_idx + limit]

        for row in sliced:
            yield row.model_copy(deep=True)

    def count(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
    ) -> int:
        return sum(
            1
            for _ in self.records(
                record_keys=record_keys,
                year=year,
                doc_date_between=doc_date_between,
                where=where,
                page_size=5000,
            )
        )

    def save(self, partition: Optional[str] = None) -> Dict[str, Any]:
        started = time.perf_counter()

        if partition is None:
            partitions_to_save = sorted(self._dirty_partitions)
        else:
            part = _normalize_partition(partition)
            partitions_to_save = [part] if part in self._dirty_partitions else []

        years_saved: List[str] = []
        files_written = 0
        rows_written = 0

        for part in partitions_to_save:
            rows = self.conn.execute(
                "SELECT payload_json FROM records WHERE partition = ? ORDER BY record_key",
                (part,),
            ).fetchall()
            records_list = [self._row_to_record(row) for row in rows]

            part_files, part_rows_written = _write_partition_to_json(
                self.sources,
                part,
                records_list,
            )
            files_written += part_files
            rows_written += part_rows_written
            years_saved.append(part)
            self._dirty_partitions.discard(part)

        duration_ms = int((time.perf_counter() - started) * 1000)
        return {
            "years_saved": years_saved,
            "files_written": files_written,
            "rows_written": rows_written,
            "duration_ms": duration_ms,
        }

    def close(self) -> None:
        self.conn.close()


class Ledger:
    def __init__(
        self,
        sources: SourceMap,
        *,
        backend: Literal["json", "sqlite"] = "json",
        sqlite_path: Optional[str | Path] = None,
        auto_load: bool = True,
        cache_years: int = 2,
        index_fields: Optional[List[str]] = None,
    ) -> None:
        if backend not in ("json", "sqlite"):
            raise ValidationError(f"unsupported backend: {backend}")
        if not isinstance(auto_load, bool):
            raise ValidationError("auto_load must be a boolean")
        if not isinstance(cache_years, int) or cache_years < 0:
            raise ValidationError("cache_years must be a non-negative integer")
        if not sources:
            raise ValidationError("sources must be non-empty")

        normalized_sources: SourceMap = {}
        for source_name, source_config in sources.items():
            if not isinstance(source_name, str) or not source_name.strip():
                raise ValidationError(f"invalid source name: {source_name!r}")
            if not isinstance(source_config, dict):
                raise ValidationError(f"invalid source config for {source_name!r}")
            if "directory" not in source_config or "model" not in source_config:
                raise ValidationError(
                    f"source '{source_name}' config must include directory and model"
                )

            model_cls = source_config["model"]
            if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
                raise ValidationError(
                    f"source '{source_name}' model must be a Pydantic BaseModel subclass"
                )

            normalized_sources[source_name] = {
                "directory": Path(source_config["directory"]),
                "model": model_cls,
            }

        self.sources: SourceMap = normalized_sources
        self.backend_name: Literal["json", "sqlite"] = backend
        self.RecordModel: type[Record] = build_record_model(self.sources, name="LedgerRecord")

        if backend == "json":
            self._backend: Backend = JsonBackend(
                self.sources,
                self.RecordModel,
                auto_load=auto_load,
                cache_years=cache_years,
            )
        else:
            self._backend = SqliteBackend(
                self.sources,
                self.RecordModel,
                sqlite_path=sqlite_path,
                index_fields=index_fields,
            )

    def load_year(self, year: int | str) -> int:
        return self._backend.load_partition(_normalize_partition(year))

    def unload_year(self, year: int | str) -> None:
        self._backend.unload_partition(_normalize_partition(year))

    def loaded_years(self) -> List[str]:
        return self._backend.loaded_partitions()

    def get(self, record_key: str) -> Optional[Record]:
        return self._backend.get(record_key)

    def exists(self, record_key: str) -> bool:
        return self._backend.exists(record_key)

    def insert(self, record: Record | Dict[str, Any]) -> None:
        self._backend.insert(record)

    def update(self, patch: Dict[str, object]) -> None:
        self._backend.update(patch)

    def upsert(self, record: Record | Dict[str, Any]) -> None:
        self._backend.upsert(record)

    def upsert_many(self, records: List[Record | Dict[str, Any]], *, atomic: bool = True) -> Dict[str, Any]:
        return self._backend.upsert_many(records, atomic=atomic)

    def records(
        self,
        *,
        record_keys: Optional[List[str]] = None,
        year: Optional[int | str] = None,
        doc_date_between: Optional[Tuple[str, str]] = None,
        where: Optional[Where] = None,
        order_by: Optional[str | List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page_size: int = 1000,
    ) -> Iterator[Record]:
        return self._backend.records(
            record_keys=record_keys,
            year=year,
            doc_date_between=doc_date_between,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
            page_size=page_size,
        )

    def first(self, **filters: object) -> Optional[Record]:
        params = dict(filters)
        params.setdefault("limit", 1)
        return next(self.records(**params), None)

    def count(self, **filters: object) -> int:
        return self._backend.count(**filters)

    def save(self, year: Optional[int | str] = None) -> Dict[str, Any]:
        if year is None:
            return self._backend.save()
        return self._backend.save(_normalize_partition(year))

    def save_all(self) -> Dict[str, Any]:
        return self._backend.save()

    def close(self) -> None:
        self._backend.close()
