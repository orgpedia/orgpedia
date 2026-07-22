# Ledger Specification (v0.7)

## 1. Objective

Define a production-ready `Ledger` contract that:

1. Joins fixed source datasets (`urlinfo`, `uploadinfo`, `pdfinfo`) by `record_key`.
2. Supports fast key lookup, year loading, filtered iteration, and updates.
3. Keeps filtering inside ledger implementation (not caller-side post-filter loops).
4. Allows backend swap (`json` and `sqlite`) with no API changes.
5. Accepts init-time source configuration mapping each fixed source slot to `directory` and Pydantic `model`.

Source configuration shape:

1. Keys are fixed: `urlinfo`, `uploadinfo`, `pdfinfo`.
2. Value: `{"directory": <str | Path>, "model": <Pydantic model class>}` where model includes required `record_key` and `doc_date`.

`partition` is always `YYYY` derived from `doc_date`.


## 2. Scope

In scope:

1. API contract for reads, writes, save, and filters.
2. Filter grammar and semantic rules.
3. Backend abstraction and parity requirements.
4. Save behavior and atomicity guarantees.
5. Test plan and acceptance criteria.

Out of scope:

1. Business workflow states beyond data persistence/query semantics.
2. UI, CLI command design, and deployment concerns.


## 3. Canonical Types

```python
import datetime
from pathlib import Path
from pydantic import create_model
from pydantic import BaseModel
from typing import Literal, TypedDict

Partition = str                 # "YYYY"
RecordKey = str                 # non-empty ASCII

class SourceConfig(TypedDict):
    directory: str | Path
    model: type[BaseModel]      # each model must declare required `record_key` and `doc_date`

class SourceMap(TypedDict):
    urlinfo: SourceConfig
    uploadinfo: SourceConfig
    pdfinfo: SourceConfig

class Record(BaseModel):
    record_key: RecordKey
    doc_date: datetime.date
    partition: Partition
    urlinfo: BaseModel
    uploadinfo: BaseModel
    pdfinfo: BaseModel

# Invariants:
# 1) `partition == str(doc_date.year)`
# 2) each source model has required `record_key` and `doc_date`
# 3) each source `record_key` equals Record.record_key
# 4) each source `doc_date` equals Record.doc_date

# Concrete Record field model types may be bound at runtime from SourceMap.
# Example:
# Record = create_model(
#     "Record",
#     record_key=(str, ...),
#     doc_date=(datetime.date, ...),
#     partition=(str, ...),
#     urlinfo=(sources["urlinfo"]["model"], ...),
#     uploadinfo=(sources["uploadinfo"]["model"], ...),
#     pdfinfo=(sources["pdfinfo"]["model"], ...),
# )

FilterOp = Literal[
    "eq", "ne",
    "gt", "gte", "lt", "lte",
    "in", "notin",
    "contains", "startswith", "endswith",
    "isnull", "isnotnull",
    "between",
]

WhereClause = tuple[str, FilterOp, object]  # (field, op, value)
Where = list[WhereClause]
```


## 4. Public API Contract

```python
class Ledger:
    def __init__(
        self,
        sources: SourceMap,
        *,
        backend: Literal["json", "sqlite"] = "json",
        sqlite_path: str | Path | None = None,
        auto_load: bool = True,
        cache_years: int = 2,
    ) -> None: ...

    def load_year(self, year: int | str) -> int: ...
    def unload_year(self, year: int | str) -> None: ...
    def loaded_years(self) -> list[str]: ...

    def get(self, record_key: str) -> Record | None: ...
    def exists(self, record_key: str) -> bool: ...

    def insert(self, record: Record) -> None: ...
    def update(self, patch: dict[str, object]) -> None: ...
    def upsert(self, record: Record) -> None: ...
    def upsert_many(self, records: list[Record], *, atomic: bool = True) -> dict: ...

    def records(
        self,
        *,
        record_keys: list[str] | None = None,
        year: int | str | None = None,
        doc_date_between: tuple[str, str] | None = None,
        where: Where | None = None,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        page_size: int = 1000,
    ) -> "Iterator[Record]": ...

    def first(self, **filters: object) -> Record | None: ...
    def count(self, **filters: object) -> int: ...
    def save(self, year: int | str | None = None) -> dict: ...
    def save_all(self) -> dict: ...
    def refresh_index(self) -> None: ...
    def close(self) -> None: ...
```


## 5. Method Semantics

### 5.1 `load_year(year) -> int`

1. Normalize `year` to partition token `YYYY`.
2. Load/join that partition into backend working set.
3. Return number of joined records loaded.
4. Idempotent when already loaded.

Errors:

1. `ValidationError` for invalid partition token.
2. `PersistenceError` for unreadable partition files/table access failure.

### 5.2 `get(record_key) -> Record | None`

1. Normalize key.
2. Resolve partition from global key index.
3. Auto-load partition when needed and `auto_load=True`.
4. Return a validated `Record` instance or `None`.

### 5.3 `insert(record)`

1. Input must be a `Record` Pydantic instance.
2. Require `record.record_key`.
3. Require `record.partition == str(record.doc_date.year)`.
4. Fail if key exists.
5. Materialize missing source members (`urlinfo`, `uploadinfo`, `pdfinfo`) using top-level `record_key`/`doc_date` plus model defaults.
6. Mark affected partition dirty.

Errors:

1. `DuplicateRecordKeyError`
2. `ValidationError`

### 5.4 `update(patch)`

1. Input must be a patch `dict` containing `record_key`.
2. Fail if key does not exist.
3. Apply patch semantics (only provided fields updated).
4. Partition value is derived and immutable through patching.
5. If `doc_date` patch implies different year than current partition, raise `PartitionMismatchError`.
6. Re-validate the resulting in-memory record as `Record` plus configured source models.

Errors:

1. `RecordNotFoundError`
2. `PartitionMismatchError`
3. `ValidationError`

### 5.5 `upsert(record)`

1. `upsert` means update-or-insert by `record_key`.
2. If key exists, behavior is equivalent to `update(...)` semantics.
3. If key does not exist, behavior is equivalent to `insert(record)`.
4. Return no value; observable via `save()` stats.

### 5.6 `upsert_many(records, atomic=True) -> dict`

Return shape:

```python
{
    "inserted": int,
    "updated": int,
    "skipped": int,
    "failed": int,
    "errors": list[str],
    "touched_partitions": list[str],
}
```

Rules:

1. Empty input returns zeroed stats.
2. If `atomic=True`, all in-memory changes in this call rollback on first failure.
3. If `atomic=False`, apply best-effort and accumulate errors.

### 5.7 `records(...) -> Iterator[Record]`

Rules:

1. Must stream results in chunks (`page_size`) without full result materialization.
2. All filters combine using AND.
3. Default order is backend deterministic key order (`record_key ASC`).
4. `offset` requires deterministic ordering.

### 5.8 `save(year=None) -> dict`

Return shape:

```python
{
    "years_saved": list[str],
    "files_written": int,
    "rows_written": int,
    "duration_ms": int,
}
```

Rules:

1. Save only dirty partitions.
2. If `year` is provided, save only that partition and no-op if clean.
3. On failure, dirty flags for failed writes remain set.


## 6. Uniform Record Contract

Every record returned by `get`, `records`, and `first` must be a `Record` Pydantic model:

```python
class Record(BaseModel):
    record_key: str
    doc_date: datetime.date
    partition: str
    urlinfo: BaseModel
    uploadinfo: BaseModel
    pdfinfo: BaseModel
```

Rules:

1. `urlinfo`, `uploadinfo`, and `pdfinfo` are required members on every `Record`.
2. Each member payload must validate against that source's configured Pydantic model.
3. Each source model must include required fields `record_key` and `doc_date`.
4. For each source member, `member.record_key == Record.record_key` and `member.doc_date == Record.doc_date`.
5. `Record.partition` must always equal `str(Record.doc_date.year)`.
6. Missing source rows are default-materialized in memory using model defaults.
7. Default materialization must be backend-identical.
8. Unknown fields from source rows are preserved unless explicitly rejected/normalized by model schema rules.


## 7. Filter Contract

### 7.1 Static filters

1. `record_keys`: key inclusion filter.
2. `year`: partition equality filter (`year == str(doc_date.year)` invariant).
3. `doc_date_between=(start, end)`: inclusive range; both ISO dates.

### 7.2 Dynamic `where` Tuple Form

Input format:

1. `where` is a list of tuples.
2. Each tuple must be `(field, op, value)`.
3. `field` is a dot path. Examples: `urlinfo.department_code`, `uploadinfo.hf.path`, `pdfinfo.language.inferred`.
4. `op` must be one of `FilterOp`.

Supported operators:

1. `eq`, `ne`
2. `gt`, `gte`, `lt`, `lte`
3. `in`, `notin`
4. `contains`, `startswith`, `endswith`
5. `isnull`, `isnotnull`
6. `between`

Value requirements:

1. `in`, `notin`: non-empty list/tuple/set.
2. `between`: two-item tuple/list.
3. `isnull`, `isnotnull`: value ignored; presence of tuple is enough.

Example:

1. `where=[("urlinfo.department_code", "eq", "FIN"), ("pdfinfo.language.inferred", "in", ["en", "hi"])]`

Invalid filter syntax raises `QuerySyntaxError`.

### 7.3 `order_by`

Allowed forms:

1. `"record_key"`
2. `"record_key:desc"`
3. `"doc_date:asc"`
4. `["doc_date:asc", "record_key:asc"]`

If omitted, default is `record_key:asc`.


## 8. Backend Abstraction Contract

Internal backend protocol:

```python
class LedgerBackend(Protocol):
    def refresh_index(self) -> None: ...
    def load_partition(self, partition: str) -> int: ...
    def unload_partition(self, partition: str) -> None: ...
    def loaded_partitions(self) -> list[str]: ...

    def get(self, record_key: str) -> Record | None: ...
    def exists(self, record_key: str) -> bool: ...
    def upsert_many(self, records: list[Record], atomic: bool) -> dict: ...

    def iter_records(self, *, filters: dict, page_size: int) -> "Iterator[Record]": ...
    def count(self, *, filters: dict) -> int: ...

    def save(self, partition: str | None = None) -> dict: ...
    def close(self) -> None: ...
```

Parity rule:

1. For identical input dataset and API calls, JSON and SQLite backends must return the same records and counts.


## 9. JSON Backend Contract

1. Canonical source is `*.jsonl` per configured source directory and partition.
2. Dirty partition write rewrites entire file.
3. Use deterministic row ordering (`record_key ASC`) on write.
4. Write pipeline uses: create temp file, write all rows, `flush + fsync`, atomic rename, and fsync parent directory when supported.

Rationale:

1. JSONL lines are variable-length; safe in-place single-row updates are not reliable.


## 10. SQLite Backend Contract

1. SQLite file is local acceleration artifact (not canonical, not committed).
2. Keep indexed columns: `record_key`, `partition`, `doc_date`, plus configured accelerator fields where needed.
3. Keep payload columns to reconstruct full uniform record.
4. `records()` must compile filters into SQL and stream via `fetchmany`.
5. `upsert_many(atomic=True)` must execute inside one transaction.


## 11. Backend Switching and Sync

Rules:

1. API shape and semantics must not change by backend choice.
2. `backend="json"`: direct JSONL reads/writes.
3. `backend="sqlite"`: primary read path from SQLite.
4. JSON remains source of truth for repository persistence.

Sync operations required:

1. Bootstrap SQLite from JSON.
2. Incrementally refresh SQLite from changed JSON partitions.
3. Export changed SQLite partitions back to JSON when explicitly requested.


## 12. Validation Rules

Hard validation errors:

1. `record_key` missing/empty/not-ASCII.
2. `doc_date` missing/invalid in `Record`.
3. Any model in `Ledger.__init__(sources=...)` missing required `record_key` or `doc_date` fields.
4. Source member `record_key` not equal to top-level `Record.record_key`.
5. Source member `doc_date` not equal to top-level `Record.doc_date`.
6. `partition != str(doc_date.year)`.
7. Duplicate `record_key` in same source partition.
8. Partition token invalid.

Soft warnings:

1. Missing source row on disk (auto-materialized).
2. Unknown extra fields.


## 13. Error Contract

Exception classes:

1. `RecordNotFoundError`
2. `DuplicateRecordKeyError`
3. `ValidationError`
4. `PartitionMismatchError`
5. `PersistenceError`
6. `QuerySyntaxError`

Error message requirements:

1. Include `record_key` when relevant.
2. Include partition when relevant.
3. Include offending `where` tuple index and tuple contents for query errors.


## 14. Concurrency and Safety

1. Single-writer model.
2. Optional lock file (`.ledger.lock`) to prevent concurrent writers.
3. Readers may run concurrently; they must never observe partially written files.
4. Crash during write must leave original file intact due to atomic rename flow.


## 15. Performance Targets

Targets on current dataset scale (~172k records, largest partition under 50 MB):

1. Key index build (cold): <= 2.0 s.
2. `load_year("2025")`: <= 1.5 s.
3. `get()` on loaded key p95: <= 2 ms.
4. `records()` scan with selective indexed filters: >= 20k rows/s effective throughput.
5. `upsert_many` for 1k records in one partition (in-memory): <= 150 ms.
6. `save(year)` for ~20k rows dirty partition: <= 2.5 s.


## 16. Testing Plan

### 16.1 Unit tests

1. Key normalization and partition normalization (`partition == str(doc_date.year)`).
2. Source config validation (directory/model shape and required `record_key` + `doc_date` fields).
3. Uniform record materialization with missing source rows.
4. CRUD semantics: insert/update/upsert/upsert_many.
5. Atomic rollback for `upsert_many(atomic=True)`.
6. Dynamic filter parser: valid and invalid DSL cases.
7. `order_by` parsing and deterministic ordering.
8. Validation rule coverage for hard vs soft violations.
9. Save stats shape and dirty-flag lifecycle.

### 16.2 Integration tests (JSON backend)

1. Load real partition and sample random keys.
2. Apply mixed batch updates, save, reload, verify persistence.
3. Partition rewrite correctness and deterministic output ordering.
4. Crash safety simulation by forced exception before rename.
5. Ensure only dirty partitions are rewritten.

### 16.3 Integration tests (SQLite backend)

1. Bootstrap from JSON and compare counts per partition.
2. Compare `get` and `count` against JSON backend.
3. Verify SQL filter correctness for `doc_date_between` and dynamic `where` operators.
4. Transaction rollback on atomic batch failure.

### 16.4 Backend parity tests

1. Same seed data, same API calls, same outputs.
2. Compare record sets for deterministic sorted query windows.
3. Compare post-save JSON output snapshots after identical mutations.

### 16.5 Performance tests

1. Cold start key index benchmark.
2. `load_year` latency benchmark.
3. Key lookup p50/p95 benchmark.
4. Filtered iteration throughput benchmark.
5. Batch upsert and save latency benchmark.

### 16.6 Fuzz and property tests

1. Generate random valid/invalid filter expressions.
2. Random mutation sequences followed by save/reload invariance check.
3. Property: `count(filters) == len(list(records(filters)))` for bounded sets.

### 16.7 Acceptance gates

Release criteria:

1. All unit and integration tests pass.
2. JSON/SQLite parity suite passes.
3. No data-loss in crash-safety tests.
4. Performance targets met or documented with measured deviations.


## 17. Implementation Phases

Phase 1:

1. Finalize API + filter grammar.
2. Implement JSON backend with full tests.
3. Ship read/query/save stable path.

Phase 2:

1. Implement SQLite backend.
2. Add parity and transaction tests.
3. Add bootstrap/incremental sync tooling.

Phase 3:

1. Performance tuning.
2. Optional cached key index artifact.
3. Operational docs and benchmarks in CI.
