"""Atomic file output and non-overwriting failure samples.

JSON/CSV updates are serialized with an advisory per-file lock, written and
fsynced to a same-directory temporary file, and published with os.replace().
This protects the previous file on serialization, disk-write and replacement
errors. It is not a multi-destination transaction or a power-loss guarantee.
"""

from __future__ import annotations

import csv
import io
import json
import os
import stat
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, TextIO


@contextmanager
def _output_lock(filename: str) -> Iterator[None]:
    """Serialize read/merge/replace; keep the lock inode stable between calls."""
    directory, basename = os.path.split(os.path.abspath(filename))
    lock_path = os.path.join(directory, f".{basename}.lock")
    with open(lock_path, "a+b") as lock:
        if os.name == "nt":
            import msvcrt

            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(b"\0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


@contextmanager
def _atomic_text_file(filename: str) -> Iterator[TextIO]:
    directory = os.path.dirname(os.path.abspath(filename))
    previous_mode: int | None = None
    try:
        previous_mode = stat.S_IMODE(os.stat(filename).st_mode)
    except FileNotFoundError:
        pass
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=directory,
            prefix=".parsedmarc-",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary_name = output.name
            if previous_mode is not None:
                os.chmod(temporary_name, previous_mode)
            yield output
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, filename)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def _json_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _merge_replay_rows(existing: list[Any], incoming: Sequence[Any]) -> list[Any]:
    """Suppress exact replays while preserving duplicate multiplicity in a batch.

    There is no universal report identifier for failure reports. Compare the
    complete serialized value, not subject or message count. A batch of two
    identical rows retains two rows; retrying it does not add two more.
    This cannot distinguish a byte-identical independent delivery in a later
    batch from a replay; source-level exactly-once requires a durable input ID.
    """
    stored = Counter(_json_key(row) for row in existing)
    encountered: Counter[str] = Counter()
    merged = list(existing)
    for row in incoming:
        key = _json_key(row)
        encountered[key] += 1
        if encountered[key] > stored[key]:
            merged.append(row)
    return merged


def append_json_file(
    filename: str, reports: Sequence[Any], *, deduplicate: bool = False
) -> None:
    """Atomically append JSON, refusing to overwrite unreadable/corrupt history.

    Missing and zero-byte files retain the historical empty-bootstrap
    semantics. Nonempty malformed JSON must be repaired explicitly.
    """
    if not reports:
        return
    with _output_lock(filename):
        try:
            with open(filename, encoding="utf-8") as source:
                existing = json.load(source)
        except FileNotFoundError:
            existing = []
        except json.JSONDecodeError as error:
            if error.doc == "":
                existing = []
            else:
                raise ValueError(
                    f"Refusing to overwrite invalid JSON history in {filename}: {error}"
                ) from error
        except UnicodeError as error:
            raise ValueError(
                f"Refusing to overwrite invalid JSON history in {filename}: {error}"
            ) from error
        if not isinstance(existing, list):
            raise ValueError(
                f"Expected a JSON array in {filename}; existing data was not changed"
            )
        merged = (
            _merge_replay_rows(existing, reports)
            if deduplicate
            else existing + list(reports)
        )
        if merged == existing:
            return
        with _atomic_text_file(filename) as output:
            json.dump(merged, output, ensure_ascii=False, indent=2)
            output.write("\n")


def _csv_rows(text: str) -> list[list[str]]:
    # The writer accepts large fields; the CSV reader's smaller default
    # must not reject the program's own output. Raise (never lower) the
    # process-wide parser limit so concurrent readers are not disrupted.
    if len(text) > csv.field_size_limit():
        csv.field_size_limit(min(sys.maxsize, len(text)))
    try:
        return list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except csv.Error as error:
        raise ValueError(f"Invalid CSV data: {error}") from error


def append_csv_file(filename: str, text: str, *, deduplicate: bool = False) -> None:
    """Atomically merge CSV with matching headers and optional exact replay checks."""
    incoming = _csv_rows(text)
    if not incoming:
        return
    header, *rows = incoming
    if not header or any(len(row) != len(header) for row in rows):
        raise ValueError("Invalid CSV output shape")
    with _output_lock(filename):
        try:
            with open(filename, encoding="utf-8", newline="") as source:
                existing = _csv_rows(source.read())
        except FileNotFoundError:
            existing = []
        except (csv.Error, UnicodeError) as error:
            raise ValueError(
                f"Refusing to overwrite invalid CSV history in {filename}: {error}"
            ) from error
        if existing:
            previous_header, *previous_rows = existing
            if previous_header != header or any(
                len(row) != len(header) for row in previous_rows
            ):
                raise ValueError(
                    f"CSV schema mismatch in {filename}; existing data was not changed"
                )
        else:
            previous_rows = []
        merged = (
            _merge_replay_rows(previous_rows, rows)
            if deduplicate
            else previous_rows + rows
        )
        if existing and merged == previous_rows:
            return
        with _atomic_text_file(filename) as output:
            writer = csv.writer(output)
            writer.writerow(header)
            writer.writerows(merged)


def write_sample_file(directory: str, subject: str, sample: str) -> str:
    """Write a sanitized-subject sample without replacing a different sample.

    The caller supplies a single, already-sanitized filename component.
    Publish a fully written temporary inode with an exclusive hard link so
    existing files and symlinks cannot be overwritten. Same-directory temp
    storage keeps both paths on the same filesystem. Exact retries reuse the
    existing sample. Unsupported hard links fail rather than lose a sample.
    """
    if (
        not subject
        or subject in (".", "..")
        or any(c in subject for c in ("/", "\\", "\0"))
    ):
        raise ValueError("A failure sample subject must be a safe filename component")
    encoded = sample.encode("utf-8")
    with _output_lock(os.path.join(directory, "samples")):
        sequence = 0
        while True:
            stem = subject if sequence == 0 else f"{subject} ({sequence})"
            path = os.path.join(directory, stem + ".eml")
            if os.path.lexists(path):
                # Never follow a pre-existing symlink to compare a sample.
                if not os.path.islink(path) and os.path.isfile(path):
                    with open(path, "rb") as previous:
                        if previous.read() == encoded:
                            return path
                sequence += 1
                continue
            temporary_name: str | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=directory,
                    prefix=".parsedmarc-sample-",
                    suffix=".tmp",
                    delete=False,
                ) as temporary:
                    temporary_name = temporary.name
                    temporary.write(encoded)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                try:
                    os.link(temporary_name, path)
                except FileExistsError:
                    # A writer not using our lock won the name. Compare or
                    # select a different name instead of overwriting it.
                    continue
                return path
            finally:
                if temporary_name is not None:
                    try:
                        os.unlink(temporary_name)
                    except FileNotFoundError:
                        pass


def save_json_csv_pair(
    json_filename: str,
    csv_filename: str,
    reports: Sequence[Any],
    render_csv: Callable[[Any], str],
    *,
    deduplicate: bool,
) -> None:
    """Persist report objects in JSON and regenerate their derived CSV view.

    Compare complete reports, never flattened CSV rows: different messages can
    legitimately have the same CSV representation. Serialize both paths in a
    fixed lock order. JSON is the authoritative history, so a retry can repair
    CSV after a failed second replacement without forgetting older reports.
    This is recoverable ordered publication, not a two-file transaction.

    Failure reports have no dependable unique ID; their caller must pass False
    to retain at-least-once delivery instead of guessing which events repeat.
    Refuse an orphaned nonempty CSV rather than erase history without its JSON.
    """
    from contextlib import ExitStack

    paths = sorted({os.path.abspath(json_filename), os.path.abspath(csv_filename)})
    if len(paths) != 2:
        raise ValueError("JSON and CSV output paths must be different")
    with ExitStack() as locks:
        for filename in paths:
            locks.enter_context(_output_lock(filename))
        json_exists = True
        try:
            with open(json_filename, encoding="utf-8") as source:
                existing = json.load(source)
        except FileNotFoundError:
            json_exists = False
            existing = []
        except json.JSONDecodeError as error:
            if error.doc == "":
                # A zero-byte file may be a bootstrap placeholder, but is
                # not a source from which nonempty CSV can be reconstructed.
                json_exists = False
                existing = []
            else:
                raise ValueError(
                    f"Refusing to overwrite invalid JSON history in {json_filename}: {error}"
                ) from error
        except UnicodeError as error:
            raise ValueError(
                f"Refusing to overwrite invalid JSON history in {json_filename}: {error}"
            ) from error
        if not isinstance(existing, list):
            raise ValueError(
                f"Expected a JSON array in {json_filename}; existing data was not changed"
            )
        try:
            with open(csv_filename, encoding="utf-8", newline="") as source:
                previous_csv = source.read()
        except FileNotFoundError:
            previous_csv = None
        if not json_exists and previous_csv and len(_csv_rows(previous_csv)) > 1:
            raise ValueError(
                f"CSV history in {csv_filename} has no JSON source; reconcile it before retrying"
            )
        merged = (
            _merge_replay_rows(existing, reports)
            if deduplicate
            else existing + list(reports)
        )
        # Rendering must succeed before either file is changed.
        csv_text = render_csv(merged)
        if not isinstance(csv_text, str):
            raise TypeError("The CSV renderer must return text")
        if merged != existing:
            with _atomic_text_file(json_filename) as output:
                json.dump(merged, output, ensure_ascii=False, indent=2)
                output.write("\n")
        if csv_text != previous_csv:
            with _atomic_text_file(csv_filename) as output:
                output.write(csv_text)
