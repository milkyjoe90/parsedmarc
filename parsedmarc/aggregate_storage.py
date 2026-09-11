"""Complete, retry-safe aggregate writes shared by the search backends.

A report is complete only when the multiset of its stored rows matches the
expected multiset. New rows have content-derived IDs (with an occurrence number
for genuinely identical rows), so retries are safe even before a search refresh.
Legacy randomly named rows are reused, never deleted or treated as proof that
an entire report was saved. Conflicting legacy data fails closed.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

AggregateReportKey = tuple[str, str | None, str, str]
_MGET_BATCH_SIZE = 500


class AggregateStorageError(RuntimeError):
    """The stored row set cannot safely be acknowledged or completed."""


def aggregate_report_key(report: Mapping[str, Any]) -> AggregateReportKey:
    """Use the same full, domain-scoped identity for ingestion and storage."""
    metadata = report["report_metadata"]
    report_id = metadata["report_id"]
    if report_id.startswith("<") and report_id.endswith(">"):
        report_id = report_id[1:-1]
    return (
        metadata["org_name"],
        metadata["org_email"],
        report["policy_published"]["domain"].lower(),
        report_id,
    )


def _stored_key(source: Mapping[str, Any]) -> AggregateReportKey | None:
    """A full-text search is a candidate search, not an exact identity test."""
    try:
        metadata = {
            "org_name": source["org_name"],
            "org_email": source.get("org_email"),
            "report_id": source["report_id"],
        }
        return aggregate_report_key(
            {
                "report_metadata": metadata,
                "policy_published": source["published_policy"],
            }
        )
    except (KeyError, TypeError, AttributeError):
        return None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _utc_text(value: Any) -> str:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _empty_to_none(value: Any) -> Any:
    # Document.save() omits empty strings, arrays and nulls by default.
    if value is None or value == "" or value == [] or value == {}:
        return None
    return value


def _result_rows(value: Any, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    rows = value or []
    if isinstance(rows, Mapping):
        rows = [rows]
    projected = [
        {field: _empty_to_none(row.get(field)) for field in fields} for row in rows
    ]
    return sorted(projected, key=_json)


def _row_signature(source: Mapping[str, Any]) -> str:
    """Hash the stored report facts, excluding variable IP enrichment.

    Ignore source attribution, reporter diagnostics, namespace spelling and
    composed dashboard labels; none determines the underlying row count.
    Authentication results, policy, identities and UTC intervals do. Use the
    same projection for prospective DSL documents and actual stored _source.
    """
    policy = source["published_policy"]
    normalized_policy = {
        field: _empty_to_none(policy.get(field))
        for field in (
            "domain",
            "adkim",
            "aspf",
            "p",
            "sp",
            "pct",
            "fo",
            "np",
            "testing",
            "discovery_method",
        )
    }
    normalized_policy["domain"] = policy["domain"].lower()
    if normalized_policy["pct"] is not None:
        normalized_policy["pct"] = int(normalized_policy["pct"])
    row = {
        "begin": _utc_text(source["date_begin"]),
        "end": _utc_text(source["date_end"]),
        "ip": str(ipaddress.ip_address(source["source_ip_address"])),
        "count": int(source["message_count"]),
        "policy": normalized_policy,
        "disposition": source.get("disposition"),
        "dkim_aligned": source.get("dkim_aligned", False),
        "spf_aligned": source.get("spf_aligned", False),
        "normalized_timespan": source.get("normalized_timespan", False),
        "identifiers": {
            field: _empty_to_none(source.get(field))
            for field in ("header_from", "envelope_from", "envelope_to")
        },
        "dkim": _result_rows(
            source.get("dkim_results"), ("domain", "selector", "result", "human_result")
        ),
        "spf": _result_rows(
            source.get("spf_results"), ("domain", "scope", "result", "human_result")
        ),
        "overrides": _result_rows(source.get("policy_overrides"), ("type", "comment")),
    }
    return hashlib.sha256(_json(row).encode("utf-8")).hexdigest()


def save_aggregate_documents(
    report: Mapping[str, Any],
    documents: Sequence[Any],
    candidates: Iterable[Any],
    client: Any,
) -> bool:
    """Reconcile and write a complete expected row set.

    ``documents`` and ``candidates`` are actual backend DSL documents/hits;
    ``client`` is its actual Elasticsearch/OpenSearch connection. Returns
    False only when the full expected row multiset was already present.
    Acknowledged writes (not a hit count) determine success on a new/partial
    report. No state index, delete privileges or destructive migration is used.

    MGET is explicitly real-time and every per-document error is checked.
    Search is used only to reconcile legacy IDs and previously chosen index
    layouts. Run only one parsedmarc version during upgrade: an old writer
    still creating random IDs cannot participate in the new idempotency scheme.
    """
    if not documents:
        raise AggregateStorageError(
            "Cannot acknowledge an aggregate report with no rows"
        )
    key = aggregate_report_key(report)
    report_hash = hashlib.sha256(_json(key).encode("utf-8")).hexdigest()
    signatures = [_row_signature(document.to_dict()) for document in documents]
    expected = Counter(signatures)
    occurrences: Counter[str] = Counter()
    descriptors: list[dict[str, str]] = []
    for document, signature in zip(documents, signatures):
        ordinal = occurrences[signature]
        occurrences[signature] += 1
        document_id = f"parsedmarc-v1-{report_hash}-{signature}-{ordinal}"
        document.meta.id = document_id
        descriptors.append({"_index": document.meta.index, "_id": document_id})

    # Remember physical IDs so a row returned by both scan and MGET counts
    # only once. Preserve multiplicity for distinct, identical report rows.
    physical: set[tuple[str, str]] = set()
    present: Counter[str] = Counter()
    canonical_present: set[tuple[str, str]] = set()

    def observe(
        index: str, doc_id: str, source: Mapping[str, Any], *, exact: bool
    ) -> None:
        if _stored_key(source) != key:
            if exact:
                raise AggregateStorageError(
                    "A deterministic aggregate ID has a conflicting identity"
                )
            return
        signature = _row_signature(source)
        identity = (index, doc_id)
        if identity in physical:
            return
        physical.add(identity)
        present[signature] += 1
        if present[signature] > expected[signature]:
            raise AggregateStorageError(
                "Existing aggregate rows conflict with or exceed the source report; "
                "retain the source and reconcile the stored report before retrying"
            )

    # scan(), not execute()'s first page: all rows and all shards must be read.
    # Use its hits only to discover legacy IDs. Verify every candidate through
    # real-time MGET; a stale search hit is not proof that a row still exists.
    requested = {(item["_index"], item["_id"]): item for item in descriptors}
    targets = set(requested)
    for hit in candidates:
        if _stored_key(hit.to_dict()) == key:
            identity = (hit.meta.index, hit.meta.id)
            requested[identity] = {"_index": identity[0], "_id": identity[1]}
    verification = list(requested.values())

    # Newly written IDs may not be visible to search yet. MGET reads them
    # directly without depending on refresh intervals or a result-page limit.
    for start in range(0, len(verification), _MGET_BATCH_SIZE):
        batch = verification[start : start + _MGET_BATCH_SIZE]
        response = client.mget(body={"docs": batch}, realtime=True)
        results = response["docs"]
        if len(results) != len(batch):
            raise AggregateStorageError(
                "Incomplete MGET response while checking aggregate rows"
            )
        for descriptor, result in zip(batch, results):
            if "error" in result:
                if result["error"].get("type") == "index_not_found_exception":
                    continue
                raise AggregateStorageError(
                    f"Could not verify aggregate row: {result['error']}"
                )
            if result.get("found") is False:
                continue
            if result.get("found") is not True or not isinstance(
                result.get("_source"), Mapping
            ):
                raise AggregateStorageError(
                    "Aggregate MGET result is missing its source or status"
                )
            identity = (result["_index"], result["_id"])
            if identity != (descriptor["_index"], descriptor["_id"]):
                raise AggregateStorageError(
                    "Aggregate MGET returned an unexpected document ID"
                )
            if identity in targets:
                # A canonical ID includes the row signature. Verify the
                # payload too, rather than trusting existence at that ID.
                wanted = descriptor["_id"].rsplit("-", 2)[1]
                if _row_signature(result["_source"]) != wanted:
                    raise AggregateStorageError(
                        "A deterministic aggregate row has conflicting content"
                    )
                canonical_present.add(identity)
            observe(*identity, result["_source"], exact=True)

    if present == expected:
        return False

    # Reserve each already-persisted canonical row for its own occurrence,
    # and consume any remaining legacy matches only for missing occurrences.
    # Otherwise a legacy match could consume an existing canonical slot and
    # cause the still-missing slot to be skipped.
    legacy_matches = present.copy()
    for descriptor, signature in zip(descriptors, signatures):
        if (descriptor["_index"], descriptor["_id"]) in canonical_present:
            legacy_matches[signature] -= 1

    for document, descriptor, signature in zip(documents, descriptors, signatures):
        if (descriptor["_index"], descriptor["_id"]) in canonical_present:
            continue
        if legacy_matches[signature] > 0:
            legacy_matches[signature] -= 1
            continue
        document.save()
    return True
