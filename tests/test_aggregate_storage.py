"""Observable persistence tests at the search SDK document/client boundary."""

from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from parsedmarc.aggregate_storage import AggregateStorageError, save_aggregate_documents


def report(
    email: str = "rua@reporter.example", report_id: str = "full@id.example"
) -> dict[str, Any]:
    return {
        "report_metadata": {
            "org_name": "Reporter",
            "org_email": email,
            "report_id": report_id,
        },
        "policy_published": {"domain": "example.com"},
    }


def source(count: int = 1, **overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        **report()["report_metadata"],
        "published_policy": {
            "domain": "example.com",
            "p": "reject",
            "adkim": "r",
            "aspf": "r",
            "sp": "reject",
        },
        "date_begin": "2026-07-01T00:00:00+00:00",
        "date_end": "2026-07-01T23:59:59+00:00",
        "source_ip_address": "192.0.2.10",
        "message_count": count,
        "header_from": "example.com",
        "envelope_from": "example.com",
        "disposition": "none",
        "dkim_aligned": True,
        "spf_aligned": False,
        "normalized_timespan": False,
        "dkim_results": [{"domain": "example.com", "selector": "s", "result": "pass"}],
    }
    value.update(overrides)
    return value


class DocumentBoundary:
    def __init__(
        self,
        client: ClientBoundary,
        value: dict[str, Any],
        *,
        index: str = "dmarc_aggregate-2026-07-01",
        doc_id: str | None = None,
    ):
        self.client = client
        self.value = deepcopy(value)
        self.meta = SimpleNamespace(index=index, id=doc_id)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.value)

    def save(self) -> None:
        self.client.index(self.meta.index, self.meta.id, self.value)


class ClientBoundary:
    def __init__(self) -> None:
        self.stored: dict[tuple[str, str], dict[str, Any]] = {}
        self.writes = 0
        self.fail_at: int | None = None
        self.fail_after_commit = False
        self.mget_error: dict[str, str] | None = None
        self.mget_batches: list[int] = []

    def index(self, index: str, doc_id: str, value: dict[str, Any]) -> None:
        self.writes += 1
        if self.writes == self.fail_at and not self.fail_after_commit:
            raise OSError("simulated second-row write failure")
        self.stored[(index, doc_id)] = deepcopy(value)
        if self.writes == self.fail_at:
            raise OSError("write accepted but acknowledgment lost")

    def mget(self, *, body: dict[str, Any], realtime: bool) -> dict[str, Any]:
        assert realtime is True
        self.mget_batches.append(len(body["docs"]))
        result = []
        for descriptor in body["docs"]:
            item = dict(descriptor)
            if self.mget_error is not None:
                item["error"] = self.mget_error
            else:
                existing = self.stored.get((descriptor["_index"], descriptor["_id"]))
                item["found"] = existing is not None
                if existing is not None:
                    item["_source"] = deepcopy(existing)
            result.append(item)
        return {"docs": result}

    def candidates(self) -> list[DocumentBoundary]:
        return [
            DocumentBoundary(self, value, index=index, doc_id=doc_id)
            for (index, doc_id), value in self.stored.items()
        ]

    def documents(self, values: list[dict[str, Any]]) -> list[DocumentBoundary]:
        return [DocumentBoundary(self, value) for value in values]


class TestAggregateStorage(unittest.TestCase):
    def test_partial_save_then_retry_before_search_refresh(self):
        client = ClientBoundary()
        values = [source(1), source(7), source(9)]
        client.fail_at = 2
        with self.assertRaises(OSError):
            save_aggregate_documents(report(), client.documents(values), [], client)
        self.assertEqual(len(client.stored), 1)
        client.fail_at = None
        self.assertTrue(
            save_aggregate_documents(report(), client.documents(values), [], client)
        )
        self.assertEqual(
            sorted(row["message_count"] for row in client.stored.values()), [1, 7, 9]
        )
        writes = client.writes
        self.assertFalse(
            save_aggregate_documents(report(), client.documents(values), [], client)
        )
        self.assertEqual(client.writes, writes)

    def test_lost_acknowledgment_does_not_duplicate_rows(self):
        client = ClientBoundary()
        client.fail_at = 2
        client.fail_after_commit = True
        values = [source(2), source(5), source(8)]
        with self.assertRaises(OSError):
            save_aggregate_documents(report(), client.documents(values), [], client)
        self.assertEqual(len(client.stored), 2)
        client.fail_at = None
        save_aggregate_documents(report(), client.documents(values), [], client)
        self.assertEqual(len(client.stored), 3)
        self.assertEqual(
            sum(row["message_count"] for row in client.stored.values()), 15
        )

    def test_complete_legacy_report_is_checked_in_full(self):
        client = ClientBoundary()
        values = [source(i) for i in range(1, 22)]
        for i, value in enumerate(values):
            client.stored[("dmarc_aggregate-2026-07-01", f"legacy-{i}")] = value
        self.assertFalse(
            save_aggregate_documents(
                report(), client.documents(values), client.candidates(), client
            )
        )
        self.assertEqual(client.writes, 0)

    def test_partial_legacy_rows_are_reused_without_deletion(self):
        client = ClientBoundary()
        existing_id = ("dmarc_aggregate-2026-07-01", "legacy-random")
        client.stored[existing_id] = source(7)
        values = [source(1), source(7), source(9)]
        self.assertTrue(
            save_aggregate_documents(
                report(), client.documents(values), client.candidates(), client
            )
        )
        self.assertIn(existing_id, client.stored)
        self.assertEqual(client.writes, 2)
        self.assertEqual(len(client.stored), 3)

    def test_duplicate_row_multiplicity_is_preserved(self):
        client = ClientBoundary()
        values = [source(4), source(4), source(4)]
        save_aggregate_documents(report(), client.documents(values), [], client)
        self.assertEqual(len(client.stored), 3)
        self.assertEqual(
            sum(row["message_count"] for row in client.stored.values()), 12
        )
        self.assertFalse(
            save_aggregate_documents(
                report(), client.documents(values), client.candidates(), client
            )
        )

    def test_mixed_legacy_and_later_canonical_occurrence(self):
        client = ClientBoundary()
        values = [source(4)] * 3
        docs = client.documents(values)
        save_aggregate_documents(report(), docs, [], client)
        later_id = (docs[1].meta.index, docs[1].meta.id)
        client.stored = {later_id: source(4), (docs[0].meta.index, "legacy"): source(4)}
        client.writes = 0
        save_aggregate_documents(
            report(), client.documents(values), client.candidates(), client
        )
        self.assertEqual(client.writes, 1)
        self.assertEqual(len(client.stored), 3)

    def test_order_and_ip_enrichment_do_not_change_identity(self):
        client = ClientBoundary()
        values = [source(3), source(8)]
        save_aggregate_documents(report(), client.documents(values), [], client)
        changed = [
            dict(values[1], source_country="GB", source_reverse_dns="new.example"),
            values[0],
        ]
        self.assertFalse(
            save_aggregate_documents(report(), client.documents(changed), [], client)
        )
        self.assertEqual(len(client.stored), 2)

    def test_optional_report_id_brackets_and_domain_case_match(self):
        client = ClientBoundary()
        save_aggregate_documents(report(), client.documents([source()]), [], client)
        wrapped = report(report_id="<full@id.example>")
        wrapped["policy_published"]["domain"] = "EXAMPLE.COM"
        value = source(report_id="<full@id.example>")
        value["published_policy"]["domain"] = "EXAMPLE.COM"
        self.assertFalse(
            save_aggregate_documents(wrapped, client.documents([value]), [], client)
        )

    def test_different_contact_does_not_collide(self):
        client = ClientBoundary()
        save_aggregate_documents(report(), client.documents([source()]), [], client)
        other = "other@reporter.example"
        self.assertTrue(
            save_aggregate_documents(
                report(email=other),
                client.documents([source(org_email=other)]),
                client.candidates(),
                client,
            )
        )
        self.assertEqual(len(client.stored), 2)

    def test_conflicting_legacy_content_fails_before_any_write(self):
        client = ClientBoundary()
        client.stored[("dmarc_aggregate-2026-07-01", "legacy")] = source(99)
        with self.assertRaises(AggregateStorageError):
            save_aggregate_documents(
                report(), client.documents([source(3)]), client.candidates(), client
            )
        self.assertEqual(client.writes, 0)
        self.assertEqual(next(iter(client.stored.values()))["message_count"], 99)

    def test_excess_duplicate_legacy_rows_are_not_acknowledged(self):
        client = ClientBoundary()
        for i in range(2):
            client.stored[("dmarc_aggregate-2026-07-01", str(i))] = source()
        with self.assertRaises(AggregateStorageError):
            save_aggregate_documents(
                report(), client.documents([source()]), client.candidates(), client
            )
        self.assertEqual(client.writes, 0)

    def test_partial_mget_failure_is_not_treated_as_missing(self):
        client = ClientBoundary()
        client.mget_error = {
            "type": "unavailable_shards_exception",
            "reason": "offline shard",
        }
        with self.assertRaisesRegex(AggregateStorageError, "Could not verify"):
            save_aggregate_documents(report(), client.documents([source()]), [], client)
        self.assertEqual(client.writes, 0)

    def test_missing_index_is_a_new_destination(self):
        client = ClientBoundary()
        client.mget_error = {"type": "index_not_found_exception"}
        self.assertTrue(
            save_aggregate_documents(report(), client.documents([source()]), [], client)
        )
        self.assertEqual(client.writes, 1)

    def test_stale_search_hit_is_not_completion(self):
        client = ClientBoundary()
        stale = DocumentBoundary(client, source(), doc_id="deleted-legacy")
        self.assertTrue(
            save_aggregate_documents(
                report(), client.documents([source()]), [stale], client
            )
        )
        self.assertEqual(len(client.stored), 1)

    def test_search_failure_does_not_start_writing(self):
        client = ClientBoundary()

        def broken_scan():
            yield DocumentBoundary(client, source(), doc_id="a")
            raise OSError("shard unavailable during scan")

        with self.assertRaises(OSError):
            save_aggregate_documents(
                report(), client.documents([source()]), broken_scan(), client
            )
        self.assertEqual(client.writes, 0)

    def test_same_row_seen_in_scan_and_mget_counts_once(self):
        client = ClientBoundary()
        save_aggregate_documents(report(), client.documents([source()]), [], client)
        self.assertFalse(
            save_aggregate_documents(
                report(), client.documents([source()]), client.candidates(), client
            )
        )

    def test_datetime_representations_do_not_change_row_ids(self):
        client = ClientBoundary()
        save_aggregate_documents(report(), client.documents([source()]), [], client)
        value = source(
            date_begin=datetime(2026, 7, 1, tzinfo=timezone.utc),
            date_end=datetime(2026, 7, 1, 23, 59, 59),
        )
        self.assertFalse(
            save_aggregate_documents(report(), client.documents([value]), [], client)
        )

    def test_mget_requests_are_bounded(self):
        client = ClientBoundary()
        save_aggregate_documents(
            report(), client.documents([source(i) for i in range(1001)]), [], client
        )
        self.assertEqual(client.mget_batches, [500, 500, 1])
        self.assertEqual(len(client.stored), 1001)

    def test_empty_report_cannot_be_acknowledged(self):
        with self.assertRaises(AggregateStorageError):
            save_aggregate_documents(report(), [], [], ClientBoundary())

    def test_tampered_canonical_content_is_not_acknowledged(self):
        client = ClientBoundary()
        save_aggregate_documents(report(), client.documents([source()]), [], client)
        key = next(iter(client.stored))
        client.stored[key]["message_count"] = 900
        with self.assertRaisesRegex(AggregateStorageError, "conflicting content"):
            save_aggregate_documents(report(), client.documents([source()]), [], client)
