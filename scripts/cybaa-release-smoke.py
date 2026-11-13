#!/usr/bin/env python3
"""Verify a Cybaa distribution installed outside the source checkout.

RFC 9990 section 3.1.1.4 defines aggregate date_range epochs in UTC:
https://www.rfc-editor.org/rfc/rfc9990.html#section-3.1.1.4
The interval checks exercise DST folds without changing deliberately local
report metadata or the public timestamp conversion helpers.
"""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from importlib.metadata import version

import parsedmarc
from parsedmarc.types import AggregateReport
from parsedmarc.utils import timestamp_to_datetime, timestamp_to_human


def report_xml(begin: str, end: str, count: int) -> str:
    """Build a wholly synthetic report using documentation-only addresses."""
    begin_epoch = int(datetime.fromisoformat(begin).timestamp())
    end_epoch = int(datetime.fromisoformat(end).timestamp())
    return f"""<?xml version="1.0"?>
<feedback>
  <report_metadata>
    <org_name>Example Reporter</org_name><email>dmarc@example.net</email>
    <report_id>cybaa-installed-smoke</report_id>
    <date_range><begin>{begin_epoch}</begin><end>{end_epoch}</end></date_range>
  </report_metadata>
  <policy_published><domain>example.com</domain><p>none</p></policy_published>
  <record>
    <row><source_ip>192.0.2.1</source_ip><count>{count}</count>
      <policy_evaluated><disposition>none</disposition><dkim>pass</dkim><spf>pass</spf></policy_evaluated>
    </row>
    <identifiers><header_from>example.com</header_from></identifiers>
    <auth_results><spf><domain>example.com</domain><result>pass</result></spf></auth_results>
  </record>
</feedback>"""


def check_report(
    report: AggregateReport,
    begin: str,
    end: str,
    expected_records: list[tuple[str, str, int]],
) -> None:
    """Assert source epochs, local metadata, and exact UTC record buckets."""
    begin_epoch = int(datetime.fromisoformat(begin).timestamp())
    end_epoch = int(datetime.fromisoformat(end).timestamp())
    metadata = report["report_metadata"]
    assert metadata["begin_date"] == timestamp_to_human(begin_epoch)
    assert metadata["end_date"] == timestamp_to_human(end_epoch)
    assert metadata["original_timespan_seconds"] == end_epoch - begin_epoch
    assert timestamp_to_datetime(begin_epoch) == datetime.fromtimestamp(begin_epoch)
    actual = [
        (row["interval_begin"], row["interval_end"], row["count"])
        for row in report["records"]
    ]
    assert actual == expected_records, (actual, expected_records)


def main(expected_version: str) -> None:
    assert re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\+cybaa\.[1-9][0-9]*", expected_version)
    assert version("parsedmarc") == expected_version
    assert parsedmarc.__version__ == expected_version
    assert (
        Path(parsedmarc.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    )
    cli = Path(sys.prefix) / "bin" / "parsedmarc"
    # Configuration environment variables may enable remote outputs. The smoke
    # run exercises only the explicit offline synthetic file and local JSON.
    cli_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PARSEDMARC_")
    }
    cli_env["GITHUB_ACTIONS"] = "true"
    cli_version = subprocess.run(
        [str(cli), "--version"],
        check=True,
        capture_output=True,
        text=True,
        env=cli_env,
        timeout=30,
    )
    assert cli_version.stdout.strip() == expected_version

    cases = [
        (
            "Europe/London",
            "2026-10-25T00:45:00+00:00",
            "2026-10-25T01:15:00+00:00",
            7,
            [("2026-10-25 00:45:00", "2026-10-25 01:15:00", 7)],
        ),
        (
            "America/New_York",
            "2026-11-01T06:15:00+00:00",
            "2026-11-01T06:45:00+00:00",
            7,
            [("2026-11-01 06:15:00", "2026-11-01 06:45:00", 7)],
        ),
        (
            "America/New_York",
            "2026-11-01T06:15:00+00:00",
            "2026-11-02T07:15:00+00:00",
            100,
            [
                ("2026-11-01 06:15:00", "2026-11-02 00:00:00", 71),
                ("2026-11-02 00:00:00", "2026-11-02 07:15:00", 29),
            ],
        ),
    ]
    original_tz = os.environ.get("TZ")
    try:
        for index, (zone, begin, end, count, expected_records) in enumerate(cases):
            os.environ["TZ"] = zone
            time.tzset()
            xml = report_xml(begin, end, count)
            report = parsedmarc.parse_aggregate_report_xml(xml, offline=True)
            check_report(report, begin, end, expected_records)
            with tempfile.TemporaryDirectory(
                prefix=f"cybaa-smoke-{index}-"
            ) as temporary:
                directory = Path(temporary)
                source = directory / "report.xml"
                source.write_text(xml, encoding="utf-8")
                output = directory / "output"
                subprocess.run(
                    [
                        str(cli),
                        "--offline",
                        "--silent",
                        "--output",
                        str(output),
                        str(source),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    env={**cli_env, "TZ": zone},
                    timeout=60,
                )
                reports = json.loads(
                    (output / "aggregate.json").read_text(encoding="utf-8")
                )
                assert len(reports) == 1
                check_report(reports[0], begin, end, expected_records)
    finally:
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        time.tzset()
    print(
        f"Verified installed parsedmarc {expected_version}: metadata, CLI, DST intervals, 71/29 buckets"
    )


if __name__ == "__main__":
    main(sys.argv[1])
