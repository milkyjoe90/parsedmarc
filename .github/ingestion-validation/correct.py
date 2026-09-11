"""Small integration corrections to the reviewed implementation candidate."""
import ast
from pathlib import Path


def replace_once(path, old, new):
    text = path.read_text()
    assert text.count(old) == 1, (str(path), old[:100], text.count(old))
    path.write_text(text.replace(old, new, 1))


p = Path('parsedmarc/output_storage.py')
replace_once(p, 'from typing import Any, TextIO', 'from typing import IO, Any')
replace_once(p, 'def _atomic_text_file(filename: str) -> Iterator[TextIO]:',
             'def _atomic_text_file(filename: str) -> Iterator[IO[str]]:')
replace_once(p, '            yield output\n', '            yield output.file\n')
p = Path('tests/test_init.py')
text = p.read_text()
functions = {
    'test_result_zip_has_unique_safe_paths_and_no_lock_artifacts',
    'test_save_output_identical_retry_keeps_json_and_csv_in_step',
}
spans = []
for node in ast.walk(ast.parse(text)):
    if isinstance(node, ast.FunctionDef) and node.name in functions:
        spans.append((node.lineno, node.end_lineno))
assert len(spans) == 2
lines = text.splitlines(keepends=True)
for first, last in sorted(spans, reverse=True):
    part = ''.join(lines[first-1:last])
    assert part.count('        results = {') == 1
    lines[first-1:last] = [part.replace('        results = {', '        results: ParsingResults = {')]
p.write_text(''.join(lines))
replace_once(p,
    '                result = parsedmarc._decode_mime_payload(part, part.get_payload())',
    '                fallback = part.get_payload()\n'
    '                assert isinstance(fallback, str)\n'
    '                result = parsedmarc._decode_mime_payload(part, fallback)')
text = p.read_text()
lines = text.splitlines(keepends=True)
for node in ast.walk(ast.parse(text)):
    if isinstance(node, ast.FunctionDef) and node.name == 'test_existing_file_with_non_list_root_is_overwritten':
        replacement = '''    def test_existing_non_array_history_is_preserved(self):
        """An unexpected JSON root is not permission to erase prior data."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "aggregate.json"
            original = b'{"not": "a list"}'
            path.write_bytes(original)
            with self.assertRaisesRegex(ValueError, "Expected a JSON array"):
                parsedmarc.append_json(
                    str(path), cast(list[AggregateReport], [{"new": "data"}])
                )
            self.assertEqual(path.read_bytes(), original)
'''
        lines[node.lineno-1:node.end_lineno] = [replacement]
        break
else:
    raise AssertionError('Expected legacy overwrite test was not found')
p.write_text(''.join(lines))
for filename in ('parsedmarc/output_storage.py', 'tests/test_init.py'):
    ast.parse(Path(filename).read_text(), feature_version=(3, 10))

usage = Path('docs/source/usage.md')
section = '''

## Ingestion recovery and output history

Local input files and aggregate duplicate keys are acknowledged only after
all configured report outputs accept the batch. A failed output leaves the
source available for retry. Mailbox retry limits and the `Unsaved` folder
continue to apply. These guarantees are not a distributed transaction:
destinations that already succeeded may receive the retry again.

Elasticsearch and OpenSearch no longer consider a single matching aggregate
row a complete report. Each expected row is reconciled using the reporting
organization, contact email, policy domain and full Report-ID, plus the row
contents and duplicate occurrence number. New rows use deterministic IDs;
real-time multi-get checks avoid depending on search refresh timing. Matching
legacy rows are reused without deleting or rewriting historical documents.
Conflicting or unverifiable stored rows cause an output error and retain the
source for investigation. This does not automatically repair reports that
were partially saved and whose original inputs have already been removed.

File output treats the JSON array as authoritative history and rebuilds its
CSV view from complete report objects. JSON and CSV writes use temporary
files and atomic replacement under advisory locks. Nonempty corrupt JSON,
a non-array JSON root, and nonempty CSV without its JSON history are rejected
rather than silently overwritten. Preserve both files when rotating or moving
output history. To recover an inconsistent pair, back it up and restore or
repair the JSON first; a subsequent successful save regenerates the CSV.
Do not delete history merely to bypass a diagnostic.

Exact aggregate and TLS payload replays are suppressed while duplicate
multiplicity within an input batch is retained. Failure reports remain
at-least-once: identical-looking failure events cannot safely be identified
as duplicates without a durable source ID. A failure sample reuses an
identical existing file, while different content with the same sanitized
subject receives a numeric suffix instead of replacing the old file.

File output requires a filesystem that supports the advisory locks used by
the host and hard links for exclusive sample publication. Lock files are
internal sidecars, not report data, and must not be removed while a writer
is active. Files are flushed before replacement, but this is neither a
multi-file transaction nor a guarantee against every power-loss scenario.
JSON/CSV rewriting grows with accumulated history; rotate history as a pair
or use a database output for sustained high-volume ingestion.

Mbox imports preserve bytes, isolate known invalid report content and retain
the original mbox unchanged. Operational failures still propagate, and an
aborted import does not commit the pending aggregate duplicate keys.
'''
assert '## Ingestion recovery and output history' not in usage.read_text()
usage.write_text(usage.read_text() + section)
print('Corrected five type diagnostics, replaced the legacy destructive expectation, and documented recovery semantics.')
