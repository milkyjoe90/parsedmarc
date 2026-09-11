"""Exercise retries against disposable live search services, not mocked storage.

Elasticsearch/OpenSearch multi-get is real-time. Only the SDK Document.save
boundary is wrapped to inject a partial-write failure or a lost acknowledgment.
"""
import copy
import os
import sys
import time
import uuid
from unittest.mock import patch

sys.path.insert(0, os.getcwd())
import parsedmarc.elastic as elastic
import parsedmarc.opensearch as opensearch
from tests.test_elastic import _aggregate_report


def verify(module, endpoint, label):
    client = module.set_hosts(endpoint, use_ssl=False)
    save = getattr(module, 'save_aggregate_report_to_' + ('elasticsearch' if label == 'elastic' else 'opensearch'))
    error_type = module.ElasticsearchError if label == 'elastic' else module.OpenSearchError
    run_prefix = f'validation_{label}_{uuid.uuid4().hex}_'
    report = _aggregate_report()
    report['report_metadata']['begin_date'] = '2026-07-15 00:00:00'
    report['report_metadata']['end_date'] = '2026-07-16 00:00:00'
    first = report['records'][0]
    first['interval_begin'] = report['report_metadata']['begin_date']
    first['interval_end'] = report['report_metadata']['end_date']
    second = copy.deepcopy(first)
    second['source']['ip_address'] = '192.0.2.2'
    second['count'] = 9
    report['records'] = [first, second, copy.deepcopy(first)]
    original = copy.deepcopy(report)
    real_save = module.Document.save
    previous_tz = os.environ.get('TZ')
    try:
        for case, acknowledge_lost in [('partial', False), ('lost_ack', True)]:
            prefix = run_prefix + case + '_'
            pattern = prefix + 'dmarc_aggregate*'
            calls = 0

            def flaky_save(document, *args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2 and not acknowledge_lost:
                    raise RuntimeError('injected second-row failure')
                result = real_save(document, *args, **kwargs)
                if calls == 2 and acknowledge_lost:
                    raise RuntimeError('injected lost acknowledgment after write')
                return result

            with patch.object(module.Document, 'save', autospec=True, side_effect=flaky_save):
                try:
                    save(report, index_prefix=prefix)
                except error_type as error:
                    assert 'injected' in str(error), str(error)
                else:
                    raise AssertionError('Expected injected storage failure')
            if not acknowledge_lost:
                client.indices.refresh(index=pattern)
                assert client.count(index=pattern)['count'] == 1
            # The lost-ack case deliberately retries without an explicit refresh.
            os.environ['TZ'] = 'Europe/London'
            time.tzset()
            save(report, index_prefix=prefix)
            client.indices.refresh(index=pattern)
            assert client.count(index=pattern)['count'] == 3
            hits = client.search(index=pattern, body={'query': {'match_all': {}}, 'size': 100})['hits']['hits']
            assert sum(hit['_source']['message_count'] for hit in hits) == 17
            assert report == original
            os.environ['TZ'] = 'America/New_York'
            time.tzset()
            try:
                save(report, index_prefix=prefix)
            except module.AlreadySaved:
                pass
            else:
                raise AssertionError('Complete duplicate was not recognized')
            client.indices.refresh(index=pattern)
            assert client.count(index=pattern)['count'] == 3
            print(label, case, 'PASS: all three rows exactly once, duplicate counts preserved, timezone-independent retry')

        legacy_prefix = run_prefix + 'legacy_'
        legacy_pattern = legacy_prefix + 'dmarc_aggregate*'
        legacy_source = copy.deepcopy(hits[0]['_source'])
        for name in ('aggregate_report_key', 'aggregate_row_signature', 'aggregate_expected_rows'):
            legacy_source.pop(name, None)
        legacy_index = legacy_prefix + 'dmarc_aggregate-2026-07-15'
        if label == 'elastic':
            client.index(index=legacy_index, id='legacy-row', document=legacy_source)
        else:
            client.index(index=legacy_index, id='legacy-row', body=legacy_source)
        client.indices.refresh(index=legacy_index)
        save(report, index_prefix=legacy_prefix)
        client.indices.refresh(index=legacy_pattern)
        assert client.count(index=legacy_pattern)['count'] == 3
        assert client.get(index=legacy_index, id='legacy-row')['_source'] == legacy_source
        other_reporter = copy.deepcopy(report)
        other_reporter['report_metadata']['org_email'] = 'distinct-reporter@example.com'
        save(other_reporter, index_prefix=legacy_prefix)
        client.indices.refresh(index=legacy_pattern)
        assert client.count(index=legacy_pattern)['count'] == 6
        print(label, 'legacy/contact', 'PASS: legacy row retained, remaining rows repaired, distinct contact not discarded')
    finally:
        if previous_tz is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = previous_tz
        time.tzset()
        try:
            # Delete only explicit UUID-scoped disposable index names. Do not
            # weaken the cluster's wildcard-deletion safety setting.
            indexes = client.indices.get(index=run_prefix + '*', allow_no_indices=True, ignore_unavailable=True)
            for index in indexes:
                assert index.startswith(run_prefix)
                client.indices.delete(index=index)
        finally:
            client.close()


verify(elastic, 'http://localhost:9200', 'elastic')
verify(opensearch, 'http://localhost:9201', 'opensearch')
print('LIVE_SEARCH_VALIDATION=PASS')
