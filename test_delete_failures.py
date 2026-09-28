import unittest
import xml.etree.ElementTree as ET
import threading
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import sf_bulk_loader as loader


class DeleteFailureTests(unittest.TestCase):
    def setUp(self):
        loader.clear_stop_flag()
        self.addCleanup(loader.clear_stop_flag)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.error_path = str(Path(self.directory.name) / 'failed.csv')
        self.rows = pd.DataFrame({'Id': ['001000000000001AAA', '001000000000002AAA']})
        resolver = patch.object(loader, 'resolve_sf_object_api_name', return_value='Account')
        resolver.start()
        self.addCleanup(resolver.stop)

    def run_delete(self, on_status=None, on_error=None):
        return loader.bulk_delete_v2(
            'unused.csv', 'Account', Mock(), id_column='Id', error_file=self.error_path,
            num_parallel_chunks=1, chunk_iterator=iter([self.rows]),
            on_status=on_status, on_error=on_error
        )

    def test_serial_delete_job_xml_schema_order(self):
        payload = loader._bulk1_build_job_payload('delete', 'Account', concurrency_mode='Serial')
        root = ET.fromstring(payload)
        self.assertEqual(root.tag, '{http://www.force.com/2009/06/asyncapi/dataload}jobInfo')
        self.assertEqual([child.tag.split('}')[1] for child in root],
                         ['operation', 'object', 'concurrencyMode', 'contentType'])
        self.assertEqual([child.text for child in root], ['delete', 'Account', 'Serial', 'CSV'])

    def test_worker_status_is_delivered_on_caller_thread_for_all_apis(self):
        for selected_api in ('bulk_v2', 'bulk_v1', 'rest'):
            with self.subTest(api=selected_api):
                owner = threading.get_ident()
                messages = []
                def status(message, level='info'):
                    self.assertEqual(threading.get_ident(), owner)
                    messages.append(message)
                def worker(api_name):
                    def process(chunk_num, frame, object_name, sf, operation, external_id, *callbacks):
                        callbacks[-1](f'[{api_name}] worker status')
                        if api_name == selected_api:
                            return len(frame), [], {'api': api_name}
                        return 0, [], {'error': 'JOB_LEVEL_FAILED: test fallback'}
                    return process
                with patch.object(loader, '_process_chunk_v2', side_effect=worker('bulk_v2')), \
                     patch.object(loader, '_process_chunk_bulk1', side_effect=worker('bulk_v1')), \
                     patch.object(loader, '_process_chunk_rest', side_effect=worker('rest')):
                    result = self.run_delete(on_status=status)
                self.assertEqual(result['total_success'], 2)
                self.assertTrue(any(message.endswith(f'[{selected_api}] worker status') for message in messages))

    def test_ambiguous_failure_is_reported_without_splitting_or_fallback(self):
        errors = []
        with patch.object(loader, '_process_chunk_v2', return_value=(
            0, self.rows.to_dict('records'), {}
        )) as bulk2, patch.object(loader, '_process_chunk_bulk1') as bulk1, \
             patch.object(loader, '_process_chunk_rest') as rest:
            result = self.run_delete(on_error=errors.append)
        bulk2.assert_called_once()
        bulk1.assert_not_called()
        rest.assert_not_called()
        self.assertEqual(result['total_failed'], 2)
        self.assertIn('Automatic retry stopped', errors[0])
        failed = pd.read_csv(self.error_path)
        self.assertTrue(failed['sf__Error'].str.contains('Automatic retry stopped').all())

    def test_permanent_salesforce_error_is_exposed_without_retry(self):
        errors = []
        rows = [{**row, 'sf__Error': 'INSUFFICIENT_ACCESS_OR_READONLY: cannot delete'}
                for row in self.rows.to_dict('records')]
        with patch.object(loader, '_process_chunk_v2', return_value=(0, rows, {})) as bulk2:
            result = self.run_delete(on_error=errors.append)
        bulk2.assert_called_once()
        self.assertEqual(result['total_failed'], 2)
        self.assertIn('INSUFFICIENT_ACCESS_OR_READONLY', errors[0])

    def test_rest_preserves_completed_batches_when_later_request_fails(self):
        frame = pd.DataFrame({'Id': [f'001{index:015d}' for index in range(201)]})
        with patch.object(loader, '_rest_composite_submit', side_effect=[(200, []), RuntimeError()]):
            success, failed, timing = loader._process_chunk_rest(1, frame, 'Account', Mock(), 'delete')
        self.assertEqual(success, 200)
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]['sf__Id'], frame.iloc[-1]['Id'])
        self.assertIn('RuntimeError: No exception message', failed[0]['sf__Error'])
        self.assertTrue(timing['error'])

    def test_transient_errors_have_a_finite_retry_budget(self):
        failure = (0, [], {'error': 'QUERY_TIMEOUT: timed out'})
        with patch.object(loader, '_process_chunk_v2', return_value=failure) as bulk2, \
             patch.object(loader, '_process_chunk_bulk1', return_value=failure) as bulk1, \
             patch.object(loader, '_process_chunk_rest', return_value=failure) as rest:
            result = self.run_delete()
        self.assertEqual(bulk2.call_count + bulk1.call_count + rest.call_count, 3)
        self.assertEqual(result['total_failed'], 2)
        self.assertEqual(result['total_processed'], 2)

    def test_cumulative_totals_reach_dashboard(self):
        messages = []
        with patch.object(loader, '_process_chunk_v2', return_value=(2, [], {})):
            self.run_delete(on_status=lambda message, level='info': messages.append(message))
        self.assertIn('Delete totals: 2 success, 0 failed', messages)


if __name__ == '__main__':
    unittest.main()