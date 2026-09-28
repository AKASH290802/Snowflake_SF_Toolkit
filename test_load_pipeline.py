import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import sf_bulk_loader as loader


class LoadPipelineTests(unittest.TestCase):
    def test_multi_api_source_failure_closes_producer_and_finishes_submitted_work(self):
        loader.clear_stop_flag()
        closed = threading.Event()
        completed = threading.Event()
        def source():
            try:
                yield 1, pd.DataFrame({'Name': ['first']})
                raise RuntimeError('source read failed')
            finally:
                closed.set()
        def upload(*args):
            completed.set()
            return 1, [], {'rows': 1, 'total_s': 0.01}
        with patch.object(loader, '_process_chunk_v2', new=upload):
            with self.assertRaisesRegex(RuntimeError, 'source read failed'):
                loader._run_multi_api_simultaneous(source(), 'Account', Mock(), 'insert', None, 8)
        self.assertTrue(closed.is_set())
        self.assertTrue(completed.is_set())

    def test_coordinator_stop_flag_is_not_cleared_by_child_load(self):
        loader.set_stop_flag()
        self.addCleanup(loader.clear_stop_flag)
        sf = Mock()
        sf.Account.describe.return_value = {'fields': []}
        with patch.object(loader, '_process_chunk_v2') as worker, \
             patch.object(loader, 'get_quota_tracker') as quota, \
             patch.object(loader, '_install_cancel_handler'):
            quota.return_value.is_near_limit.return_value = False
            result = loader.bulk_load_v2(
                None, 'Account', sf, [], 1000, num_parallel_chunks=1,
                chunk_iterator=iter([pd.DataFrame({'Name': ['ignored']})]), manage_stop_flag=False
            )
        worker.assert_not_called()
        self.assertTrue(loader.is_stopped())
        self.assertEqual(result['total_processed'], 0)

    def test_multi_api_backpressure_bounds_source_read_ahead(self):
        loader.clear_stop_flag()
        gate = threading.Event()
        capacity_reached = threading.Event()
        read_count = []
        errors = []
        results = []
        def source():
            for index in range(40):
                read_count.append(index)
                if len(read_count) == 17:
                    capacity_reached.set()
                yield index, pd.DataFrame({'Name': [str(index)]})
        def worker(chunk_num, frame, *args):
            if not gate.wait(5):
                raise AssertionError('Worker gate timed out')
            return 1, [], {'chunk': chunk_num, 'rows': 1, 'total_s': 0.01}
        def run():
            try:
                results.append(loader._run_multi_api_simultaneous(source(), 'Account', Mock(),
                               'insert', None, 8))
            except BaseException as error:
                errors.append(error)
        with patch.object(loader, '_process_chunk_v2', side_effect=worker), \
             patch.object(loader, '_process_chunk_bulk1', side_effect=worker):
            runner = threading.Thread(target=run)
            runner.start()
            try:
                self.assertTrue(capacity_reached.wait(3))
                self.assertLessEqual(len(read_count), 17)
            finally:
                gate.set()
                runner.join(10)
        self.assertFalse(runner.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0][0], 40)
        self.assertEqual(results[0][1], 0)

    def test_single_api_submits_before_source_eof(self):
        for operation in ('insert', 'update'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                submitted = threading.Event()
                frames = []
                owner = threading.get_ident()
                statuses = []
                def status(message, level='info'):
                    self.assertEqual(threading.get_ident(), owner)
                    statuses.append(message)
                def source():
                    yield pd.DataFrame({'source_name': ['first'], 'Id': ['001000000000001AAA']})
                    if not submitted.wait(2):
                        raise AssertionError('Source consumed before first upload')
                    yield pd.DataFrame({'source_name': ['second'], 'Id': ['001000000000002AAA']})
                def worker(chunk_num, frame, *args):
                    frames.append(frame.copy())
                    args[-1]('worker status')
                    submitted.set()
                    return len(frame), [], {'chunk': chunk_num, 'rows': len(frame), 'total_s': 0.01}
                sf = Mock()
                sf.Account.describe.return_value = {'fields': []}
                with patch.object(loader, '_process_chunk_v2', side_effect=worker), \
                     patch.object(loader, 'get_quota_tracker') as quota, \
                     patch.object(loader, '_install_cancel_handler'):
                    quota.return_value.is_near_limit.return_value = False
                    result = loader.bulk_load_v2(
                        None, 'Account', sf, required_columns=['Name', 'Id'], chunk_size=1000,
                        operation=operation, column_mapping={'source_name': 'Name'},
                        num_parallel_chunks=1, chunk_iterator=source(),
                        on_status=status,
                        error_file=str(Path(directory) / 'failed.csv')
                    )
                self.assertEqual(result['total_success'], 2)
                self.assertEqual(result['total_failed'], 0)
                self.assertEqual([frame['Name'].iloc[0] for frame in frames], ['first', 'second'])
                self.assertTrue(any('worker status' in message for message in statuses))


if __name__ == '__main__':
    unittest.main()