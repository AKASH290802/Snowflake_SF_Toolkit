import threading
import ast
import os
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import sf_bulk_loader as loader

from load_sources import stream_snowflake_batches
from load_events import LoadEventBus
from load_capacity import SharedLoadCapacity


class SnowflakeStreamingTests(unittest.TestCase):
    def test_duplicate_mapping_targets_fail_before_snowflake_cursor_is_opened(self):
        connection = Mock()

        with self.assertRaisesRegex(ValueError, 'duplicate Salesforce target field.*Name'):
            list(stream_snowflake_batches(
                connection,
                'SELECT "A", "B" FROM source_table',
                100,
                column_mapping={'A': 'Name', 'B': 'Name'},
            ))

        connection.cursor.assert_not_called()

    def test_queued_upload_obeys_capacity_and_can_cancel_before_submission(self):
        stopped = threading.Event()
        capacity = SharedLoadCapacity(1, should_stop=stopped.is_set)
        entered = threading.Event()
        attempted = threading.Event()
        worker = Mock()
        def run():
            attempted.set()
            try:
                capacity.run(worker)
            except InterruptedError:
                entered.set()
        with capacity.slot():
            waiting = threading.Thread(target=run)
            waiting.start()
            self.assertTrue(attempted.wait(2))
            worker.assert_not_called()
            stopped.set()
            self.assertTrue(entered.wait(2))
            waiting.join(2)
            self.assertFalse(waiting.is_alive())
        worker.assert_not_called()
        self.assertEqual(capacity._active, 0)
        self.assertEqual(len(capacity._waiting), 0)

    def test_actual_multi_object_workers_upload_three_sources_concurrently(self):
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        tab = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_tab_multi_object')
        worker_node = next(node for node in ast.walk(tab) if isinstance(node, ast.FunctionDef) and node.name == '_run_one')
        barrier = threading.Barrier(3)
        bus = LoadEventBus()
        connection = Mock()
        cursors = []
        def new_cursor():
            cursor = Mock()
            cursor.description = [('SOURCE_NAME',)]
            def execute(query):
                cursor.fetchmany.side_effect = [[(query.split()[-1],)], []]
            cursor.execute.side_effect = execute
            cursors.append(cursor)
            return cursor
        connection.cursor.side_effect = new_cursor
        sf = Mock()
        sf.Account.describe.return_value = {'fields': []}
        received = []
        def upload(chunk_num, frame, *args):
            received.extend(frame['Name'].tolist())
            barrier.wait(timeout=3)
            return len(frame), [], {'rows': len(frame), 'total_s': 0.01}
        loader.clear_stop_flag()
        with tempfile.TemporaryDirectory() as directory:
            namespace = {
                'os': os, '__file__': str(Path(directory) / 'app.py'),
                'load_events': bus, '_snow_conn': connection, '_sf_client': sf,
                'mq_chunk_size': 1000, 'MAX_CHUNK_SIZE': 25000, 'threads_per_obj': 1,
                'shared_capacity': SharedLoadCapacity(3), 'is_stopped': loader.is_stopped,
                'stream_snowflake_batches': stream_snowflake_batches,
            }
            exec(compile(ast.Module(body=[worker_node], type_ignores=[]), '<multi-worker>', 'exec'), namespace)
            items = [{'_mq_key': str(index), 'object': 'Account', 'source': 'snowflake',
                      'file': f'source{index}', 'operation': 'insert', 'external_id': None,
                      'column_mapping': {'SOURCE_NAME': 'Name'}} for index in range(3)]
            with patch.object(loader, '_process_chunk_v2', side_effect=upload), \
                 patch.object(loader, 'get_quota_tracker') as quota, \
                 patch.object(loader, '_install_cancel_handler'):
                quota.return_value.is_near_limit.return_value = False
                with ThreadPoolExecutor(max_workers=3) as executor:
                    results = dict(executor.map(namespace['_run_one'], items))
            self.assertEqual(sorted(received), ['source0', 'source1', 'source2'])
            for index in range(3):
                result = results[str(index)]
                self.assertEqual(result['total_success'], 1, result)
                self.assertIn(f'source{index}', result['_source_label'])
            for cursor in cursors:
                cursor.close.assert_called_once()

    def test_shared_upload_slots_overlap_three_datasets_and_release_on_error(self):
        capacity = SharedLoadCapacity(3)
        barrier = threading.Barrier(3)
        active = []
        peak = []
        lock = threading.Lock()
        def upload(dataset):
            with capacity.slot():
                with lock:
                    active.append(dataset)
                    peak.append(len(active))
                barrier.wait(timeout=3)
                with lock:
                    active.remove(dataset)
            return dataset
        with ThreadPoolExecutor(max_workers=3) as executor:
            self.assertEqual(list(executor.map(upload, range(3))), [0, 1, 2])
        self.assertEqual(max(peak), 3)
        with self.assertRaisesRegex(RuntimeError, 'failure'):
            with capacity.slot():
                raise RuntimeError('failure')
        self.assertEqual(capacity.run(lambda: 'available'), 'available')
        self.assertEqual(capacity._active, 0)

    def test_dashboard_events_render_only_when_caller_drains(self):
        bus = LoadEventBus()
        owner = threading.get_ident()
        seen = []
        dashboards = {}
        for dataset in range(3):
            dashboard = Mock()
            def render(value, level='info'):
                self.assertEqual(threading.get_ident(), owner)
                seen.append(value)
            dashboard.on_status.side_effect = render
            dashboards[dataset] = dashboard
        def publish(dataset):
            callback = bus.callback(dataset, 'status')
            for index in range(1000):
                callback(f'{dataset}:{index}')
            bus.callback(dataset, 'progress')(0.5)
            bus.callback(dataset, 'error')('example error')
        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(publish, range(3)))
        self.assertEqual(seen, [])
        bus.drain(dashboards)
        self.assertEqual(sorted(seen), ['0:999', '1:999', '2:999'])
        for dashboard in dashboards.values():
            dashboard.on_progress.assert_called_once_with(0.5)
            dashboard.on_error.assert_called_once_with('example error')
        bus.drain(dashboards)
        self.assertEqual(len(seen), 3)

    def test_lazy_batches_mapping_and_cursor_cleanup(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.description = [('SOURCE_NAME',), ('IGNORED',)]
        cursor.fetchmany.side_effect = [[('first', 1)], [('second', 2)], []]
        batches = stream_snowflake_batches(connection, 'SELECT * FROM source WHERE active = true',
                                           1000, {'SOURCE_NAME': 'Name'})
        connection.cursor.assert_not_called()
        first = next(batches)
        self.assertEqual(first.to_dict('records'), [{'Name': 'first'}])
        cursor.fetchmany.assert_called_once_with(1000)
        second = next(batches)
        self.assertEqual(second['Name'].tolist(), ['second'])
        self.assertEqual(list(batches), [])
        cursor.close.assert_called_once()
        cursor.fetchall.assert_not_called()
        cursor.fetch_pandas_all.assert_not_called()

    def test_early_close_and_fetch_error_release_cursor(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                connection = Mock()
                cursor = connection.cursor.return_value
                cursor.description = [('Name',)]
                cursor.fetchmany.side_effect = [[('first',)], RuntimeError('source failure')]
                batches = stream_snowflake_batches(connection, 'SELECT Name FROM source', 1000)
                next(batches)
                if fail:
                    with self.assertRaisesRegex(RuntimeError, 'source failure'):
                        next(batches)
                else:
                    batches.close()
                cursor.close.assert_called_once()

    def test_three_sources_stream_concurrently_with_independent_cursors(self):
        barrier = threading.Barrier(3)
        connections = [Mock() for index in range(3)]
        def run(index):
            cursor = connections[index].cursor.return_value
            cursor.description = [('Name',)]
            cursor.fetchmany.side_effect = [[(str(index),)], []]
            batches = stream_snowflake_batches(connections[index], f'SELECT Name FROM source{index}', 1000)
            first = next(batches)
            barrier.wait(timeout=3)
            self.assertEqual(list(batches), [])
            return first['Name'].iloc[0]
        with ThreadPoolExecutor(max_workers=3) as executor:
            self.assertEqual(list(executor.map(run, range(3))), ['0', '1', '2'])
        for connection in connections:
            connection.cursor.return_value.close.assert_called_once()

    def test_stop_does_not_fetch_another_batch(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.description = [('Name',)]
        cursor.fetchmany.return_value = [('first',)]
        stopped = threading.Event()
        batches = stream_snowflake_batches(connection, 'SELECT Name FROM source', 1000,
                                           should_stop=stopped.is_set)
        next(batches)
        stopped.set()
        self.assertEqual(list(batches), [])
        cursor.fetchmany.assert_called_once()
        cursor.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()