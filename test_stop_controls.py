import ast
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import sf_bulk_loader as loader


class StopControlTests(unittest.TestCase):
    def tearDown(self):
        loader.clear_stop_flag()

    def test_stopped_bulk1_worker_does_not_submit(self):
        loader.set_stop_flag()
        with patch.object(loader, '_bulk1_create_job') as create:
            result = loader._process_chunk_bulk1(1, pd.DataFrame({'Name': ['test']}), 'Account', Mock())
        create.assert_not_called()
        self.assertEqual(result[0], 0)
        self.assertIn('stopped', str(result))

    def test_bulk2_stop_requests_abort_and_keeps_terminal_counts(self):
        loader.set_stop_flag()
        sf = Mock(sf_instance='example.invalid', session_id='test')
        session = Mock()
        info = {'state': 'Aborted', 'numberRecordsProcessed': 4, 'numberRecordsFailed': 1}
        session.get.return_value.json.return_value = info
        with patch.object(loader, '_get_http_session', return_value=session):
            self.assertEqual(loader._bulk2_poll_job(sf, 'job'), info)
        self.assertEqual(session.patch.call_args.kwargs['json'], {'state': 'Aborted'})

    def test_bulk1_stop_requests_abort_before_poll(self):
        loader.set_stop_flag()
        sf = Mock(sf_instance='example.invalid', session_id='test')
        session = Mock()
        session.get.return_value.text = f'<batchInfo xmlns="{loader._BULK1_INGEST_NS}"><state>Not Processed</state></batchInfo>'
        with patch.object(loader, '_get_http_session', return_value=session):
            result = loader._bulk1_poll_batch(sf, 'job', 'batch')
        self.assertEqual(result[0], 'Not Processed')
        self.assertIn(b'<state>Aborted</state>', session.post.call_args.kwargs['data'])

    def test_stop_prevents_next_record_retry_batch(self):
        loader.clear_stop_flag()
        records = [{'Name': str(index), 'sf__Error': 'UNABLE_TO_LOCK_ROW'} for index in range(201)]
        def submit(*args):
            loader.set_stop_flag()
            return 0, records[:200]
        with patch.object(loader, '_rest_composite_submit', side_effect=submit) as submit_mock:
            recovered, failed = loader._retry_failed_records(Mock(), 'Account', records)
        self.assertEqual(submit_mock.call_count, 1)
        self.assertEqual(recovered, 0)
        self.assertEqual(len(failed), 201)

    def test_long_running_tabs_are_not_fragments(self):
        tree = ast.parse(Path('app.py').read_text(encoding='utf-8-sig'))
        tabs = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name.startswith('_tab_')]
        self.assertEqual(len(tabs), 9)
        for tab in tabs:
            self.assertFalse(tab.decorator_list, tab.name)

    def test_cleanup_does_not_reset_cancellation(self):
        tree = ast.parse(Path('sf_bulk_loader.py').read_text(encoding='utf-8-sig'))
        for function in tree.body:
            if isinstance(function, ast.FunctionDef) and function.name in ('bulk_load_v2', 'bulk_delete_v2'):
                resets = [node for node in ast.walk(function)
                          if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                          and node.func.id == 'clear_stop_flag']
                self.assertEqual(len(resets), 1, function.name)

    def test_app_loaders_preserve_tab_stop_signal(self):
        tree = ast.parse(Path('app.py').read_text(encoding='utf-8-sig'))
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name)
                 and node.func.id in ('bulk_load_v2', 'bulk_delete_v2')]
        self.assertEqual(len(calls), 6)
        for call in calls:
            flag = next((kw.value for kw in call.keywords if kw.arg == 'manage_stop_flag'), None)
            self.assertIsInstance(flag, ast.Constant)
            self.assertIs(flag.value, False)

    def test_every_stop_button_has_early_callback(self):
        tree = ast.parse(Path('app.py').read_text(encoding='utf-8-sig'))
        buttons = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'button'
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and 'STOP' in str(node.args[0].value)
        ]
        self.assertEqual(len(buttons), 9)
        for button in buttons:
            with self.subTest(line=button.lineno):
                callback = next((kw.value for kw in button.keywords if kw.arg == 'on_click'), None)
                self.assertIsInstance(callback, ast.Name)
                self.assertEqual(callback.id, 'set_stop_flag')

    def test_queued_snowflake_workers_check_stop_before_io(self):
        tree = ast.parse(Path('app.py').read_text(encoding='utf-8-sig'))
        for name in ('_snow_query', '_compress_chunk', '_put_file'):
            function = next(node for node in ast.walk(tree)
                            if isinstance(node, ast.FunctionDef) and node.name == name)
            module = ast.Module(body=[function], type_ignores=[])
            guard = Mock(side_effect=InterruptedError('Stopped'))
            namespace = {'ensure_not_stopped': guard}
            exec(compile(module, 'isolated_stop_worker', 'exec'), namespace)
            with self.assertRaises(InterruptedError):
                namespace[name](None)
            guard.assert_called_once()


if __name__ == '__main__':
    unittest.main()