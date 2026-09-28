import ast
from functools import partial
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import pandas as pd
from streamlit.testing.v1 import AppTest


APP_PATH = Path(__file__).with_name('app.py')
TREE = ast.parse(APP_PATH.read_text(encoding='utf-8-sig'))
DELETE_TAB = next(node for node in TREE.body if isinstance(node, ast.FunctionDef) and node.name == '_tab_delete')
DELETE_TAB.decorator_list = []
TEST_APP = '''
import os
import pandas as pd
import streamlit as st
from unittest.mock import Mock
tempfile = st.session_state['test_tempfile']
__file__ = st.session_state['test_app_path']
bulk_delete_v2 = st.session_state['delete_mock']
bulk2_query_ids_to_csv = st.session_state['export_mock']
LiveDashboard = Mock(return_value=Mock())
def clear_stop_flag(): pass
def set_stop_flag(): pass
DATA_DIR = '.'
MIN_CHUNK_SIZE = 1000
MAX_CHUNK_SIZE = 10000
MAX_PARALLEL_JOBS = 32
def section_header(*args, **kwargs): pass
def render_steps(*args, **kwargs): pass
def render_bulk_jobs_quota_panel(): pass
def get_file_list(*args, **kwargs): return []
''' + ast.unparse(DELETE_TAB) + '\n_tab_delete()\n'


def query_result(count=12, object_name='Account', include_id=True):
    records = []
    for index in range(min(count, 20)):
        record = {'attributes': {'type': object_name}, 'Name': f'Record {index}'}
        if include_id:
            record['Id'] = f'001{index:015d}'
        records.append(record)
    return {'totalSize': count, 'records': records, 'done': count <= 20}


class DeleteSoqlTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.salesforce = Mock()
        self.salesforce.query.return_value = query_result()
        self.delete_mock = Mock(return_value=None)
        self.export_path = Path(self.temp_dir.name) / 'export.csv'
        self.export_mock = Mock(return_value=(str(self.export_path), 3))
        self.app = AppTest.from_string(TEST_APP, default_timeout=15)
        self.app.session_state['sf'] = self.salesforce
        self.app.session_state['delete_mock'] = self.delete_mock
        self.app.session_state['export_mock'] = self.export_mock
        self.app.session_state['test_tempfile'] = SimpleNamespace(
            NamedTemporaryFile=partial(tempfile.NamedTemporaryFile, dir=self.temp_dir.name)
        )
        self.app.session_state['test_app_path'] = str(Path(self.temp_dir.name) / 'app.py')
        self.app.run()
        self.app.radio(key='delete_source_mode').set_value(self.app.radio[0].options[1]).run()

    def preview(self, query='SELECT Id FROM Account'):
        self.app.text_area(key='delete_soql_query').set_value(query)
        self.app.button(key='delete_soql_count').click().run()
        self.assertEqual(len(self.app.exception), 0)

    def test_preview_uses_original_query_and_only_displays_ten(self):
        query = "Select Id from Account where Name = 'LIMIT 99' ORDER BY Id LIMIT 12"
        self.preview(query)
        self.salesforce.query.assert_called_once_with(query)
        self.assertEqual(self.app.session_state['_del_soql_total'], 12)
        self.assertEqual(len(self.app.dataframe[0].value), 10)

    def test_run_uses_query_object_and_current_settings(self):
        self.app.session_state['_del_object_name'] = 'OldObject'
        self.app.session_state['_del_chunk_size'] = 5000
        self.app.session_state['_del_parallel'] = 1
        self.preview()
        self.app.button(key='delete_soql_run').click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(self.app.session_state['_del_object_name'], 'Account')
        self.assertEqual(self.app.session_state['_del_chunk_size'], 10000)
        self.assertEqual(self.app.session_state['_del_parallel'], 32)
        self.assertTrue(self.app.session_state['_del_confirm_pending'])
        self.assertIn('Account', self.app.warning[0].value)

    def test_edit_query_invalidates_preview_and_confirmation(self):
        self.preview()
        self.app.button(key='delete_soql_run').click().run()
        self.app.text_area(key='delete_soql_query').set_value('SELECT Id FROM Contact').run()
        self.assertNotIn('_del_soql_preview_df', self.app.session_state)
        self.assertNotIn('_del_confirm_pending', self.app.session_state)
        self.assertNotIn('delete_soql_run', [button.key for button in self.app.button])

    def test_missing_id_is_rejected(self):
        self.salesforce.query.return_value = query_result(include_id=False)
        self.preview('SELECT Name FROM Account')
        self.assertIn('Id field', self.app.error[0].value)
        self.assertNotIn('_del_soql_preview_df', self.app.session_state)

    def test_mismatched_object_is_rejected(self):
        self.app.text_input(key='delete_object').set_value('Contact')
        self.preview()
        self.assertIn('Query returns Account', self.app.error[0].value)
        self.assertNotIn('_del_soql_preview_df', self.app.session_state)

    def test_query_error_is_visible(self):
        self.salesforce.query.side_effect = ValueError('INVALID_FIELD: unknown field')
        self.preview()
        self.assertIn('INVALID_FIELD', self.app.error[0].value)
        self.assertNotIn('_del_soql_preview_df', self.app.session_state)

    def test_confirmation_passes_bulk_export_to_bulk_delete(self):
        query = 'Select Id from Account LIMIT 3'
        self.preview(query)
        expected_ids = ['001000000000001AAA', '001000000000002AAA', '001000000000003AAA']
        pd.DataFrame({'Id': expected_ids}).to_csv(self.export_path, index=False)
        captured_ids = []
        def capture_delete(**kwargs):
            captured_ids.extend(pd.read_csv(kwargs['csv_file_path'], dtype=str)['Id'].tolist())
        self.delete_mock.side_effect = capture_delete
        self.app.button(key='delete_soql_run').click().run()
        self.app.button(key='del_confirm_yes').click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.salesforce.query.assert_called_once_with(query)
        self.salesforce.query_more.assert_not_called()
        self.export_mock.assert_called_once()
        self.assertEqual(self.export_mock.call_args.args[1], query)
        self.delete_mock.assert_called_once()
        arguments = self.delete_mock.call_args.kwargs
        self.assertEqual(arguments['object_name'], 'Account')
        self.assertEqual(arguments['id_column'], 'Id')
        self.assertEqual(arguments['chunk_size'], 10000)
        self.assertEqual(captured_ids, expected_ids)
        self.assertFalse(self.export_path.exists())

    def test_disconnect_cancels_confirmation(self):
        self.preview()
        self.app.button(key='delete_soql_run').click().run()
        self.app.session_state['sf'] = None
        self.app.run()
        self.assertNotIn('_del_confirm_pending', self.app.session_state)
        self.assertNotIn('_del_soql_preview_df', self.app.session_state)
        self.delete_mock.assert_not_called()

    def test_switching_source_cancels_soql_confirmation(self):
        self.preview()
        self.app.button(key='delete_soql_run').click().run()
        self.app.radio(key='delete_source_mode').set_value(self.app.radio[0].options[0]).run()
        self.assertNotIn('_del_confirm_pending', self.app.session_state)
        self.delete_mock.assert_not_called()

    def test_fetch_preserves_query_limit(self):
        query = 'Select Id from Account LIMIT 2'
        self.preview(query + ';')
        self.app.button(key='delete_soql_run').click().run()
        self.app.button(key='del_confirm_yes').click().run()
        self.assertEqual(self.export_mock.call_args.args[1], query)

    def test_failed_or_cancelled_export_never_deletes(self):
        for error in (RuntimeError('Bulk query failed'), InterruptedError('Export stopped')):
            with self.subTest(error=error):
                self.export_mock.side_effect = error
                self.preview()
                self.app.button(key='delete_soql_run').click().run()
                self.app.button(key='del_confirm_yes').click().run()
                self.assertEqual(len(self.app.exception), 0)
                self.delete_mock.assert_not_called()
                self.salesforce.query_more.assert_not_called()

    def test_empty_export_never_deletes(self):
        pd.DataFrame(columns=['Id']).to_csv(self.export_path, index=False)
        self.export_mock.return_value = (str(self.export_path), 0)
        self.preview()
        self.app.button(key='delete_soql_run').click().run()
        self.app.button(key='del_confirm_yes').click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.delete_mock.assert_not_called()
        self.assertFalse(self.export_path.exists())


if __name__ == '__main__':
    unittest.main()