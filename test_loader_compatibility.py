import ast
import inspect
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd
from streamlit.testing.v1 import AppTest

import sf_bulk_loader


class LoaderCompatibilityTests(unittest.TestCase):
    def check_backend(self, load, delete):
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == 'loader_restart_required')
        namespace = {'inspect': inspect, 'bulk_load_v2': load, 'bulk_delete_v2': delete}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'compatibility_check', 'exec'), namespace)
        return namespace['loader_restart_required']()

    def test_current_backend_is_compatible(self):
        self.assertFalse(self.check_backend(sf_bulk_loader.bulk_load_v2, sf_bulk_loader.bulk_delete_v2))

    def test_old_delete_requires_restart_without_calling_it(self):
        def old_delete(csv_file_path=None):
            self.fail('Compatibility check must not submit a delete')
        self.assertTrue(self.check_backend(sf_bulk_loader.bulk_load_v2, old_delete))

    def test_old_load_requires_restart_without_calling_it(self):
        def old_load(manage_stop_flag=True):
            self.fail('Compatibility check must not submit a load')
        self.assertTrue(self.check_backend(old_load, sf_bulk_loader.bulk_delete_v2))


class UpdateOperationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.data = pd.DataFrame({'RecordId': ['001000000000001AAA'], 'Name': ['Existing account']})
        self.load = Mock(return_value=None)
        self.upsert_fields = Mock(side_effect=AssertionError('Only Upsert may request upsert fields'))
        self.fields = [
            {'name': 'Id', 'type': 'id', 'updateable': False, 'createable': False},
            {'name': 'Name', 'type': 'string', 'updateable': True, 'createable': True},
            {'name': 'CreateOnly__c', 'type': 'string', 'updateable': False, 'createable': True},
        ]

    def make_app(self, operation='update'):
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        render = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == 'render_operation_tab')
        normalize = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == 'normalize_sf_api_name')
        source = '''
import os
import re
import time
import pandas as pd
import streamlit as st
from unittest.mock import Mock
from sf_bulk_loader import auto_match_csv_to_sf
DATA_DIR = st.session_state['test_dir']
__file__ = os.path.join(DATA_DIR, 'app.py')
MIN_CHUNK_SIZE = 1000
MAX_CHUNK_SIZE = 25000
MAX_PARALLEL_JOBS = 32
bulk_load_v2 = st.session_state['test_load']
LiveDashboard = Mock(return_value=Mock())
def section_header(*args, **kwargs): pass
def render_steps(*args, **kwargs): pass
def render_bulk_jobs_quota_panel(): pass
def clear_stop_flag(): pass
def set_stop_flag(): pass
def get_file_list(*args): return ['records.csv']
def read_file_preview_cached(*args): return st.session_state['test_data']
def count_file_rows(*args): return len(st.session_state['test_data'])
def get_cached_object_fields(*args): return st.session_state['test_fields']
get_cached_upsert_fields = st.session_state['test_upsert_fields']
''' + ast.unparse(normalize) + '\n' + ast.unparse(render) + f'\nrender_operation_tab({operation.title()!r}, {operation!r})\n'
        app = AppTest.from_string(source, default_timeout=15)
        app.session_state['test_dir'] = self.temp_dir.name
        app.session_state['test_data'] = self.data
        app.session_state['test_load'] = self.load
        app.session_state['test_fields'] = self.fields
        app.session_state['test_upsert_fields'] = self.upsert_fields
        app.session_state['sf'] = Mock()
        app.session_state['sf_conn'] = None
        app.session_state['upsert_external_id'] = 0
        app.session_state[f'{operation}_object'] = 'Account'
        app.run()
        self.assertEqual(len(app.exception), 0)
        return app

    def test_update_submits_update_with_no_external_id(self):
        app = self.make_app()
        app.selectbox(key='update_map_RecordId').select('Id').run()
        app.button(key='update_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_called_once()
        arguments = self.load.call_args.kwargs
        self.assertEqual(arguments['operation'], 'update')
        self.assertIsNone(arguments['external_id_field'])
        self.assertEqual(arguments['column_mapping'], {'RecordId': 'Id', 'Name': 'Name'})

    def test_update_requires_id_mapping(self):
        app = self.make_app()
        app.selectbox(key='update_map_RecordId').select('-- Skip --').run()
        app.button(key='update_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_not_called()
        self.assertTrue(any('mapped to Salesforce Id' in error.value for error in app.error))

    def test_update_fields_exclude_create_only_fields(self):
        app = self.make_app()
        options = app.selectbox(key='update_map_RecordId').options
        self.assertIn('Id', options)
        self.assertIn('Name', options)
        self.assertNotIn('CreateOnly__c', options)
        self.assertFalse(any('Upsert' in widget.label for widget in app.selectbox))

    def test_insert_still_submits_insert_without_id(self):
        self.data = pd.DataFrame({'Name': ['New account']})
        app = self.make_app('insert')
        app.button(key='insert_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_called_once()
        self.assertEqual(self.load.call_args.kwargs['operation'], 'insert')
        self.assertIsNone(self.load.call_args.kwargs['external_id_field'])

    def test_upsert_submits_upsert_with_selected_external_id(self):
        self.data = pd.DataFrame({'External_Id__c': ['ACCOUNT-1'], 'Name': ['Account']})
        self.fields.append({'name': 'External_Id__c', 'type': 'string', 'externalId': True})
        self.upsert_fields.side_effect = None
        self.upsert_fields.return_value = [
            {'name': 'Id', 'tags': 'Record ID'},
            {'name': 'External_Id__c', 'tags': 'External ID'},
        ]
        app = self.make_app('upsert')
        app.selectbox(key='upsert_external_id').select(1).run()
        app.button(key='upsert_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_called_once()
        arguments = self.load.call_args.kwargs
        self.assertEqual(arguments['operation'], 'upsert')
        self.assertEqual(arguments['external_id_field'], 'External_Id__c')
        self.assertEqual(arguments['column_mapping']['External_Id__c'], 'External_Id__c')
        self.assertNotIn('Id', arguments['required_columns'])

    def test_upsert_rejects_invalid_matching_key(self):
        self.upsert_fields.side_effect = None
        self.upsert_fields.return_value = [{'name': 'Invalid key', 'tags': 'External ID'}]
        app = self.make_app('upsert')
        app.button(key='upsert_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_not_called()
        self.assertTrue(any('valid External ID field' in error.value for error in app.error))

    def test_upsert_requires_matching_key_mapping(self):
        self.upsert_fields.side_effect = None
        self.upsert_fields.return_value = [{'name': 'External_Id__c', 'tags': 'External ID'}]
        app = self.make_app('upsert')
        app.button(key='upsert_run').click().run()
        self.assertEqual(len(app.exception), 0)
        self.load.assert_not_called()
        self.assertTrue(any('mapped to Salesforce External_Id__c' in error.value for error in app.error))

    def test_upsert_has_its_own_tab(self):
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        tabs = next(node for node in tree.body if isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == 'tabs')
        self.assertEqual(tabs.targets[0].elts[1].id, 'tab_update')
        self.assertEqual(tabs.targets[0].elts[2].id, 'tab_upsert')
        self.assertTrue(ast.literal_eval(tabs.value.args[0])[2].endswith('Upsert'))
        tab = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                   and node.name == '_tab_upsert')
        render = Mock()
        namespace = {'render_operation_tab': render}
        exec(compile(ast.Module(body=[tab], type_ignores=[]), 'upsert_tab', 'exec'), namespace)
        namespace['_tab_upsert']()
        render.assert_called_once_with('Upsert', 'upsert')
        container = next(node for node in tree.body if isinstance(node, ast.With)
                         and isinstance(node.items[0].context_expr, ast.Name)
                         and node.items[0].context_expr.id == 'tab_upsert')
        self.assertEqual(container.body[0].value.func.id, '_tab_upsert')

    def test_upsert_handles_no_matching_fields(self):
        self.upsert_fields.side_effect = None
        self.upsert_fields.return_value = []
        app = self.make_app('upsert')
        self.load.assert_not_called()
        self.assertTrue(any('No upsert matching fields' in error.value for error in app.error))


class TabNavigationTests(unittest.TestCase):
    def test_navigation_lists_match_tab_order_and_containers(self):
        source = Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig')
        tree = ast.parse(source)
        tabs = next(node for node in tree.body if isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == 'tabs')
        expected = ['Insert', 'Update', 'Upsert', 'Delete', 'Multi-Object', 'Snowflake',
                    'SF\u2192Snowflake', 'TestCase']
        labels = ast.literal_eval(tabs.value.args[0])
        self.assertEqual([label.split(' ', 1)[1].replace(' ', '').replace('Generator', '')
                          for label in labels], expected)
        lists = re.findall(r'const _?TAB_OPS\s*=\s*(\[[^;]+\]);', source)
        self.assertEqual(len(lists), 2)
        for operations in lists:
            self.assertEqual(ast.literal_eval(operations), expected)
        for target in tabs.targets[0].elts:
            containers = [node for node in tree.body if isinstance(node, ast.With)
                          and isinstance(node.items[0].context_expr, ast.Name)
                          and node.items[0].context_expr.id == target.id]
            self.assertEqual(len(containers), 1, target.id)
            self.assertIsInstance(containers[0].body[0].value, ast.Call)

    def test_sf_to_snowflake_shortcut_matches_current_index(self):
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        tabs = next(node for node in tree.body if isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                    and node.value.func.attr == 'tabs')
        index = [target.id for target in tabs.targets[0].elts].index('tab_sf_to_snowflake')
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == '_tab_sf_to_snowflake')
        shortcuts = [node for node in ast.walk(function) if isinstance(node, ast.Call)
                     and isinstance(node.func, ast.Name) and node.func.id == 'js_click_tab']
        self.assertEqual(len(shortcuts), 1)
        self.assertEqual(ast.literal_eval(shortcuts[0].args[0]), index)


if __name__ == '__main__':
    unittest.main()