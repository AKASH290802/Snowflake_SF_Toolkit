import ast
from pathlib import Path
import unittest
from unittest.mock import Mock


class SnowflakeCopyColumnsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8-sig'))
        helper = next(node for node in cls.tree.body
                      if isinstance(node, ast.FunctionDef) and node.name == '_snowflake_copy_columns')
        namespace = {}
        exec(compile(ast.Module(body=[helper], type_ignores=[]), 'app.py', 'exec'), namespace)
        cls.resolve = staticmethod(namespace['_snowflake_copy_columns'])

    def cursor(self, columns):
        return Mock(description=[(column,) for column in columns])

    def test_uppercase_destination_preserves_source_order(self):
        cursor = self.cursor(['NAME', 'OWNERID', 'ID', 'EXTRA'])
        self.assertEqual(self.resolve(cursor, 'DB.SCHEMA.TARGET', ['Id', 'OwnerId', 'Name']),
                         '"ID", "OWNERID", "NAME"')
        cursor.execute.assert_called_once_with('SELECT * FROM DB.SCHEMA.TARGET LIMIT 0')

    def test_quoted_mixed_case_destination_is_preserved(self):
        self.assertEqual(self.resolve(self.cursor(['Id', 'OwnerId']), 'TARGET', ['Id', 'OwnerId']),
                         '"Id", "OwnerId"')

    def test_exact_match_takes_precedence(self):
        self.assertEqual(self.resolve(self.cursor(['OWNERID', 'OwnerId']), 'TARGET', ['OwnerId']),
                         '"OwnerId"')

    def test_unique_lowercase_destination(self):
        self.assertEqual(self.resolve(self.cursor(['ownerid']), 'TARGET', ['OwnerId']), '"ownerid"')

    def test_missing_column_is_actionable(self):
        with self.assertRaisesRegex(ValueError, "no column matching 'OwnerId'"):
            self.resolve(self.cursor(['ID']), 'TARGET', ['OwnerId'])

    def test_ambiguous_case_match_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Ambiguous Snowflake column'):
            self.resolve(self.cursor(['OWNERID', 'ownerid']), 'TARGET', ['OwnerId'])

    def test_duplicate_mapping_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Multiple source columns map'):
            self.resolve(self.cursor(['OWNERID']), 'TARGET', ['OwnerId', 'OWNERID'])

    def test_identifiers_are_escaped(self):
        self.assertEqual(self.resolve(self.cursor(['A"B']), 'TARGET', ['A"B']), '"A""B"')

    def test_metadata_errors_propagate(self):
        cursor = self.cursor([])
        cursor.execute.side_effect = RuntimeError('not authorized')
        with self.assertRaisesRegex(RuntimeError, 'not authorized'):
            self.resolve(cursor, 'TARGET', ['OwnerId'])

    def test_bulk_and_rest_copy_use_resolved_identifiers(self):
        tab = next(node for node in self.tree.body
                   if isinstance(node, ast.FunctionDef) and node.name == '_tab_sf_to_snowflake')
        for variable, cursor_name, columns_name in (
            ('quoted_cols', 'sf_cur', 'col_names_stream'),
            ('quoted_cols_r', 'sf_cur_r', 'col_names_r'),
        ):
            with self.subTest(path=variable):
                assignment = next(node for node in ast.walk(tab) if isinstance(node, ast.Assign)
                                  and any(isinstance(target, ast.Name) and target.id == variable
                                          for target in node.targets))
                cursor = self.cursor(['OWNERID', 'ID'])
                namespace = {'_snowflake_copy_columns': self.resolve, cursor_name: cursor,
                             columns_name: ['Id', 'OwnerId'], 'fq_table_name': 'TARGET'}
                exec(compile(ast.Module(body=[assignment], type_ignores=[]), 'app.py', 'exec'), namespace)
                self.assertEqual(namespace[variable], '"ID", "OWNERID"')


if __name__ == '__main__':
    unittest.main()