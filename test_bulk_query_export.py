import io
from functools import partial
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import sf_bulk_loader as loader


def response(data=None, csv_text=None, headers=None):
    result = Mock(ok=True, headers=headers or {})
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock(return_value=False)
    result.json.return_value = data
    if csv_text is not None:
        result.raw = io.BytesIO(csv_text.encode('utf-8'))
    return result


class BulkQueryExportTests(unittest.TestCase):
    def setUp(self):
        loader.clear_stop_flag()
        self.addCleanup(loader.clear_stop_flag)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        temporary = patch.object(loader.tempfile, 'NamedTemporaryFile', partial(
            tempfile.NamedTemporaryFile, dir=self.directory.name
        ))
        temporary.start()
        self.addCleanup(temporary.stop)
        self.session = Mock()
        self.session.post.return_value = response({'id': 'query-job'})
        self.session.patch.return_value = response({})
        session_patch = patch.object(loader, '_get_http_session', return_value=self.session)
        session_patch.start()
        self.addCleanup(session_patch.stop)
        self.sf = SimpleNamespace(sf_instance='example.invalid', session_id='test-session')

    def assert_no_partial_csv(self):
        self.assertEqual(list(Path(self.directory.name).glob('*.csv')), [])

    def test_streamed_pages_preserve_query_and_write_only_ids(self):
        query = "SELECT Id, Name FROM Account WHERE Name = 'LIMIT 99' LIMIT 3"
        first = response(csv_text='Id,Name\n001000000000001AAA,"first, name"\n001000000000002AAA,"two\nlines"\n',
                         headers={'Sforce-Locator': 'next', 'Sforce-NumberOfRecords': '2'})
        last = response(csv_text='Id,Name\n001000000000003AAA,last\n',
                        headers={'Sforce-Locator': 'null', 'Sforce-NumberOfRecords': '1'})
        self.session.get.side_effect = [response({'state': 'JobComplete', 'numberRecordsProcessed': 3}), first, last]
        progress = Mock()
        csv_path, count = loader.bulk2_query_ids_to_csv(self.sf, query, on_progress=progress)
        self.assertEqual(count, 3)
        frame = pd.read_csv(csv_path, dtype=str)
        self.assertEqual(list(frame.columns), ['Id'])
        self.assertEqual(frame['Id'].tolist(), ['001000000000001AAA', '001000000000002AAA', '001000000000003AAA'])
        self.assertEqual(self.session.post.call_args.kwargs['json']['query'], query)
        downloads = self.session.get.call_args_list[1:]
        self.assertTrue(all(call.kwargs['stream'] for call in downloads))
        self.assertEqual(downloads[1].kwargs['params']['locator'], 'next')
        self.assertTrue(first.raw.decode_content)
        self.assertGreaterEqual(progress.call_count, 4)

    def test_cancel_during_polling_aborts_job(self):
        self.session.get.return_value = response({'state': 'InProgress'})
        def cancel(message):
            if 'InProgress' in message:
                loader.set_stop_flag()
        with self.assertRaises(InterruptedError):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account', on_progress=cancel)
        self.session.patch.assert_called_once()
        self.assertEqual(self.session.patch.call_args.kwargs['json'], {'state': 'Aborted'})
        self.assert_no_partial_csv()

    def test_large_page_is_written_in_bounded_chunks(self):
        row_count = 75001
        csv_text = 'Id\n' + ''.join(f'001{index:015d}\n' for index in range(row_count))
        self.session.get.side_effect = [response({'state': 'JobComplete', 'numberRecordsProcessed': row_count}),
                                       response(csv_text=csv_text)]
        chunk_sizes = []
        write_csv = pd.DataFrame.to_csv
        def record_write(frame, *args, **kwargs):
            chunk_sizes.append(len(frame))
            return write_csv(frame, *args, **kwargs)
        with patch.object(pd.DataFrame, 'to_csv', record_write):
            csv_path, count = loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account')
        self.assertEqual(count, row_count)
        self.assertEqual(chunk_sizes, [25000, 25000, 25000, 1])
        with open(csv_path, encoding='utf-8') as exported:
            self.assertEqual(sum(1 for line in exported), row_count + 1)

    def test_rejected_query_is_not_rewritten_or_retried(self):
        rejected = response()
        rejected.ok = False
        rejected.status_code = 400
        rejected.text = 'Unsupported query clause'
        self.session.post.return_value = rejected
        query = 'SELECT Id FROM Account LIMIT 2'
        with self.assertRaisesRegex(RuntimeError, 'Unsupported query clause'):
            loader.bulk2_query_ids_to_csv(self.sf, query)
        self.session.post.assert_called_once()
        self.assertEqual(self.session.post.call_args.kwargs['json']['query'], query)
        self.session.get.assert_not_called()
        self.assert_no_partial_csv()

    def test_cancel_during_download_removes_partial_csv(self):
        self.session.get.side_effect = [response({'state': 'JobComplete'}), response(
            csv_text='Id\n001000000000001AAA\n', headers={'Sforce-Locator': 'next'}
        )]
        def cancel(message):
            if 'downloaded' in message:
                loader.set_stop_flag()
        with self.assertRaises(InterruptedError):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account', on_progress=cancel)
        self.assert_no_partial_csv()

    def test_timeout_aborts_query(self):
        with self.assertRaises(TimeoutError):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account', timeout=0)
        self.session.patch.assert_called_once()
        self.assert_no_partial_csv()

    def test_failed_job_has_no_download(self):
        self.session.get.return_value = response({'state': 'Failed', 'errorMessage': 'query failed'})
        with self.assertRaisesRegex(RuntimeError, 'query failed'):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account')
        self.assertEqual(self.session.get.call_count, 1)
        self.assert_no_partial_csv()

    def test_invalid_or_missing_ids_block_export(self):
        for csv_text in ('Id\nnot-an-id\n', 'Name\nSome name\n'):
            with self.subTest(csv_text=csv_text):
                self.session.get.side_effect = [response({'state': 'JobComplete'}), response(csv_text=csv_text)]
                with self.assertRaises(ValueError):
                    loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account')
                self.assert_no_partial_csv()

    def test_truncated_page_blocks_export(self):
        self.session.get.side_effect = [response({'state': 'JobComplete'}), response(
            csv_text='Id\n001000000000001AAA\n', headers={'Sforce-NumberOfRecords': '2'}
        )]
        with self.assertRaisesRegex(RuntimeError, 'Incomplete'):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account')
        self.assert_no_partial_csv()

    def test_empty_result_writes_header_only(self):
        self.session.get.side_effect = [response({'state': 'JobComplete'}), response(csv_text='Id\n')]
        csv_path, count = loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account')
        self.assertEqual(count, 0)
        self.assertEqual(Path(csv_path).read_text(), 'Id\n')

    def test_ui_interruption_aborts_pending_query(self):
        class Rerun(BaseException):
            pass
        with self.assertRaises(Rerun):
            loader.bulk2_query_ids_to_csv(self.sf, 'SELECT Id FROM Account', on_progress=Mock(side_effect=Rerun))
        self.session.patch.assert_called_once()
        self.assert_no_partial_csv()


if __name__ == '__main__':
    unittest.main()