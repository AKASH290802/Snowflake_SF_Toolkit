"""Compare buffered and streamed synthetic sources; never contacts Salesforce."""

import argparse
import contextlib
import io
import json
import tempfile
import time
import tracemalloc
import warnings
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import sf_bulk_loader as loader


def measure(mode, chunks, rows_per_chunk):
    first_submission = None
    serialized_bytes = 0
    serialization_seconds = 0.0
    started = time.perf_counter()
    tracemalloc.start()

    def source():
        for chunk_number in range(chunks):
            yield pd.DataFrame({
                'Name': [f'record-{chunk_number}-{index}' for index in range(rows_per_chunk)],
                'Description': ['synthetic benchmark data, including CSV quoting'] * rows_per_chunk,
                'Amount__c': list(range(rows_per_chunk)),
            })

    def upload(chunk_number, frame, *args):
        nonlocal first_submission, serialized_bytes, serialization_seconds
        if first_submission is None:
            first_submission = time.perf_counter() - started
        serialize_started = time.perf_counter()
        payload = loader._df_to_csv_bytes(frame)
        serialization_seconds += time.perf_counter() - serialize_started
        serialized_bytes += len(payload)
        return len(frame), [], {'chunk': chunk_number, 'rows': len(frame), 'total_s': 0.01}

    try:
        inputs = list(source()) if mode == 'buffered-source' else source()
        sf = Mock()
        sf.Account.describe.return_value = {'fields': []}
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(loader, '_process_chunk_v2', new=upload), \
             patch.object(loader, 'get_quota_tracker') as quota, \
             patch.object(loader, '_install_cancel_handler'), \
             contextlib.redirect_stdout(io.StringIO()), warnings.catch_warnings():
            warnings.simplefilter('ignore', FutureWarning)
            quota.return_value.is_near_limit.return_value = False
            result = loader.bulk_load_v2(
                None, 'Account', sf, [], rows_per_chunk,
                num_parallel_chunks=1, chunk_iterator=iter(inputs),
                error_file=str(Path(directory) / 'failed.csv')
            )
        elapsed = time.perf_counter() - started
        peak = tracemalloc.get_traced_memory()[1]
        expected = chunks * rows_per_chunk
        if result['total_success'] != expected or result['total_failed']:
            raise AssertionError(f'Unexpected benchmark outcomes: {result}')
        return {
            'mode': mode, 'rows': expected,
            'first_submission_seconds': round(first_submission, 4),
            'elapsed_seconds': round(elapsed, 4),
            'serialization_seconds': round(serialization_seconds, 4),
            'serialized_bytes': serialized_bytes,
            'peak_traced_python_mib': round(peak / (1024 * 1024), 2),
        }
    finally:
        tracemalloc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--chunks', type=int, default=100)
    parser.add_argument('--rows-per-chunk', type=int, default=1000)
    args = parser.parse_args()
    if args.chunks < 1 or args.rows_per_chunk < 1:
        parser.error('Counts must be positive')
    measure('streamed-source', 2, min(args.rows_per_chunk, 100))
    print(json.dumps([measure(mode, args.chunks, args.rows_per_chunk)
                      for mode in ('buffered-source', 'streamed-source')], indent=2))