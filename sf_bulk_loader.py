import io
import os
import re
import csv
import json
import time
import tempfile
import queue
from functools import partial
import threading
import concurrent.futures
import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from load_events import CallerThreadStatus

# -------------------------------------------------
# Constants
# -------------------------------------------------
SF_API_VERSION = '59.0'

# Salesforce Bulk API 2.0 Limits
MAX_RECORDS_PER_BATCH = 10000  # SF limit for Bulk API 2.0
MAX_PARALLEL_JOBS = 32  # LOADING_BABA uses up to 32 concurrent threads
# LOADING_BABA: 25K rows/job with 32 threads = 800K records in-flight
# SF processes 25K in ~5s; with 32 threads pipeline: 30M in ~12 min
MAX_CHUNK_SIZE = 25000
MIN_CHUNK_SIZE = 1000  # Minimum for efficiency

# Thread ladder for AUTO mode — matches LOADING_BABA exactly
# [32, 24, 16, 12, 8, 4, 2, 1] — starts aggressive, scales down on errors
THREAD_LADDER = [32, 24, 16, 12, 8, 4, 2, 1]

# Multi-API weights for simultaneous mode (LOADING_BABA rr_weight)
# bulk_v2 gets 75% of chunks, bulk_v1 gets 20%, REST gets 5%
MULTI_API_WEIGHTS = {'bulk_v2': 15, 'bulk_v1': 4, 'rest': 1}
REST_MINI_CHUNK = 2000   # REST splits into 2K mini-batches
BULK1_MINI_CHUNK = 10000  # Bulk v1 limit-safe split (<=10001 incl. header)
# Per-API thread caps (LOADING_BABA: BulkV2=32, BulkV1=11, REST=15)
MAX_THREADS_BULK_V2 = 32
MAX_THREADS_BULK_V1 = 11
MAX_THREADS_REST = 15

# Straggler detection: alert if a job takes > 2x the median (LOADING_BABA uses 2×)
STRAGGLER_MULTIPLIER = 2.0
# Fire straggler alerts at these thresholds when NO jobs have completed yet
STRAGGLER_NO_COMPLETION_THRESHOLDS = [120, 300, 600]  # 2min, 5min, 10min

# Per-job timeout (seconds) — abort jobs that hang
JOB_TIMEOUT_SECONDS = 1800  # 30 minutes

# Max concurrent Bulk API 2.0 jobs submitted to Salesforce at any time.
# OPTIMIZED (2026-08-12): Increased from 15 → 28 for high-volume loads (100M+ records).
# SF tolerates 28 concurrent jobs for enterprise orgs; gracefully falls back with 429.
# For smaller orgs, this may cause temporary throttling — monitor and reduce if needed.
# The semaphore below gates job creation so SF always has a free processing slot.
MAX_CONCURRENT_BULK_V2_JOBS = 28

DEFAULT_DATE_COLUMNS_BY_OBJECT = {
    'WOD_2__Warranty_Coverages__c': ['WOD_2__WARRANTY_END_DATE__C', 'WOD_2__WARRANTY_START_DATE__C'],
}

# Global stop flag
_stop_flag = threading.Event()

# Semaphore: limits concurrent Bulk API 2.0 jobs in-flight with Salesforce.
# Threads wait here (in Python, not creating idle SF jobs) until a slot is free.
# Released when polling confirms the job is JobComplete/Failed/Aborted.
_bulk_v2_semaphore = threading.Semaphore(MAX_CONCURRENT_BULK_V2_JOBS)

# =============================================================================
# CANCELLATION HANDLER (LOADING_BABA cancellation.py)
# Tracks in-flight SF job IDs and aborts them on Ctrl-C to free quota immediately.
# =============================================================================
import signal

_active_jobs_lock = threading.Lock()
_active_jobs = {}  # job_id -> sf instance (for abort on cancel)
_cancel_handler_installed = False


def _normalize_sf_api_name(value):
    """Return a strict Salesforce API token from raw widget/user input."""
    raw = (value or '').strip()
    if not raw:
        return ''
    return raw if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', raw) else ''


def _normalize_bulk_inputs(object_name, operation='insert', external_id_field=None):
    """Normalize object / external ID inputs shared across all ingest APIs."""
    normalized_operation = (operation or 'insert').strip().lower()
    normalized_object = _normalize_sf_api_name(object_name)
    normalized_external_id = _normalize_sf_api_name(external_id_field)
    if not normalized_object:
        raise ValueError('Salesforce object name is required')
    if normalized_operation == 'upsert' and not normalized_external_id:
        raise ValueError('Upsert requires a valid External ID field')
    return normalized_object, normalized_operation, normalized_external_id


def resolve_sf_object_api_name(sf, object_name):
    """Resolve a user-entered object name to Salesforce's canonical API casing."""
    normalized_object = _normalize_sf_api_name(object_name)
    if not normalized_object or sf is None:
        return normalized_object
    try:
        global_desc = sf.describe() or {}
        for sobject in global_desc.get('sobjects', []) or []:
            candidate = (sobject or {}).get('name') or ''
            if candidate.lower() == normalized_object.lower():
                return candidate
    except Exception:
        pass
    return normalized_object


def _register_active_job(sf, job_id):
    """Track a SF job ID for cancellation on Ctrl-C."""
    with _active_jobs_lock:
        _active_jobs[job_id] = sf


def _unregister_active_job(job_id):
    """Remove a completed/failed job from the active tracking."""
    with _active_jobs_lock:
        _active_jobs.pop(job_id, None)


def _cancel_all_active_jobs():
    """Abort ALL in-flight Salesforce Bulk API jobs (called on SIGINT)."""
    with _active_jobs_lock:
        jobs_copy = dict(_active_jobs)
    aborted = 0
    for job_id, sf in jobs_copy.items():
        try:
            _bulk2_abort_job(sf, job_id)
            aborted += 1
        except Exception:
            pass
    if aborted:
        print(f'\n⛔ Cancelled: aborted {aborted} in-flight SF jobs (quota freed)')
    return aborted


def _sigint_handler(signum, frame):
    """SIGINT (Ctrl-C) handler: stop flag + abort all tracked SF jobs."""
    _stop_flag.set()
    _cancel_all_active_jobs()
    # Re-raise KeyboardInterrupt so the main thread can catch it
    raise KeyboardInterrupt("Cancelled by user (Ctrl-C)")


def _install_cancel_handler():
    """Install SIGINT handler (once per process). Safe to call multiple times."""
    global _cancel_handler_installed
    if _cancel_handler_installed:
        return
    try:
        signal.signal(signal.SIGINT, _sigint_handler)
        _cancel_handler_installed = True
    except (OSError, ValueError):
        # Can't install signal handler from a non-main thread (e.g. Streamlit)
        # Fall back to just using _stop_flag
        pass


# =============================================================================
# ERROR CLASSIFICATION (from LOADING_BABA failure_handler)
# =============================================================================
class ErrorClass:
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"
    SWITCH_API = "SWITCH_API"
    UNKNOWN = "UNKNOWN"


RETRYABLE_CODES = {
    "API_CURRENTLY_UNAVAILABLE", "SERVER_UNAVAILABLE",
    "UNABLE_TO_LOCK_ROW", "LOCK_ROW_FAILED",
    "QUERY_TIMEOUT", "REQUEST_RUNNING_TOO_LONG",
    "OPERATION_TOO_LARGE", "INSUFFICIENT_RESOURCES",
}

SWITCH_API_CODES = {
    "REQUEST_LIMIT_EXCEEDED", "TOTAL_REQUESTS_LIMIT_EXCEEDED",
    "API_DISABLED_FOR_ORG", "EXCEEDED_MAX_SEMIJOIN_SUBSELECTS",
}

NON_RETRYABLE_CODES = {
    "INVALID_FIELD", "INVALID_TYPE", "INVALID_FIELD_FOR_INSERT_UPDATE",
    "REQUIRED_FIELD_MISSING", "DUPLICATE_VALUE", "DUPLICATES_DETECTED",
    "INVALID_CROSS_REFERENCE_KEY", "MALFORMED_ID", "STRING_TOO_LONG",
    "FIELD_INTEGRITY_EXCEPTION", "FIELD_CUSTOM_VALIDATION_EXCEPTION",
    "INSUFFICIENT_ACCESS_OR_READONLY", "ENTITY_IS_DELETED",
    "INVALID_EMAIL_ADDRESS",
}

_CODE_PREFIX_RE = re.compile(r"\b([A-Z][A-Z0-9_]+)\b")


def classify_error(error_text):
    """Classify a Salesforce error into RETRYABLE/NON_RETRYABLE/SWITCH_API/UNKNOWN."""
    if not error_text:
        return ErrorClass.UNKNOWN, ""
    # Extract SF error code
    head = error_text.split(":", 1)[0].strip()
    code = ""
    if head and head.replace("_", "").isalnum() and head.isupper():
        code = head
    else:
        m = _CODE_PREFIX_RE.search(error_text)
        code = m.group(1) if m else ""

    if code in SWITCH_API_CODES:
        return ErrorClass.SWITCH_API, code
    if code in RETRYABLE_CODES:
        return ErrorClass.RETRYABLE, code
    if code in NON_RETRYABLE_CODES:
        return ErrorClass.NON_RETRYABLE, code

    lower = error_text.lower()
    # Job-level Bulk v2 failure — fall back to Bulk v1 → REST
    if 'job_level_failed' in lower or 'state=failed' in lower or 'state=aborted' in lower:
        return ErrorClass.SWITCH_API, 'JOB_LEVEL_FAILED'
    # Salesforce session/auth errors — switch API (same session used, but triggers retry path)
    if 'nosessioncontext' in lower or 'session expired' in lower or 'session disconnected' in lower or 'salesforceexpiredsession' in lower:
        return ErrorClass.SWITCH_API, 'SESSION_ERROR'
    if "limit exceeded" in lower or "too many requests" in lower:
        return ErrorClass.SWITCH_API, code or "REQUEST_LIMIT_EXCEEDED"
    if "timeout" in lower or "unavailable" in lower or "try again" in lower:
        return ErrorClass.RETRYABLE, code or "TRANSIENT"
    if "invalidbatch" in lower or "field name not found" in lower or "no such column" in lower:
        return ErrorClass.NON_RETRYABLE, code or "INVALID_BATCH"
    return ErrorClass.UNKNOWN, code


# =============================================================================
# THREAD LADDER CONTROLLER (from LOADING_BABA thread_manager)
# =============================================================================
class ThreadController:
    """Thread ladder: starts at top, reduces on errors. Signals when exhausted."""

    def __init__(self, ladder=None, initial_cap=None):
        self.ladder = list(ladder or THREAD_LADDER)
        if initial_cap:
            self.ladder = [t for t in self.ladder if t <= initial_cap] or [initial_cap]
        self.index = 0
        self._lock = threading.Lock()

    @property
    def current(self):
        if self.index >= len(self.ladder):
            return self.ladder[-1]
        return self.ladder[self.index]

    @property
    def exhausted(self):
        return self.index >= len(self.ladder) - 1

    def reduce(self):
        """Step down one rung. Returns (new_thread_count, is_at_bottom)."""
        with self._lock:
            self.index = min(self.index + 1, len(self.ladder) - 1)
            return self.ladder[self.index], self.index >= len(self.ladder) - 1

    def reset(self):
        with self._lock:
            self.index = 0
            return self.ladder[0]


# =============================================================================
# PER-DAY API QUOTA TRACKER (from LOADING_BABA quota_tracker.py)
# Tracks daily Salesforce API calls to avoid hitting org limits.
# Persists state to a local JSON file so it survives app restarts.
# =============================================================================
from datetime import date as _date_cls

_QUOTA_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api_quota_state.json')


class QuotaTracker:
    """Track daily Salesforce API usage. Thread-safe."""

    def __init__(self, daily_limit=100000):
        self.daily_limit = daily_limit
        self._lock = threading.Lock()
        self._state = self._load()

    def _load(self):
        try:
            if os.path.exists(_QUOTA_STATE_FILE):
                with open(_QUOTA_STATE_FILE, 'r') as f:
                    data = json.load(f)
                if data.get('date') == str(_date_cls.today()):
                    return data
        except Exception:
            pass
        return {'date': str(_date_cls.today()), 'api_calls': 0, 'bulk_jobs': 0}

    def _save(self):
        try:
            with open(_QUOTA_STATE_FILE, 'w') as f:
                json.dump(self._state, f)
        except Exception:
            pass

    def _check_reset(self):
        """Reset counters if date has rolled over."""
        today = str(_date_cls.today())
        if self._state.get('date') != today:
            self._state = {'date': today, 'api_calls': 0, 'bulk_jobs': 0}

    def increment(self, api_calls=1, bulk_jobs=0):
        """Record API calls made."""
        with self._lock:
            self._check_reset()
            self._state['api_calls'] += api_calls
            self._state['bulk_jobs'] += bulk_jobs
            self._save()

    @property
    def remaining(self):
        """Estimated remaining API calls for today."""
        with self._lock:
            self._check_reset()
            return max(self.daily_limit - self._state['api_calls'], 0)

    @property
    def usage(self):
        """Return (used, limit) tuple."""
        with self._lock:
            self._check_reset()
            return self._state['api_calls'], self.daily_limit

    def is_near_limit(self, threshold=0.9):
        """True if usage exceeds threshold% of daily limit."""
        with self._lock:
            self._check_reset()
            return self._state['api_calls'] >= self.daily_limit * threshold

    def summary(self):
        """Human-readable quota summary."""
        used, limit = self.usage
        pct = (used / limit) * 100 if limit > 0 else 0
        return f'API Quota: {used:,}/{limit:,} ({pct:.1f}%) — {self.remaining:,} remaining'


# Global quota tracker instance (lazy — shared across all operations)
_quota_tracker = None


def get_quota_tracker(daily_limit=100000):
    """Get or create the global QuotaTracker."""
    global _quota_tracker
    if _quota_tracker is None:
        _quota_tracker = QuotaTracker(daily_limit=daily_limit)
    return _quota_tracker


# -------------------------------------------------
# HTTP Session with Connection Pooling
# -------------------------------------------------
# Reuses TCP+TLS connections across all SF Bulk API calls — saves ~200-300 ms
# per request (3 handshakes per chunk × N chunks = huge cumulative win).
# Keyed by sf.session_id so multiple SF orgs in one app stay isolated.
_http_sessions = {}
_http_sessions_lock = threading.Lock()


def _get_http_session(sf):
    """Return a process-wide pooled requests.Session for this SF instance."""
    key = (sf.sf_instance, sf.session_id)
    with _http_sessions_lock:
        sess = _http_sessions.get(key)
        if sess is None:
            sess = requests.Session()
            # Pool size matches MAX_PARALLEL_JOBS x 4 (create/upload/close/poll)
            adapter = HTTPAdapter(
                pool_connections=MAX_PARALLEL_JOBS,
                pool_maxsize=MAX_PARALLEL_JOBS * 4,
                max_retries=0,
            )
            sess.mount('https://', adapter)
            sess.mount('http://', adapter)
            _http_sessions[key] = sess
    return sess


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def merge_unique_columns(*column_groups):
    merged = []
    for group in column_groups:
        for column in group or []:
            if column not in merged:
                merged.append(column)
    return merged


def _extract_error_message(row, default='Unknown Salesforce error'):
    """Normalize row-level error text across Bulk v1/v2 and REST variants."""
    if not isinstance(row, dict):
        text = str(row).strip()
        return text or default

    candidates = [
        row.get('sf__Error'),
        row.get('Error'),
        row.get('error'),
        row.get('errorMessage'),
        row.get('ErrorMessage'),
        row.get('sf__ErrorMessage'),
        row.get('message'),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    errors = row.get('errors')
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            status = str(first.get('statusCode') or 'ERROR').strip()
            message = str(first.get('message') or '').strip()
            combined = f'{status}: {message}'.strip(': ').strip()
            if combined:
                return combined
        text = str(first).strip()
        if text:
            return text

    for key, value in row.items():
        key_l = str(key).lower()
        if 'error' in key_l or 'message' in key_l:
            text = str(value).strip()
            if text:
                return text

    return default


def _normalize_failed_record(row, default='Unknown Salesforce error'):
    """Return a failed-record row with stable sf__Id and sf__Error fields."""
    normalized = dict(row or {})
    normalized['sf__Id'] = (
        normalized.get('sf__Id') or normalized.get('Id') or normalized.get('id') or ''
    )
    normalized['sf__Error'] = _extract_error_message(normalized, default=default)
    return normalized


def _write_failed_records_csv(error_file, failed_records, default='Unknown Salesforce error'):
    """Write failed rows with guaranteed error text so exports stay useful across environments."""
    normalized_records = [
        _normalize_failed_record(row, default=default) for row in (failed_records or [])
    ]
    pd.DataFrame(normalized_records).to_csv(error_file, index=False, encoding='utf-8-sig')


def format_salesforce_date(series):
    parsed = pd.to_datetime(series, errors='coerce')
    return parsed.dt.strftime('%Y-%m-%d').where(parsed.notna(), '')


def format_salesforce_datetime(series):
    parsed = pd.to_datetime(series, errors='coerce')
    return parsed.dt.strftime('%Y-%m-%dT%H:%M:%S.000+0000').where(parsed.notna(), '')


def normalize_salesforce_temporal_fields(df_chunk, date_columns=None, datetime_columns=None):
    for column in date_columns or []:
        if column in df_chunk.columns:
            df_chunk[column] = format_salesforce_date(df_chunk[column])
    for column in datetime_columns or []:
        if column in df_chunk.columns:
            df_chunk[column] = format_salesforce_datetime(df_chunk[column])
    return df_chunk


def _df_to_csv_bytes(df):
    """Serialize DataFrame directly to UTF-8 CSV bytes with no intermediate string.
    ~30-40% faster and uses ~2x less peak RAM than to_csv() + .encode()."""
    # Vectorized null normalization — strip whitespace and replace empty strings with NaN.
    obj_cols = df.select_dtypes(include='object').columns
    if len(obj_cols):
        df = df.copy()
        df[obj_cols] = df[obj_cols].apply(
            lambda s: s.where(s.notna(), '').astype(str).str.strip().replace('', np.nan)
        )
    buf = io.BytesIO()
    df.to_csv(buf, index=False, lineterminator='\n', encoding='utf-8', na_rep='')
    return buf.getvalue()


def _df_to_csv_string(df):
    # Vectorized null normalization — same approach as _df_to_csv_bytes
    obj_cols = df.select_dtypes(include='object').columns
    if len(obj_cols):
        df = df.copy()
        df[obj_cols] = df[obj_cols].apply(
            lambda s: s.where(s.notna(), '').astype(str).str.strip().replace('', np.nan)
        )
    return df.to_csv(index=False, lineterminator='\n', na_rep='')


def set_stop_flag():
    """Set the global stop flag to cancel ongoing operations"""
    _stop_flag.set()


def clear_stop_flag():
    """Clear the stop flag before starting new operations"""
    _stop_flag.clear()


def is_stopped():
    """Check if stop has been requested"""
    return _stop_flag.is_set()


# -------------------------------------------------
# Bulk API 2.0 Low-Level Helpers
# -------------------------------------------------
def _bulk2_base(sf):
    return f'https://{sf.sf_instance}/services/data/v{SF_API_VERSION}/jobs/ingest'


def _bulk2_headers(sf, content_type='application/json'):
    return {'Authorization': f'Bearer {sf.session_id}', 'Content-Type': content_type}


def _bulk2_create_job(sf, object_name, operation='insert', external_id_field=None):
    object_name, operation, external_id_field = _normalize_bulk_inputs(
        object_name, operation, external_id_field
    )
    payload = {
        'object': object_name,
        'operation': operation,
        'contentType': 'CSV',
        'lineEnding': 'LF'
    }
    if operation == 'upsert' and external_id_field:
        payload['externalIdFieldName'] = external_id_field

    # 400 error codes that are transient throttling signals — safe to retry
    _RETRYABLE_JOB_CODES = {'REQUEST_LIMIT_EXCEEDED', 'TXN_SECURITY_METERING_ERROR',
                             'LIMIT_EXCEEDED', 'SERVER_UNAVAILABLE'}
    _BACKOFF_SECONDS = [10, 30, 60]   # wait before each retry attempt

    r = None
    for attempt in range(len(_BACKOFF_SECONDS) + 1):
        r = _get_http_session(sf).post(_bulk2_base(sf), headers=_bulk2_headers(sf), json=payload)
        if r.ok:
            break
        # Parse Salesforce error details
        try:
            sf_errors = r.json()
            err_list = sf_errors if isinstance(sf_errors, list) else [sf_errors]
            error_codes = {e.get('errorCode', '') for e in err_list}
            msg = '; '.join(
                f"{e.get('errorCode', '?')}: {e.get('message', str(e))}"
                for e in err_list
            )
        except Exception:
            error_codes = set()
            msg = r.text[:500]

        # Retry only on throttling/quota 400s; raise immediately on schema/auth errors
        is_retryable = r.status_code == 400 and bool(error_codes & _RETRYABLE_JOB_CODES)
        if is_retryable and attempt < len(_BACKOFF_SECONDS):
            wait = _BACKOFF_SECONDS[attempt]
            print(
                f'[bulk_v2] Job creation throttled ({msg[:80]}), '
                f'retrying in {wait}s (attempt {attempt + 1}/{len(_BACKOFF_SECONDS)})'
            )
            time.sleep(wait)
            continue

        # Non-retryable or exhausted retries — surface the actual Salesforce error
        raise RuntimeError(
            f"Salesforce Bulk API 2.0 job creation failed ({r.status_code}): {msg}\n"
            f"Object: {object_name!r}  Operation: {operation!r}"
        )

    # Track API quota
    get_quota_tracker().increment(api_calls=1, bulk_jobs=1)
    return r.json()['id']


def _bulk2_upload_csv(sf, job_id, csv_data):
    """Upload CSV body. Accepts either str or bytes (bytes is faster — no extra encode)."""
    url = f'{_bulk2_base(sf)}/{job_id}/batches'
    if isinstance(csv_data, str):
        csv_data = csv_data.encode('utf-8')
    r = _get_http_session(sf).put(url, headers=_bulk2_headers(sf, 'text/csv'), data=csv_data)
    r.raise_for_status()


def _bulk2_close_job(sf, job_id):
    url = f'{_bulk2_base(sf)}/{job_id}'
    r = _get_http_session(sf).patch(url, headers=_bulk2_headers(sf), json={'state': 'UploadComplete'})
    r.raise_for_status()


def _bulk2_abort_job(sf, job_id):
    url = f'{_bulk2_base(sf)}/{job_id}'
    try:
        _get_http_session(sf).patch(url, headers=_bulk2_headers(sf), json={'state': 'Aborted'}, timeout=30)
    except Exception:
        pass


def _bulk2_poll_job(sf, job_id, poll_interval=None, max_wait=JOB_TIMEOUT_SECONDS, on_poll_status=None):
    """Poll a Bulk API 2.0 job with adaptive intervals: 0.5s → 1s → 2s → 3s → 5s."""
    url = f'{_bulk2_base(sf)}/{job_id}'
    sess = _get_http_session(sf)
    elapsed = 0.0
    last_log = -10.0
    abort_requested = False
    while elapsed < max_wait:
        if is_stopped() and not abort_requested:
            _bulk2_abort_job(sf, job_id)
            abort_requested = True
        r = sess.get(url, headers=_bulk2_headers(sf), timeout=30)
        r.raise_for_status()
        info = r.json()
        state = info.get('state', '')
        if state == 'Aborted' and is_stopped():
            return info
        if state == 'JobComplete':
            if on_poll_status:
                processed = info.get('numberRecordsProcessed', 0)
                on_poll_status(
                    f'    Polling {job_id[:8]}... state={state}, processed={processed} ({elapsed:.0f}s)'
                )
            return info
        if state in ('Failed', 'Aborted'):
            raw_msg = info.get('errorMessage')
            error_msg = raw_msg if raw_msg else info.get('error', 'No error message from Salesforce')
            if on_poll_status:
                processed = info.get('numberRecordsProcessed', 0)
                on_poll_status(
                    f'    Polling {job_id[:8]}... state={state}, processed={processed} ({elapsed:.0f}s)'
                )
            # Extra debug: print error message and job info
            print(f"[BULK2 ERROR] Job {job_id[:8]} state={state}: {error_msg}")
            print(f"[BULK2 ERROR] Full job info: {info}")
            # If the error is about compound fields, make it explicit
            if error_msg and 'compound' in str(error_msg).lower() and 'not supported' in str(error_msg).lower():
                raise RuntimeError(f'❌ Bulk API failed due to compound fields: {error_msg}\nFields in job: {info.get("object", "")}')
            raise RuntimeError(f'Job {job_id[:8]} state={state}: {error_msg}')
        if elapsed - last_log >= 10:
            processed = info.get('numberRecordsProcessed', '?')
            # Explain SF internal queuing when processed stays at 0 for a long time
            queue_hint = ''
            if processed == 0 and elapsed > 30:
                queue_hint = ' ⏳ SF queuing internally — awaiting a processing slot'
            msg = f'    Polling {job_id[:8]}... state={state}, processed={processed} ({elapsed:.0f}s){queue_hint}'
            print(msg)
            if on_poll_status:
                on_poll_status(msg)
            last_log = elapsed
        # OPTIMIZED (2026-08-12): More aggressive polling schedule for faster job completion detection
        # Previous: 0.5s, 1s, 2s, 3s, 5s  →  New: 0.2s, 0.5s, 1s, 2s, 3s
        if elapsed < 2:
            sleep_time = 0.2  # Very fast for quick small jobs
        elif elapsed < 10:
            sleep_time = 0.5  # Fast during active processing
        elif elapsed < 60:
            sleep_time = 1    # Normal polling during processing
        elif elapsed < 180:
            sleep_time = 2    # Slower during long waits
        else:
            sleep_time = 3    # Back off on very long jobs (>3min)
        time.sleep(sleep_time)
        elapsed += sleep_time
    raise TimeoutError(f'Job {job_id} did not complete within {max_wait}s')


def _bulk2_get_failed_results(sf, job_id):
    url = f'{_bulk2_base(sf)}/{job_id}/failedResults'
    r = _get_http_session(sf).get(url, headers=_bulk2_headers(sf, 'text/csv'))
    r.raise_for_status()
    if not r.text.strip():
        return []
    rows = list(csv.DictReader(io.StringIO(r.text)))
    normalized = []
    for row in rows:
        normalized_row = dict(row)
        normalized.append(
            _normalize_failed_record(
                normalized_row,
                default='Bulk API 2.0 failed record with no error text returned'
            )
        )
    return normalized


def _bulk2_get_unprocessed_results(sf, job_id):
    url = f'{_bulk2_base(sf)}/{job_id}/unprocessedrecords'
    r = _get_http_session(sf).get(url, headers=_bulk2_headers(sf, 'text/csv'))
    r.raise_for_status()
    if not r.text.strip():
        return []
    rows = list(csv.DictReader(io.StringIO(r.text)))
    normalized = []
    for row in rows:
        normalized_row = dict(row)
        normalized.append(
            _normalize_failed_record(
                normalized_row,
                default='Bulk API 2.0 unprocessed record with no error text returned'
            )
        )
    return normalized


# =============================================================================
# BULK API 1.0 INGEST (fallback when Bulk v2 hits limits)
# =============================================================================
_BULK1_INGEST_NS = 'http://www.force.com/2009/06/asyncapi/dataload'


def _bulk1_build_job_payload(operation, object_name, external_id_field=None, concurrency_mode=None):
    """Build a schema-safe Bulk API 1.0 ingest job XML payload."""
    import xml.etree.ElementTree as ET

    ET.register_namespace('', _BULK1_INGEST_NS)
    job = ET.Element(f'{{{_BULK1_INGEST_NS}}}jobInfo')

    op_el = ET.SubElement(job, f'{{{_BULK1_INGEST_NS}}}operation')
    op_el.text = operation

    obj_el = ET.SubElement(job, f'{{{_BULK1_INGEST_NS}}}object')
    obj_el.text = object_name

    if operation == 'upsert' and external_id_field:
        ext_el = ET.SubElement(job, f'{{{_BULK1_INGEST_NS}}}externalIdFieldName')
        ext_el.text = external_id_field

    if concurrency_mode:
        cm_el = ET.SubElement(job, f'{{{_BULK1_INGEST_NS}}}concurrencyMode')
        cm_el.text = concurrency_mode

    ct_el = ET.SubElement(job, f'{{{_BULK1_INGEST_NS}}}contentType')
    ct_el.text = 'CSV'

    xml_bytes = ET.tostring(job, encoding='utf-8', xml_declaration=True)
    return xml_bytes


def _bulk1_base(sf):
    return f'https://{sf.sf_instance}/services/async/{SF_API_VERSION}/job'


def _bulk1_headers(sf, content_type='application/xml; charset=UTF-8'):
    return {
        'Authorization': f'Bearer {sf.session_id}',
        'X-SFDC-Session': sf.session_id,
        'Content-Type': content_type,
        'Accept': 'application/xml',
    }


def _bulk1_create_job(sf, object_name, operation='insert', external_id_field=None, concurrency_mode=None):
    """Create a Bulk API 1.0 job. Returns job_id."""
    object_name, operation, external_id_field = _normalize_bulk_inputs(
        object_name, operation, external_id_field
    )
    payload = _bulk1_build_job_payload(operation, object_name, external_id_field, concurrency_mode)
    r = _get_http_session(sf).post(
        _bulk1_base(sf),
        headers=_bulk1_headers(sf),
        data=payload
    )
    try:
        r.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f'Bulk API 1.0 job creation failed ({r.status_code}) for {object_name} '
            f'[{operation}]: {r.text[:400]}'
        ) from exc
    # Track API quota
    get_quota_tracker().increment(api_calls=1, bulk_jobs=1)
    # Parse XML response for job ID
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)
    ns = {'ns': _BULK1_INGEST_NS}
    job_id = root.find('ns:id', ns)
    if job_id is None:
        raise RuntimeError(f'Bulk v1 create job failed: {r.text[:200]}')
    return job_id.text


def _bulk1_add_batch(sf, job_id, csv_data):
    """Add a CSV batch to a Bulk API 1.0 job. Returns batch_id."""
    url = f'{_bulk1_base(sf)}/{job_id}/batch'
    if isinstance(csv_data, str):
        csv_data = csv_data.encode('utf-8')
    r = _get_http_session(sf).post(
        url,
        headers=_bulk1_headers(sf, 'text/csv; charset=UTF-8'),
        data=csv_data
    )
    r.raise_for_status()
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)
    ns = {'ns': _BULK1_INGEST_NS}
    batch_id = root.find('ns:id', ns)
    if batch_id is None:
        raise RuntimeError(f'Bulk v1 add batch failed: {r.text[:200]}')
    return batch_id.text


def _bulk1_close_job(sf, job_id):
    """Close a Bulk API 1.0 job."""
    payload = f'''<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="{_BULK1_INGEST_NS}">
    <state>Closed</state>
</jobInfo>'''
    r = _get_http_session(sf).post(
        f'{_bulk1_base(sf)}/{job_id}', headers=_bulk1_headers(sf), data=payload.encode('utf-8')
    )
    r.raise_for_status()


def _bulk1_abort_job(sf, job_id):
    payload = f'<jobInfo xmlns="{_BULK1_INGEST_NS}"><state>Aborted</state></jobInfo>'
    response = _get_http_session(sf).post(
        f'{_bulk1_base(sf)}/{job_id}', headers=_bulk1_headers(sf),
        data=payload.encode('utf-8'), timeout=30,
    )
    response.raise_for_status()


def _bulk1_poll_batch(sf, job_id, batch_id, max_wait=JOB_TIMEOUT_SECONDS):
    """Poll a Bulk v1 batch until completion. Returns state string."""
    import xml.etree.ElementTree as ET
    url = f'{_bulk1_base(sf)}/{job_id}/batch/{batch_id}'
    headers = _bulk1_headers(sf)
    elapsed = 0.0
    abort_requested = False
    while elapsed < max_wait:
        if is_stopped() and not abort_requested:
            abort_requested = True
            try:
                _bulk1_abort_job(sf, job_id)
            except requests.RequestException:
                pass
        r = _get_http_session(sf).get(url, headers=headers, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {'ns': _BULK1_INGEST_NS}
        state = root.find('ns:state', ns)
        state_text = state.text if state is not None else ''
        if state_text in ('Completed', 'Failed', 'Not Processed', 'Aborted'):
            state_msg = root.find('ns:stateMessage', ns)
            return state_text, (state_msg.text if state_msg is not None else '')
        sleep_time = 2 if elapsed < 30 else 5
        time.sleep(sleep_time)
        elapsed += sleep_time
    raise TimeoutError(f'Bulk v1 batch {batch_id} did not complete within {max_wait}s')


def _bulk1_get_batch_state_message(sf, job_id, batch_id):
    """Fetch the stateMessage from a failed Bulk v1 batch for better error reporting."""
    try:
        import xml.etree.ElementTree as ET
        url = f'{_bulk1_base(sf)}/{job_id}/batch/{batch_id}'
        r = _get_http_session(sf).get(url, headers=_bulk1_headers(sf))
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {'ns': _BULK1_INGEST_NS}
        msg = root.find('ns:stateMessage', ns)
        return msg.text if msg is not None else ''
    except Exception:
        return ''


def _bulk1_get_batch_results(sf, job_id, batch_id):
    """Get results from Bulk v1 batch. Returns (success_count, failed_records_list)."""
    url = f'{_bulk1_base(sf)}/{job_id}/batch/{batch_id}/result'
    headers = _bulk1_headers(sf)
    headers['Accept'] = 'text/csv'
    r = _get_http_session(sf).get(url, headers=headers)
    r.raise_for_status()
    if not r.text.strip():
        return 0, []
    reader = csv.DictReader(io.StringIO(r.text))
    success = 0
    failed = []
    for row in reader:
        # Bulk v1 result CSV uses lowercase column names (id, success, created, error)
        _success_val = (row.get('Success') or row.get('success') or row.get('SUCCESS') or '').strip().lower()
        if _success_val == 'true':
            success += 1
        else:
            # Normalize to sf__ prefix so the error table renders consistently
            err_msg = _extract_error_message(
                row,
                default='Bulk v1 batch-level failure — no per-record detail from Salesforce'
            )
            rec_id = row.get('Id') or row.get('id') or row.get('ID') or ''
            failed.append({'sf__Id': rec_id, 'sf__Error': err_msg, **row})
    return success, failed


def _process_chunk_bulk1(chunk_num, df_chunk, object_name, sf, operation='insert',
                         external_id_field=None, on_status=None):
    """Submit one chunk via Bulk API 1.0. Returns (success, failed_records, timing)."""
    t0 = time.time()
    try:
        if is_stopped():
            raise InterruptedError('Operation stopped by user before submission')
        csv_bytes = _df_to_csv_bytes(df_chunk)
        t_prep = time.time()

        if is_stopped():
            raise InterruptedError('Operation stopped by user before submission')
        _bulk1_concurrency = 'Serial' if (operation or 'insert').strip().lower() == 'delete' else None
        job_id = _bulk1_create_job(sf, object_name, operation, external_id_field, _bulk1_concurrency)
        batch_id = _bulk1_add_batch(sf, job_id, csv_bytes)
        _bulk1_close_job(sf, job_id)
        if on_status:
            on_status(f'[bulk_v1] Job {job_id[:8]} batch {batch_id[:8]} state=Uploaded')
        t_upload = time.time()

        state, state_msg = _bulk1_poll_batch(sf, job_id, batch_id)
        if on_status:
            msg_hint = f' — {state_msg}' if state_msg else ''
            on_status(f'[bulk_v1] Job {job_id[:8]} batch {batch_id[:8]} state={state}{msg_hint}')
        t_done = time.time()

        if state == 'Completed':
            success, failed = _bulk1_get_batch_results(sf, job_id, batch_id)
        elif state == 'Failed':
            try:
                success, failed = _bulk1_get_batch_results(sf, job_id, batch_id)
            except Exception:
                success, failed = 0, []
            detail = state_msg or 'No detail returned by Salesforce'
            if on_status:
                on_status(f'[bulk_v1] Job {job_id[:8]} batch-level Failed: {detail}')
            # Return with JOB_LEVEL_FAILED so _drain_delete retries via REST
            timing = {
                'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'bulk_v1',
                'csv_prep_s': round(t_prep - t0, 2),
                'upload_s': round(t_upload - t_prep, 2),
                'sf_process_s': round(t_done - t_upload, 2),
                'total_s': round(time.time() - t0, 2),
                'error': f'JOB_LEVEL_FAILED: bulk v1 batch Failed — {detail[:150]}',
            }
            return success, failed, timing
        else:
            detail = state_msg or ''
            timing = {
                'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'bulk_v1',
                'csv_prep_s': round(t_prep - t0, 2),
                'upload_s': round(t_upload - t_prep, 2),
                'sf_process_s': round(t_done - t_upload, 2),
                'total_s': round(time.time() - t0, 2),
                'error': f'JOB_LEVEL_FAILED: bulk v1 batch state={state} — {detail[:150]}',
            }
            return 0, [], timing

        # LOADING_BABA: Record-level retry with exponential backoff
        if failed and (operation or 'insert').strip().lower() != 'delete':
            recovered, failed = _retry_failed_records(
                sf, object_name, failed, operation, external_id_field
            )
            success += recovered

        timing = {
            'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'bulk_v1',
            'csv_prep_s': round(t_prep - t0, 2),
            'upload_s': round(t_upload - t_prep, 2),
            'sf_process_s': round(t_done - t_upload, 2),
            'total_s': round(time.time() - t0, 2),
        }
        return success, failed, timing
    except Exception as e:
        error_rows = df_chunk.copy()
        error_rows['sf__Error'] = str(e)
        error_rows['sf__Id'] = ''
        error_rows['chunk_num'] = chunk_num
        timing = {'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'bulk_v1',
                  'csv_prep_s': 0, 'upload_s': 0, 'sf_process_s': 0,
                  'total_s': round(time.time() - t0, 2), 'error': str(e)[:120]}
        return 0, error_rows.to_dict(orient='records'), timing


# =============================================================================
# REST COMPOSITE INGEST (200 records/request — fallback for rate-limited orgs)
# =============================================================================
REST_COMPOSITE_MAX = 200
SPECIAL_DELETE_OBJECT_WARRANTY_CODE = 'WOD_2__Warranty_Code__c'
SPECIAL_DELETE_LOOKUP_QUERY_CHUNK = 400
SPECIAL_DELETE_UPDATE_CHUNK = 8000
SPECIAL_DELETE_PRODUCT2_LOOKUPS = [
    'PRSM_Component_DVCM__c',
    'PRSM_Component_IMACS__c',
    'PRSM_Component__c',
    'PRSM_SEAG_Code__c',
]


def _rest_composite_submit(sf, records, object_name, operation='insert', external_id_field=None):
    """Submit up to 200 records via REST Composite SObjects.
    Returns (processed_count, failed_records_list)."""
    object_name, operation, external_id_field = _normalize_bulk_inputs(
        object_name, operation, external_id_field if (operation or 'insert').strip().lower() == 'upsert' else None
    )
    base_url = f'https://{sf.sf_instance}/services/data/v{SF_API_VERSION}'
    headers = {
        'Authorization': f'Bearer {sf.session_id}',
        'Content-Type': 'application/json',
    }

    # Build endpoint and method
    op = operation.lower()
    if op == 'upsert':
        if not external_id_field:
            raise ValueError("upsert via REST requires external_id_field")
        url = f'{base_url}/composite/sobjects/{object_name}/{external_id_field}'
        method = 'PATCH'
    elif op == 'update':
        url = f'{base_url}/composite/sobjects'
        method = 'PATCH'
    elif op == 'delete':
        ids = [str(r.get('Id') or r.get('id') or '') for r in records]
        ids = [i for i in ids if i]
        if not ids:
            return 0, []
        url = f'{base_url}/composite/sobjects?ids={",".join(ids)}&allOrNone=false'
        r = _get_http_session(sf).delete(url, headers=headers)
        if r.status_code >= 400:
            raise RuntimeError(f'REST DELETE failed ({r.status_code}): {r.text[:200]}')
        return _parse_rest_results(records, r.json())
    else:
        url = f'{base_url}/composite/sobjects'
        method = 'POST'

    # Add attributes to each record
    payload_records = []
    for rec in records:
        out = {k: v for k, v in rec.items() if not (isinstance(v, float) and pd.isna(v))}
        out['attributes'] = {'type': object_name}
        payload_records.append(out)

    body = {'allOrNone': False, 'records': payload_records}
    sess = _get_http_session(sf)
    if method == 'POST':
        r = sess.post(url, headers=headers, json=body)
    else:
        r = sess.patch(url, headers=headers, json=body)

    # Track API quota (1 REST call per 200 records)
    get_quota_tracker().increment(api_calls=1)

    if r.status_code >= 400:
        raise RuntimeError(f'REST {method} failed ({r.status_code}): {r.text[:200]}')
    return _parse_rest_results(records, r.json())


def _parse_rest_results(inputs, results):
    """Parse Composite SObjects response. Returns (success_count, failed_list)."""
    processed = 0
    failed = []
    for rec, res in zip(inputs, results):
        if res.get('success'):
            processed += 1
        else:
            errors = res.get('errors') or []
            err_msg = ''
            if errors:
                first = errors[0]
                err_msg = f"{first.get('statusCode', 'ERROR')}:{first.get('message', '')}"
            else:
                err_msg = 'UNKNOWN'
            row = dict(rec)
            row.pop('attributes', None)
            row['sf__Id'] = res.get('id') or ''
            row['sf__Error'] = err_msg
            failed.append(_normalize_failed_record(row, default='REST composite request failed'))
    return processed, failed


def _sf_query_all_records(sf, soql):
    """Fetch all rows for a SOQL query via query/query_more."""
    result = sf.query(soql)
    records = list(result.get('records', []) or [])
    while not result.get('done', True):
        result = sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)
        records.extend(result.get('records', []) or [])
    return records


def _iter_delete_ids_from_source(csv_file_path, id_column, chunk_size, chunk_iterator=None):
    """Yield cleaned Salesforce IDs from a file or caller-provided chunk iterator."""
    source = chunk_iterator
    if source is None:
        source = pd.read_csv(csv_file_path, chunksize=max(int(chunk_size or 1), 1), low_memory=False)

    for df_chunk in source:
        if df_chunk is None:
            continue
        df_chunk = df_chunk.dropna(how='all').copy()
        if df_chunk.empty:
            continue
        df_chunk.columns = df_chunk.columns.str.strip()
        if id_column not in df_chunk.columns:
            raise ValueError(f'missing ID column "{id_column}"')
        ids = df_chunk[id_column].fillna('').astype(str).str.strip()
        for record_id in ids[ids != ''].tolist():
            yield record_id


def _rest_delete_ids_recursive(sf, object_name, ids, chunk_label, on_status=None):
    """Delete IDs via REST composite; split recursively on DELETE_OPERATION_TOO_LARGE."""
    records = [{'Id': record_id} for record_id in ids if record_id]
    if not records:
        return 0, []

    try:
        success, failed = _rest_composite_submit(sf, records, object_name, 'delete')
    except Exception as exc:
        err_text = str(exc)
        if 'DELETE_OPERATION_TOO_LARGE' in err_text and len(records) > 1:
            midpoint = max(1, len(records) // 2)
            if on_status:
                on_status(
                    f'✂️ REST delete batch {chunk_label} too large at {len(records):,} rows — retrying as smaller batches',
                    level='warning'
                )
            left_success, left_failed = _rest_delete_ids_recursive(
                sf, object_name, [r['Id'] for r in records[:midpoint]], f'{chunk_label}.1', on_status
            )
            right_success, right_failed = _rest_delete_ids_recursive(
                sf, object_name, [r['Id'] for r in records[midpoint:]], f'{chunk_label}.2', on_status
            )
            return left_success + right_success, left_failed + right_failed
        failed_rows = []
        for rec in records:
            failed_rows.append({
                'Id': rec['Id'],
                'sf__Id': rec['Id'],
                'sf__Error': err_text[:200],
                'chunk_num': chunk_label,
            })
        return 0, failed_rows

    too_large_failed = []
    final_failed = []
    for row in failed:
        row = _normalize_failed_record(row, default='Delete failed in Salesforce')
        row['chunk_num'] = chunk_label
        err_text = row['sf__Error']
        if 'DELETE_OPERATION_TOO_LARGE' in err_text and len(records) > 1:
            too_large_failed.append(row.get('Id') or row.get('sf__Id') or '')
        else:
            final_failed.append(row)

    if too_large_failed:
        if on_status:
            on_status(
                f'✂️ REST delete batch {chunk_label} hit DELETE_OPERATION_TOO_LARGE for {len(too_large_failed):,} rows — retrying smaller batches',
                level='warning'
            )
        split_success, split_failed = _rest_delete_ids_recursive(
            sf, object_name, too_large_failed, f'{chunk_label}.split', on_status
        )
        success += split_success
        final_failed.extend(split_failed)

    return success, final_failed


def _bulk_delete_warranty_code_special(
    csv_file_path, object_name, sf, id_column, chunk_size,
    error_file, on_progress=None, on_status=None, on_error=None,
    chunk_iterator=None,
):
    """Delete Warranty Codes by first nulling Product2 lookups, then REST-deleting small batches."""
    start_time = time.time()
    all_failed_records = []
    total_success = 0
    total_failed = 0
    chunk_timings = []

    if on_status:
        on_status(
            '🧩 Special delete strategy for WOD_2__Warranty_Code__c: clear Product2 lookups, then delete in small REST batches',
            level='warning'
        )

    ids = list(_iter_delete_ids_from_source(csv_file_path, id_column, chunk_size, chunk_iterator=chunk_iterator))
    total_processed = len(ids)
    if total_processed == 0:
        return {
            'total_processed': 0,
            'total_success': 0,
            'total_failed': 0,
            'total_completed': 0,
            'elapsed': 0.0,
            'error_file': None,
            'chunk_timings': [],
            'timing_summary': _build_timing_summary([], 0.0),
            'auto_mode_stats': {'thread_reductions': 0, 'api_switches': 0, 'final_api': 'rest'},
        }

    if on_progress:
        on_progress(0.02)

    for field in SPECIAL_DELETE_PRODUCT2_LOOKUPS:
        if is_stopped():
            break
        if on_status:
            on_status(f'🔧 Clearing Product2.{field} references before delete...')
        field_updates = 0
        for start in range(0, len(ids), SPECIAL_DELETE_LOOKUP_QUERY_CHUNK):
            if is_stopped():
                break
            chunk_ids = ids[start:start + SPECIAL_DELETE_LOOKUP_QUERY_CHUNK]
            id_list = "','".join(chunk_ids)
            soql = f"SELECT Id FROM Product2 WHERE {field} IN ('{id_list}') LIMIT 100000"
            try:
                rows = _sf_query_all_records(sf, soql)
            except Exception as exc:
                if on_error:
                    on_error(f'Could not query Product2 for {field}: {exc}')
                continue
            row_ids = [r.get('Id') for r in rows if isinstance(r, dict) and r.get('Id')]
            if not row_ids:
                continue

            update_df = pd.DataFrame([{'Id': product_id, field: ''} for product_id in row_ids])
            for upd_start in range(0, len(update_df), SPECIAL_DELETE_UPDATE_CHUNK):
                upd_df = update_df.iloc[upd_start:upd_start + SPECIAL_DELETE_UPDATE_CHUNK].copy()
                success, failed, timing = _process_chunk_bulk1(
                    f'preclear-{field}-{start + upd_start}', upd_df, 'Product2', sf, 'update', None, on_status
                )
                field_updates += int(success or 0)
                if isinstance(failed, list) and failed:
                    for row in failed:
                        row = dict(row)
                        row['chunk_num'] = f'preclear-{field}'
                        row['sf__Error'] = row.get('sf__Error') or f'Failed to clear Product2.{field}'
                        all_failed_records.append(row)
                if timing is not None:
                    chunk_timings.append(timing)

        if on_status:
            on_status(f'✅ Product2.{field}: cleared {field_updates:,} referencing rows')

    if all_failed_records and on_status:
        on_status('⚠️ Some Product2 lookup clears failed; delete will continue on remaining records', level='warning')

    if on_progress:
        on_progress(0.15)

    rest_batches = max((total_processed + REST_COMPOSITE_MAX - 1) // REST_COMPOSITE_MAX, 1)
    for batch_index, start in enumerate(range(0, len(ids), REST_COMPOSITE_MAX), start=1):
        if is_stopped():
            break
        batch_ids = ids[start:start + REST_COMPOSITE_MAX]
        batch_label = f'rest-{batch_index}'
        t0 = time.time()
        success, failed = _rest_delete_ids_recursive(sf, object_name, batch_ids, batch_label, on_status)
        total_success += success
        total_failed += len(failed)
        all_failed_records.extend(failed)
        chunk_timings.append({
            'chunk': batch_label,
            'rows': len(batch_ids),
            'api': 'rest',
            'csv_prep_s': 0,
            'upload_s': 0,
            'sf_process_s': round(time.time() - t0, 2),
            'total_s': round(time.time() - t0, 2),
        })
        if on_status and (batch_index == 1 or batch_index % 10 == 0 or failed):
            on_status(
                f'✅ REST delete batch {batch_index:,}/{rest_batches:,}: {success:,} deleted, {len(failed):,} failed'
            )
        if on_progress:
            on_progress(min(0.15 + (0.85 * batch_index / rest_batches), 0.99))

    if all_failed_records:
        try:
            _write_failed_records_csv(
                error_file,
                all_failed_records,
                default='Delete failed in Salesforce'
            )
        except Exception as exc:
            if on_error:
                on_error(f'⚠️ Could not write error file: {exc}')

    elapsed = time.time() - start_time
    if on_progress:
        on_progress(1.0)

    return {
        'total_processed': total_processed,
        'total_success': total_success,
        'total_failed': total_failed,
        'total_completed': total_success + total_failed,
        'elapsed': elapsed,
        'error_file': error_file if all_failed_records else None,
        'chunk_timings': chunk_timings,
        'timing_summary': _build_timing_summary(chunk_timings, elapsed),
        'auto_mode_stats': {
            'thread_reductions': 0,
            'api_switches': 0,
            'final_api': 'rest',
        },
    }


def _bulk_delete_single_job_v1(
    csv_file_path, object_name, sf, id_column, chunk_size,
    error_file, on_progress=None, on_status=None, on_error=None,
    chunk_iterator=None,
):
    """Delete via salesforce_bulk one-job flow, matching the user's legacy helper pattern."""
    start_time = time.time()
    ids = list(_iter_delete_ids_from_source(csv_file_path, id_column, chunk_size, chunk_iterator=chunk_iterator))
    total_processed = len(ids)
    batch_size = min(max(int(chunk_size or BULK1_MINI_CHUNK), 1), BULK1_MINI_CHUNK)
    total_success = 0
    total_failed = 0
    all_failed_records = []
    chunk_timings = []

    if total_processed == 0:
        return {
            'total_processed': 0,
            'total_success': 0,
            'total_failed': 0,
            'total_completed': 0,
            'elapsed': 0.0,
            'error_file': None,
            'chunk_timings': [],
            'timing_summary': _build_timing_summary([], 0.0),
            'auto_mode_stats': {'thread_reductions': 0, 'api_switches': 0, 'final_api': 'bulk_v1'},
        }

    if on_status:
        on_status(
            f'⚙️ Delete mode: one Bulk API 1.0 helper-style job with {((total_processed + batch_size - 1) // batch_size):,} batch(es) '
            f'(capped at {batch_size:,} rows/batch)',
            level='warning'
        )

    batch_refs = []
    try:
        bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
        job_id = bulk.create_delete_job(object_name)
        total_batches = max((total_processed + batch_size - 1) // batch_size, 1)
        for batch_num, start in enumerate(range(0, total_processed, batch_size), start=1):
            if is_stopped():
                break
            batch_ids = ids[start:start + batch_size]
            delete_rows = [{'Id': record_id} for record_id in batch_ids]
            batch_id = bulk.post_batch(job_id, CsvDictsAdapter(iter(delete_rows)))
            batch_refs.append((batch_num, batch_id, batch_ids))
            if on_status:
                on_status(
                    f'[bulk_v1] Batch {batch_num}/{total_batches} Job {job_id[:8]} batch {batch_id[:8]} state=Queued ({len(batch_ids):,} rows)'
                )
            if on_progress:
                on_progress(min(0.2 * batch_num / total_batches, 0.2))

        bulk.close_job(job_id)

        total_batches = len(batch_refs)
        for idx, (batch_num, batch_id, batch_ids) in enumerate(batch_refs, start=1):
            if is_stopped():
                break
            t0 = time.time()
            state_msg = ''
            elapsed_batch = 0.0
            last_status_emit = -15.0
            while elapsed_batch < JOB_TIMEOUT_SECONDS:
                status = bulk.batch_status(batch_id, job_id=job_id, reload=True) or {}
                state = status.get('state', '')
                state_msg = status.get('stateMessage') or ''
                if on_status and elapsed_batch - last_status_emit >= 15:
                    msg_hint = f' — {state_msg}' if state_msg else ''
                    on_status(
                        f'[bulk_v1] Batch {batch_num}/{total_batches} Job {job_id[:8]} batch {batch_id[:8]} state={state or "Queued"}{msg_hint}'
                    )
                    last_status_emit = elapsed_batch
                if state in ('Completed', 'Failed', 'Not Processed'):
                    break
                time.sleep(2 if elapsed_batch < 30 else 5)
                elapsed_batch += 2 if elapsed_batch < 30 else 5
            else:
                state = 'Failed'
                state_msg = f'Bulk API 1.0 batch {batch_id} timed out'

            if on_status:
                msg_hint = f' — {state_msg}' if state_msg else ''
                on_status(f'[bulk_v1] Job {job_id[:8]} batch {batch_id[:8]} state={state}{msg_hint}')

            if state == 'Completed':
                success, failed = _bulk1_get_batch_results(sf, job_id, batch_id)
            elif state == 'Failed':
                try:
                    success, failed = _bulk1_get_batch_results(sf, job_id, batch_id)
                except Exception:
                    success, failed = 0, []
                if not failed:
                    failed = [
                        {
                            'Id': record_id,
                            'sf__Id': record_id,
                            'sf__Error': state_msg or 'Bulk API 1.0 batch failed',
                            'chunk_num': batch_num,
                        }
                        for record_id in batch_ids
                    ]
            else:
                success = 0
                failed = [
                    {
                        'Id': record_id,
                        'sf__Id': record_id,
                        'sf__Error': state_msg or f'Bulk API 1.0 batch state={state}',
                        'chunk_num': batch_num,
                    }
                    for record_id in batch_ids
                ]

            real_failed = []
            extra_success = 0
            for row in failed:
                row = dict(row)
                err_text = str(row.get('sf__Error') or row.get('Error') or row.get('error') or '')
                if 'ENTITY_IS_DELETED' in err_text:
                    extra_success += 1
                else:
                    row['sf__Id'] = row.get('sf__Id') or row.get('Id') or ''
                    row['sf__Error'] = err_text or (state_msg or 'Bulk API 1.0 delete failed')
                    row['chunk_num'] = batch_num
                    real_failed.append(row)

            success += extra_success
            total_success += success
            total_failed += len(real_failed)
            all_failed_records.extend(real_failed)
            chunk_timings.append({
                'chunk': batch_num,
                'rows': len(batch_ids),
                'api': 'bulk_v1',
                'csv_prep_s': 0,
                'upload_s': 0,
                'sf_process_s': round(time.time() - t0, 2),
                'total_s': round(time.time() - t0, 2),
            })

            if on_status:
                on_status(f'✅ Batch {batch_num}: {success:,} deleted, {len(real_failed):,} failed')
            if on_progress:
                on_progress(min(0.2 + (0.8 * idx / max(total_batches, 1)), 0.99))

    except Exception as exc:
        if on_error:
            on_error(f'❌ Delete job failed: {exc}')
        if not all_failed_records:
            all_failed_records = [
                {'Id': record_id, 'sf__Id': record_id, 'sf__Error': str(exc)[:200], 'chunk_num': 'job'}
                for record_id in ids
            ]
            total_failed = len(all_failed_records)

    if all_failed_records:
        try:
            _write_failed_records_csv(error_file, all_failed_records)
        except Exception as exc:
            if on_error:
                on_error(f'⚠️ Could not write error file: {exc}')

    elapsed = time.time() - start_time
    if on_progress:
        on_progress(1.0)

    return {
        'total_processed': total_processed,
        'total_success': total_success,
        'total_failed': total_failed,
        'total_completed': total_success + total_failed,
        'elapsed': elapsed,
        'error_file': error_file if all_failed_records else None,
        'chunk_timings': chunk_timings,
        'timing_summary': _build_timing_summary(chunk_timings, elapsed),
        'auto_mode_stats': {
            'thread_reductions': 0,
            'api_switches': 0,
            'final_api': 'bulk_v1',
        },
    }


def _process_chunk_rest(chunk_num, df_chunk, object_name, sf, operation='insert',
                        external_id_field=None, on_status=None):
    """Submit one chunk via REST Composite (200 records at a time).
    Returns (success, failed_records, timing)."""
    t0 = time.time()
    total_success = 0
    total_failed = []
    completed_records = 0
    try:
        records = df_chunk.to_dict(orient='records')

        # Process in batches of 200
        for i in range(0, len(records), REST_COMPOSITE_MAX):
            if is_stopped():
                raise RuntimeError('Operation stopped by user')
            batch = records[i:i + REST_COMPOSITE_MAX]
            if on_status:
                on_status(f'[rest] Submitting {len(batch):,} records from chunk {chunk_num}')
            success, failed = _rest_composite_submit(
                sf, batch, object_name, operation, external_id_field
            )
            total_success += success
            total_failed.extend(failed)
            completed_records += len(batch)
            if on_status:
                on_status(
                    f'[rest] Completed {len(batch):,} records from chunk {chunk_num}: '
                    f'{success:,} success, {len(failed):,} failed'
                )

        timing = {
            'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'rest',
            'csv_prep_s': 0, 'upload_s': 0,
            'sf_process_s': round(time.time() - t0, 2),
            'total_s': round(time.time() - t0, 2),
        }
        return total_success, total_failed, timing
    except Exception as e:
        error_text = f'{type(e).__name__}: {str(e).strip() or "No exception message was provided"}'
        error_rows = df_chunk.iloc[completed_records:].copy()
        error_rows['sf__Error'] = error_text
        error_rows['sf__Id'] = error_rows.get('Id', '')
        error_rows['chunk_num'] = chunk_num
        timing = {'chunk': chunk_num, 'rows': len(df_chunk), 'api': 'rest',
                  'csv_prep_s': 0, 'upload_s': 0, 'sf_process_s': 0,
                  'total_s': round(time.time() - t0, 2), 'error': error_text}
        return total_success, total_failed + error_rows.to_dict(orient='records'), timing


# -------------------------------------------------
# Process one chunk
# -------------------------------------------------
def _check_job_state(sf, job_id):
    """Return (state, numberRecordsProcessed, numberRecordsFailed) or None on error."""
    try:
        url = f'{_bulk2_base(sf)}/{job_id}'
        r = _get_http_session(sf).get(url, headers=_bulk2_headers(sf))
        if r.ok:
            info = r.json()
            return info.get('state', ''), info.get('numberRecordsProcessed', 0), info.get('numberRecordsFailed', 0)
    except Exception:
        pass
    return None, 0, 0


# =============================================================================
# RECORD-LEVEL BOUNDED RETRY WITH EXPONENTIAL BACKOFF
# (From LOADING_BABA retry_engine.py)
# After a chunk completes, retryable failed records are re-submitted via REST
# Composite with exponential backoff: 5s → 15s → 45s, max 3 attempts per record.
# =============================================================================
RETRY_BACKOFF_SCHEDULE = [5, 15, 45]  # seconds between attempts
MAX_RECORD_RETRIES = 3


def _retry_failed_records(sf, object_name, failed_records, operation='insert',
                          external_id_field=None, on_status=None):
    """Retry individual RETRYABLE records via REST Composite with exponential backoff.

    Parameters
    ----------
    failed_records : list of dict — records with 'sf__Error' field from prior attempt
    Returns: (recovered_count, still_failed_records)
    """
    if not failed_records:
        return 0, []

    # Separate retryable from non-retryable
    retryable = []
    non_retryable = []
    for rec in failed_records:
        err_text = rec.get('sf__Error', '')
        err_class, _ = classify_error(err_text)
        if err_class == ErrorClass.RETRYABLE:
            retryable.append(rec)
        else:
            non_retryable.append(rec)

    if not retryable:
        return 0, failed_records

    total_recovered = 0
    remaining = retryable

    for attempt in range(MAX_RECORD_RETRIES):
        if not remaining or is_stopped():
            break

        # Backoff sleep
        if attempt > 0:
            sleep_time = RETRY_BACKOFF_SCHEDULE[min(attempt, len(RETRY_BACKOFF_SCHEDULE) - 1)]
            if on_status:
                on_status(
                    f'⏱️ Record retry: attempt {attempt + 1}/{MAX_RECORD_RETRIES}, '
                    f'{len(remaining)} records, backoff {sleep_time}s'
                )
            if _stop_flag.wait(sleep_time):
                break

        # Clean records for re-submission (strip sf__ metadata columns)
        clean_records = []
        for rec in remaining:
            clean = {k: v for k, v in rec.items()
                     if not k.startswith('sf__') and k != 'chunk_num'}
            # Remove NaN/None values
            clean = {k: v for k, v in clean.items()
                     if v is not None and not (isinstance(v, float) and pd.isna(v))}
            clean_records.append(clean)

        # Submit via REST Composite in batches of 200
        batch_success = 0
        still_failed = []
        for i in range(0, len(clean_records), REST_COMPOSITE_MAX):
            if is_stopped():
                still_failed.extend(remaining[i:])
                break
            batch = clean_records[i:i + REST_COMPOSITE_MAX]
            try:
                success, failed = _rest_composite_submit(
                    sf, batch, object_name, operation, external_id_field
                )
                batch_success += success
                still_failed.extend(failed)
            except Exception as e:
                # Entire batch failed — mark all as failed
                for rec in batch:
                    rec['sf__Error'] = str(e)
                    rec['sf__Id'] = ''
                still_failed.extend(batch)

        total_recovered += batch_success

        # Re-classify still-failed for next attempt
        next_remaining = []
        for rec in still_failed:
            err_text = rec.get('sf__Error', '')
            err_class, _ = classify_error(err_text)
            if err_class == ErrorClass.RETRYABLE:
                next_remaining.append(rec)
            else:
                non_retryable.append(rec)

        remaining = next_remaining

        if on_status and batch_success > 0:
            on_status(
                f'✅ Record retry attempt {attempt + 1}: recovered {batch_success}, '
                f'{len(remaining)} still retryable'
            )

    # Any remaining retryable that exhausted all attempts go to non-retryable
    non_retryable.extend(remaining)

    return total_recovered, non_retryable


def _process_chunk_v2(chunk_num, df_chunk, object_name, sf, operation='insert',
                      external_id_field=None, verbose=False, on_poll_status=None):
    """Submit one chunk to Salesforce Bulk API 2.0.

    NO retries after the job has been submitted to Salesforce.
    Retrying after submission causes duplicate inserts — the root cause of
    the 2× / 3× row count bug. If something goes wrong mid-job we check the
    actual SF job state and either recover the result or report it as failed.

    Returns: (success_count, failed_records, timing_dict)
    timing_dict keys: chunk, rows, csv_prep_s, upload_s, sf_process_s, total_s
    """
    job_id = None
    _sem_acquired = False   # tracks whether we hold the concurrency semaphore
    t0 = time.time()
    try:
        if is_stopped():
            raise RuntimeError('Operation stopped by user')

        t_prep = time.time()
        # Serialize CSV while waiting for a free SF job slot (true parallelism).
        # Direct bytes serialization — ~30% faster than str + encode()
        csv_bytes = _df_to_csv_bytes(df_chunk)
        t_csv_done = time.time()

        # ── Throttle: wait for a free SF processing slot ──────────────────
        # SF only processes MAX_CONCURRENT_BULK_V2_JOBS jobs simultaneously.
        # Without this gate, 100+ jobs pile up, each showing processed=0 for
        # minutes, and job creation #101+ returns 400 REQUEST_LIMIT_EXCEEDED.
        _bulk_v2_semaphore.acquire()
        _sem_acquired = True
        if is_stopped():
            raise RuntimeError('Operation stopped by user')

        job_id = _bulk2_create_job(sf, object_name, operation, external_id_field)
        _register_active_job(sf, job_id)  # Track for Ctrl-C abort
        _bulk2_upload_csv(sf, job_id, csv_bytes)
        _bulk2_close_job(sf, job_id)
        t_upload_done = time.time()
        # From this point on the data is in Salesforce's hands — never retry

        job_info = _bulk2_poll_job(sf, job_id, on_poll_status=on_poll_status)
        _unregister_active_job(job_id)  # Job completed, stop tracking
        # Release slot so the next queued thread can start its SF job
        _bulk_v2_semaphore.release()
        _sem_acquired = False
        t_poll_done = time.time()

        total = job_info.get('numberRecordsProcessed', 0)
        failed_count = job_info.get('numberRecordsFailed', 0)
        success_count = total - failed_count
        failed_records = _bulk2_get_failed_results(sf, job_id) if failed_count > 0 else []

        # LOADING_BABA: Record-level retry with exponential backoff
        if failed_records and (operation or 'insert').strip().lower() != 'delete':
            recovered, failed_records = _retry_failed_records(
                sf, object_name, failed_records, operation, external_id_field
            )
            success_count += recovered

        timing = {
            'chunk': chunk_num,
            'rows': len(df_chunk),
            'csv_prep_s': round(t_csv_done - t_prep, 2),
            'upload_s': round(t_upload_done - t_csv_done, 2),
            'sf_process_s': round(t_poll_done - t_upload_done, 2),
            'total_s': round(time.time() - t0, 2),
        }
        return success_count, failed_records, timing

    except Exception as e:
        error_msg = str(e)
        # Release the concurrency semaphore on any error path so other threads aren't blocked
        if _sem_acquired:
            _bulk_v2_semaphore.release()
            _sem_acquired = False
        # Always unregister from cancel tracking on error path
        if job_id:
            _unregister_active_job(job_id)

        # If a job was already submitted, check its real state on SF before giving up.
        # The job may have completed despite the exception (e.g. a poll timeout).
        if job_id:
            state, n_processed, n_failed = _check_job_state(sf, job_id)
            if state == 'JobComplete':
                success_count = n_processed - n_failed
                failed_records = _bulk2_get_failed_results(sf, job_id) if n_failed > 0 else []
                timing = {'chunk': chunk_num, 'rows': len(df_chunk),
                          'csv_prep_s': 0, 'upload_s': 0,
                          'sf_process_s': round(time.time() - t0, 2),
                          'total_s': round(time.time() - t0, 2)}
                return success_count, failed_records, timing
            if state == 'InProgress':
                # Poll one more time — SF may finish in the next few seconds
                try:
                    job_info = _bulk2_poll_job(sf, job_id, on_poll_status=on_poll_status)
                    total = job_info.get('numberRecordsProcessed', 0)
                    failed_count = job_info.get('numberRecordsFailed', 0)
                    success_count = total - failed_count
                    failed_records = _bulk2_get_failed_results(sf, job_id) if failed_count > 0 else []
                    timing = {'chunk': chunk_num, 'rows': len(df_chunk),
                              'csv_prep_s': 0, 'upload_s': 0,
                              'sf_process_s': round(time.time() - t0, 2),
                              'total_s': round(time.time() - t0, 2)}
                    return success_count, failed_records, timing
                except Exception as poll_err:
                    _poll_str = str(poll_err).strip()
                    if _poll_str:
                        error_msg = f'{error_msg} | poll: {_poll_str}'
            # Abort only if job never left Open/UploadComplete (data not yet processed)
            if state in ('Open', 'UploadComplete', None):
                _bulk2_abort_job(sf, job_id)

        # Try to get per-record SF errors before building a generic error row
        if job_id:
            try:
                _fr = _bulk2_get_failed_results(sf, job_id)
                if _fr:
                    for _row in _fr:
                        if not _row.get('sf__Error'):
                            _row['sf__Error'] = _safe_msg if '_safe_msg' in locals() else (error_msg or 'Bulk API 2.0 delete job failed')
                        if not _row.get('sf__Id'):
                            _row['sf__Id'] = _row.get('Id') or _row.get('id') or ''
                    # Tag as JOB_LEVEL_FAILED so _drain_delete retries with Bulk v1/REST
                    timing = {'chunk': chunk_num, 'rows': len(df_chunk),
                              'csv_prep_s': 0, 'upload_s': 0,
                              'sf_process_s': round(time.time() - t0, 2),
                              'total_s': round(time.time() - t0, 2),
                              'error': 'JOB_LEVEL_FAILED: bulk v2 job failed — retrying with next API'}
                    return 0, _fr, timing
            except Exception:
                pass
            try:
                _upr = _bulk2_get_unprocessed_results(sf, job_id)
                if _upr:
                    _job_error = error_msg or 'Bulk API 2.0 delete job failed before processing records'
                    for _row in _upr:
                        _row['sf__Error'] = _row.get('sf__Error') or _job_error
                        _row['sf__Id'] = _row.get('sf__Id') or _row.get('Id') or _row.get('id') or ''
                    timing = {'chunk': chunk_num, 'rows': len(df_chunk),
                              'csv_prep_s': 0, 'upload_s': 0,
                              'sf_process_s': round(time.time() - t0, 2),
                              'total_s': round(time.time() - t0, 2),
                              'error': f'JOB_LEVEL_FAILED: {_job_error[:180]}'}
                    return 0, _upr, timing
            except Exception:
                pass

        # Report the chunk as fully failed — do NOT re-submit
        _exc_type = type(e).__name__
        _raw_msg = str(error_msg).strip() if error_msg is not None else ''
        # Produce a human-readable message for common exception types
        _SESSION_ERRORS = {'NoSessionContext', 'SalesforceExpiredSession', 'SalesforceAuthenticationFailed', 'Unauthorized'}
        if _exc_type in _SESSION_ERRORS or 'nosessioncontext' in _raw_msg.lower() or 'expired' in _raw_msg.lower():
            _safe_msg = 'Salesforce session expired or disconnected — please reconnect via sidebar and retry.'
        elif _raw_msg in ('', 'None', '0', '| poll:', ' | poll: ', 'None | poll: '):
            _safe_msg = 'Job failed (no error message returned by Salesforce — check Salesforce Setup → Bulk Data Load Jobs)'
        else:
            _safe_msg = _raw_msg
        error_rows = df_chunk.copy()
        error_rows['sf__Error'] = f'{_exc_type}: {_safe_msg}'
        error_rows['sf__Id'] = ''
        error_rows['chunk_num'] = chunk_num
        # Tag with JOB_LEVEL_FAILED so _drain_delete classifies this as SWITCH_API → Bulk v1/REST fallback
        _is_job_level = (
            'state=failed' in _safe_msg.lower() or 'state=aborted' in _safe_msg.lower()
            or 'session expired' in _safe_msg.lower() or 'no error message' in _safe_msg.lower()
            or _raw_msg in ('', 'None', '0')
        )
        _timing_err = f'JOB_LEVEL_FAILED: {_safe_msg[:180]}' if _is_job_level else _safe_msg[:200]
        timing = {'chunk': chunk_num, 'rows': len(df_chunk),
                  'csv_prep_s': 0, 'upload_s': 0, 'sf_process_s': 0,
                  'total_s': round(time.time() - t0, 2), 'error': _timing_err}
        return 0, error_rows.to_dict(orient='records'), timing


# -------------------------------------------------
# Fetch all fields from a Salesforce object
# -------------------------------------------------
def get_object_fields(sf, object_name):
    """
    Returns a list of dicts for all fields on the object, including compound fields.
    Each dict contains: name, label, type, createable, updateable, externalId, idLookup, compoundFieldName.
    """
    try:
        desc = getattr(sf, object_name).describe()
        fields = []
        for f in desc['fields']:
            fields.append({
                'name': f['name'],
                'label': f['label'],
                'type': f['type'],
                'createable': f.get('createable', False),
                'updateable': f.get('updateable', False),
                'externalId': f.get('externalId', False),
                'idLookup':   f.get('idLookup', False),
                'compoundFieldName': f.get('compoundFieldName'),
            })
        # Always exclude compound fields for Bulk API extraction (not just backup/export)
        import inspect
        stack = inspect.stack()
        # If called from any function with 'bulk', 'backup', or 'export' in the name, exclude compound fields
        if any(any(key in frame.function for key in ('bulk', 'backup', 'export')) for frame in stack):
            simple_fields = [f for f in fields if not is_compound_field(f)]
            compound_fields = [f for f in fields if is_compound_field(f)]
            simple_names = [f['name'] for f in simple_fields]
            compound_names = [f['name'] for f in compound_fields]
            print(f"[INFO] Compound fields excluded during extraction: {compound_names}")
            print(f"[INFO] Fields included for extraction: {simple_names}")
            # If you have a UI/status callback, you can add similar notification here
            return simple_fields
        return fields
    except Exception as e:
        return []


# -------------------------------------------------
# Compound Field Utilities
# -------------------------------------------------
def is_compound_field(field_dict):
    """Return True if the field is a Bulk-API-incompatible compound field.

    Only 'address' and 'location' type fields cannot be used in Bulk API.
    Fields like Name (type='string') are valid in Bulk API even when they have
    a compoundFieldName set (e.g. Account.Name on Person Account-enabled orgs).
    """
    return field_dict.get('type') in ('address', 'location')

def filter_compound_fields(sf_fields):
    """Return two lists: (simple_fields, compound_fields) from a list of field dicts."""
    simple_fields = [f for f in sf_fields if not is_compound_field(f)]
    compound_fields = [f for f in sf_fields if is_compound_field(f)]
    return simple_fields, compound_fields

def exclude_compound_columns(columns, sf_fields):
    """Given a list of column names and SF field dicts, return columns with compound fields excluded."""
    compound_field_names = {f['name'] for f in sf_fields if is_compound_field(f)}
    return [col for col in columns if col not in compound_field_names]

def has_compound_columns(columns, sf_fields):
    """Return True if any column in columns is a compound field."""
    compound_field_names = {f['name'] for f in sf_fields if is_compound_field(f)}
    return any(col in compound_field_names for col in columns)

# Example usage in SOQL query construction:
#
# sf_fields = get_object_fields(sf, object_name)
# if using_bulk_api:
#     columns = exclude_compound_columns(columns, sf_fields)
#     # Warn user if any columns were excluded
# else:
#     # Use all columns (REST API supports compound fields)

# You can also check if compound columns are present and switch to REST API if needed:
# if has_compound_columns(columns, sf_fields):
#     # Use REST API


def auto_match_csv_to_sf(csv_columns, sf_fields):
    """Auto-match CSV columns to Salesforce fields using multi-tier normalization.

    Tiers 1-3 match against the field API name:
    Tier 1 — exact case-insensitive  : 'Account_Id__c' == 'account_id__c'
    Tier 2 — strip __c + collapse separators: 'Account Id' == 'AccountId__c'
    Tier 3 — alphanumeric only         : 'account.id' == 'AccountId__c'

    Tiers 4-6 match against the field Label (fallback):
    Tier 4 — label exact              : 'Account Name' matches label 'Account Name' → API 'Name'
    Tier 5 — label norm2              : 'account_name' matches label 'Account Name' → API 'Name'
    Tier 6 — label norm3              : 'accountname' matches label 'Account Name' → API 'Name'
    """
    def _norm1(s):
        return s.strip().lower()

    def _norm2(s):
        s = _norm1(s)
        s = re.sub(r'__c$', '', s)      # strip custom field suffix
        s = re.sub(r'[\s_]+', '', s)    # collapse underscores and spaces
        return s

    def _norm3(s):
        return re.sub(r'[^a-z0-9]', '', _norm2(s))

    # Build lookup tiers (first match wins to avoid collisions)
    exact_map = {}
    norm2_map = {}
    norm3_map = {}
    # Label-based fallback tiers (e.g. CSV col "Account Name" → SF field API name "Name")
    label_exact_map = {}
    label_norm2_map = {}
    label_norm3_map = {}
    for f in sf_fields:
        name = f['name']
        label = f.get('label', '')
        exact_map.setdefault(_norm1(name), name)
        norm2_map.setdefault(_norm2(name), name)
        norm3_map.setdefault(_norm3(name), name)
        if label:
            label_exact_map.setdefault(_norm1(label), name)
            label_norm2_map.setdefault(_norm2(label), name)
            label_norm3_map.setdefault(_norm3(label), name)

    matches = {}
    for csv_col in csv_columns:
        col = csv_col.strip()
        if _norm1(col) in exact_map:
            matches[csv_col] = exact_map[_norm1(col)]
        elif _norm2(col) in norm2_map:
            matches[csv_col] = norm2_map[_norm2(col)]
        elif _norm3(col) in norm3_map:
            matches[csv_col] = norm3_map[_norm3(col)]
        elif _norm1(col) in label_exact_map:
            matches[csv_col] = label_exact_map[_norm1(col)]
        elif _norm2(col) in label_norm2_map:
            matches[csv_col] = label_norm2_map[_norm2(col)]
        elif _norm3(col) in label_norm3_map:
            matches[csv_col] = label_norm3_map[_norm3(col)]
        else:
            matches[csv_col] = None
    return matches


# -------------------------------------------------
# Fetch upsert-capable fields from Salesforce object
# -------------------------------------------------
def get_upsert_fields(sf, object_name):
    try:
        desc = getattr(sf, object_name).describe()
        fields = []
        for f in desc['fields']:
            if f.get('externalId') or f.get('idLookup'):
                label = f['label']
                name = f['name']
                tags = []
                if f.get('externalId'):
                    tags.append('External ID')
                if f.get('idLookup'):
                    tags.append('ID Lookup')
                fields.append({'name': name, 'label': label, 'tags': ', '.join(tags)})
        return fields
    except Exception as e:
        return [{'name': 'Id', 'label': f'Error: {e}', 'tags': 'ID Lookup'}]


# -------------------------------------------------
# OPTIMIZATION (2026-08-12): Complexity Scoring & Auto-Chunk Sizing
# -------------------------------------------------
def get_object_complexity_score(sf_fields):
    """
    Score object complexity 1-100 based on:
    - Number of lookup/reference fields (2 pts each)
    - Total number of fields (0.5 pts each)
    
    Returns complexity level: 'simple' (1-35), 'medium' (36-70), 'complex' (71-100)
    """
    try:
        lookup_count = sum(1 for f in sf_fields if f.get('type') in ('reference', 'id'))
        total_fields = len(sf_fields)
        score = (lookup_count * 2) + (total_fields * 0.5)
        score = min(100, score)  # cap at 100
        
        if score <= 35:
            return 'simple', score
        elif score <= 70:
            return 'medium', score
        else:
            return 'complex', score
    except Exception:
        return 'medium', 50  # default


def get_auto_chunk_size(sf_fields, user_chunk_size=25000):
    """
    Auto-adjust chunk size based on object complexity.
    Smaller chunks = faster SF processing for complex objects (FK resolution).
    
    - Simple (< 20 cols, <5 lookups): 50K rows
    - Medium: 25K rows (default)
    - Complex (>80 cols, >15 lookups): 10-15K rows
    """
    complexity, score = get_object_complexity_score(sf_fields)
    
    if complexity == 'simple':
        return min(50000, max(user_chunk_size, 40000))  # Prefer larger for simple
    elif complexity == 'complex':
        return max(10000, min(user_chunk_size, 15000))  # Smaller for complex
    else:
        return user_chunk_size  # Keep user's default for medium


def parallel_snowflake_fetch(sf_conn, table_name, columns=None, where_clause='', 
                             num_threads=4, batch_size_rows=None, on_progress=None):
    """
    OPTIMIZATION (2026-08-12): Parallel Snowflake extraction for 100M+ records.
    
    Fetches rows in parallel by splitting table into row-range batches.
    3-4x faster than serial cursor.fetch_pandas_all().
    
    Parameters:
    - sf_conn: Snowflake connector connection object
    - table_name: Full table name (DB.SCHEMA.TABLE or SCHEMA.TABLE)
    - columns: List of column names to fetch (default: all)
    - where_clause: SQL WHERE condition (without WHERE keyword)
    - num_threads: Number of parallel fetch threads (default: 4)
    - batch_size_rows: Rows per parallel batch (default: auto-estimate)
    - on_progress: Callback fn(msg: str) for status updates
    
    Returns: pd.DataFrame with all rows
    """
    try:
        # Step 1: Get total row count
        if on_progress:
            on_progress(f'❄️ Counting rows in {table_name}...')
        count_sql = f'SELECT COUNT(*) FROM {table_name}'
        if where_clause.strip():
            count_sql += f' WHERE {where_clause}'
        cursor = sf_conn.cursor()
        cursor.execute(count_sql)
        total_rows = cursor.fetchone()[0]
        cursor.close()
        
        if total_rows == 0:
            return pd.DataFrame()
        
        if total_rows < 50000:
            # Small table: use serial fetch
            if on_progress:
                on_progress(f'❄️ Fetching {total_rows:,} rows (serial mode)...')
            cursor = sf_conn.cursor()
            cols_str = ', '.join(columns) if columns else '*'
            sql = f'SELECT {cols_str} FROM {table_name}'
            if where_clause.strip():
                sql += f' WHERE {where_clause}'
            cursor.execute(sql)
            df = cursor.fetch_pandas_all()
            cursor.close()
            return df
        
        # Large table: parallel fetch
        if on_progress:
            on_progress(f'❄️ Fetching {total_rows:,} rows (parallel {num_threads} threads)...')
        
        if batch_size_rows is None:
            batch_size_rows = max(10000, total_rows // (num_threads * 2))
        
        def fetch_batch(offset, limit):
            try:
                cursor = sf_conn.cursor()
                cols_str = ', '.join(columns) if columns else '*'
                # Use ROW_NUMBER() for deterministic batching
                sql = f'''
                    WITH RANKED AS (
                        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as _rn, {cols_str}
                        FROM {table_name}
                        {f"WHERE {where_clause}" if where_clause.strip() else ""}
                    )
                    SELECT * EXCEPT (_rn) FROM RANKED
                    WHERE _rn > {offset} AND _rn <= {offset + limit}
                '''
                cursor.execute(sql)
                batch_df = cursor.fetch_pandas_all()
                cursor.close()
                return batch_df
            except Exception as e:
                if on_progress:
                    on_progress(f'⚠️ Batch fetch error (offset {offset}): {e}')
                return pd.DataFrame()
        
        # Parallel batch fetching
        num_batches = (total_rows + batch_size_rows - 1) // batch_size_rows
        dfs = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(num_batches):
                offset = i * batch_size_rows
                fut = executor.submit(fetch_batch, offset, batch_size_rows)
                futures.append(fut)
            
            completed = 0
            for fut in as_completed(futures):
                df = fut.result()
                if not df.empty:
                    dfs.append(df)
                completed += 1
                if on_progress and completed % max(1, num_batches // 5) == 0:
                    on_progress(f'❄️ Fetched {completed}/{num_batches} batches ({completed * batch_size_rows:,} rows)...')
        
        if not dfs:
            return pd.DataFrame()
        
        result = pd.concat(dfs, ignore_index=True)
        if on_progress:
            on_progress(f'✅ Snowflake extraction complete: {len(result):,} rows')
        return result
        
    except Exception as e:
        if on_progress:
            on_progress(f'❌ Snowflake fetch error: {e}')
        raise


# -------------------------------------------------
# Main Bulk Loader  (streaming — no pre-load of all chunks)
# -------------------------------------------------
# -------------------------------------------------
# Timing summary helper
# -------------------------------------------------
def _build_timing_summary(chunk_timings, total_elapsed):
    """Aggregate per-chunk timings into a summary dict.
    
    Returns a dict with keys:
      total_elapsed_s, num_chunks, total_rows,
      avg_csv_prep_s, avg_upload_s, avg_sf_process_s, avg_chunk_s,
      total_csv_prep_s, total_upload_s, total_sf_process_s,
      rows_per_sec
    """
    if not chunk_timings:
        return {
            'total_elapsed_s': round(total_elapsed, 2),
            'num_chunks': 0,
            'total_rows': 0,
            'avg_csv_prep_s': 0,
            'avg_upload_s': 0,
            'avg_sf_process_s': 0,
            'avg_chunk_s': 0,
            'total_csv_prep_s': 0,
            'total_upload_s': 0,
            'total_sf_process_s': 0,
            'rows_per_sec': 0,
        }
    n = len(chunk_timings)
    total_rows = sum(t.get('rows', 0) for t in chunk_timings)
    total_csv  = sum(t.get('csv_prep_s', 0) for t in chunk_timings)
    total_up   = sum(t.get('upload_s', 0) for t in chunk_timings)
    total_sf   = sum(t.get('sf_process_s', 0) for t in chunk_timings)
    total_ch   = sum(t.get('total_s', 0) for t in chunk_timings)
    return {
        'total_elapsed_s':    round(total_elapsed, 2),
        'num_chunks':         n,
        'total_rows':         total_rows,
        'avg_csv_prep_s':     round(total_csv / n, 2),
        'avg_upload_s':       round(total_up  / n, 2),
        'avg_sf_process_s':   round(total_sf  / n, 2),
        'avg_chunk_s':        round(total_ch  / n, 2),
        'total_csv_prep_s':   round(total_csv, 2),
        'total_upload_s':     round(total_up,  2),
        'total_sf_process_s': round(total_sf,  2),
        'rows_per_sec':       round(total_rows / max(total_elapsed, 0.001)),
    }


def _drain_pending(pending, on_status, on_error, on_progress,
                   total_success, total_failed, all_failed_records,
                   chunks_submitted, error_file, wait_for_one=True,
                   total_processed=0, chunk_timings=None):
    """Drain completed futures from pending dict. Returns updated counters.
    chunk_timings: optional list to append per-chunk timing dicts into."""
    if not pending:
        return total_success, total_failed
    if wait_for_one:
        done, _ = concurrent.futures.wait(pending.keys(), return_when=concurrent.futures.FIRST_COMPLETED, timeout=2.0)
        if not done and on_status:
            # Heartbeat: show rows queued vs rows confirmed so user sees live progress
            # total_processed = rows handed to SF API queue (submitted)
            # total_success   = rows SF has fully confirmed (lags behind)
            on_status(
                f'⏳ Uploading… '
                f'{total_processed:,} rows queued | '
                f'{total_success:,} confirmed ✅ | '
                f'{len(pending)} SF job(s) in flight'
            )
    else:
        done = {f for f in pending if f.done()}
    for f in list(done):
        if f not in pending:
            continue
        c_num = pending.pop(f)
        try:
            result_tuple = f.result(timeout=300)
            # _process_chunk_v2 now returns 3-tuple (success, failed, timing)
            if len(result_tuple) == 3:
                success, failed_records, timing = result_tuple
            else:
                success, failed_records = result_tuple
                timing = None
            total_success += success
            total_failed += len(failed_records)
            all_failed_records.extend(failed_records)
            if chunk_timings is not None and timing is not None:
                chunk_timings.append(timing)
            completed = chunks_submitted - len(pending)
            if on_status:
                timing_hint = f' | chunk took {timing["total_s"]:.1f}s' if timing else ''
                on_status(
                    f'✅ Chunk {c_num}: {success:,} success, {len(failed_records):,} failed '
                    f'({completed}/{chunks_submitted} chunks done){timing_hint}'
                )
            if on_progress:
                on_progress(min(completed / max(chunks_submitted, 1), 0.99))
        except Exception as e:
            if on_error:
                on_error(f'❌ Chunk {c_num} error: {e}')
        if all_failed_records:
            _write_failed_records_csv(error_file, all_failed_records)
    return total_success, total_failed


# =============================================================================
# MULTI-API SIMULTANEOUS ENGINE (LOADING_BABA run_multi_api_streaming)
# Runs ALL 3 APIs at the SAME TIME with weighted round-robin chunk distribution.
# Each API gets its own dedicated ThreadPoolExecutor (isolated thread pools).
# =============================================================================

def _weighted_round_robin_distributor(weights):
    """Infinite generator yielding API names by weight ratio.
    weights = {'bulk_v2': 15, 'bulk_v1': 4, 'rest': 1} →
    yields 'bulk_v2' 15 times, then 'bulk_v1' 4 times, then 'rest' 1 time, repeat.
    """
    apis = list(weights.keys())
    counts = [weights[a] for a in apis]
    while True:
        for api, count in zip(apis, counts):
            for _ in range(count):
                yield api


def _detect_straggler(chunk_timings, current_elapsed):
    """Return True if current_elapsed is > STRAGGLER_MULTIPLIER × median completion time."""
    if len(chunk_timings) < 5:
        return False
    times = sorted(t.get('total_s', 0) for t in chunk_timings)
    median = times[len(times) // 2]
    if median <= 0:
        return False
    return current_elapsed > median * STRAGGLER_MULTIPLIER


def _run_multi_api_simultaneous(
    chunk_queue, object_name, sf, operation, external_id_field,
    num_threads, on_status=None, on_error=None, on_progress=None,
    error_file='failed_salesforce_records.csv', upload_capacity=None
):
    """Run ALL 3 APIs simultaneously with dedicated thread pools per API.

    LOADING_BABA exact pattern: each chunk is submitted to its API pool
    IMMEDIATELY as it arrives from chunk_queue — no waiting for all chunks
    to be collected first.  This means SF jobs start while the data source
    (Snowflake / file reader) is still producing more chunks.

    chunk_queue: iterable of (chunk_num, df_chunk) — list OR streaming queue
    Returns: (total_success, total_failed, all_failed_records, chunk_timings)
    """
    import itertools
    import statistics as _stats
    status_dispatch = CallerThreadStatus(on_status)
    on_status = status_dispatch

    # Proactive routing weights — REST is intentionally excluded from the upfront cycle.
    # REST (200 records/HTTP call) is ~50× slower than Bulk v2 and should only activate
    # when a chunk gets SWITCH_API errors from both Bulk APIs (quota exhaustion).
    _PROACTIVE_WEIGHTS = {'bulk_v2': 19, 'bulk_v1': 1}
    _p_total = sum(_PROACTIVE_WEIGHTS.values())
    threads_v2   = min(max(1, int(num_threads * _PROACTIVE_WEIGHTS['bulk_v2'] / _p_total)),
                       MAX_THREADS_BULK_V2)
    threads_v1   = min(max(1, int(num_threads * _PROACTIVE_WEIGHTS['bulk_v1'] / _p_total)),
                       MAX_THREADS_BULK_V1)
    threads_rest = 1  # REST standby — activated only by SWITCH_API escalation, never upfront

    if on_status:
        on_status(
            f'🚀 Multi-API: {threads_v2} bulk_v2 + {threads_v1} bulk_v1 threads '
            f'(REST: 1 standby — activates only when Bulk quota is exhausted)'
        )

    # Slot cycle: only bulk_v2 and bulk_v1 — REST never gets chunks proactively
    _api_slots = []
    for api_name, weight in _PROACTIVE_WEIGHTS.items():
        _api_slots.extend([api_name] * weight)
    _slot_cycle = itertools.cycle(_api_slots)

    total_success = 0
    total_failed = 0
    all_failed_records = []
    chunk_timings = []
    completed_count = 0
    _results_lock = threading.Lock()
    _abort_flag = []
    _no_completion_alerted = set()
    _engine_start_time = time.time()
    _chunk_start_times = {}        # future → start time
    _completed_elapsed = []        # elapsed times of completed chunks
    _straggler_warned = set()      # chunk nums already warned
    _in_flight = {}                # future → (c_num, api_name, start_t, df_chunk_ref)
    max_pending = max(1, num_threads * 2)
    monitor_done = threading.Event()

    def _wait_for_capacity():
        while len(_in_flight) >= max_pending:
            _drain_done()
            if is_stopped() or _abort_flag:
                return False
            if len(_in_flight) >= max_pending:
                concurrent.futures.wait(tuple(_in_flight), timeout=0.1,
                                        return_when=concurrent.futures.FIRST_COMPLETED)
        return not is_stopped() and not _abort_flag

    # ── Capture Streamlit context once; propagate to worker threads ────────
    _st_pool_ctx = None
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        _st_pool_ctx = get_script_run_ctx()
    except Exception:
        pass

    def _thread_init():
        if _st_pool_ctx is not None:
            try:
                from streamlit.runtime.scriptrunner import add_script_run_ctx
                import threading as _th
                add_script_run_ctx(_th.current_thread(), _st_pool_ctx)
            except Exception:
                pass

    # ── One dedicated ThreadPool per API — exactly like LOADING_BABA ──────
    _pools = {
        'bulk_v2': ThreadPoolExecutor(max_workers=threads_v2, initializer=_thread_init,
                                      thread_name_prefix='bv2'),
        'bulk_v1': ThreadPoolExecutor(max_workers=threads_v1, initializer=_thread_init,
                                      thread_name_prefix='bv1'),
        'rest':    ThreadPoolExecutor(max_workers=threads_rest, initializer=_thread_init,
                                      thread_name_prefix='rest'),
    }

    def _submit_to_pool(api_name, c_num, df_chunk):
        """Submit one chunk (or its mini-chunks) to the correct pool immediately."""
        fn = {
            'bulk_v2': _process_chunk_v2,
            'bulk_v1': _process_chunk_bulk1,
            'rest':    _process_chunk_rest,
        }[api_name]
        if upload_capacity is not None:
            fn = partial(upload_capacity.run, fn)
        pool = _pools[api_name]

        if api_name == 'rest' and len(df_chunk) > REST_MINI_CHUNK * 1.2:
            # OPTIMIZED (2026-08-12): Skip re-chunking if chunk is only slightly over limit
            minis = [df_chunk.iloc[i:i+REST_MINI_CHUNK]
                     for i in range(0, len(df_chunk), REST_MINI_CHUNK)]
            for mini in minis:
                if not _wait_for_capacity():
                    return
                fut = pool.submit(fn, c_num, mini, object_name, sf, operation, external_id_field, on_status)
                t0 = time.time()
                with _results_lock:
                    _in_flight[fut] = (c_num, api_name, t0, mini)
        elif api_name == 'bulk_v1' and len(df_chunk) > BULK1_MINI_CHUNK * 1.2:
            # OPTIMIZED (2026-08-12): Skip re-chunking if chunk is only slightly over limit
            minis = [df_chunk.iloc[i:i+BULK1_MINI_CHUNK]
                     for i in range(0, len(df_chunk), BULK1_MINI_CHUNK)]
            for mini in minis:
                if not _wait_for_capacity():
                    return
                fut = pool.submit(fn, c_num, mini, object_name, sf, operation, external_id_field, on_status)
                t0 = time.time()
                with _results_lock:
                    _in_flight[fut] = (c_num, api_name, t0, mini)
        else:
            if not _wait_for_capacity():
                return
            if api_name == 'bulk_v2':
                def _bulk2_status(message, _api=api_name):
                    if on_status:
                        on_status(f'[{_api}] {message}')
                fut = pool.submit(fn, c_num, df_chunk, object_name, sf,
                                  operation, external_id_field, False, _bulk2_status)
            else:
                fut = pool.submit(fn, c_num, df_chunk, object_name, sf,
                                  operation, external_id_field, on_status)
            t0 = time.time()
            with _results_lock:
                _in_flight[fut] = (c_num, api_name, t0, df_chunk)

    # Escalation chain: bulk_v2 → bulk_v1 → rest (REST activates ONLY here, never upfront).
    _API_ESCALATION = {'bulk_v2': 'bulk_v1', 'bulk_v1': 'rest', 'rest': None}

    def _drain_done():
        """Collect any futures that have already finished — non-blocking."""
        nonlocal total_success, total_failed, completed_count
        status_dispatch.flush()
        done_futs = [f for f in list(_in_flight) if f.done()]
        for fut in done_futs:
            with _results_lock:
                if fut not in _in_flight:
                    continue
                c_num, api_name, start_t, df_chunk_ref = _in_flight.pop(fut)
            elapsed_this = time.time() - start_t
            _completed_elapsed.append(elapsed_this)
            try:
                result = fut.result(timeout=10)
                if len(result) == 3:
                    success, failed_records, timing = result
                else:
                    success, failed_records = result
                    timing = None
                if success == 0 and timing and timing.get('error'):
                    err_class, code = classify_error(timing['error'])
                    if err_class == ErrorClass.NON_RETRYABLE and not _abort_flag:
                        _abort_flag.append(timing['error'])
                        if on_error:
                            on_error(f'🛑 Fatal schema error chunk {c_num} [{api_name}]: '
                                     f'{code} — stopping all APIs')
                    elif err_class == ErrorClass.SWITCH_API:
                        # Escalate to next API: bulk_v2 → bulk_v1 → rest (REST activates only here)
                        next_api = _API_ESCALATION.get(api_name)
                        if next_api and next_api in _pools:
                            if on_status:
                                on_status(
                                    f'⚡ Quota hit [{api_name}] chunk {c_num} ({code}) '
                                    f'→ escalating to {next_api}',
                                    level='warning'
                                )
                            _submit_to_pool(next_api, c_num, df_chunk_ref)
                            continue  # requeued — don't count as completed yet
                with _results_lock:
                    total_success += success
                    if isinstance(failed_records, list):
                        total_failed += len(failed_records)
                        all_failed_records.extend(failed_records)
                    if timing:
                        chunk_timings.append(timing)
                    completed_count += 1
                # Straggler detection (2× median)
                if _detect_straggler(chunk_timings, elapsed_this) and c_num not in _straggler_warned:
                    _straggler_warned.add(c_num)
                    if on_status:
                        on_status(
                            f'⚠️ Straggler chunk {c_num} [{api_name}] '
                            f'took {elapsed_this:.1f}s (>{STRAGGLER_MULTIPLIER}× median)',
                            level='warning'
                        )
                if on_status and completed_count % 5 == 0:
                    on_status(
                        f'✅ {completed_count} chunks done | '
                        f'{total_success:,} success | {total_failed:,} failed'
                    )
                if on_progress:
                    on_progress(min(completed_count / max(completed_count + len(_in_flight), 1), 0.99))
            except Exception as exc:
                with _results_lock:
                    completed_count += 1
                if on_error:
                    on_error(f'❌ [{api_name}] chunk {c_num}: {str(exc)[:100]}')

    # ── No-completion straggler monitor ───────────────────────────────────
    def _no_completion_monitor():
        while not monitor_done.wait(10) and not is_stopped() and not _abort_flag:
            if completed_count > 0:
                return
            elapsed = time.time() - _engine_start_time
            for threshold in STRAGGLER_NO_COMPLETION_THRESHOLDS:
                if elapsed >= threshold and threshold not in _no_completion_alerted:
                    _no_completion_alerted.add(threshold)
                    if on_status:
                        on_status(
                            f'⚠️ No chunks completed after {int(elapsed)}s — '
                            f'SF may be queuing internally. {len(_in_flight)} in-flight.',
                            level='warning'
                        )

    monitor = threading.Thread(target=_no_completion_monitor, daemon=True)
    monitor.start()

    # ── STREAMING PRODUCER LOOP — submit each chunk the moment it arrives ──
    # This is the key LOADING_BABA pattern: the data source (file / Snowflake)
    # and Salesforce submission run in TRUE PARALLEL — no batch-collect-then-dispatch.
    chunks_seen = 0
    try:
        for c_num, df_chunk in chunk_queue:
            if is_stopped() or _abort_flag:
                break
            api_name = next(_slot_cycle)
            chunks_seen += 1
            _submit_to_pool(api_name, c_num, df_chunk)
            _drain_done()

        if on_status:
            on_status(f'All {chunks_seen} chunks submitted; waiting for outstanding uploads.')

        while _in_flight:
            _drain_done()
            if _in_flight:
                concurrent.futures.wait(tuple(_in_flight), timeout=0.1,
                                        return_when=concurrent.futures.FIRST_COMPLETED)
    finally:
        monitor_done.set()
        for pool in _pools.values():
            pool.shutdown(wait=True)
        status_dispatch.flush()
        close_source = getattr(chunk_queue, 'close', None)
        if close_source:
            close_source()

    # Save failed records
    if all_failed_records:
        _write_failed_records_csv(error_file, all_failed_records)

    if _abort_flag and on_status:
        on_status(
            f'🛑 Multi-API aborted early: {_abort_flag[0][:80]}',
            level='warning'
        )

    return total_success, total_failed, all_failed_records, chunk_timings


def bulk_load_v2(
    csv_file_path, object_name, sf, required_columns, chunk_size,
    operation='insert', external_id_field=None, column_mapping=None,
    error_file='failed_salesforce_records.csv',
    num_parallel_chunks=3, date_columns=None, datetime_columns=None,
    verbose=False, on_progress=None, on_status=None, on_error=None,
    chunk_iterator=None,
    use_rest_api=False, manage_stop_flag=True, upload_capacity=None
):
    """Stream-process chunks with AUTO mode orchestrator (LOADING_BABA pattern).

    AUTO mode features:
    - Multi-API SIMULTANEOUS: All 3 APIs (Bulk v2 + Bulk v1 + REST) run at same time
    - Weighted round-robin: bulk_v2=75%, bulk_v1=20%, REST=5% of chunks
    - Thread ladder: starts at num_parallel_chunks, scales down on rate-limit errors
    - Error classification: retryable → requeue, non-retryable → fail file, switch → next API
    - Per-job timeout: 30 minutes (aborts stalled jobs)
    - Auto-reprocess: retryable chunks retried up to 3 rounds
    - Straggler detection: warns when jobs take >2× median time
    - Fatal abort: stops all APIs on first NON_RETRYABLE schema error
    - Cancellation: Ctrl-C aborts all in-flight SF jobs and frees quota
    """
    object_name, operation, external_id_field = _normalize_bulk_inputs(
        object_name,
        operation,
        external_id_field if (operation or 'insert').strip().lower() == 'upsert' else None
    )

    if manage_stop_flag:
        clear_stop_flag()
    status_dispatch = CallerThreadStatus(on_status)
    on_status = status_dispatch
    _install_cancel_handler()  # Install SIGINT handler for Ctrl-C job abort
    chunk_size = min(max(chunk_size, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE)
    num_parallel_chunks = min(num_parallel_chunks, MAX_PARALLEL_JOBS)

    # Initialize thread ladder starting from user's requested parallelism
    # OPTIMIZED (2026-08-12): Auto-detect high-volume loads and use aggressive thread ladder
    # For 100K+ records, use [64, 48, 32, 24, 16, 8, 4, 1] instead of default [32, 24, 16, ...]
    thread_ladder_to_use = THREAD_LADDER
    if not chunk_iterator:
        try:
            import os as _os_size
            if _os_size.path.exists(csv_file_path):
                file_size_bytes = _os_size.path.getsize(csv_file_path)
                estimated_rows = max(1, file_size_bytes // 500)
                if estimated_rows > 50000:
                    thread_ladder_to_use = [64, 48, 32, 24, 16, 8, 4, 1]
        except Exception:
            pass
    
    thread_ctrl = ThreadController(ladder=thread_ladder_to_use, initial_cap=num_parallel_chunks)
    current_api = 'bulk_v2'  # API chain: bulk_v2 → bulk_v1 → rest
    API_CHAIN = ['bulk_v2', 'bulk_v1', 'rest'] if not use_rest_api else ['rest']
    api_index = 0

    # Quota awareness: if near daily limit, start with REST (uses fewer API calls)
    quota = get_quota_tracker()
    if quota.is_near_limit(threshold=0.85) and not use_rest_api:
        api_index = len(API_CHAIN) - 1
        current_api = API_CHAIN[api_index]
        if on_status:
            on_status(
                f'⚠️ API quota near limit ({quota.summary()}). Starting with REST to conserve.',
                level='warning'
            )

    if on_status:
        on_status(
            f'⚙️ AUTO Mode: chunk_size={chunk_size:,}, '
            f'start_threads={thread_ctrl.current}, '
            f'API_chain={" → ".join(API_CHAIN)}, '
            f'quota={quota.summary()}'
        )

    date_cols = merge_unique_columns(
        DEFAULT_DATE_COLUMNS_BY_OBJECT.get(object_name, []), date_columns
    )
    datetime_cols = merge_unique_columns(datetime_columns)

    if chunk_iterator is None:
        try:
            chunk_iterator = pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False)
        except Exception as e:
            if on_error:
                on_error(f'Failed to read file: {e}')
            return None

    start_time = time.time()
    total_processed = 0
    total_success = 0
    total_failed = 0
    all_failed_records = []
    retryable_chunks = []  # chunks to retry in next round
    chunks_submitted = 0
    chunk_timings = []
    pending = {}  # future -> (chunk_num, df_chunk, api_used)

    # Stats for AUTO mode
    thread_reductions = 0
    api_switches = 0

    def _get_submitter_fn(api_name):
        """Return the chunk processor function for the given API."""
        if api_name == 'bulk_v2':
            return _process_chunk_v2
        elif api_name == 'bulk_v1':
            return _process_chunk_bulk1
        else:
            return _process_chunk_rest

    def _submit_chunk(executor, c_num, df_chunk, api_name):
        """Submit a chunk using the specified API."""
        fn = _get_submitter_fn(api_name)
        if upload_capacity is not None:
            fn = partial(upload_capacity.run, fn)
        if api_name == 'bulk_v2':
            def _bulk2_status(message, _api=api_name):
                if on_status:
                    on_status(f'[{_api}] {message}')
            future = executor.submit(fn, c_num, df_chunk, object_name, sf,
                                     operation, external_id_field, verbose, _bulk2_status)
        elif api_name == 'bulk_v1' and len(df_chunk) > BULK1_MINI_CHUNK * 1.2:
            # OPTIMIZED (2026-08-12): Skip re-chunking if chunk is only slightly over limit
            def _bulk1_split_runner(_c_num, _df_chunk):
                t0 = time.time()
                total_success_local = 0
                all_failed_local = []
                minis = [_df_chunk.iloc[i:i + BULK1_MINI_CHUNK]
                         for i in range(0, len(_df_chunk), BULK1_MINI_CHUNK)]
                for mini in minis:
                    s_local, f_local, _ = _process_chunk_bulk1(
                        _c_num, mini, object_name, sf, operation, external_id_field, on_status
                    )
                    total_success_local += int(s_local or 0)
                    if isinstance(f_local, list) and f_local:
                        all_failed_local.extend(f_local)
                timing_local = {
                    'chunk': _c_num,
                    'rows': len(_df_chunk),
                    'api': 'bulk_v1',
                    'total_s': round(time.time() - t0, 2),
                }
                return total_success_local, all_failed_local, timing_local

            runner = partial(upload_capacity.run, _bulk1_split_runner) if upload_capacity is not None else _bulk1_split_runner
            future = executor.submit(runner, c_num, df_chunk)
        else:
            future = executor.submit(fn, c_num, df_chunk, object_name, sf,
                                     operation, external_id_field, on_status)
        return future

    def _drain_with_classification(pending_dict, executor):
        status_dispatch.flush()
        """Drain completed futures with error classification and auto-recovery.
        Returns updated (total_success, total_failed)."""
        nonlocal total_success, total_failed, thread_reductions, api_switches
        nonlocal current_api, api_index

        if not pending_dict:
            return

        done, _ = concurrent.futures.wait(
            pending_dict.keys(), return_when=concurrent.futures.FIRST_COMPLETED, timeout=3.0
        )
        if not done and on_status:
            on_status(
                f'⏳ [{current_api}] {thread_ctrl.current} threads | '
                f'{total_processed:,} queued | {total_success:,} ✅ | '
                f'{len(pending_dict)} in-flight'
            )
            return

        for f in list(done):
            if f not in pending_dict:
                continue
            c_num, df_chunk_ref, api_used = pending_dict.pop(f)
            try:
                result_tuple = f.result(timeout=300)
                if len(result_tuple) == 3:
                    success, failed_records, timing = result_tuple
                else:
                    success, failed_records = result_tuple
                    timing = None

                # Classify errors on failed chunk
                chunk_error_class = None
                if success == 0 and timing and timing.get('error'):
                    chunk_error_class, code = classify_error(timing['error'])
                elif isinstance(failed_records, list) and len(failed_records) > 0:
                    # Check if ALL records failed with same retryable error
                    if isinstance(failed_records[0], dict):
                        first_err = failed_records[0].get('sf__Error', '')
                        chunk_error_class, code = classify_error(first_err)

                # Handle based on classification
                if chunk_error_class == ErrorClass.SWITCH_API:
                    api_switches += 1
                    if on_status:
                        on_status(
                            f'🔄 API switch triggered (chunk {c_num}): {code} — '
                            f'switching from {current_api}',
                            level='warning'
                        )
                    # Move to next API in chain
                    if api_index < len(API_CHAIN) - 1:
                        api_index += 1
                        current_api = API_CHAIN[api_index]
                        thread_ctrl.reset()
                        if on_status:
                            on_status(f'➡️ Now using: {current_api} (threads reset to {thread_ctrl.current})')
                    # Requeue this chunk for the new API
                    retryable_chunks.append((c_num, df_chunk_ref))

                elif chunk_error_class == ErrorClass.RETRYABLE:
                    # Reduce threads and requeue
                    new_threads, at_bottom = thread_ctrl.reduce()
                    thread_reductions += 1
                    if on_status:
                        on_status(
                            f'⬇️ Thread reduction (chunk {c_num}): {code} → '
                            f'threads now {new_threads}',
                            level='warning'
                        )
                    if at_bottom and api_index < len(API_CHAIN) - 1:
                        # Thread ladder exhausted → switch API
                        api_index += 1
                        current_api = API_CHAIN[api_index]
                        thread_ctrl.reset()
                        api_switches += 1
                        if on_status:
                            on_status(f'➡️ Ladder exhausted, switching to: {current_api}')
                    retryable_chunks.append((c_num, df_chunk_ref))

                elif chunk_error_class == ErrorClass.NON_RETRYABLE:
                    # Permanent failure — save to error file
                    total_failed += len(failed_records) if isinstance(failed_records, list) else 0
                    if isinstance(failed_records, list):
                        all_failed_records.extend(failed_records)
                    if on_status:
                        on_status(f'❌ Chunk {c_num}: non-retryable error ({code})')

                else:
                    # Normal success (or partial success with non-retryable record errors)
                    total_success += success
                    if isinstance(failed_records, list):
                        total_failed += len(failed_records)
                        all_failed_records.extend(failed_records)
                    if chunk_timings is not None and timing is not None:
                        chunk_timings.append(timing)
                    completed = chunks_submitted - len(pending_dict) - len(retryable_chunks)
                    if on_status:
                        api_tag = f'[{api_used}]' if api_used != 'bulk_v2' else ''
                        timing_hint = f' | {timing["total_s"]:.1f}s' if timing and 'total_s' in timing else ''
                        on_status(
                            f'✅ Chunk {c_num}{api_tag}: {success:,} success, '
                            f'{len(failed_records) if isinstance(failed_records, list) else 0:,} failed'
                            f'{timing_hint}'
                        )
                    if on_progress:
                        on_progress(min(completed / max(chunks_submitted, 1), 0.99))

            except Exception as e:
                err_class, code = classify_error(str(e))
                if err_class in (ErrorClass.RETRYABLE, ErrorClass.SWITCH_API):
                    retryable_chunks.append((c_num, df_chunk_ref))
                    if on_status:
                        on_status(f'⚠️ Chunk {c_num} exception ({code}): will retry', level='warning')
                else:
                    if on_error:
                        on_error(f'❌ Chunk {c_num} error: {e}')

        # Save failed records incrementally
        if all_failed_records:
            _write_failed_records_csv(error_file, all_failed_records)

    try:
        sf_fields = get_object_fields(sf, object_name)
        
        # OPTIMIZED (2026-08-12): Auto-tune chunk size based on object complexity
        # Complex objects (many lookups/FK fields) process slower in Salesforce
        # Reduce chunk size for complex objects to parallelize better within SF
        original_chunk_size = chunk_size
        auto_chunk_size = get_auto_chunk_size(sf_fields, chunk_size)
        if auto_chunk_size != chunk_size:
            complexity, score = get_object_complexity_score(sf_fields)
            if on_status:
                on_status(
                    f'📊 Auto-tuning chunk size: {original_chunk_size:,} → {auto_chunk_size:,} '
                    f'({complexity} object, score={score:.0f})',
                    level='info'
                )
            chunk_size = auto_chunk_size
        
        orig_required_columns = list(required_columns)
        compound_field_names = {f['name'] for f in sf_fields if is_compound_field(f)}
        lookup_id_field_names = {
            f['name'] for f in sf_fields
            if f.get('type') in ('reference', 'id')
        }

        def _normalize_lookup_id_values(_df):
            # Keep lookup/id blanks truly blank (avoid sending literal None/null strings)
            if _df is None or _df.empty:
                return _df
            _tokens = {'', 'none', 'null', 'nan', '<na>'}
            for _col in (_lookup for _lookup in lookup_id_field_names if _lookup in _df.columns):
                _s = _df[_col]
                _mask = _s.isna() | _s.astype(str).str.strip().str.lower().isin(_tokens)
                _df[_col] = _s.mask(_mask, np.nan)
            return _df

        if not use_rest_api:
            # --- Bulk API: Exclude compound fields automatically ---
            filtered_required_columns = exclude_compound_columns(required_columns, sf_fields)
            if len(filtered_required_columns) < len(required_columns):
                excluded = set(required_columns) - set(filtered_required_columns)
                if on_status:
                    on_status(f'⚠️ Excluding compound columns: {sorted(excluded)}', level='warning')
            required_columns = filtered_required_columns
            compound_field_names = {f['name'] for f in sf_fields if is_compound_field(f)}
            present_compound_cols = [col for col in required_columns if col in compound_field_names]
            if present_compound_cols:
                err_msg = (
                    f'❌ Cannot use Bulk API with compound columns: {present_compound_cols}. '
                    'Remove these columns or use the REST API for compound fields.'
                )
                if on_error:
                    on_error(err_msg)
                return None
        else:
            if on_status:
                on_status('ℹ️ Using REST API: all columns (including compound fields) will be included.')

        # ===== MAIN LOOP: Stream chunks with dynamic thread pool =====
        # LOADING_BABA uses multi-API simultaneous when all 3 APIs are available
        use_multi_api = (not use_rest_api and len(API_CHAIN) == 3 and
                         num_parallel_chunks >= 8)  # Only for high-thread runs
        max_reprocess_rounds = 3
        round_num = 0

        while round_num <= max_reprocess_rounds:
            # Determine chunk source: first round = iterator, subsequent = retryable_chunks
            if round_num == 0:
                chunk_source = chunk_iterator
            else:
                if not retryable_chunks:
                    break
                if on_status:
                    on_status(
                        f'🔁 Auto-reprocess round {round_num}: '
                        f'{len(retryable_chunks)} chunk(s) to retry via {current_api}'
                    )
                chunk_source = iter(retryable_chunks)
                retryable_chunks = []

            # --- Multi-API streaming mode: producer-consumer pattern ---
            if use_multi_api and round_num == 0:
                # ── TRUE STREAMING MULTI-API PIPELINE ─────────────────────────────
                # _run_multi_api_simultaneous now accepts ANY iterable — including a
                # generator.  We pass a generator that prepares chunks on-the-fly so
                # the first SF job starts before the entire file/Snowflake query is
                # read.  Reading and SF submission run in TRUE PARALLEL.
                # This matches LOADING_BABA's run_multi_api_streaming() exactly.

                def _prepared_chunk_stream():
                    """Generator: read → clean → yield (c_num, df_chunk) immediately."""
                    nonlocal chunks_submitted, total_processed
                    for item in chunk_source:
                        if is_stopped():
                            if on_status:
                                on_status('⛔ Operation stopped by user', level='warning')
                            return

                        df_chunk = item
                        df_chunk = df_chunk.dropna(how='all')
                        # Vectorized row filter — column-wise apply is ~300× faster than axis=1.
                        # Cast object columns to string first to avoid .str errors on mixed types.
                        _nonempty = df_chunk.apply(
                            lambda s: (s.astype(str).str.strip().ne('') & s.notna())
                            if s.dtype == object else s.notna()
                        ).any(axis=1)
                        df_chunk = df_chunk[_nonempty]
                        if required_columns:
                            present_req = [c for c in required_columns if c in df_chunk.columns]
                            if present_req:
                                _req_nonempty = df_chunk[present_req].apply(
                                    lambda s: (s.astype(str).str.strip().ne('') & s.notna())
                                    if s.dtype == object else s.notna()
                                ).any(axis=1)
                                df_chunk = df_chunk[_req_nonempty]
                        if df_chunk.empty:
                            continue

                        if not use_rest_api:
                            present_compound_cols = [
                                col for col in df_chunk.columns if col in compound_field_names
                            ]
                            if present_compound_cols:
                                df_chunk = df_chunk.drop(columns=present_compound_cols)
                            if df_chunk.empty:
                                continue

                        chunks_submitted += 1
                        c_num = chunks_submitted
                        df_chunk.columns = df_chunk.columns.str.strip()
                        # Null normalization is handled inside _df_to_csv_bytes — no applymap here.
                        if column_mapping:
                            df_chunk = df_chunk.rename(columns=column_mapping)
                        if required_columns:
                            missing_cols = [c for c in required_columns if c not in df_chunk.columns]
                            if missing_cols:
                                if on_status:
                                    on_status(
                                        f'Chunk {c_num}: missing columns {missing_cols}',
                                        level='warning'
                                    )
                                chunks_submitted -= 1
                                continue
                            df_chunk = df_chunk[required_columns]
                        df_chunk = _normalize_lookup_id_values(df_chunk)
                        df_chunk = normalize_salesforce_temporal_fields(df_chunk, date_cols, datetime_cols)
                        total_processed += len(df_chunk)

                        if on_status and chunks_submitted % 20 == 0:
                            on_status(
                                f'📖 {chunks_submitted} chunks ({total_processed:,} rows) — '
                                f'submitting to SF in parallel...'
                            )
                        yield (c_num, df_chunk)   # ← SF job starts IMMEDIATELY

                # Run: generator feeds pools; pools submit to SF; all overlap.
                s, f, fr, t = _run_multi_api_simultaneous(
                    _prepared_chunk_stream(),
                    object_name, sf, operation, external_id_field,
                    num_threads=num_parallel_chunks,
                    on_status=on_status, on_error=on_error, on_progress=on_progress,
                    error_file=error_file, upload_capacity=upload_capacity
                )
                total_success += s
                total_failed += f
                all_failed_records.extend(fr)
                chunk_timings.extend(t)

                if on_status:
                    on_status(
                        f'🚀 Multi-API pipeline done: {chunks_submitted} chunks, '
                        f'{total_processed:,} rows | '
                        f'{total_success:,} success | {total_failed:,} failed'
                    )

            else:
                # --- Single-API mode (fallback, REST-only, or reprocess rounds) ---
                def _prepared_single_stream():
                    nonlocal chunks_submitted, total_processed
                    for item in chunk_source:
                        if is_stopped():
                            return
                        if round_num == 0:
                            df_chunk = item
                        else:
                            c_num_retry, df_chunk = item

                        df_chunk = df_chunk.dropna(how='all')
                        _nonempty_single = df_chunk.apply(
                            lambda s: s.fillna('').astype(str).str.strip().ne('')
                            if s.dtype == object else s.notna()
                        ).any(axis=1)
                        df_chunk = df_chunk[_nonempty_single]
                        if required_columns:
                            present_req = [c for c in required_columns if c in df_chunk.columns]
                            if present_req:
                                _req_nonempty_single = df_chunk[present_req].apply(
                                    lambda s: s.fillna('').astype(str).str.strip().ne('')
                                    if s.dtype == object else s.notna()
                                ).any(axis=1)
                                df_chunk = df_chunk[_req_nonempty_single]
                        if df_chunk.empty:
                            continue

                        if not use_rest_api and current_api != 'rest':
                            present_compound_cols = [col for col in df_chunk.columns if col in compound_field_names]
                            if present_compound_cols:
                                df_chunk = df_chunk.drop(columns=present_compound_cols)
                            if df_chunk.empty:
                                continue

                        if round_num == 0:
                            chunks_submitted += 1
                            c_num = chunks_submitted
                        else:
                            c_num = c_num_retry

                        df_chunk.columns = df_chunk.columns.str.strip()
                        df_chunk = df_chunk.applymap(
                            lambda x: np.nan if (pd.isna(x) or (isinstance(x, str) and x.strip() == '')) else x
                        )
                        if column_mapping and round_num == 0:
                            df_chunk = df_chunk.rename(columns=column_mapping)
                        if required_columns:
                            missing_cols = [c for c in required_columns if c not in df_chunk.columns]
                            if missing_cols:
                                if on_status:
                                    on_status(f'Chunk {c_num}: missing columns {missing_cols}', level='warning')
                                if round_num == 0:
                                    chunks_submitted -= 1
                                continue
                            df_chunk = df_chunk[required_columns]
                        df_chunk = _normalize_lookup_id_values(df_chunk)
                        df_chunk = normalize_salesforce_temporal_fields(df_chunk, date_cols, datetime_cols)
                        if round_num == 0:
                            total_processed += len(df_chunk)
                        yield c_num, df_chunk

                # Dispatch single-API with thread pool
                with ThreadPoolExecutor(max_workers=thread_ctrl.current) as executor:
                    for c_num, df_chunk in _prepared_single_stream():
                        if is_stopped():
                            break
                        future = _submit_chunk(executor, c_num, df_chunk, current_api)
                        pending[future] = (c_num, df_chunk, current_api)
                        while len(pending) >= thread_ctrl.current:
                            _drain_with_classification(pending, executor)
                            if is_stopped():
                                break
                    while pending:
                        _drain_with_classification(pending, executor)

            round_num += 1
            if not retryable_chunks:
                break

    except KeyboardInterrupt:
        if on_status:
            on_status('⛔ Operation interrupted', level='warning')
    finally:
        status_dispatch.flush()

    if on_progress:
        on_progress(1.0)
    elapsed = time.time() - start_time

    timing_summary = _build_timing_summary(chunk_timings, elapsed)

    # Report AUTO mode stats
    if on_status and (thread_reductions > 0 or api_switches > 0):
        on_status(
            f'📊 AUTO mode: {thread_reductions} thread reductions, '
            f'{api_switches} API switches, final API={current_api}'
        )

    return {
        'total_processed': total_processed,
        'total_success': total_success,
        'total_failed': total_failed,
        'elapsed': elapsed,
        'error_file': error_file if all_failed_records else None,
        'chunk_timings': chunk_timings,
        'timing_summary': timing_summary,
        'auto_mode_stats': {
            'thread_reductions': thread_reductions,
            'api_switches': api_switches,
            'final_api': current_api,
            'reprocess_rounds': round_num,
        },
    }


# =============================================================================
# PARALLEL MULTI-API QUERY ENGINE — For Test Case Generator
# Fetches Salesforce data using all 3 APIs simultaneously (Bulk 2.0, Bulk 1.0, REST)
# split by record key ranges for maximum throughput.
# =============================================================================

def _bulk2_query_base(sf):
    """Bulk API 2.0 Query endpoint."""
    return f'https://{sf.sf_instance}/services/data/v{SF_API_VERSION}/jobs/query'


def bulk2_query_ids_to_csv(sf, soql, on_progress=None, timeout=1800):
    """Export query IDs to a temporary CSV with bounded memory; caller owns successful output."""
    session = _get_http_session(sf)
    headers = {
        'Authorization': f'Bearer {sf.session_id}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    job_url = None
    state = ''
    csv_path = None
    completed = False
    started = time.monotonic()

    def check_stopped():
        if is_stopped():
            raise InterruptedError('Query export stopped. No delete jobs were started.')

    def report(message):
        if on_progress:
            on_progress(message)

    try:
        check_stopped()
        with session.post(
            _bulk2_query_base(sf), headers=headers,
            json={'operation': 'query', 'query': soql.strip().rstrip(';')}, timeout=(15, 60)
        ) as response:
            if not response.ok:
                raise RuntimeError(f'Bulk API query rejected ({response.status_code}): {response.text[:2000]}')
            job_id = response.json()['id']
        job_url = f'{_bulk2_query_base(sf)}/{job_id}'
        report(f'Bulk API 2.0 query {job_id}: submitted. No deletion has started.')

        while True:
            check_stopped()
            if time.monotonic() - started >= timeout:
                raise TimeoutError(f'Bulk query {job_id} exceeded {timeout}s; no deletion started.')
            with session.get(job_url, headers=headers, timeout=(15, 60)) as response:
                response.raise_for_status()
                info = response.json()
            state = info.get('state', '')
            total_records = int(info.get('numberRecordsProcessed') or 0)
            report(f'Bulk query {job_id}: {state}, {total_records:,} records processed, '
                   f'{time.monotonic() - started:.0f}s elapsed.')
            if state == 'JobComplete':
                break
            if state in ('Failed', 'Aborted'):
                raise RuntimeError(f'Bulk query {state}: {info.get("errorMessage", "No details provided")}')
            _stop_flag.wait(2)

        check_stopped()
        downloaded = 0
        locator = None
        seen_locators = set()
        with tempfile.NamedTemporaryFile(
            suffix='.csv', prefix='soql_del_', mode='w', newline='', encoding='utf-8', delete=False
        ) as output:
            csv_path = output.name
            output.write('Id\n')
            while True:
                check_stopped()
                params = {'maxRecords': 500000}
                if locator:
                    params['locator'] = locator
                with session.get(
                    f'{job_url}/results', headers={**headers, 'Accept': 'text/csv'},
                    params=params, stream=True, timeout=(15, 120)
                ) as response:
                    response.raise_for_status()
                    response.raw.decode_content = True
                    page_records = 0
                    with pd.read_csv(
                        response.raw, usecols=['Id'], dtype=str, keep_default_na=False,
                        encoding='utf-8-sig', chunksize=25000
                    ) as chunks:
                        for chunk in chunks:
                            check_stopped()
                            if not chunk['Id'].str.fullmatch(r'[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?').all():
                                raise ValueError('Bulk query returned an empty or invalid Salesforce Id.')
                            chunk.to_csv(output, index=False, header=False, lineterminator='\n')
                            page_records += len(chunk)
                            downloaded += len(chunk)
                            report(f'Bulk query {job_id}: downloaded {downloaded:,} IDs'
                                   f' (Salesforce reports {total_records:,}), '
                                   f'{time.monotonic() - started:.0f}s elapsed. Writing CSV to disk.')
                    expected_page_records = response.headers.get('Sforce-NumberOfRecords')
                    if expected_page_records is not None and page_records != int(expected_page_records):
                        raise RuntimeError('Incomplete Bulk query result page; no deletion started.')
                    locator = response.headers.get('Sforce-Locator')
                if not locator or locator.lower() == 'null':
                    break
                if locator in seen_locators:
                    raise RuntimeError('Repeated Bulk query result locator; no deletion started.')
                seen_locators.add(locator)
        check_stopped()
        report(f'Bulk export complete: {downloaded:,} IDs downloaded in {time.monotonic() - started:.0f}s.')
        completed = True
        return csv_path, downloaded
    finally:
        if not completed:
            if job_url and state not in ('JobComplete', 'Failed', 'Aborted'):
                try:
                    with session.patch(job_url, headers=headers, json={'state': 'Aborted'}, timeout=(5, 10)) as response:
                        response.raise_for_status()
                except Exception:
                    pass
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)


def bulk2_query_df(sf, soql, on_progress=None):
    """Execute SOQL via Bulk API 2.0 Query job → returns pandas DataFrame.

    Best for large datasets (50K+ records). Creates an async query job,
    polls for completion, then streams CSV results with locator pagination.
    """
    sess = _get_http_session(sf)
    # Must include Accept: application/json for query job endpoints
    headers = {
        'Authorization': f'Bearer {sf.session_id}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    base_url = _bulk2_query_base(sf)

    # Create query job
    payload = {'operation': 'query', 'query': soql}
    r = sess.post(base_url, headers=headers, json=payload)
    r.raise_for_status()
    job_info = r.json()
    job_id = job_info['id']

    if on_progress:
        on_progress(f'Bulk API 2.0: Job created: {job_id}')

    # Poll until complete
    job_url = f'{base_url}/{job_id}'
    elapsed = 0.0
    state = ''
    num_records = 0
    while elapsed < 1800:  # 30 min max
        r = sess.get(job_url, headers=headers)
        r.raise_for_status()
        info = r.json()
        state = info.get('state', '')
        num_records = info.get('numberRecordsProcessed', 0)
        if state == 'JobComplete':
            break
        if state in ('Failed', 'Aborted'):
            raise RuntimeError(f'Bulk2 Query job {state}: {info.get("errorMessage", "")}')
        if on_progress:
            on_progress(f'Bulk API 2.0: state={state}, records={num_records:,}, elapsed={elapsed:.0f}s')
        sleep_time = 1 if elapsed < 10 else (2 if elapsed < 60 else 5)
        time.sleep(sleep_time)
        elapsed += sleep_time

    if state != 'JobComplete':
        raise TimeoutError(f'Bulk2 Query job did not complete in 1800s (last state: {state})')

    if on_progress:
        on_progress(f'Bulk API 2.0: Job complete. Records processed: {num_records:,}')

    # Download results — always attempt even if numberRecordsProcessed shows 0
    # (some orgs don't report this field accurately)

    # Download results with locator pagination
    results_url = f'{job_url}/results'
    csv_headers = {
        'Authorization': f'Bearer {sf.session_id}',
        'Accept': 'text/csv',
    }
    all_dfs = []
    locator = None
    page = 0

    while True:
        page += 1
        params = {'maxRecords': 500000}
        if locator:
            params['locator'] = locator
        r = sess.get(results_url, headers=csv_headers, params=params)
        r.raise_for_status()

        if r.text.strip():
            chunk_df = pd.read_csv(io.StringIO(r.text), dtype=str, keep_default_na=False)
            if not chunk_df.empty:
                all_dfs.append(chunk_df)
                if on_progress:
                    total_so_far = sum(len(d) for d in all_dfs)
                    on_progress(f'Bulk API 2.0: Downloaded page {page}, {total_so_far:,}/{num_records:,} rows')

        locator = r.headers.get('Sforce-Locator', '')
        if not locator or locator == 'null':
            break

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


def bulk1_query_df(sf, soql, on_progress=None):
    """Execute SOQL via Bulk API 1.0 batch query → returns pandas DataFrame.

    Fallback for orgs where Bulk API 2.0 is unavailable, or as a parallel
    fetch channel. Uses XML job creation + CSV batch results.
    """
    sess = _get_http_session(sf)
    base_url = f'https://{sf.sf_instance}/services/async/{SF_API_VERSION}/job'

    # Extract object name from SOQL (FROM <object>)
    match = re.search(r'\bFROM\s+(\w+)', soql, re.IGNORECASE)
    if not match:
        raise ValueError(f'Cannot extract object name from SOQL: {soql}')
    object_name = match.group(1)

    # Create job (XML)
    job_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataAccess">'
        f'<operation>query</operation>'
        f'<object>{object_name}</object>'
        '<contentType>CSV</contentType>'
        '</jobInfo>'
    )

    headers_xml = {
        'Authorization': f'Bearer {sf.session_id}',
        'Content-Type': 'application/xml; charset=UTF-8',
        'X-SFDC-Session': sf.session_id,
    }
    r = sess.post(base_url, headers=headers_xml, data=job_xml)
    r.raise_for_status()

    # Parse job ID from XML response
    job_id_match = re.search(r'<id>([^<]+)</id>', r.text)
    if not job_id_match:
        raise RuntimeError(f'Bulk1: Failed to parse job ID from: {r.text[:200]}')
    job_id = job_id_match.group(1)

    # Add batch (the SOQL query is the batch body for Bulk 1.0 query)
    batch_url = f'{base_url}/{job_id}/batch'
    batch_headers = {
        'Authorization': f'Bearer {sf.session_id}',
        'Content-Type': 'text/csv; charset=UTF-8',
        'X-SFDC-Session': sf.session_id,
    }
    r = sess.post(batch_url, headers=batch_headers, data=soql.encode('utf-8'))
    r.raise_for_status()

    batch_id_match = re.search(r'<id>([^<]+)</id>', r.text)
    if not batch_id_match:
        raise RuntimeError(f'Bulk1: Failed to parse batch ID from: {r.text[:200]}')
    batch_id = batch_id_match.group(1)

    # Close job
    close_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataAccess">'
        '<state>Closed</state>'
        '</jobInfo>'
    )
    sess.post(f'{base_url}/{job_id}', headers=headers_xml, data=close_xml)

    # Poll batch status
    batch_status_url = f'{base_url}/{job_id}/batch/{batch_id}'
    elapsed = 0.0
    state = ''
    while elapsed < 1800:
        r = sess.get(batch_status_url, headers=headers_xml)
        r.raise_for_status()
        state_match = re.search(r'<state>([^<]+)</state>', r.text)
        state = state_match.group(1) if state_match else 'Unknown'
        if state == 'Completed':
            break
        if state in ('Failed', 'NotProcessed'):
            msg_match = re.search(r'<stateMessage>([^<]+)</stateMessage>', r.text)
            msg = msg_match.group(1) if msg_match else 'Unknown error'
            raise RuntimeError(f'Bulk1 batch {state}: {msg}')
        if on_progress:
            on_progress(f'Bulk API 1.0: batch state={state}, elapsed={elapsed:.0f}s')
        sleep_time = 1 if elapsed < 10 else (2 if elapsed < 60 else 5)
        time.sleep(sleep_time)
        elapsed += sleep_time

    if state != 'Completed':
        raise TimeoutError(f'Bulk1 batch did not complete in 1800s')

    # Get result IDs
    result_url = f'{batch_status_url}/result'
    r = sess.get(result_url, headers=headers_xml)
    r.raise_for_status()

    result_ids = re.findall(r'<result>([^<]+)</result>', r.text)
    if not result_ids:
        # Might be CSV directly
        if r.text.strip() and not r.text.strip().startswith('<?xml'):
            return pd.read_csv(io.StringIO(r.text), dtype=str, keep_default_na=False)
        return pd.DataFrame()

    # Download each result
    all_dfs = []
    for rid in result_ids:
        r = sess.get(f'{result_url}/{rid}', headers={'Authorization': f'Bearer {sf.session_id}',
                                                      'X-SFDC-Session': sf.session_id})
        r.raise_for_status()
        if r.text.strip():
            chunk_df = pd.read_csv(io.StringIO(r.text), dtype=str, keep_default_na=False)
            if not chunk_df.empty:
                all_dfs.append(chunk_df)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


def rest_query_df(sf, soql, on_progress=None):
    """Execute SOQL via REST API query_all → returns pandas DataFrame.

    Best for small datasets (<50K records). Simple and fast with no job overhead.
    Auto-paginates through all records using query_more.
    """
    all_records = []
    result = sf.query(soql)
    total_size = result.get('totalSize', 0)
    records = result.get('records', [])
    for r in records:
        row = {k: v for k, v in r.items() if k != 'attributes'}
        all_records.append(row)

    # Paginate through remaining records
    while not result.get('done', True) and result.get('nextRecordsUrl'):
        result = sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)
        records = result.get('records', [])
        for r in records:
            row = {k: v for k, v in r.items() if k != 'attributes'}
            all_records.append(row)
        if on_progress and len(all_records) % 50000 < 2000:
            on_progress(f'REST API: fetched {len(all_records):,}/{total_size:,} records...')

    if on_progress:
        on_progress(f'REST API: fetched {len(all_records):,} records total')

    if all_records:
        df = pd.DataFrame(all_records)
        # Convert all to string for consistent comparison (match bulk API behavior)
        for col in df.columns:
            df[col] = df[col].astype(str).replace({'None': '', 'nan': '', 'NaN': ''})
        return df
    return pd.DataFrame()


def parallel_sf_fetch(sf, object_name, fields, key_field, total_record_count,
                      where_filter='', on_progress=None):
    """Fetch Salesforce data using optimal API strategy based on record count.

    For large datasets (>50K): runs Bulk API 2.0 as primary + Bulk API 1.0 as
    parallel fallback channel. For small datasets: REST API only.

    The 3-API parallel strategy:
      - Bulk API 2.0: Primary channel (handles the full dataset)
      - Bulk API 1.0: Parallel backup (if Bulk2 fails, this catches it)
      - REST API: Fast fallback for small data / emergency recovery

    Parameters
    ----------
    sf : simple_salesforce.Salesforce instance
    object_name : str — Salesforce object API name
    fields : list[str] — Fields to SELECT (must include key_field)
    key_field : str — Field used for key matching
    total_record_count : int — Expected number of records (for API selection)
    where_filter : str — Additional WHERE conditions (without WHERE keyword)
    on_progress : callable — Progress callback f(msg: str)

    Returns
    -------
    pd.DataFrame with all fetched records
    """
    if total_record_count == 0:
        return pd.DataFrame(columns=fields)

    # Ensure key_field is in the field list
    fields_list = list(fields)
    if key_field not in fields_list:
        fields_list = [key_field] + fields_list

    # SOQL field batching: if >80 fields, split into multiple queries
    MAX_FIELDS_PER_QUERY = 80
    if len(fields_list) > MAX_FIELDS_PER_QUERY:
        # Split fields into batches, always include key_field
        other_fields = [f for f in fields_list if f != key_field]
        field_batches = []
        for i in range(0, len(other_fields), MAX_FIELDS_PER_QUERY - 1):
            batch = [key_field] + other_fields[i:i + MAX_FIELDS_PER_QUERY - 1]
            field_batches.append(batch)
    else:
        field_batches = [fields_list]

    where_clause = where_filter if where_filter else ""

    # --- API Selection Strategy ---
    if total_record_count <= 2000:
        # Small: REST only (instant, no job overhead)
        if on_progress:
            on_progress(f'📡 REST API mode ({total_record_count:,} records)')
        all_dfs = []
        for batch_fields in field_batches:
            soql = f"SELECT {', '.join(batch_fields)} FROM {object_name}"
            if where_clause:
                soql += f" WHERE {where_clause}"
            df = rest_query_df(sf, soql, on_progress)
            all_dfs.append(df)
        if len(all_dfs) == 1:
            return all_dfs[0]
        # Merge multiple field batches on key
        result = all_dfs[0]
        for df in all_dfs[1:]:
            if not df.empty:
                result = result.merge(df, on=key_field, how='outer')
        return result

    elif total_record_count <= 50000:
        # Medium: Bulk API 2.0 only (fast, single channel sufficient)
        if on_progress:
            on_progress(f'⚡ Bulk API 2.0 mode ({total_record_count:,} records)')
        all_dfs = []
        for batch_fields in field_batches:
            soql = f"SELECT {', '.join(batch_fields)} FROM {object_name}"
            if where_clause:
                soql += f" WHERE {where_clause}"
            df = bulk2_query_df(sf, soql, on_progress)
            all_dfs.append(df)
        if len(all_dfs) == 1:
            return all_dfs[0]
        result = all_dfs[0]
        for df in all_dfs[1:]:
            if not df.empty:
                result = result.merge(df, on=key_field, how='outer')
        return result

    else:
        # Large (>50K): Sequential Bulk API strategy (no threading)
        # Threading with shared sf session causes silent failures.
        # Bulk API 2.0 is the proven best path for large datasets.
        primary_fields = field_batches[0]
        soql = f"SELECT {', '.join(primary_fields)} FROM {object_name}"
        if where_clause:
            soql += f" WHERE {where_clause}"

        if on_progress:
            on_progress(
                f'⚡ Bulk API 2.0 mode ({total_record_count:,} records) — '
                f'this may take 20-60 seconds for large objects...'
            )

        primary_df = pd.DataFrame()

        # Try Bulk API 2.0 first (best for large datasets)
        try:
            primary_df = bulk2_query_df(sf, soql, on_progress)
        except Exception as e:
            import traceback
            if on_progress:
                on_progress(f'⚠️ Bulk API 2.0 failed: {e}\n{traceback.format_exc()}')

        # Fallback to Bulk API 1.0
        if primary_df.empty:
            try:
                if on_progress:
                    on_progress('🔄 Trying Bulk API 1.0...')
                primary_df = bulk1_query_df(sf, soql, on_progress)
            except Exception as e:
                import traceback
                if on_progress:
                    on_progress(f'⚠️ Bulk API 1.0 failed: {e}\n{traceback.format_exc()}')

        # Last resort: REST API full pagination
        if primary_df.empty:
            try:
                if on_progress:
                    on_progress('🔄 Falling back to REST API (full pagination)...')
                primary_df = rest_query_df(sf, soql, on_progress)
            except Exception as e:
                import traceback
                raise RuntimeError(
                    f'All Salesforce APIs failed to fetch data.\n'
                    f'SOQL: {soql}\n'
                    f'Last error: {e}\n'
                    f'Traceback: {traceback.format_exc()}\n'
                    f'This usually means the connected user lacks access to this object.'
                )

        if primary_df.empty:
            raise RuntimeError(
                f'All Salesforce APIs returned 0 records.\n'
                f'SOQL: {soql}\n'
                f'This usually means the connected user lacks access to this object.'
            )

        if on_progress:
            on_progress(f'✅ Fetched {len(primary_df):,} records from Salesforce')

        # If we have multiple field batches, fetch remaining batches
        if len(field_batches) > 1:
            for batch_fields in field_batches[1:]:
                extra_soql = f"SELECT {', '.join(batch_fields)} FROM {object_name}"
                if where_clause:
                    extra_soql += f" WHERE {where_clause}"
                try:
                    extra_df = bulk2_query_df(sf, extra_soql, on_progress)
                    if not extra_df.empty:
                        primary_df = primary_df.merge(extra_df, on=key_field, how='left')
                except Exception:
                    try:
                        extra_df = bulk1_query_df(sf, extra_soql, on_progress)
                        if not extra_df.empty:
                            primary_df = primary_df.merge(extra_df, on=key_field, how='left')
                    except Exception:
                        pass

        return primary_df


# -------------------------------------------------
# Bulk Delete via Bulk API 2.0  (streaming)
# -------------------------------------------------
def bulk_delete_v2(
    csv_file_path, object_name, sf, id_column, chunk_size=10000,
    error_file='failed_salesforce_deletes.csv',
    num_parallel_chunks=3, verbose=False,
    on_progress=None, on_status=None, on_error=None,
    chunk_iterator=None, manage_stop_flag=True
):
    """Stream-process delete chunks with AUTO mode (thread ladder + API fallback).
    Submits to Salesforce immediately, never pre-loads the whole file."""
    if manage_stop_flag:
        clear_stop_flag()
    resolved_object_name = resolve_sf_object_api_name(sf, object_name)
    if on_status and resolved_object_name and resolved_object_name != (object_name or '').strip():
        on_status(f'ℹ️ Delete object resolved to Salesforce API name: {resolved_object_name}')

    if resolved_object_name == SPECIAL_DELETE_OBJECT_WARRANTY_CODE:
        return _bulk_delete_warranty_code_special(
            csv_file_path=csv_file_path,
            object_name=resolved_object_name,
            sf=sf,
            id_column=id_column,
            chunk_size=chunk_size,
            error_file=error_file,
            on_progress=on_progress,
            on_status=on_status,
            on_error=on_error,
            chunk_iterator=chunk_iterator,
        )

    chunk_size = min(max(chunk_size, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE)
    num_parallel_chunks = min(num_parallel_chunks, MAX_PARALLEL_JOBS)

    # AUTO mode: thread ladder + API fallback for deletes
    thread_ctrl = ThreadController(initial_cap=num_parallel_chunks)
    current_api = 'bulk_v2'
    API_CHAIN = ['bulk_v2', 'bulk_v1', 'rest']
    api_index = 0

    if on_status:
        on_status(
            f'⚙️ AUTO Delete: chunk_size={chunk_size:,}, '
            f'start_threads={thread_ctrl.current}, chain={" → ".join(API_CHAIN)}'
        )

    if chunk_iterator is None:
        try:
            chunk_iterator = pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False)
        except Exception as e:
            if on_error:
                on_error(f'Failed to read file: {e}')
            return None

    start_time = time.time()
    total_processed = 0
    total_success = 0
    total_failed = 0
    all_failed_records = []
    retryable_chunks = []
    chunks_submitted = 0
    chunk_timings = []
    pending = {}  # future -> (chunk_num, df_chunk, api_used)
    thread_reductions = 0
    api_switches = 0
    worker_status = queue.SimpleQueue()
    delete_attempts = {}

    def _queue_delete_status(message, level='info'):
        worker_status.put((message, level))

    def _flush_delete_status():
        while not worker_status.empty():
            message, level = worker_status.get()
            if on_status:
                on_status(message, level=level)

    def _persist_delete_failures():
        try:
            if all_failed_records:
                _write_failed_records_csv(error_file, all_failed_records)
        except Exception as _csv_err:
            if on_error:
                on_error(f'⚠️ Could not write error file: {_csv_err}')

    def _split_or_fail_delete_chunk(c_num, df_ref, error_text):
        nonlocal total_failed
        if len(df_ref) > 1 and str(c_num).count('.') < 2 and delete_attempts.get(str(c_num), 0) < 3:
            split_size = max(1, len(df_ref) // 2)
            if split_size < len(df_ref):
                split_chunks = [
                    df_ref.iloc[i:i + split_size].copy().reset_index(drop=True)
                    for i in range(0, len(df_ref), split_size)
                ]
                for idx, split_df in enumerate(split_chunks, start=1):
                    retryable_chunks.append((f'{c_num}.{idx}', split_df))
                if on_status:
                    on_status(
                        f'✂️ Split delete chunk {c_num} ({len(df_ref):,} rows) into '
                        f'{len(split_chunks)} smaller bulk chunks after failure: {error_text[:120]}',
                        level='warning'
                    )
                return

        failed_df = df_ref.copy()
        failed_df['sf__Error'] = error_text or 'Delete failed in Salesforce Bulk API'
        failed_df['sf__Id'] = failed_df.get('Id', '')
        failed_df['chunk_num'] = c_num
        total_failed += len(failed_df)
        all_failed_records.extend(failed_df.to_dict(orient='records'))
        _persist_delete_failures()
        if on_error:
            on_error(f'❌ Delete chunk {c_num} failed: {error_text}')

    def _is_ambiguous_full_chunk_failure(success, failed_records, df_ref):
        if success != 0 or not isinstance(failed_records, list):
            return False
        if len(failed_records) != len(df_ref) or len(df_ref) == 0:
            return False
        for row in failed_records:
            if str(row.get('sf__Error') or row.get('Error') or row.get('error') or '').strip():
                return False
        return True

    def _reconcile_delete_chunk_counts(c_num, df_ref, success, failed_records, api_used):
        """Ensure every source row is counted as either success or failure."""
        rows_in_chunk = len(df_ref)
        failed_records = list(failed_records) if isinstance(failed_records, list) else []
        accounted = int(success or 0) + len(failed_records)
        if accounted >= rows_in_chunk:
            return int(success or 0), failed_records

        missing = rows_in_chunk - accounted
        failed_ids = {
            str(row.get('sf__Id') or row.get('Id') or row.get('id') or '').strip()
            for row in failed_records
            if str(row.get('sf__Id') or row.get('Id') or row.get('id') or '').strip()
        }

        placeholders = []
        for record_id in df_ref['Id'].fillna('').astype(str).tolist():
            normalized_id = record_id.strip()
            if not normalized_id or normalized_id in failed_ids:
                continue
            placeholders.append({
                'Id': normalized_id,
                'sf__Id': normalized_id,
                'sf__Error': (
                    f'{api_used} returned no final outcome for this delete row; '
                    f'counted as failed to keep totals accurate'
                )[:200],
                'chunk_num': c_num,
            })
            failed_ids.add(normalized_id)
            if len(placeholders) >= missing:
                break

        if len(placeholders) < missing:
            for idx in range(missing - len(placeholders)):
                placeholders.append({
                    'Id': '',
                    'sf__Id': '',
                    'sf__Error': (
                        f'{api_used} dropped {missing} delete row(s) without a final outcome; '
                        f'row placeholder {idx + 1}/{missing}'
                    )[:200],
                    'chunk_num': c_num,
                })

        return int(success or 0), failed_records + placeholders

    def _submit_delete(executor, c_num, delete_df, api_name):
        attempt_key = str(c_num)
        delete_attempts[attempt_key] = delete_attempts.get(attempt_key, 0) + 1
        if api_name == 'bulk_v2':
            def _bulk2_delete_status(message, _api=api_name):
                _queue_delete_status(f'[{_api}] {message}')
            return executor.submit(_process_chunk_v2, c_num, delete_df, object_name, sf,
                                   'delete', None, verbose, _bulk2_delete_status)
        elif api_name == 'bulk_v1':
            if len(delete_df) > BULK1_MINI_CHUNK:
                def _bulk1_delete_split_runner(_c_num, _df_chunk):
                    t0 = time.time()
                    total_success_local = 0
                    all_failed_local = []
                    minis = [_df_chunk.iloc[i:i + BULK1_MINI_CHUNK]
                             for i in range(0, len(_df_chunk), BULK1_MINI_CHUNK)]
                    _job_level_error = None
                    for mini in minis:
                        s_local, f_local, t_local = _process_chunk_bulk1(
                            _c_num, mini, object_name, sf, 'delete', None, _queue_delete_status
                        )
                        total_success_local += int(s_local or 0)
                        # Propagate JOB_LEVEL_FAILED so _drain_delete can switch to REST
                        if t_local and t_local.get('error') and 'JOB_LEVEL_FAILED' in str(t_local['error']):
                            _job_level_error = t_local['error']
                            if isinstance(f_local, list) and f_local:
                                all_failed_local.extend(f_local)
                            else:
                                for record_id in mini['Id'].fillna('').astype(str).tolist():
                                    all_failed_local.append({
                                        'Id': record_id,
                                        'sf__Id': record_id,
                                        'sf__Error': str(t_local.get('error') or 'Bulk API 1.0 delete mini-batch failed')[:200],
                                        'chunk_num': _c_num,
                                    })
                        elif isinstance(f_local, list) and f_local:
                            all_failed_local.extend(f_local)
                    timing_local = {
                        'chunk': _c_num,
                        'rows': len(_df_chunk),
                        'api': 'bulk_v1',
                        'total_s': round(time.time() - t0, 2),
                    }
                    # If every mini-batch failed at the job level, propagate it so the caller can switch APIs.
                    if _job_level_error and total_success_local == 0:
                        timing_local['error'] = _job_level_error
                    return total_success_local, all_failed_local, timing_local

                return executor.submit(_bulk1_delete_split_runner, c_num, delete_df)
            return executor.submit(_process_chunk_bulk1, c_num, delete_df, object_name, sf, 'delete', None, _queue_delete_status)
        else:
            return executor.submit(_process_chunk_rest, c_num, delete_df, object_name, sf, 'delete', None, _queue_delete_status)

    def _drain_delete(pending_dict):
        nonlocal total_success, total_failed, thread_reductions, api_switches
        nonlocal current_api, api_index
        if not pending_dict:
            return
        done, _ = concurrent.futures.wait(
            pending_dict.keys(), return_when=concurrent.futures.FIRST_COMPLETED, timeout=3.0
        )
        _flush_delete_status()
        if not done:
            return
        for f in list(done):
            if f not in pending_dict:
                continue
            c_num, df_ref, api_used = pending_dict.pop(f)
            try:
                result_tuple = f.result(timeout=300)
                if len(result_tuple) == 3:
                    success, failed_records, timing = result_tuple
                else:
                    success, failed_records = result_tuple
                    timing = None

                # Classify errors
                chunk_error_class = None
                if success == 0 and timing and timing.get('error'):
                    chunk_error_class, code = classify_error(timing['error'])

                if chunk_error_class in (ErrorClass.SWITCH_API, ErrorClass.RETRYABLE) and delete_attempts.get(str(c_num), 0) >= 3:
                    _split_or_fail_delete_chunk(c_num, df_ref, timing['error'])
                    continue

                if chunk_error_class == ErrorClass.SWITCH_API:
                    api_switches += 1
                    next_api_index = API_CHAIN.index(api_used) + 1
                    if next_api_index < len(API_CHAIN):
                        api_index = max(api_index, next_api_index)
                        current_api = API_CHAIN[api_index]
                        thread_ctrl.reset()
                        retryable_chunks.append((c_num, df_ref))
                        if on_status:
                            on_status(f'🔄 API switch: {code} → now using {current_api}', level='warning')
                    else:
                        _split_or_fail_delete_chunk(c_num, df_ref, timing.get('error', 'Delete job failed'))
                elif chunk_error_class == ErrorClass.RETRYABLE:
                    new_threads, at_bottom = thread_ctrl.reduce()
                    thread_reductions += 1
                    if at_bottom and api_index < len(API_CHAIN) - 1:
                        api_index += 1
                        current_api = API_CHAIN[api_index]
                        thread_ctrl.reset()
                        api_switches += 1
                        retryable_chunks.append((c_num, df_ref))
                    elif at_bottom:
                        _split_or_fail_delete_chunk(c_num, df_ref, timing.get('error', 'Retryable delete failure'))
                    else:
                        retryable_chunks.append((c_num, df_ref))
                    if on_status:
                        on_status(f'⬇️ Threads → {thread_ctrl.current} ({code})', level='warning')
                else:
                    if _is_ambiguous_full_chunk_failure(success, failed_records, df_ref):
                        _api_error = (
                            f'{api_used} returned {len(df_ref):,} failed rows without error details. '
                            'Automatic retry stopped; inspect Salesforce job results before retrying.'
                        )
                        failed_records = [{**row, 'sf__Error': _api_error} for row in failed_records]
                        set_stop_flag()
                        if on_error:
                            on_error(_api_error)
                    # For deletes: ENTITY_IS_DELETED means already gone — count as success (O(n) scan)
                    if isinstance(failed_records, list):
                        _real_failed = []
                        _extra_success = 0
                        for _r in failed_records:
                            _err_val = str(
                                _r.get('sf__Error') or _r.get('error') or _r.get('Error') or ''
                            )
                            if 'ENTITY_IS_DELETED' in _err_val:
                                _extra_success += 1
                            else:
                                _real_failed.append(_r)
                        success += _extra_success
                        failed_records = _real_failed
                    success, failed_records = _reconcile_delete_chunk_counts(
                        c_num, df_ref, success, failed_records, api_used
                    )
                    total_success += success
                    if isinstance(failed_records, list):
                        total_failed += len(failed_records)
                        all_failed_records.extend(failed_records)
                    if timing is not None:
                        chunk_timings.append(timing)
                    completed = chunks_submitted - len(pending_dict) - len(retryable_chunks)
                    if on_status:
                        on_status(
                            f'✅ Chunk {c_num}: {success:,} deleted, '
                            f'{len(failed_records) if isinstance(failed_records, list) else 0:,} failed'
                        )
                    if failed_records and on_error:
                        on_error(f'Chunk {c_num} [{api_used}]: {_extract_error_message(failed_records[0])}')
                    if on_progress:
                        on_progress(min(completed / max(chunks_submitted, 1), 0.99))
            except Exception as e:
                err_class, _ = classify_error(str(e))
                if err_class in (ErrorClass.RETRYABLE, ErrorClass.SWITCH_API):
                    if api_index < len(API_CHAIN) - 1 and delete_attempts.get(str(c_num), 0) < 3:
                        retryable_chunks.append((c_num, df_ref))
                    else:
                        _split_or_fail_delete_chunk(c_num, df_ref, str(e))
                else:
                    # Unknown exception: count all records as failed so they appear in the error file
                    _err_df = df_ref.copy()
                    _err_df['sf__Error'] = f'{type(e).__name__}: {str(e)[:200]}'
                    _err_df['sf__Id'] = ''
                    total_failed += len(_err_df)
                    all_failed_records.extend(_err_df.to_dict(orient='records'))
                    if on_error:
                        on_error(f'❌ Chunk {c_num} error: {e}')
            _persist_delete_failures()
            if on_status:
                on_status(f'Delete totals: {total_success:,} success, {total_failed:,} failed')

    try:
        round_num = 0

        while True:
            if round_num == 0:
                source = chunk_iterator
            else:
                if not retryable_chunks:
                    break
                if on_status:
                    on_status(f'🔁 Delete reprocess round {round_num}: {len(retryable_chunks)} chunk(s)')
                source = iter(retryable_chunks)
                retryable_chunks = []

            with ThreadPoolExecutor(max_workers=thread_ctrl.current) as executor:
                for item in source:
                    if is_stopped():
                        if on_status:
                            on_status('⛔ Operation stopped by user', level='warning')
                        break

                    if round_num == 0:
                        df_chunk = item
                    else:
                        _, df_chunk = item

                    df_chunk = df_chunk.dropna(how='all')
                    _nonempty_delete = df_chunk.apply(
                        lambda s: s.fillna('').astype(str).str.strip().ne('')
                        if s.dtype == object else s.notna()
                    ).any(axis=1)
                    df_chunk = df_chunk[_nonempty_delete]
                    if id_column in df_chunk.columns:
                        df_chunk = df_chunk[df_chunk[id_column].fillna('').astype(str).str.strip() != '']

                    if df_chunk.empty:
                        continue

                    chunks_submitted += 1
                    c_num = chunks_submitted if round_num == 0 else item[0]
                    df_chunk.columns = df_chunk.columns.str.strip()
                    df_chunk = df_chunk.fillna('')

                    if id_column not in df_chunk.columns:
                        if on_status:
                            on_status(f'Chunk {c_num}: missing ID column "{id_column}"', level='warning')
                        if round_num == 0:
                            chunks_submitted -= 1
                        continue

                    delete_df = df_chunk[[id_column]].copy()
                    delete_df = delete_df.rename(columns={id_column: 'Id'})
                    delete_df = delete_df[delete_df['Id'].fillna('').astype(str).str.strip() != '']
                    if delete_df.empty:
                        if round_num == 0:
                            chunks_submitted -= 1
                        continue

                    if round_num == 0:
                        total_processed += len(delete_df)

                    future = _submit_delete(executor, c_num, delete_df, current_api)
                    pending[future] = (c_num, delete_df, current_api)

                    while len(pending) >= thread_ctrl.current:
                        _drain_delete(pending)
                        if is_stopped():
                            break

                while pending:
                    _drain_delete(pending)

            round_num += 1
            if not retryable_chunks:
                break

    except KeyboardInterrupt:
        if on_status:
            on_status('⛔ Operation interrupted', level='warning')

    if on_progress:
        on_progress(1.0)
    elapsed = time.time() - start_time

    timing_summary = _build_timing_summary(chunk_timings, elapsed)

    if on_status and (thread_reductions > 0 or api_switches > 0):
        on_status(f'📊 AUTO: {thread_reductions} thread reductions, {api_switches} API switches')

    return {
        'total_processed': total_processed,
        'total_success': total_success,
        'total_failed': total_failed,
        'total_completed': total_success + total_failed,
        'elapsed': elapsed,
        'error_file': error_file if all_failed_records else None,
        'chunk_timings': chunk_timings,
        'timing_summary': timing_summary,
        'auto_mode_stats': {
            'thread_reductions': thread_reductions,
            'api_switches': api_switches,
            'final_api': current_api,
        },
    }
