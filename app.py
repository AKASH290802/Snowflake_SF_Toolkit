import os

import io

import re

import csv

import gzip

import json

import time

import queue

import tempfile

import datetime

import threading

import traceback

import importlib.util
import inspect

import concurrent.futures

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import streamlit as st

import streamlit.components.v1 as components
from load_sources import stream_snowflake_batches, validate_snowflake_column_mapping
from load_events import LoadEventBus
from load_capacity import SharedLoadCapacity
from ui_theme import render_theme_control, render_startup_splash

from sf_bulk_loader import (get_upsert_fields, get_object_fields, auto_match_csv_to_sf,

                             bulk_load_v2, bulk_delete_v2, bulk2_query_ids_to_csv, set_stop_flag,

                             clear_stop_flag, is_stopped,

                             MAX_CHUNK_SIZE, MAX_PARALLEL_JOBS, MIN_CHUNK_SIZE,

                             parallel_sf_fetch, parallel_snowflake_fetch, 

                             get_object_complexity_score, get_auto_chunk_size)

import re

# Fast presence check via find_spec — does NOT import the module (instant)

# snowflake.connector takes ~2s to import, cryptography ~1s, simple_salesforce ~0.3s

# All three are deferred to the moment the user clicks Connect.

HAS_SNOWFLAKE = importlib.util.find_spec('snowflake') is not None

STOP_REQUESTED_ERROR = 'Operation stopped by user'


def ensure_not_stopped():

    if is_stopped():

        raise RuntimeError(STOP_REQUESTED_ERROR)


def normalize_sf_api_name(value):

    raw = (value or '').strip()

    if not raw:

        return ''

    return raw if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', raw) else ''

def js_click_tab(tab_index):

    """Inject JavaScript to click a specific tab after Streamlit rerun.

    This prevents the UI from resetting to the first tab."""

    components.html(f"""

        <script>

            (function() {{

                var tabs = window.parent.document.querySelectorAll('[data-baseweb="tab"]');

                if (tabs.length > {tab_index}) {{

                    tabs[{tab_index}].click();

                }}

            }})();

        </script>

    """, height=0)

def install_tab_persistence():

    """Persist the active Streamlit tab across Python reruns.

    Strategy:
    - A per-session rerun counter is used as the nonce so the script
      re-fires after each Python rerun, but is stable within a rerun.
    - Tab restore uses dispatchEvent(MouseEvent) instead of .click() so
      Streamlit does NOT see it as a user interaction → no extra rerun.
    - Listeners are attached only once (guarded by dataset flag).
    """

    # Increment a rerun counter so the nonce changes each rerun but is
    # deterministic (not time-based), avoiding double-firing.
    if '_tab_rerun' not in st.session_state:
        st.session_state['_tab_rerun'] = 0
    st.session_state['_tab_rerun'] += 1
    nonce = st.session_state['_tab_rerun']

    components.html(f"""

        <script>

        /* nonce={nonce} */

        (function() {{

            const doc = window.parent.document;

            const KEY = 'sfbulk_active_tab_op';

            const _TAB_OPS = ['Insert','Update','Upsert','Delete','Multi-Object','Snowflake','SF\u2192Snowflake','TestCase'];

            function broadcastOp(idx) {{

                try {{ window.top.__sfActiveTabOp = _TAB_OPS[idx] || 'Insert'; }} catch(e) {{}}

            }}

            function getTabs() {{

                const list = doc.querySelector('[role="tablist"]');

                return list ? Array.from(list.querySelectorAll('[role="tab"]')) : [];

            }}

            function attachListeners(tabs) {{

                tabs.forEach((tab, idx) => {{

                    /* Always remove the old listener (from a previous iframe that was
                       destroyed on rerun) and attach a fresh one in THIS iframe's
                       context. Storing on _sfTabListener avoids duplicates. */

                    if (tab._sfTabListener) {{

                        tab.removeEventListener('mousedown', tab._sfTabListener);
                        tab.removeEventListener('click', tab._sfTabListener);

                    }}

                    tab._sfTabListener = function() {{

                        localStorage.setItem(KEY, _TAB_OPS[idx]);

                        broadcastOp(idx);

                    }};

                    tab.addEventListener('click', tab._sfTabListener);

                }});

            }}

            function broadcastCurrentAriaTab(tabs) {{

                /* Read aria-selected as the ground-truth of which tab is VISUALLY
                   active right now. Used after restore and on init. */

                tabs.forEach(function(t, i) {{

                    if (t.getAttribute('aria-selected') === 'true') broadcastOp(i);

                }});

            }}

            function restoreTab(tabs) {{

                const saved = localStorage.getItem(KEY);

                if (saved === null) return;

                const idx = _TAB_OPS.indexOf(saved);

                if (isNaN(idx) || idx < 0 || idx >= tabs.length) return;

                const target = tabs[idx];

                if (target.getAttribute('aria-selected') === 'true') return;

                /* Use dispatchEvent instead of .click() so Streamlit does NOT
                   treat this as a widget interaction → prevents a new rerun. */

                target.dispatchEvent(new MouseEvent('click', {{

                    bubbles: true, cancelable: true, view: window.parent

                }}));

            }}

            let tries = 0;

            const timer = setInterval(() => {{

                tries++;

                const tabs = getTabs();

                if (tabs.length > 0) {{

                    /* Re-attach fresh listeners every rerun — old listeners point
                       into the destroyed previous iframe and silently do nothing. */

                    attachListeners(tabs);

                    /* Broadcast the tab that localStorage says was active, then
                       after restore settle, re-broadcast actual aria-selected. */

                    var _saved = localStorage.getItem(KEY);

                    var _savedIdx = _TAB_OPS.indexOf(_saved);

                    if (_savedIdx >= 0 && _savedIdx < tabs.length) {{

                        broadcastOp(_savedIdx);

                    }} else {{

                        broadcastCurrentAriaTab(tabs);

                    }}

                    restoreTab(tabs);

                    /* After restore click Streamlit updates aria-selected async;
                       re-broadcast the real selection 300 ms later. */

                    setTimeout(function() {{

                        broadcastCurrentAriaTab(getTabs());

                    }}, 300);

                    clearInterval(timer);

                }} else if (tries >= 50) {{

                    clearInterval(timer);

                }}

            }}, 100);

        }})();

        </script>

    """, height=0)

def install_button_retagger():
    """Retag buttons whose text matches danger keywords so CSS can style them red.

    Runs once per rerun via a MutationObserver — picks up dynamically rendered
    buttons (e.g. STOP buttons that only appear during an active job).
    """
    if '_btn_retag_rerun' not in st.session_state:
        st.session_state['_btn_retag_rerun'] = 0
    st.session_state['_btn_retag_rerun'] += 1
    nonce = st.session_state['_btn_retag_rerun']
    components.html(f"""
        <script>
        /* nonce={nonce} */
        (function() {{
            const doc = window.parent.document;
            const DANGER_RE = /(🛑|🗑️|STOP\\b|Delete\\b|Disconnect\\b|Drop\\s+Table|Remove\\b)/i;
            function retag() {{
                doc.querySelectorAll('.stButton > button, [data-testid="stBaseButton-secondary"], [data-testid="stBaseButton-primary"]').forEach(btn => {{
                    if (btn.dataset.sfRetagged === '1') return;
                    const txt = btn.innerText || btn.textContent || '';
                    if (DANGER_RE.test(txt)) {{
                        btn.setAttribute('data-danger', 'true');
                    }}
                    btn.dataset.sfRetagged = '1';
                }});
            }}
            retag();
            if (!doc.body.dataset.sfBtnObserver) {{
                doc.body.dataset.sfBtnObserver = '1';
                const obs = new MutationObserver(() => {{
                    /* throttle */
                    if (window.__sfRetagPending) return;
                    window.__sfRetagPending = true;
                    setTimeout(() => {{ window.__sfRetagPending = false; retag(); }}, 80);
                }});
                obs.observe(doc.body, {{ childList: true, subtree: true }});
            }}
        }})();
        </script>
    """, height=0)


def install_command_palette():
    """Inject a Linear/Notion-style ⌘K command palette overlay.

    Lists all 8 tabs; typing filters them. Click or Enter dispatches a
    real click event on the matching tab (re-uses the tab-persistence
    pattern so no extra rerun is triggered).
    """
    if '_cmdk_rerun' not in st.session_state:
        st.session_state['_cmdk_rerun'] = 0
    st.session_state['_cmdk_rerun'] += 1
    nonce = st.session_state['_cmdk_rerun']
    components.html(f"""
        <script>
        /* nonce={nonce} */
        (function() {{
            const doc = window.parent.document;
            const top = window.top;

            /* ---- Build/recover overlay markup ---- */
            const _needsBuild = !doc.getElementById('sf-cmdk-overlay') ||
                !doc.getElementById('sf-cmdk-fab') ||
                !doc.getElementById('sf-cmdk-input') ||
                !doc.getElementById('sf-cmdk-list');

            /* Streamlit reruns can leave custom parent-DOM nodes partially stale.
               Rebuild the palette if any required piece is missing. */
            if (_needsBuild) {{
                ['sf-cmdk-overlay', 'sf-cmdk-fab'].forEach(id => {{
                    const el = doc.getElementById(id);
                    if (el) el.remove();
                }});
                const wrap = doc.createElement('div');
                wrap.id = 'sf-cmdk-overlay';
                wrap.style.cssText = `
                    position:fixed; inset:0; z-index:1000000;
                    background:rgba(5,8,18,0.65); backdrop-filter:blur(8px);
                    display:none; align-items:flex-start; justify-content:center;
                    padding-top:12vh;
                `;
                wrap.innerHTML = `
                    <style>
                    @keyframes sf-cmdk-in {{
                        from {{ opacity:0; }} to {{ opacity:1; }}
                    }}
                    @keyframes sf-cmdk-pop {{
                        from {{ opacity:0; transform:translateY(-12px) scale(0.96); }}
                        to   {{ opacity:1; transform:translateY(0) scale(1); }}
                    }}
                    #sf-cmdk-panel {{
                        width:min(620px, 92vw);
                        background:var(--sf-panel-wash, #15232b);
                        border:1px solid var(--sf-border, #414b60); border-radius:18px;
                        box-shadow:var(--sf-shadow, 0 24px 64px rgba(0,0,0,0.3));
                        overflow:hidden; animation:sf-cmdk-pop 0.22s ease-out;
                        font-family:Inter, system-ui, sans-serif;
                    }}
                    #sf-cmdk-panel input {{
                        width:100%; box-sizing:border-box; padding:18px 22px; font-size:1.05rem;
                        background:transparent; border:none; outline:none;
                        color:var(--sf-text, #f0f4ff); font-family:inherit;
                        border-bottom:1px solid var(--sf-border, #414b60);
                    }}
                    #sf-cmdk-panel input::placeholder {{ color:var(--sf-muted, #b2bdcd); }}
                    #sf-cmdk-list {{
                        max-height:420px; overflow-y:auto; padding:8px; margin:0; list-style:none;
                    }}
                    #sf-cmdk-list::-webkit-scrollbar {{ width:6px; }}
                    #sf-cmdk-list::-webkit-scrollbar-thumb {{
                        background:var(--sf-border, #414b60); border-radius:999px;
                    }}
                    #sf-cmdk-list li {{
                        display:flex; align-items:center; gap:12px; padding:11px 14px; border-radius:10px;
                        cursor:pointer; color:var(--sf-text, #f0f4ff);
                        font-size:0.92rem; font-weight:500;
                        transition:background 0.12s ease;
                    }}
                    #sf-cmdk-list li:hover, #sf-cmdk-list li.active {{
                        background:var(--sf-selected, #164e63); color:var(--sf-text, #f0f4ff);
                    }}
                    #sf-cmdk-list li.active {{ box-shadow:inset 3px 0 0 var(--sf-accent, #5eead4); }}
                    .sf-cmdk-icon {{ font-size:1.15rem; min-width:24px; text-align:center; }}
                    .sf-cmdk-kind {{
                        margin-left:auto; font-size:0.68rem; letter-spacing:0.05em;
                        text-transform:uppercase; color:var(--sf-muted, #b2bdcd);
                        background:var(--sf-field, #1d3039); padding:3px 8px; border-radius:6px;
                    }}
                    #sf-cmdk-foot {{
                        display:flex; gap:14px; padding:10px 18px;
                        border-top:1px solid var(--sf-border, #414b60);
                        color:var(--sf-muted, #b2bdcd); font-size:0.74rem;
                    }}
                    #sf-cmdk-foot kbd {{
                        background:var(--sf-field, #1d3039); border:1px solid var(--sf-border, #414b60);
                        padding:2px 6px; border-radius:5px; font-family:Inter, monospace;
                        font-size:0.70rem; color:var(--sf-text, #f0f4ff); margin-right:5px;
                    }}
                    #sf-cmdk-empty {{
                        padding:32px 18px; text-align:center; color:var(--sf-muted, #b2bdcd); font-size:0.88rem;
                    }}
                    </style>
                    <div id="sf-cmdk-panel">
                        <input id="sf-cmdk-input" placeholder="🔍 Type to search tabs…" autocomplete="off" spellcheck="false" />
                        <ul id="sf-cmdk-list"></ul>
                        <div id="sf-cmdk-foot">
                            <span><kbd>↑↓</kbd>navigate</span>
                            <span><kbd>↵</kbd>select</span>
                            <span><kbd>esc</kbd>close</span>
                            <span style="margin-left:auto;color:var(--sf-accent);">Ctrl+/</span>
                        </div>
                    </div>
                `;
                doc.body.appendChild(wrap);

                /* FAB */
                const fab = doc.createElement('div');
                fab.id = 'sf-cmdk-fab';
                fab.title = 'Search tabs (Ctrl+/)';
                fab.style.cssText = `
                    position:fixed; bottom:28px; right:28px; z-index:9998;
                    width:46px; height:46px; border-radius:50%;
                    background:var(--sf-primary-wash, var(--sf-primary, #0f766e));
                    color:var(--sf-primary-text, #ffffff);
                    box-shadow:0 4px 22px color-mix(in srgb, var(--sf-primary, #0f766e) 35%, transparent);
                    cursor:pointer; display:flex; align-items:center; justify-content:center;
                    font-size:1.15rem; transition:transform 0.18s ease,box-shadow 0.18s ease;
                    user-select:none;
                `;
                fab.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true" style="font-family:Material Symbols Rounded;font-size:24px;color:inherit;">search</span>';
                fab.addEventListener('mouseenter', () => {{
                    fab.style.transform = 'scale(1.14)';
                    fab.style.boxShadow = '0 6px 30px color-mix(in srgb, var(--sf-primary, #0f766e) 45%, transparent)';
                }});
                fab.addEventListener('mouseleave', () => {{
                    fab.style.transform = 'scale(1)';
                    fab.style.boxShadow = '0 4px 22px color-mix(in srgb, var(--sf-primary, #0f766e) 35%, transparent)';
                }});
                /* FAB click uses the stored open fn so it always gets the latest closure */
                fab.addEventListener('click', () => {{ if (top.__sfCmdkOpen) top.__sfCmdkOpen(); }});
                doc.body.appendChild(fab);
            }}

            /* ---- Wire up palette logic (runs every Streamlit render) ---- */
            const wrap  = doc.getElementById('sf-cmdk-overlay');
            let input = doc.getElementById('sf-cmdk-input');
            const list  = doc.getElementById('sf-cmdk-list');

            function getCommands() {{
                const tabs = doc.querySelectorAll('[role="tab"]');
                const cmds = [];
                tabs.forEach((tab, idx) => {{
                    const label = (tab.innerText || tab.textContent || '').trim();
                    if (!label) return;
                    const m = label.match(/^(\\p{{Emoji}}+\\s*)?(.*)$/u);
                    const icon = (m && m[1]) ? m[1].trim() : '📄';
                    const text = (m && m[2]) ? m[2].trim() : label;
                    cmds.push({{
                        icon, label: text, kind: 'Tab', idx,
                        action: () => tab.dispatchEvent(new MouseEvent('click', {{
                            bubbles:true, cancelable:true, view:window.parent
                        }}))
                    }});
                }});
                return cmds;
            }}

            let activeIdx = 0;
            let filtered = [];

            function render() {{
                const q = input.value.trim().toLowerCase();
                const all = getCommands();
                filtered = q ? all.filter(c =>
                    (c.icon+' '+c.label+' '+c.kind).toLowerCase().includes(q)) : all;
                if (activeIdx >= filtered.length) activeIdx = 0;
                if (filtered.length === 0) {{
                    list.innerHTML = '<div id="sf-cmdk-empty">No matches for "'+
                        q.replace(/</g,'&lt;')+'"</div>';
                    return;
                }}
                list.innerHTML = filtered.map((c,i) =>
                    `<li data-i="${{i}}" class="${{i===activeIdx?'active':''}}">
                        <span class="sf-cmdk-icon">${{c.icon}}</span>
                        <span>${{c.label}}</span>
                        <span class="sf-cmdk-kind">${{c.kind}}</span>
                    </li>`
                ).join('');
                list.querySelectorAll('li').forEach(li => {{
                    li.addEventListener('mouseenter', () => {{
                        activeIdx = parseInt(li.dataset.i, 10);
                        list.querySelectorAll('li').forEach(x => x.classList.remove('active'));
                        li.classList.add('active');
                    }});
                    li.addEventListener('click', () => execute());
                }});
            }}

            function execute() {{
                if (filtered.length === 0) return;
                const cmd = filtered[activeIdx];
                close();
                setTimeout(() => cmd.action(), 60);
            }}

            function open() {{
                wrap.style.display = 'flex';
                input.value = '';
                activeIdx = 0;
                render();
                setTimeout(() => input.focus(), 30);
            }}
            function close() {{ wrap.style.display = 'none'; }}

            /* Always re-attach input listeners (cloneNode trick to wipe old ones) */
            const newInput = input.cloneNode(true);
            input.parentNode.replaceChild(newInput, input);
            input = newInput;
            input.addEventListener('input', render);
            input.addEventListener('keydown', e => {{
                if (e.key === 'Escape')     {{ e.preventDefault(); close(); return; }}
                if (e.key === 'Enter')      {{ e.preventDefault(); execute(); return; }}
                if (e.key === 'ArrowDown')  {{
                    e.preventDefault(); if (!filtered.length) return;
                    activeIdx = (activeIdx+1) % filtered.length; render();
                }} else if (e.key === 'ArrowUp') {{
                    e.preventDefault(); if (!filtered.length) return;
                    activeIdx = (activeIdx-1+filtered.length) % filtered.length; render();
                }}
            }});
            wrap.addEventListener('click', e => {{ if (e.target === wrap) close(); }});

            /* ---- Hotkey: Ctrl+/ ----
               KEY INSIGHT: Streamlit renders in the TOP document, not inside iframes.
               Standard keyboard events fire on doc (window.parent.document from here).
               We ALWAYS remove the old handler and re-attach so reruns don't stack
               dead closures or leave the listener missing after iframe recreation. */
            function _sfIsEditable(el) {{
                if (!el) return false;
                const tag = (el.tagName||'').toUpperCase();
                return tag==='INPUT' || tag==='TEXTAREA' || el.isContentEditable;
            }}
            function _sfKeyHandler(e) {{
                if (!((e.ctrlKey||e.metaKey) && e.key==='/')) return;
                /* Allow Ctrl+/ inside text inputs to pass through (e.g. URL fields) */
                const active = doc.activeElement || e.target;
                if (_sfIsEditable(active)) return;
                e.preventDefault();
                try {{ e.stopImmediatePropagation(); }} catch(_) {{}}
                if (wrap.style.display === 'flex') close(); else open();
            }}
            /* Remove previous handler stored on top so we never double-stack */
            if (top.__sfCmdkKeyHandler) {{
                try {{ doc.removeEventListener('keydown', top.__sfCmdkKeyHandler, true); }} catch(_) {{}}
                try {{ top.removeEventListener('message', top.__sfCmdkMsgHandler); }} catch(_) {{}}
            }}
            /* Attach to the parent document in CAPTURE phase so it fires before
               any widget's own keydown handler can consume the event */
            doc.addEventListener('keydown', _sfKeyHandler, true);
            top.__sfCmdkKeyHandler = _sfKeyHandler;

            /* postMessage fallback — for any deeply nested component iframe
               that intercepts keydown before it can bubble to doc */
            function _sfMsgHandler(ev) {{
                if (ev.data === '__sf_cmdk_toggle__') {{
                    if (wrap.style.display === 'flex') close(); else open();
                }}
            }}
            top.addEventListener('message', _sfMsgHandler);
            top.__sfCmdkMsgHandler = _sfMsgHandler;

            /* Also listen inside THIS iframe so the postMessage path stays working */
            window.addEventListener('keydown', function(e) {{
                if (!((e.ctrlKey||e.metaKey) && e.key==='/')) return;
                if (_sfIsEditable(e.target)) return;
                e.preventDefault();
                try {{ top.postMessage('__sf_cmdk_toggle__', '*'); }} catch(_) {{}}
            }}, true);

            /* Store open fn so FAB + hero pill can always reach it */
            top.__sfCmdkOpen = open;
            try {{ window.parent.__sfOpenCmdk = open; }} catch(_) {{}}
            try {{ top.__sfOpenCmdk = open; }} catch(_) {{}}

            function bindFab() {{
                let fab = doc.getElementById('sf-cmdk-fab');
                if (!fab) return;
                const freshFab = fab.cloneNode(true);
                fab.parentNode.replaceChild(freshFab, fab);
                fab = freshFab;
                fab.addEventListener('mouseenter', () => {{
                    fab.style.transform = 'scale(1.14)';
                    fab.style.boxShadow = '0 6px 30px rgba(168,85,247,0.75),0 0 0 2px rgba(168,85,247,0.35)';
                }});
                fab.addEventListener('mouseleave', () => {{
                    fab.style.transform = 'scale(1)';
                    fab.style.boxShadow = '0 4px 22px rgba(168,85,247,0.55),0 0 0 2px rgba(168,85,247,0.20)';
                }});
                fab.addEventListener('click', () => {{
                    if (top.__sfCmdkOpen) top.__sfCmdkOpen();
                }});
            }}
            bindFab();
        }})();
        </script>
    """, height=0)


# ------------------------------------------------------------------
# Job history (small JSON-backed log of last N runs, used by sidebar)
# ------------------------------------------------------------------
JOB_HISTORY_FILE = os.path.join(os.path.dirname(__file__), 'job_history.json') if '__file__' in dir() else 'job_history.json'
JOB_HISTORY_MAX = 20

def load_job_history():
    try:
        files_to_read = [JOB_HISTORY_FILE]
        parent_hist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'job_history.json') if '__file__' in dir() else None
        if parent_hist and parent_hist not in files_to_read and os.path.exists(parent_hist):
            files_to_read.append(parent_hist)

        merged = []
        seen = set()
        for hist_file in files_to_read:
            if not os.path.exists(hist_file):
                continue
            with open(hist_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue
                for r in data:
                    key = (
                        str(r.get('ts', '')),
                        str(r.get('op', '')),
                        str(r.get('obj', '')),
                        int(r.get('success', 0) or 0),
                        int(r.get('failed', 0) or 0),
                        str(r.get('api', '')),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(r)

        merged.sort(key=lambda x: str(x.get('ts', '')))
        return merged[-JOB_HISTORY_MAX:]
    except Exception:
        pass
    return []

def record_job_run(operation, object_name, success, failed, elapsed, api='', source='', sub_op=''):
    """Append a finished run to the job-history log."""
    try:
        history = []
        if os.path.exists(JOB_HISTORY_FILE):
            with open(JOB_HISTORY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    history = data
        history.append({
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'op': str(operation or ''),
            'sub_op': str(sub_op or ''),
            'obj': str(object_name or ''),
            'success': int(success or 0),
            'failed': int(failed or 0),
            'elapsed': float(elapsed or 0),
            'api': str(api or ''),
            'source': str(source or ''),
        })
        history = history[-JOB_HISTORY_MAX:]
        with open(JOB_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2)
    except Exception:
        pass


from functools import lru_cache

# ------------------------------------

# st.fragment compatibility shim

# ------------------------------------

try:

    _fragment = st.fragment

except AttributeError:

    def _fragment(func=None, *, run_every=None):

        if func is None:

            return lambda f: f

        return func

# Path to the folder containing CSV data files (parent of this app folder)

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

CREDS_FILE = os.path.join(os.path.dirname(__file__), 'saved_credentials.json')

SNOWFLAKE_CREDS_FILE = os.path.join(os.path.dirname(__file__), 'saved_snowflake_credentials.json')

TESTCASE_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'saved_testcase_configs.json')

# -------------------------------------------------

# Credential helpers

# -------------------------------------------------

def load_saved_credentials():

    if os.path.exists(CREDS_FILE):

        with open(CREDS_FILE, 'r') as f:

            return json.load(f)

    return {}

def save_credentials(all_creds):

    with open(CREDS_FILE, 'w') as f:

        json.dump(all_creds, f, indent=2)

def load_saved_snowflake_credentials():

    if os.path.exists(SNOWFLAKE_CREDS_FILE):

        with open(SNOWFLAKE_CREDS_FILE, 'r') as f:

            return json.load(f)

    return {}

def save_snowflake_credentials(all_creds):

    with open(SNOWFLAKE_CREDS_FILE, 'w') as f:

        json.dump(all_creds, f, indent=2)

def load_testcase_configs():

    if os.path.exists(TESTCASE_CONFIG_FILE):

        with open(TESTCASE_CONFIG_FILE, 'r') as f:

            return json.load(f)

    return {}

def save_testcase_configs(all_configs):

    with open(TESTCASE_CONFIG_FILE, 'w') as f:

        json.dump(all_configs, f, indent=2)


def show_feedback(msg_type, msg_text, success_fn=None, warning_fn=None, error_fn=None):

    st.toast(msg_text)

    if msg_type == 'success' and success_fn is not None:

        success_fn(msg_text)

    elif msg_type == 'warning' and warning_fn is not None:

        warning_fn(msg_text)

    elif msg_type == 'error' and error_fn is not None:

        error_fn(msg_text)


def queue_action_feedback(action, message, tone='success'):
    """Show one acknowledgement dialog after a persisted UI action."""
    st.session_state['_action_feedback'] = {
        'action': action,
        'message': message,
        'tone': tone,
    }


def _clear_action_feedback():
    st.session_state.pop('_action_feedback', None)


@st.dialog('Action complete')
def _action_feedback_dialog(feedback):
    tone = feedback.get('tone', 'success')
    icon = {'success': '✅', 'warning': '⚠️', 'error': '❌'}.get(tone, 'ℹ️')
    st.markdown(f'### {icon} {feedback.get("action", "Action") }')
    st.write(feedback.get('message', 'The action finished.'))
    if st.button('OK', type='primary', width='stretch'):
        _clear_action_feedback()
        st.rerun()


def render_action_feedback():
    feedback = st.session_state.get('_action_feedback')
    if feedback:
        _action_feedback_dialog(feedback)

# -------------------------------------------------

# Cached helper functions for performance

# -------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)

def get_cached_upsert_fields(_sf, object_name):

    """Cache upsert fields for 5 minutes to avoid repeated SF API calls"""

    return get_upsert_fields(_sf, object_name)

@st.cache_data(ttl=300, show_spinner=False)

def get_cached_object_fields(_sf, object_name):

    """Cache object fields for 5 minutes to avoid repeated SF API calls"""

    return get_object_fields(_sf, object_name)

def _open_text_file(filepath):
    """Open a text file trying UTF-8-BOM, UTF-8, then cp1252."""
    for enc in ('utf-8-sig', 'utf-8', 'cp1252', 'latin-1'):
        try:
            fh = open(filepath, 'r', encoding=enc, errors='strict')
            fh.read(512)   # probe
            fh.seek(0)
            return fh
        except (UnicodeDecodeError, LookupError):
            try: fh.close()
            except Exception: pass
    return open(filepath, 'r', encoding='utf-8', errors='replace')


def _detect_delimiter(filepath):
    """Detect delimiter using csv.Sniffer on the first data line.
    Falls back to character-frequency counting across |  \\t  ,  ;  ."""
    import csv as _csv, re as _re
    _SEP_PAT = _re.compile(r'^[\s\-=|+:]+$')
    candidates = '|\t,;'
    data_lines = []
    try:
        with _open_text_file(filepath) as fh:
            for _ in range(30):
                raw = fh.readline()
                if not raw:
                    break
                line = raw.strip()
                # strip border pipes to avoid confusing Sniffer
                if line.startswith('|'): line = line[1:]
                if line.endswith('|'):   line = line[:-1]
                inner = line.replace('|', '').replace('-', '').replace('=', '').strip()
                if inner:
                    data_lines.append(line)
                    if len(data_lines) >= 5:
                        break
    except Exception:
        pass
    if data_lines:
        try:
            dialect = _csv.Sniffer().sniff('\n'.join(data_lines), delimiters=candidates)
            return dialect.delimiter
        except Exception:
            pass
        # fallback: count frequency in first data line
        counts = {c: data_lines[0].count(c) for c in candidates}
        best = max(counts, key=counts.get)
        if counts[best] > 0:
            return best
    return ','


def _clean_lines(filepath, sep):
    """Read file, strip BOM/encoding issues, remove border delimiters and
    pure separator rows. Returns list of cleaned strings."""
    import re as _re
    # A separator row: after removing the delimiter, only dash/equals/space/+ remain
    _SEP_PAT = _re.compile(r'^[\s\-=+:]+$')
    cleaned = []
    try:
        with _open_text_file(filepath) as fh:
            for raw in fh:
                line = raw.rstrip('\r\n')
                # Strip border delimiters (Markdown / SQL dump style)
                if line.startswith(sep):
                    line = line[len(sep):]
                if line.endswith(sep):
                    line = line[:-len(sep)]
                # Drop separator rows
                check = line.replace(sep, '').strip()
                if _SEP_PAT.match(check) or check == '':
                    continue
                cleaned.append(line)
    except Exception:
        pass
    return cleaned


def _read_text_file(filepath, nrows=None, dtype=None, keep_default_na=True):
    """Read ANY delimited flat file robustly.

    Handles: CSV · TSV · pipe-delimited · semicolon · Markdown tables ·
    SQL*Plus spool output · BOM · cp1252 / latin-1 / UTF-8 · quoted fields.
    """
    import io as _io
    import re as _re

    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.tsv':
        sep = '\t'
    else:
        sep = _detect_delimiter(filepath)

    cleaned = _clean_lines(filepath, sep)

    if not cleaned:
        return pd.DataFrame()

    if nrows is not None:
        # header + first nrows data lines
        cleaned = [cleaned[0]] + cleaned[1:nrows + 1]

    content = '\n'.join(cleaned)

    # ── Detect fixed-width (SQL*Plus / db2look style) ──────────────────────
    # If after cleaning the detected sep doesn't split the header into >1 col,
    # try pandas fixed-width reader using the separator row as column widths.
    _test_split = cleaned[0].split(sep)
    _is_fixed_width = len(_test_split) < 2

    if _is_fixed_width:
        # Re-read original file to find the dashes separator row for widths
        _all_lines = []
        with _open_text_file(filepath) as _fh:
            for _raw in _fh:
                _all_lines.append(_raw.rstrip('\r\n'))
        # Find first separator row (all dashes/spaces)
        _dash_pat = _re.compile(r'^[\s\-=]+$')
        _widths = None
        for _ln in _all_lines:
            if _dash_pat.match(_ln) and len(_ln.strip()) > 0:
                # widths from consecutive dash-groups
                import re as _re2
                _widths = [len(m.group()) + (1 if m.end() < len(_ln) else 0)
                           for m in _re2.finditer(r'\-+\s*', _ln)]
                break
        if _widths:
            # header line is the line BEFORE the separator row
            _sep_idx = next((i for i, l in enumerate(_all_lines)
                             if _dash_pat.match(l) and l.strip()), None)
            _header_idx = _sep_idx - 1 if _sep_idx and _sep_idx > 0 else 0
            _data_lines = [l for i, l in enumerate(_all_lines)
                           if i > (_sep_idx or 0) and l.strip()]
            if nrows:
                _data_lines = _data_lines[:nrows]
            _fw_content = '\n'.join([_all_lines[_header_idx]] + _data_lines)
            try:
                df = pd.read_fwf(_io.StringIO(_fw_content), widths=_widths)
                df.columns = [str(c).strip() for c in df.columns]
                obj_cols = df.select_dtypes(include='object').columns
                df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())
                return df.reset_index(drop=True)
            except Exception:
                pass  # fall through to normal csv read

    common_kw = dict(
        sep=sep,
        on_bad_lines='skip',
        skipinitialspace=True,
        low_memory=False,
    )
    if dtype is not None:
        common_kw['dtype'] = dtype
    if not keep_default_na:
        common_kw['keep_default_na'] = False

    df = pd.read_csv(_io.StringIO(content), **common_kw)

    # Trim column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop any unnamed/empty columns produced by trailing delimiters
    df = df.loc[:, ~df.columns.str.fullmatch(r'Unnamed:.*|')]

    # Trim whitespace from string cells
    obj_cols = df.select_dtypes(include='object').columns
    df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())

    return df.reset_index(drop=True)




@st.cache_data(ttl=60, show_spinner=False)

def read_file_preview_cached(filepath, nrows=5):

    """Cache file previews for 1 minute"""

    ext = os.path.splitext(filepath)[1].lower()

    if ext in ('.xlsx', '.xls'):

        return pd.read_excel(filepath, nrows=nrows)

    return _read_text_file(filepath, nrows=nrows)

@st.cache_data(ttl=300, show_spinner=False)

def read_file_preview_with_sheet(filepath, sheet_name, nrows=5):

    """Cache file previews with optional Excel sheet selection for 5 minutes"""

    ext = os.path.splitext(filepath)[1].lower()

    if ext in ('.xlsx', '.xls'):

        return pd.read_excel(filepath, sheet_name=sheet_name, nrows=nrows)

    return read_file_preview_cached(filepath, nrows=nrows)

@st.cache_data(ttl=60, show_spinner=False)

def get_file_list(directory, extensions):

    """Cache directory listing for 1 minute"""

    return [f for f in os.listdir(directory)

            if f.lower().endswith(extensions) and not f.startswith('failed_')]

@st.cache_data(ttl=300, show_spinner=False)

def count_file_rows(filepath):

    """Fast row count without loading whole file into memory.

    For CSV/TSV/TXT: counts newlines (instant even for millions of rows).

    For Excel: reads row count from workbook metadata."""

    ext = os.path.splitext(filepath)[1].lower()

    if ext in ('.xlsx', '.xls'):

        try:

            import openpyxl

            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)

            ws = wb.active

            count = ws.max_row - 1  # minus header

            wb.close()

            return max(count, 0)

        except Exception:

            return None

    # For text-based files count newlines in binary mode — very fast

    try:

        count = 0

        with open(filepath, 'rb') as f:

            for chunk in iter(lambda: f.read(1 << 20), b''):  # read 1MB at a time

                count += chunk.count(b'\n')

        return max(count - 1, 0)  # minus header row

    except Exception:

        return None

# -------------------------------------------------

# Snowflake Helper Functions

# -------------------------------------------------

def sanitize_column(col: str) -> str:

    """Sanitize column names for Snowflake"""

    col = str(col).strip()

    col = re.sub(r'\s+', '_', col)

    col = re.sub(r'[^\w]', '_', col)

    col = re.sub(r'_+', '_', col)

    col = col.strip('_')

    return col.upper()

def sanitize_table_name(name: str) -> str:

    """Sanitize a Snowflake table identifier without collapsing valid double underscores."""

    name = str(name).strip()

    name = re.sub(r'\s+', '_', name)

    name = re.sub(r'[^0-9A-Za-z_]', '_', name)

    name = name.strip('_')

    if name and name[0].isdigit():

        name = f'_{name}'

    return name.upper()

def sanitize_column_preserve_case(col: str) -> str:

    """Sanitize column names for Snowflake while preserving original case exactly as Salesforce"""

    col = str(col).strip()

    return col  # Keep column name exactly as-is from Salesforce

def make_unique_columns(columns: list) -> list:

    """Make column names unique by appending numbers"""

    seen = {}

    result = []

    for c in columns:

        if c not in seen:

            seen[c] = 1

            result.append(c)

        else:

            i = seen[c] + 1

            new_c = f"{c}_{i}"

            while new_c in seen:

                i += 1

                new_c = f"{c}_{i}"

            seen[c] = i

            seen[new_c] = 1

            result.append(new_c)

    return result

def read_excel_preserve_formatting(file_path: str, sheet_name: str = None):

    """Read Excel with preserved formatting"""

    try:

        import openpyxl

        wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)

        sheet_names = wb.sheetnames

        if not sheet_names:

            st.warning('❌ The selected Excel file has no worksheets to load.')

            return pd.DataFrame()

        ws = wb[sheet_name] if sheet_name else wb[sheet_names[0]]

        header = next(ws.iter_rows(min_row=1, max_row=1))

        headers_raw = [c.value if c.value else f"COL_{i+1}" for i, c in enumerate(header)]

        headers = make_unique_columns([sanitize_column(h) for h in headers_raw])

        rows = []

        for row in ws.iter_rows(min_row=2, values_only=True):

            rows.append([None if x in ("", None) else str(x) for x in row])

        return pd.DataFrame(rows, columns=headers)

    except ImportError:

        st.error("openpyxl not installed. Install with: pip install openpyxl")

        return None

def read_data_for_snowflake(file_path: str, sheet_name: str = None):

    """Read CSV, TSV, TXT, or Excel data for Snowflake loading"""

    ext = os.path.splitext(file_path)[1].lower()

    if ext in ('.csv', '.tsv', '.txt'):

        df = _read_text_file(file_path, dtype=str, keep_default_na=False)

        df.columns = make_unique_columns([sanitize_column(c) for c in df.columns])

        return df.map(lambda x: None if x == '' else str(x))

    else:

        return read_excel_preserve_formatting(file_path, sheet_name)

def apply_zero_padding(df, zero_pad_config: dict):

    """Apply zero padding to specified columns"""

    if not zero_pad_config:

        return df

    

    df = df.copy()

    for col, width in zero_pad_config.items():

        col = sanitize_column(col)

        if col in df.columns:

            df[col] = df[col].apply(lambda x: None if x is None else str(x).zfill(int(width)))

    return df

def normalize_salesforce_date_string(v):

    """Normalize various date formats to YYYY-MM-DD"""

    if v in (None, ""):

        return None

    if isinstance(v, (datetime.date, datetime.datetime, pd.Timestamp)):

        return pd.to_datetime(v).date().isoformat()

    s = str(v).strip()

    # Handle Excel serial dates

    if re.fullmatch(r"\d{3,6}", s):

        try:

            base = datetime.date(1899, 12, 30)

            return (base + datetime.timedelta(days=int(s))).isoformat()

        except Exception:

            pass

    # Try parsing

    for dfmt in (False, True):

        try:

            return pd.to_datetime(s, dayfirst=dfmt).date().isoformat()

        except Exception:

            pass

    return None

def normalize_dates(df, date_columns: list):

    """Normalize date columns"""

    if not date_columns:

        return df

    

    df = df.copy()

    for col in date_columns:

        col = sanitize_column(col)

        if col in df.columns:

            df[col] = df[col].apply(normalize_salesforce_date_string)

    return df

def infer_column_lengths(df):

    """Return column names — VARCHAR without length limit (Snowflake doesn't use storage based on declared length)"""

    return {col: None for col in df.columns}

def _snowflake_copy_columns(cursor, fq_table_name, source_columns):
    """Resolve CSV columns to quoted destination identifiers in source order."""
    cursor.execute(f'SELECT * FROM {fq_table_name} LIMIT 0')
    target_columns = [column[0] for column in cursor.description]
    target_set = set(target_columns)
    folded_targets = {}
    for target in target_columns:
        folded_targets.setdefault(target.upper(), []).append(target)

    resolved = []
    for source in source_columns:
        matches = [source] if source in target_set else folded_targets.get(source.upper(), [])
        if not matches:
            raise ValueError(
                f'Snowflake table {fq_table_name} has no column matching {source!r}. '
                'Check the export query and destination schema.'
            )
        if len(matches) != 1:
            raise ValueError(
                f'Ambiguous Snowflake column {source!r} in {fq_table_name}: {matches}. '
                'Use an exact destination column name.'
            )
        target = matches[0]
        if target in resolved:
            raise ValueError(
                f'Multiple source columns map to Snowflake column {target!r} in {fq_table_name}.'
            )
        resolved.append(target)

    return ', '.join('"' + target.replace('"', '""') + '"' for target in resolved)

# -------------------------------------------------

# Shared fast Snowflake loader — parallel compress + PUT + COPY INTO

# Used by both the ❄️ Snowflake tab and the SF→Snowflake tab.

# -------------------------------------------------

def _fast_snowflake_load(conn, df, fq_table_name, status_fn=None, progress_fn=None,

                          file_chunk_rows=5_000_000):

    """

    Load a DataFrame into Snowflake using parallel gzip compression + PUT + COPY INTO.

    Returns (rows_loaded, load_seconds).

    status_fn(msg) and progress_fn(0..1) are optional callbacks.

    """

    ensure_not_stopped()

    nrows = len(df)

    ncols = len(df.columns)

    total_chunks = max(1, (nrows + file_chunk_rows - 1) // file_chunk_rows)

    if status_fn:

        status_fn(f'⚙️ Compressing {nrows:,} rows into {total_chunks} file(s) in parallel...')

    # --- Step 1: Compress all chunks IN PARALLEL ---

    def _compress_chunk(args):

        ensure_not_stopped()
        idx, chunk_df = args

        tmp = tempfile.NamedTemporaryFile(

            suffix='.csv.gz', delete=False, prefix=f'sf_load_{idx}_'

        )

        tmp.close()

        # Normalize empty/whitespace-only strings to NaN so they write as unquoted
        # empty fields, which Snowflake EMPTY_FIELD_AS_NULL converts to NULL.
        # (QUOTE_ALL would produce """ quoted empty"" which EMPTY_FIELD_AS_NULL ignores.)
        chunk_df = chunk_df.copy()
        chunk_df = chunk_df.applymap(
            lambda x: float('nan')
            if (x is None or (isinstance(x, float) and pd.isna(x))
                or (isinstance(x, str) and x.strip() == ''))
            else x
        )
        # Stream directly into gzip — avoids building full CSV string+bytes in RAM

        with gzip.open(tmp.name, 'wt', compresslevel=1, encoding='utf-8', newline='') as gz:

            chunk_df.to_csv(gz, index=False, lineterminator='\n', na_rep='', quoting=csv.QUOTE_MINIMAL)

        return tmp.name

    chunks_args = [

        (i // file_chunk_rows, df.iloc[i:i + file_chunk_rows])

        for i in range(0, nrows, file_chunk_rows)

    ]

    temp_files = []

    with ThreadPoolExecutor(max_workers=min(total_chunks, 2)) as pool:

        futures = {pool.submit(_compress_chunk, arg): arg[0] for arg in chunks_args}

        done_compress = 0

        ordered = [None] * total_chunks

        for fut in as_completed(futures):

            ensure_not_stopped()

            idx = futures[fut]

            ordered[idx] = fut.result()

            done_compress += 1

            if progress_fn:

                progress_fn(0.1 + 0.2 * done_compress / total_chunks)

    temp_files = ordered

    if status_fn:

        status_fn(f'📤 Uploading {len(temp_files)} file(s) to Snowflake stage in parallel...')

    # --- Step 2: Create stage + PUT all files in parallel ---

    stage_name = f'_FAST_LOAD_STAGE_{int(time.time())}'

    ensure_not_stopped()
    cursor = conn.cursor()

    cursor.execute(f'CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}')

    def _put_file(args):

        ensure_not_stopped()
        idx, tmp_path = args

        escaped = tmp_path.replace('\\', '/')

        cur = conn.cursor()

        cur.execute(

            f"PUT 'file://{escaped}' @{stage_name} "

            f"AUTO_COMPRESS=FALSE PARALLEL=4 OVERWRITE=TRUE"

        )

        cur.close()

        return idx

    put_args = list(enumerate(temp_files))

    with ThreadPoolExecutor(max_workers=min(len(temp_files), 4)) as pool:

        put_futures = {pool.submit(_put_file, arg): arg[0] for arg in put_args}

        done_put = 0

        for fut in as_completed(put_futures):

            ensure_not_stopped()

            fut.result()

            done_put += 1

            if progress_fn:

                progress_fn(0.3 + 0.5 * done_put / len(temp_files))

    if status_fn:

        status_fn(f'❄️ COPY INTO {fq_table_name} (parallel Snowflake load)...')

    if progress_fn:

        progress_fn(0.82)

    # --- Step 3: Single COPY INTO (Snowflake loads all staged files in parallel) ---

    ensure_not_stopped()

    quoted_cols = ', '.join([f'"{c}"' for c in df.columns])

    t0 = time.time()

    copy_rows = cursor.execute(f"""

        COPY INTO {fq_table_name} ({quoted_cols})

        FROM @{stage_name}

        FILE_FORMAT = (

            TYPE = 'CSV'

            FIELD_OPTIONALLY_ENCLOSED_BY = '"'

            SKIP_HEADER = 1

            COMPRESSION = 'GZIP'

            ENCODING = 'UTF8'

            EMPTY_FIELD_AS_NULL = TRUE
            NULL_IF = ('', 'NULL', 'null')

        )

        PURGE = TRUE

    """).fetchall()

    copy_time = time.time() - t0

    total_loaded = sum(row[3] for row in copy_rows if len(row) >= 4) or nrows

    # Cleanup

    try:

        cursor.execute(f'DROP STAGE IF EXISTS {stage_name}')

    except Exception:

        pass

    cursor.close()

    for p in temp_files:

        ensure_not_stopped()

        try:

            os.unlink(p)

        except Exception:

            pass

    if progress_fn:

        progress_fn(1.0)

    return total_loaded, copy_time

# =============================================================================

# LIVE DASHBOARD — Tavant Loader-style progress panel for Streamlit

# Shows: API, Threads, Records Processed, Speed, ETA, Chunks, Events log

# =============================================================================

class LiveDashboard:

    """Real-time progress dashboard that mimics Tavant Loader's rich terminal UI.

    Usage:

        dashboard = LiveDashboard(st, total_records=1_000_000, operation='Insert', object_name='Asset')

        result = bulk_load_v2(...,

            on_progress=dashboard.on_progress,

            on_status=dashboard.on_status,

            on_error=dashboard.on_error,

        )

        dashboard.finalize(result)

    """

    def __init__(self, st_module, total_records=0, operation='Insert', object_name='', source_label='', record_op=None):

        self._st = st_module

        self.total_records = total_records

        self.operation = operation

        self.record_op = record_op  # overrides operation name in job history (e.g. 'Multi-Object')

        self.object_name = object_name

        self.source_label = source_label  # e.g. '❄️ STG_VMRS_TABLE' or '📄 accounts.csv'

        self.start_time = time.time()

        # Counters

        self.success = 0

        self.failed = 0

        self.chunks_done = 0

        self.chunks_total = 0

        self.current_api = 'bulk_v2'

        self.current_threads = 0

        self.thread_reductions = 0

        self.api_switches = 0

        self.reprocess_round = 0

        self.phase = 'init'  # 'init' | 'fetching' | 'loading' | 'done'

        self.events = []

        # Per-API live stats (populated from poll messages)
        self.api_stats = {
            'bulk_v2': {'job_id': '—', 'state': '—', 'processed': 0, 'last_ts': None},
            'bulk_v1': {'job_id': '—', 'state': '—', 'processed': 0, 'last_ts': None},
            'rest':    {'job_id': '—', 'state': '—', 'processed': 0, 'last_ts': None},
        }

        self._max_events = 50  # premium feed shows last 50 events

        # Streamlit containers — ordered top to bottom

        self._livebar_area = self._st.empty()

        self.progress_bar = None  # removed — progress shown in livebar CSS, was causing double bar

        self._metrics_area = self._st.empty()

        self._header_area = self._st.empty()

        self._events_area = self._st.empty()

        self._finalize_area = self._st.empty()  # dedicated container for finalize card

        # Initial render

        self._render()

    def _elapsed(self):

        return time.time() - self.start_time

    def _rate(self):

        e = self._elapsed()

        return self.success / e if e > 0 and self.success > 0 else 0

    def _eta_str(self):

        rate = self._rate()

        if rate <= 0 or self.total_records <= 0:

            return '—'

        remaining = max(self.total_records - self.success, 0)

        secs = int(remaining / rate)

        if secs < 60:

            return f'{secs}s'

        elif secs < 3600:

            return f'{secs // 60}m {secs % 60}s'

        return f'{secs // 3600}h {(secs % 3600) // 60}m'

    def _render(self):

        """Update all display areas — premium glassmorphic dashboard."""

        elapsed = self._elapsed()

        rate = self._rate()

        eta = self._eta_str()

        # -- 1. LIVE status bar (pulsing red dot + run summary) --

        src_part = f' <span style="color:rgba(255,255,255,0.55);">·</span> {self.source_label}' if self.source_label else ''

        obj_part = f' <code style="background:rgba(168,85,247,0.15);padding:2px 8px;border-radius:6px;color:#f0f4ff;">{self.object_name}</code>' if self.object_name else ''

        threads_part = f' <span class="sf-api-chip" style="margin-left:6px;">🧵 {self.current_threads} threads</span>' if self.current_threads else ''

        api_label = {'bulk_v2': 'Bulk API 2.0', 'bulk_v1': 'Bulk API 1.0', 'rest': 'REST'}.get(self.current_api, self.current_api)

        if self.phase == 'fetching':
            _livebar_html = (
                f'<div class="sf-livebar" style="background:linear-gradient(90deg,rgba(6,182,212,0.18),rgba(6,182,212,0.05));border-color:rgba(6,182,212,0.35);">'
                f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
                f'background:#06b6d4;margin-right:8px;animation:sf-pulse 0.8s infinite;"></span>'
                f'<span style="font-weight:700;color:#22d3ee;letter-spacing:0.05em;font-size:0.85rem;">FETCHING</span>'
                f'<span style="color:rgba(255,255,255,0.75);font-size:0.9rem;margin-left:8px;">'
                f'<b>{self.operation.upper()}</b>{obj_part}{src_part}'
                f'<span style="color:rgba(255,255,255,0.45);font-size:0.80rem;margin-left:10px;">⏳ Downloading from Snowflake…</span>'
                f'</span></div>'
            )
        elif self.phase == 'init':
            _livebar_html = (
                f'<div class="sf-livebar" style="border-color:rgba(255,255,255,0.12);">'
                f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
                f'background:#6b7280;margin-right:8px;"></span>'
                f'<span style="font-weight:700;color:rgba(255,255,255,0.55);font-size:0.85rem;">STARTING</span>'
                f'<span style="color:rgba(255,255,255,0.65);font-size:0.9rem;margin-left:8px;">'
                f'<b>{self.operation.upper()}</b>{obj_part}{src_part}'
                f'</span></div>'
            )
        else:
            _livebar_html = (
                f'<div class="sf-livebar">'
                f'<span class="sf-live-dot"></span>'
                f'<span style="font-weight:700;color:#fff;letter-spacing:0.05em;font-size:0.85rem;">LIVE</span>'
                f'<span style="color:rgba(255,255,255,0.85);font-size:0.9rem;">'
                f'<b>{self.operation.upper()}</b>{obj_part}{src_part}'
                f'<span style="color:rgba(255,255,255,0.55);"> · {api_label}</span>{threads_part}'
                f'</span></div>'
            )
        self._livebar_area.markdown(_livebar_html, unsafe_allow_html=True)

        # -- 2. KPI grid (5 cards) --

        success_card = kpi_card('Success', f'{self.success:,}', icon='✅', color='green')

        failed_color = 'red' if self.failed > 0 else 'slate'

        failed_card = kpi_card('Failed', f'{self.failed:,}', icon='❌', color=failed_color)

        chunk_label = f'{self.chunks_done}/{self.chunks_total}' if self.chunks_total else f'{self.chunks_done}'

        chunks_card = kpi_card('Chunks', chunk_label, icon='📦', color='cyan')

        speed_card = kpi_card('Speed', f'{rate:,.0f}', icon='⚡', color='indigo', sub='records / sec')

        eta_card = kpi_card('ETA', eta, icon='⏱️', color='amber', sub=f'elapsed {elapsed:.0f}s')

        auto_card = ''

        if self.thread_reductions or self.api_switches or self.reprocess_round:

            auto_label = f'T↓{self.thread_reductions} API↓{self.api_switches}'

            if self.reprocess_round:

                auto_label += f' R{self.reprocess_round}'

            auto_card = kpi_card('Auto-Tune', auto_label, icon='🛡️', color='pink')

        kpi_html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:6px 0 14px 0;">'
        kpi_html += success_card + failed_card + chunks_card + speed_card + eta_card + auto_card
        kpi_html += '</div>'
        # Collapse newlines — mistune falls out of HTML mode on blank/indented lines
        self._metrics_area.markdown(' '.join(kpi_html.split()), unsafe_allow_html=True)

        # -- 3. API status panels — full live cards per API --

        # State → (color, bg_accent, step_index, label_display)
        _STATE_META = {
            '—':              ('#4b5563', 'rgba(75,85,99,0.08)',    0, '—'),
            'Idle':           ('#4b5563', 'rgba(75,85,99,0.08)',    0, 'Idle'),
            'Queued':         ('#6366f1', 'rgba(99,102,241,0.12)',  1, 'Queued'),
            'Open':           ('#6366f1', 'rgba(99,102,241,0.12)',  1, 'Open'),
            'UploadComplete': ('#f59e0b', 'rgba(245,158,11,0.12)', 2, 'Upload Done'),
            'InProgress':     ('#f59e0b', 'rgba(245,158,11,0.12)', 3, 'In Progress'),
            'Active':         ('#f59e0b', 'rgba(245,158,11,0.12)', 3, 'Active'),
            'Processing':     ('#f59e0b', 'rgba(245,158,11,0.12)', 3, 'Processing'),
            'JobComplete':    ('#22c55e', 'rgba(34,197,94,0.12)',  4, 'Complete ✓'),
            'Completed':      ('#22c55e', 'rgba(34,197,94,0.12)',  4, 'Complete ✓'),
            'Done':           ('#22c55e', 'rgba(34,197,94,0.12)',  4, 'Done ✓'),
            'Failed':         ('#ef4444', 'rgba(239,68,68,0.12)',  4, 'Failed ✗'),
            'Aborted':        ('#ef4444', 'rgba(239,68,68,0.12)',  4, 'Aborted ✗'),
        }
        _STEPS = ['—', 'Queued', 'Uploading', 'Processing', 'Complete']

        def _api_panel(api_key, label, accent_color):
            s = self.api_stats[api_key]
            is_active = self.current_api == api_key
            used = s['job_id'] != '—' or s['state'] not in ('—', 'Idle')
            if not is_active and not used:
                return ''  # never activated — hide panel
            meta = _STATE_META.get(s['state'], _STATE_META['—'])
            st_color, st_bg, step_idx, st_label = meta
            # Border: animated gradient for active, dimmed for inactive, accent for done
            if is_active:
                border_style = f'border:1.5px solid {accent_color};box-shadow:0 0 12px {accent_color}44;'
            elif s['state'] in ('JobComplete','Completed','Done'):
                border_style = 'border:1px solid rgba(34,197,94,0.35);'
            elif s['state'] in ('Failed','Aborted'):
                border_style = 'border:1px solid rgba(239,68,68,0.35);'
            else:
                border_style = 'border:1px solid rgba(255,255,255,0.08);'
            # Pulsing dot indicator
            if is_active and s['state'] not in ('JobComplete','Completed','Done','Failed','Aborted'):
                dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{accent_color};margin-right:7px;animation:sf-pulse 1.2s infinite;flex-shrink:0;"></span>'
            elif s['state'] in ('JobComplete','Completed','Done'):
                dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#22c55e;margin-right:7px;flex-shrink:0;"></span>'
            elif s['state'] in ('Failed','Aborted'):
                dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#ef4444;margin-right:7px;flex-shrink:0;"></span>'
            else:
                dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#374151;margin-right:7px;flex-shrink:0;"></span>'
            # Step trail: 4 steps with colored fill up to current
            step_dots = ''
            for i, sname in enumerate(_STEPS[1:], 1):
                if i < step_idx:
                    sc = '#22c55e'
                elif i == step_idx:
                    sc = st_color
                else:
                    sc = '#1f2937'
                step_dots += f'<span style="display:inline-block;width:18px;height:4px;border-radius:2px;background:{sc};margin-right:3px;transition:background 0.4s;"></span>'
            # Data rows
            last = s['last_ts'] or '—'
            proc_col = '#4ade80' if s['processed'] > 0 else '#6b7280'
            rows_html = (
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);font-size:0.73rem;">'
                f'<span style="color:rgba(255,255,255,0.45);">State</span>'
                f'<span style="color:{st_color};font-weight:700;background:{st_bg};padding:1px 8px;border-radius:999px;">{st_label}</span></div>'
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);font-size:0.73rem;">'
                f'<span style="color:rgba(255,255,255,0.45);">Job ID</span>'
                f'<code style="color:#c084fc;font-size:0.70rem;">{s["job_id"]}</code></div>'
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);font-size:0.73rem;">'
                f'<span style="color:rgba(255,255,255,0.45);">Processed</span>'
                f'<b style="color:{proc_col};">{s["processed"]:,}</b></div>'
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;font-size:0.73rem;">'
                f'<span style="color:rgba(255,255,255,0.45);">Last update</span>'
                f'<span style="color:rgba(255,255,255,0.7);">{last}</span></div>'
            )
            return (
                f'<div style="{border_style}border-radius:12px;padding:13px 15px 10px 15px;background:rgba(13,17,23,0.6);backdrop-filter:blur(8px);transition:all 0.3s;">'
                f'<div style="display:flex;align-items:center;margin-bottom:9px;">'
                f'{dot}<span style="color:{accent_color};font-size:0.78rem;font-weight:700;letter-spacing:0.05em;">{label}</span>'
                f'<span style="color:rgba(255,255,255,0.25);font-size:0.66rem;margin-left:6px;">({api_key})</span></div>'
                f'<div style="display:flex;align-items:center;margin-bottom:9px;">{step_dots}</div>'
                f'{rows_html}</div>'
            )

        panels = [
            _api_panel('bulk_v2', 'Bulk API 2.0', '#a855f7'),
            _api_panel('bulk_v1', 'Bulk API 1.0', '#6366f1'),
            _api_panel('rest',    'REST API',     '#06b6d4'),
        ]
        visible_panels = [p for p in panels if p]
        if visible_panels:
            api_row = ('<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;margin:4px 0 14px 0;">'
                       + ''.join(visible_panels) + '</div>')
        else:
            api_row = ''

        # -- 4. Event feed (last 50) --

        if self.events:

            recent = self.events[-self._max_events:]

            rows_html = ''

            for ev in reversed(recent):  # newest at top

                ev_str = str(ev)

                if ev_str.startswith('❌'):

                    kind = 'error'

                elif '⚠️' in ev_str or 'warn' in ev_str.lower() or 'switch' in ev_str.lower():

                    kind = 'warn'

                elif '✅' in ev_str or 'success' in ev_str.lower() or 'complete' in ev_str.lower():

                    kind = 'success'

                elif '[bulk_v' in ev_str or '[rest]' in ev_str or 'API' in ev_str:

                    kind = 'api'

                else:

                    kind = 'info'

                ts = time.strftime('%H:%M:%S')

                # escape ampersands/angle brackets in event text

                safe = ev_str.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

                rows_html += event_log_row(ts, kind, safe)

            self._events_area.markdown(
                api_row +
                f'<div style="font-size:0.72rem;font-weight:600;color:rgba(255,255,255,0.55);'
                f'text-transform:uppercase;letter-spacing:0.06em;margin:8px 0 6px 0;">📡 Live Event Feed</div>'
                f'<div class="sf-event-feed">{rows_html}</div>',
                unsafe_allow_html=True
            )

        else:

            self._events_area.markdown(api_row, unsafe_allow_html=True)

        # legacy header area kept hidden (no-op) to preserve container ordering

        self._header_area.empty()

    # --- Callbacks for bulk_load_v2 / bulk_delete_v2 ---

    def on_progress(self, pct):

        if self.progress_bar is not None:
            self.progress_bar.progress(min(pct, 1.0))

    def on_status(self, msg, level='info'):

        # Phase detection from status messages
        if '❄️ Fetching data from Snowflake' in msg or 'Fetching data from Snowflake' in msg:
            self.phase = 'fetching'
        elif '❄️ Snowflake:' in msg and 'rows fetched' in msg:
            self.phase = 'loading'
        elif 'AUTO Mode' in msg or 'submitting to SF' in msg or 'Delete mode' in msg or '[bulk_v1]' in msg or '[rest]' in msg:
            self.phase = 'loading'

        # Parse numbers from status messages

        m = re.search(r'(\d[\d,]*)\s+success', msg)

        if m:

            self.success = int(m.group(1).replace(',', ''))

        m = re.search(r'(\d[\d,]*)\s+failed', msg)

        if m:

            self.failed = int(m.group(1).replace(',', ''))

        # Chunk progress

        m = re.search(r'Chunk\s+(\d+)', msg)

        if m:

            self.chunks_done = max(self.chunks_done, int(m.group(1)))

        m = re.search(r'Batch\s+(\d+)/(\d+)', msg, re.I)

        if m:

            self.chunks_done = max(self.chunks_done, int(m.group(1)))

            self.chunks_total = max(self.chunks_total, int(m.group(2)))

        m = re.search(r'with\s+(\d[\d,]*)\s+batch\(es\)', msg, re.I)

        if m:

            self.chunks_total = max(self.chunks_total, int(m.group(1).replace(',', '')))

        # AUTO mode init

        if 'AUTO Mode' in msg or 'AUTO Delete' in msg:

            m = re.search(r'start_threads=(\d+)', msg)

            if m:

                self.current_threads = int(m.group(1))

            m = re.search(r'chain=(.+)', msg)

        # API switch

        if 'Now using:' in msg or 'now using' in msg:

            m = re.search(r'using[:\s]+(\w+)', msg)

            if m:

                self.current_api = m.group(1)

                self.api_switches += 1

        # Thread reduction

        if 'Thread reduction' in msg or 'threads now' in msg or 'Threads ?' in msg:

            self.thread_reductions += 1

            m = re.search(r'(?:now|?)\s*(\d+)', msg)

            if m:

                self.current_threads = int(m.group(1))

        # API tag in message

        m = re.search(r'\[(bulk_v[12]|rest)\]', msg, re.I)

        msg_api = m.group(1).lower() if m else self.current_api

        if m:

            self.current_api = msg_api

        # Parse Bulk v2 job poll messages, with or without an API prefix:
        # "[bulk_v2] Polling abc12345... state=InProgress, processed=500 (15s)"
        m_poll = re.search(
            r'Polling\s+([A-Za-z0-9]{8,})\.\.\.\s*state=([A-Za-z_]+),\s*processed=([\d,]+)',
            msg,
            re.I,
        )
        if m_poll:
            jid = m_poll.group(1)
            st_val = m_poll.group(2)
            proc = int(m_poll.group(3).replace(',', ''))
            slot = self.api_stats.get(msg_api)
            if slot is not None:
                slot['job_id'] = jid[:8]
                slot['state'] = st_val
                slot['processed'] = proc
                slot['last_ts'] = time.strftime('%H:%M:%S')

        # Parse Bulk v1 job/batch state:
        # "[bulk_v1] Job 750... batch 751... state=Completed"
        m_b1 = re.search(r'\[bulk_v1\].*?(?:Job\s+)?([A-Za-z0-9]{8,}).*?state=([A-Za-z_]+)', msg, re.I)
        if m_b1:
            self.current_api = 'bulk_v1'
            self.api_stats['bulk_v1']['job_id'] = m_b1.group(1)[:8]
            self.api_stats['bulk_v1']['state'] = m_b1.group(2)
            self.api_stats['bulk_v1']['last_ts'] = time.strftime('%H:%M:%S')

        # Parse REST chunk submissions/completions. REST has no SF job id, so show REST.
        m_rest = re.search(r'\[rest\].*?(Submitting|Completed).*?([\d,]+)\s+records?', msg, re.I)
        if m_rest:
            proc = int(m_rest.group(2).replace(',', ''))
            self.current_api = 'rest'
            self.api_stats['rest']['job_id'] = 'REST'
            self.api_stats['rest']['processed'] += proc if m_rest.group(1).lower() == 'completed' else 0
            self.api_stats['rest']['state'] = m_rest.group(1).capitalize()
            self.api_stats['rest']['last_ts'] = time.strftime('%H:%M:%S')

        # Reprocess round

        m = re.search(r'reprocess round (\d+)', msg, re.I)

        if m:

            self.reprocess_round = int(m.group(1))

        # Add to event log (skip debug noise)

        if not msg.startswith('[DEBUG]') and len(msg) < 200:

            self.events.append(msg[:120])

        self._render()

    def on_error(self, msg):

        self.events.append(f'❌ {msg[:100]}')

        self._render()

    def finalize(self, result):

        """Show final summary after operation completes."""

        if not result:

            return

        self.success = result.get('total_success', self.success)

        self.failed = result.get('total_failed', self.failed)

        elapsed = result.get('elapsed', self._elapsed())

        rate = self.success / max(elapsed, 0.001)

        if self.progress_bar is not None:
            self.progress_bar.progress(1.0)

        # --- Special case: source had 0 rows, nothing was loaded ---
        if result.get('skipped'):
            self._livebar_area.empty()
            self._header_area.empty()
            self._metrics_area.empty()
            self._events_area.empty()
            _skip_reason = result.get('skip_reason', 'Source returned 0 rows')
            _src_str = f' &nbsp;·&nbsp; {self.source_label}' if self.source_label else ''
            _skip_html = (
                f'<div style="background:linear-gradient(135deg,rgba(245,158,11,0.08),rgba(100,116,139,0.06));'
                f'border:1px solid rgba(245,158,11,0.35);border-radius:14px;padding:18px 22px;'
                f'display:flex;align-items:center;gap:14px;">'
                f'<div style="font-size:2rem;">\U0001F4ED</div>'
                f'<div>'
                f'<div style="font-size:1.05rem;font-weight:700;color:#fbbf24;">Empty Source — Skipped</div>'
                f'<div style="font-size:0.80rem;color:rgba(255,255,255,0.55);margin-top:3px;">'
                f'<code style="color:#c084fc;">{self.object_name}</code>{_src_str}</div>'
                f'<div style="font-size:0.75rem;color:rgba(245,158,11,0.75);margin-top:5px;">{_skip_reason}</div>'
                f'</div></div>'
            )
            self._finalize_area.markdown(_skip_html, unsafe_allow_html=True)
            return

        auto_stats = result.get('auto_mode_stats', {})

        if auto_stats:

            self.thread_reductions = auto_stats.get('thread_reductions', 0)

            self.api_switches = auto_stats.get('api_switches', 0)

            self.current_api = auto_stats.get('final_api', self.current_api)

        total = result.get('total_completed', result.get('total_processed', self.success + self.failed))

        pct_ok = (self.success / max(total, 1)) * 100

        # Bump session-level total rows counter for the hero header
        try:
            import streamlit as _st_root
            _st_root.session_state._session_total_rows = _st_root.session_state.get('_session_total_rows', 0) + int(self.success)
        except Exception:
            pass

        # Persist this run to the on-disk job-history log (used by sidebar)
        try:
            record_job_run(
                operation=self.record_op or self.operation,
                object_name=self.object_name,
                success=self.success,
                failed=self.failed,
                elapsed=elapsed,
                api=self.current_api,
                source=self.source_label,
                sub_op=self.operation if self.record_op else '',
            )
        except Exception:
            pass

        # Final success banner — render into dedicated empty() container so
        # unsafe_allow_html works reliably (direct st.markdown() can choke on
        # deeply-nested HTML and render the grid section as raw text).
        self._livebar_area.empty()
        self._header_area.empty()
        self._metrics_area.empty()
        self._events_area.empty()

        _src_str = f' &nbsp;·&nbsp; 📍 {self.source_label}' if self.source_label else ''
        _status_color = '#22c55e' if pct_ok >= 99.5 else ('#f59e0b' if pct_ok >= 90 else '#ef4444')
        _status_label = 'PERFECT' if pct_ok >= 99.5 else ('PARTIAL' if pct_ok >= 90 else 'FAILED')
        _emoji = '🎉' if pct_ok >= 99.5 else ('⚠️' if pct_ok >= 90 else '❌')

        # --- Header card (kept simple — no nested grid) ---
        _header_html = (
            f'<div style="background:linear-gradient(135deg,rgba(34,197,94,0.10),rgba(168,85,247,0.08));'
            f'border:1px solid {_status_color}55;border-radius:18px;padding:22px 26px;'
            f'margin:8px 0 0 0;position:relative;overflow:hidden;box-shadow:0 8px 32px {_status_color}22;">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;'
            f'background:linear-gradient(90deg,{_status_color},#a855f7,{_status_color});'
            f'background-size:200% 100%;animation:sf-shimmer 3s linear infinite;"></div>'
            f'<div style="display:flex;align-items:center;gap:14px;">'
            f'<div style="font-size:2.6rem;line-height:1;filter:drop-shadow(0 0 18px {_status_color}88);">{_emoji}</div>'
            f'<div style="flex:1;">'
            f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
            f'<span style="font-size:1.35rem;font-weight:800;color:#fff;letter-spacing:-0.015em;">{self.operation} Complete</span>'
            f'<span style="padding:3px 10px;border-radius:999px;background:{_status_color}22;'
            f'border:1px solid {_status_color}55;color:{_status_color};'
            f'font-size:0.72rem;font-weight:700;letter-spacing:0.06em;">● {_status_label}</span>'
            f'<span style="color:rgba(255,255,255,0.55);font-size:0.9rem;">→</span>'
            f'<code style="background:rgba(168,85,247,0.18);padding:3px 10px;border-radius:8px;'
            f'color:#f0f4ff;font-size:0.88rem;">{self.object_name}</code>'
            f'</div>'
            f'<div style="font-size:0.82rem;color:rgba(255,255,255,0.50);margin-top:5px;">{_src_str}</div>'
            f'</div></div></div>'
        )
        self._finalize_area.markdown(_header_html, unsafe_allow_html=True)

        # --- KPI grid — use the existing kpi_card() helper (guaranteed to render) ---
        _kpi_cards = [
            {'label': 'Success',    'value': f'{self.success:,}',  'icon': '✅', 'color': 'green',
             'sub': f'{pct_ok:.1f}%'},
            {'label': 'Failed',     'value': f'{self.failed:,}',   'icon': '❌',
             'color': 'red' if self.failed else 'slate'},
            {'label': 'Duration',   'value': f'{elapsed:.1f}s',    'icon': '⏱️', 'color': 'indigo'},
            {'label': 'Throughput', 'value': f'{rate:,.0f}',       'icon': '⚡', 'color': 'cyan',
             'sub': 'records / sec'},
            {'label': 'Final API',  'value': self.current_api,     'icon': '🔗', 'color': 'pink'},
        ]
        _kpi_html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin:10px 0 14px 0;">'
        for _c in _kpi_cards:
            _kpi_html += kpi_card(**_c)
        _kpi_html += '</div>'
        # Collapse newlines/indentation — mistune falls out of HTML mode on blank/indented lines
        self._st.markdown(' '.join(_kpi_html.split()), unsafe_allow_html=True)

        # Performance projection

        if total > 0 and elapsed > 0:

            rate_per_min = (self.success / elapsed) * 60

            est_3cr = 30_000_000 / max(rate_per_min, 1)

            self._st.info(

                f'⚡️ **Performance**: {rate:,.0f} records/sec  |  {rate_per_min:,.0f} records/min  \n'

                f'💡 At this speed, **3 crore (30M) records** would take ~**{est_3cr:.0f} minutes** '

                f'(Tavant Loader benchmark: 12 min)'

            )

# -------------------------------------------------

# Timing Report Helper

# -------------------------------------------------

def _show_timing_report(result, operation_name='Operation', extra_steps=None):

    """Display a per-step timing breakdown after a bulk Salesforce operation.

    Parameters

    ----------

    result : dict  — return value from bulk_load_v2 / bulk_delete_v2

    operation_name : str  — e.g. 'Insert', 'Upsert', 'Delete'

    extra_steps : list of (label, seconds) — additional steps measured outside the bulk function

                  e.g. [('Data Source Read', 1.2), ('Snowflake Fetch', 4.5)]

    """

    ts = result.get('timing_summary') or {}

    chunk_timings = result.get('chunk_timings') or []

    total_rows = result.get('total_processed', 0)

    elapsed = result.get('elapsed', ts.get('total_elapsed_s', 0))

    num_chunks = ts.get('num_chunks', len(chunk_timings))

    # -- Friendly labels & explanations for non-technical users ----------

    # Maps internal step names to (UI label, plain-English description)

    FRIENDLY_LABELS = {

        'Snowflake Query Execution': (

            '1❄️ Asking Snowflake for the data',

            'Sent your query to Snowflake and waited for it to be ready to send results.'

        ),

        'Snowflake → DataFrame Fetch': (

            '2⬇️ Downloading data from Snowflake',

            'Pulled the actual rows from Snowflake into the app’s memory.'

        ),

    }

    st.markdown('---')

    st.markdown(f'### ⏱️ Time Breakdown — {operation_name}')

    st.caption(

        'This shows where the time went so you can see what was fast and what was slow. '

        'Hover any row in the table for the full description.'

    )

    # -- Summary metrics row ----------------------------------------------

    m_cols = st.columns(4)

    m_cols[0].metric('⏳ Total Time', f"{elapsed:.1f} sec")

    m_cols[1].metric('📊 Rows Processed', f"{total_rows:,}")

    m_cols[2].metric('⚡️ Speed', f"{int(total_rows / max(elapsed, 0.001)):,} rows/sec")

    m_cols[3].metric('📦 Batches Sent', f"{num_chunks} (in parallel)")

    # -- Step breakdown table --------------------------------------------

    steps = []

    # Pre-bulk extra steps (e.g. Snowflake query, Snowflake fetch)

    extra_total = 0.0

    if extra_steps:

        for label, secs in extra_steps:

            friendly = FRIENDLY_LABELS.get(label, (label, '—'))

            pct = (secs / max(elapsed + sum(s for _, s in extra_steps), 0.001)) * 100

            steps.append({

                'Step': friendly[0],

                'Time': f'{secs:.2f} sec',

                'Share': f'{pct:.1f}%',

                'What happened': friendly[1],

            })

            extra_total += secs

    wall_time = elapsed + extra_total

    # Determine starting step number for SF steps (continues after extras)

    sf_step_start = len(steps) + 1

    if ts:

        csv_s = ts.get('total_csv_prep_s', 0)

        up_s  = ts.get('total_upload_s', 0)

        sf_s  = ts.get('total_sf_process_s', 0)

        steps += [

            {

                'Step': f'{sf_step_start}⚙️ Preparing data for Salesforce',

                'Time': f'{csv_s:.2f} sec',

                'Share': f'{(csv_s / max(wall_time, 0.001)) * 100:.1f}%',

                'What happened': (

                    'Converted your data into the file format Salesforce understands (CSV). '

                    'Faster on smaller datasets.'

                ),

            },

            {

                'Step': f'{sf_step_start + 1}📤 Sending data to Salesforce',

                'Time': f'{up_s:.2f} sec',

                'Share': f'{(up_s / max(wall_time, 0.001)) * 100:.1f}%',

                'What happened': (

                    'Uploaded the data file to Salesforce over the internet. '

                    'Mostly limited by your network speed.'

                ),

            },

            {

                'Step': f'{sf_step_start + 2}⚡️ Salesforce processing your records',

                'Time': f'{sf_s:.2f} sec',

                'Share': f'{(sf_s / max(wall_time, 0.001)) * 100:.1f}%',

                'What happened': (

                    'Salesforce was validating each record, running your business rules / '

                    'workflows / triggers, and saving rows to the database. This is usually '

                    'the longest step — and there is no way to speed it up from this app.'

                ),

            },

        ]

        notes_parallel = (

            f'{num_chunks} batch(es) ran at the same time, so the sum of step times above '

            f'is bigger than the actual wait. The number you actually waited is below.'

        ) if num_chunks > 1 else 'You waited this long in total.'

        steps.append({

            'Step': '⏳ TOTAL — actual time you waited',

            'Time': f'{elapsed:.2f} sec',

            'Share': '100%',

            'What happened': notes_parallel,

        })

    else:

        steps.append({

            'Step': '⏳ TOTAL — actual time you waited',

            'Time': f'{elapsed:.2f} sec',

            'Share': '100%',

            'What happened': 'You waited this long in total.',

        })

    st.dataframe(

        pd.DataFrame(steps).set_index('Step'),

        width='stretch',

    )

    # -- Plain-English interpretation of WHERE the time went -------------

    if ts:

        biggest = max(

            [

                ('Salesforce processing', ts.get('total_sf_process_s', 0)),

                ('Network upload to Salesforce', ts.get('total_upload_s', 0)),

                ('Preparing data', ts.get('total_csv_prep_s', 0)),

            ] + [(lbl, sec) for lbl, sec in (extra_steps or [])],

            key=lambda x: x[1],

        )

        if biggest[1] > 0:

            tip_map = {

                'Salesforce processing': (

                    '⚡️ **Most of the time was spent inside Salesforce.** This usually means '

                    'triggers, workflows, validation rules, or sharing rules ran on every record. '

                    'To speed this up, ask your Salesforce admin to disable non-essential automation '

                    'during bulk loads.'

                ),

                'Network upload to Salesforce': (

                    '📤 **Most of the time was spent uploading.** This is your internet speed to '

                    'Salesforce. Larger batch sizes reduce overhead per upload.'

                ),

                'Preparing data': (

                    '⚙️ **Most of the time was spent preparing the data file.** This usually means '

                    'a very large or very wide dataset.'

                ),

                'Snowflake → DataFrame Fetch': (

                    '⬇️ **Most of the time was spent downloading from Snowflake.** Consider using a '

                    'larger Snowflake warehouse or filtering the query to fewer rows/columns.'

                ),

                'Snowflake Query Execution': (

                    '❄️ **Most of the time was spent running the Snowflake query.** A complex query '

                    'or cold warehouse can cause this.'

                ),

            }

            tip = tip_map.get(biggest[0])

            if tip:

                st.info(tip)

    # -- Per-chunk breakdown (collapsed by default) ----------------------

    if chunk_timings:

        with st.expander(f'📊 Per-Batch Details ({num_chunks} batch(es)) — click to expand', expanded=False):

            st.caption(

                'Each batch is a group of rows sent to Salesforce together. '

                'Multiple batches run at the same time so the actual wait is much '

                'shorter than the sum of per-batch times.'

            )

            chunk_rows = []

            for t in sorted(chunk_timings, key=lambda x: x.get('chunk', 0)):

                chunk_rows.append({

                    'Batch #': t.get('chunk', '?'),

                    'Rows in Batch': f"{t.get('rows', 0):,}",

                    'Prepare (sec)': f"{t.get('csv_prep_s', 0):.2f}",

                    'Upload (sec)': f"{t.get('upload_s', 0):.2f}",

                    'Salesforce Processing (sec)': f"{t.get('sf_process_s', 0):.2f}",

                    'Batch Total (sec)': f"{t.get('total_s', 0):.2f}",

                    'Speed (rows/sec)': f"{int(t.get('rows', 0) / max(t.get('total_s', 0.001), 0.001)):,}",

                    'Error': t.get('error', '') or '—',

                })

            st.dataframe(

                pd.DataFrame(chunk_rows).set_index('Batch #'),

                width='stretch',

            )

def load_private_key(key_path: str):

    """Load private key for Snowflake — imports cryptography lazily on first call."""

    try:

        from cryptography.hazmat.primitives import serialization

        from cryptography.hazmat.backends import default_backend

        with open(key_path, 'r', encoding='utf-8') as key_file:

            private_key_str = key_file.read()

        private_key_obj = serialization.load_pem_private_key(

            private_key_str.encode(),

            password=None,

            backend=default_backend()

        )

        return private_key_obj.private_bytes(

            encoding=serialization.Encoding.DER,

            format=serialization.PrivateFormat.PKCS8,

            encryption_algorithm=serialization.NoEncryption()

        )

    except Exception as e:

        st.error(f"Error loading private key: {e}")

        return None

# =================================================

# STREAMLIT UI

# =================================================

st.set_page_config(page_title='Tavant Migration App', page_icon='⚡️', layout='wide', initial_sidebar_state='expanded')
render_startup_splash()

def loader_restart_required():
    required_parameters = (
        (bulk_load_v2, {'manage_stop_flag', 'upload_capacity'}),
        (bulk_delete_v2, {'manage_stop_flag'}),
    )
    return any(
        not required.issubset(inspect.signature(loader).parameters)
        for loader, required in required_parameters
    )


if loader_restart_required():
    st.error(
        'Server restart required: this process has an older Salesforce loader in memory. '
        'Wait for existing jobs to finish, then restart this Streamlit server. '
        'Refreshing the browser does not reload the backend. '
        'New operations are blocked to preserve cancellation safety.'
    )
    st.button('Stop existing work', key='stale_backend_stop', on_click=set_stop_flag)
    st.stop()

# ---------------------------------------------------------------
# 🎨 Premium UI Theme — modern gradient + glassmorphic look
# ---------------------------------------------------------------
st.markdown("""
<style>
/* Import Inter font for crisp modern type */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* === Global === */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}
.stApp {
    background:
        radial-gradient(circle at 0% 0%, rgba(99,102,241,0.12) 0%, transparent 40%),
        radial-gradient(circle at 100% 0%, rgba(236,72,153,0.10) 0%, transparent 40%),
        radial-gradient(circle at 50% 100%, rgba(34,211,238,0.08) 0%, transparent 50%),
        linear-gradient(180deg, #0a0e1a 0%, #0f1320 100%);
    background-attachment: fixed;
}

/* === App title === */
h1:first-of-type {
    background: linear-gradient(135deg, #818cf8 0%, #c084fc 50%, #f472b6 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: 800 !important;
    font-size: 2.6rem !important;
    letter-spacing: -0.02em;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    margin-bottom: 1.2rem !important;
}

/* === Tabs — pill style === */
.stTabs [data-baseweb="tab-list"] {
    gap: 6px;
    background: rgba(255,255,255,0.03);
    backdrop-filter: blur(10px);
    padding: 6px;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.06);
    margin-top: 0.35rem;
}
.stTabs [data-baseweb="tab"] {
    height: 42px;
    padding: 0 18px;
    background: transparent;
    border-radius: 10px;
    font-weight: 500;
    font-size: 0.92rem;
    color: rgba(255,255,255,0.65);
    transition: all 0.2s ease;
    border: 1px solid transparent;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(255,255,255,0.05);
    color: rgba(255,255,255,0.9);
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(99,102,241,0.25), rgba(168,85,247,0.20)) !important;
    color: #ffffff !important;
    border: 1px solid rgba(168,85,247,0.4) !important;
    box-shadow: 0 4px 14px rgba(99,102,241,0.25);
}

/* === Buttons === */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    transition: transform 0.18s ease, background 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease, filter 0.2s ease !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    background: rgba(255,255,255,0.04) !important;
    color: rgba(255,255,255,0.9) !important;
}
.stButton > button:hover {
    transform: translateY(-1px);
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(168,85,247,0.5) !important;
    box-shadow: 0 6px 18px rgba(99,102,241,0.18);
}
.stButton > button:active {
    transform: scale(0.98) translateY(0) !important;
    transition-duration: 0.08s !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #6366f1 0%, #a855f7 50%, #ec4899 100%) !important;
    color: #ffffff !important;
    border: none !important;
    box-shadow: 0 4px 14px rgba(99,102,241,0.35) !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 8px 22px rgba(168,85,247,0.45) !important;
    filter: brightness(1.08);
}
/* === Secondary button: outline with gradient border === */
.stButton > button[kind="secondary"] {
    background: rgba(255,255,255,0.025) !important;
    color: #f0f4ff !important;
    border: 1px solid rgba(168,85,247,0.40) !important;
}
.stButton > button[kind="secondary"]:hover {
    background: linear-gradient(135deg, rgba(99,102,241,0.10), rgba(168,85,247,0.08)) !important;
    border-color: rgba(168,85,247,0.65) !important;
    box-shadow: 0 4px 14px rgba(168,85,247,0.20);
}
/* === Danger button (marked via JS retagger or data-danger attr) === */
.stButton > button[data-danger="true"] {
    background: linear-gradient(135deg, #dc2626 0%, #ef4444 50%, #f43f5e 100%) !important;
    color: #ffffff !important;
    border: none !important;
    box-shadow: 0 4px 14px rgba(239,68,68,0.40) !important;
}
.stButton > button[data-danger="true"]:hover {
    box-shadow: 0 8px 22px rgba(239,68,68,0.55) !important;
    filter: brightness(1.08);
}
.stButton > button[data-danger="true"]:active {
    transform: scale(0.98) !important;
}

/* === Inputs === */
.stTextInput input, .stTextArea textarea, .stNumberInput input, .stSelectbox > div > div {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 10px !important;
    color: #ffffff !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
}
.stTextInput input:focus, .stTextArea textarea:focus, .stNumberInput input:focus {
    border-color: rgba(168,85,247,0.6) !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.15) !important;
}
/* Hide the textarea resize handle (was showing as a stray dot/dash) */
.stTextArea textarea {
    resize: none !important;
}
.stTextArea textarea::-webkit-resizer { display: none !important; }

/* Hide tiny utility iframes used for tab-persistence JS injection
   (they show up as a 1px dash in the bottom-right of the page) */
iframe[height="1"],
iframe[height="0"],
[data-testid="stIFrame"]:has(iframe[height="1"]),
[data-testid="stIFrame"]:has(iframe[height="0"]) {
    display: none !important;
    width: 0 !important;
    height: 0 !important;
}
[data-testid="stCustomComponentV1"]:has(iframe[height="1"]),
[data-testid="stCustomComponentV1"]:has(iframe[height="0"]) {
    display: none !important;
}

/* === Sidebar === */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(15,19,32,0.95), rgba(10,14,26,0.95)) !important;
    border-right: 1px solid rgba(255,255,255,0.06);
    backdrop-filter: blur(20px);
}
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #e0e7ff;
}

/* === Status messages — glassmorphic === */
.stAlert, div[data-baseweb="notification"] {
    border-radius: 12px !important;
    backdrop-filter: blur(8px);
    border: 1px solid rgba(255,255,255,0.08) !important;
}
div[data-testid="stNotification"] {
    border-radius: 12px !important;
}
/* Success */
.stAlert[data-baseweb="notification"][kind="success"],
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertSuccessIcon"]) {
    background: linear-gradient(135deg, rgba(34,197,94,0.15), rgba(16,185,129,0.10)) !important;
    border: 1px solid rgba(34,197,94,0.35) !important;
}
/* Info */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertInfoIcon"]) {
    background: linear-gradient(135deg, rgba(59,130,246,0.12), rgba(99,102,241,0.10)) !important;
    border: 1px solid rgba(99,102,241,0.30) !important;
}
/* Warning */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertWarningIcon"]) {
    background: linear-gradient(135deg, rgba(245,158,11,0.13), rgba(234,179,8,0.10)) !important;
    border: 1px solid rgba(245,158,11,0.32) !important;
}
/* Error */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertErrorIcon"]) {
    background: linear-gradient(135deg, rgba(239,68,68,0.15), rgba(244,63,94,0.10)) !important;
    border: 1px solid rgba(239,68,68,0.35) !important;
}

/* === Metrics — premium card === */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 16px 20px;
    backdrop-filter: blur(10px);
    transition: transform 0.2s ease, border-color 0.2s ease;
}
[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    border-color: rgba(168,85,247,0.35);
}
[data-testid="stMetricValue"] {
    background: linear-gradient(135deg, #818cf8, #c084fc);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 700 !important;
}

/* === Expanders — premium SaaS-style summary rows === */
.streamlit-expanderHeader, [data-testid="stExpander"] summary,
[data-testid="stExpander"] details > summary {
    background: rgba(255,255,255,0.03) !important;
    border-radius: 12px !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    font-weight: 500 !important;
    transition: background 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease !important;
    padding: 12px 16px !important;
    list-style: none !important;
    position: relative;
}
.streamlit-expanderHeader:hover, [data-testid="stExpander"] summary:hover,
[data-testid="stExpander"] details > summary:hover {
    background: rgba(168,85,247,0.08) !important;
    border-color: rgba(168,85,247,0.30) !important;
    box-shadow: 0 4px 14px rgba(168,85,247,0.10);
}
/* Open state: stronger border + gradient hint */
[data-testid="stExpander"] details[open] > summary {
    background: linear-gradient(135deg, rgba(99,102,241,0.10), rgba(168,85,247,0.08)) !important;
    border-color: rgba(168,85,247,0.45) !important;
    border-bottom-left-radius: 0 !important;
    border-bottom-right-radius: 0 !important;
}
/* Chevron rotation animation */
[data-testid="stExpander"] summary svg,
[data-testid="stExpander"] details > summary svg {
    transition: transform 0.25s ease !important;
    color: rgba(168,85,247,0.75) !important;
}
[data-testid="stExpander"] details[open] > summary svg {
    transform: rotate(90deg);
    color: #c084fc !important;
}
/* Expander body container styling when open */
[data-testid="stExpander"] details[open] {
    border-radius: 12px;
    background: rgba(255,255,255,0.015);
    border: 1px solid rgba(168,85,247,0.18);
    border-top: none;
}
[data-testid="stExpander"] details[open] > summary {
    border: none !important;
    border-bottom: 1px solid rgba(168,85,247,0.18) !important;
    border-radius: 12px 12px 0 0 !important;
}

/* === Dataframes === */
[data-testid="stDataFrame"], [data-testid="stTable"] {
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,0.06);
}

/* === Progress bar — clean track + bold animated fill === */
[data-testid="stProgress"] {
    margin: 14px 0 !important;
}
/* The track (outer wrapper) */
[data-testid="stProgress"] > div {
    height: 16px !important;
    border-radius: 999px !important;
    background:
        linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.08)),
        rgba(15,19,32,0.85) !important;
    border: 1px solid rgba(168,85,247,0.22) !important;
    overflow: hidden !important;
    box-shadow:
        inset 0 2px 4px rgba(0,0,0,0.45),
        inset 0 -1px 0 rgba(255,255,255,0.06),
        0 0 12px rgba(168,85,247,0.10) !important;
    padding: 0 !important;
    position: relative !important;
}
/* The fill (inner div with the inline width style) */
[data-testid="stProgress"] > div > div {
    height: 100% !important;
    border-radius: 999px !important;
    background: linear-gradient(
        90deg,
        #6366f1 0%,
        #8b5cf6 25%,
        #a855f7 50%,
        #ec4899 75%,
        #f472b6 100%
    ) !important;
    background-size: 250% 100% !important;
    animation: progress-shimmer 2.5s linear infinite, progress-glow 1.8s ease-in-out infinite !important;
    box-shadow:
        0 0 16px rgba(168,85,247,0.70),
        0 0 32px rgba(236,72,153,0.45),
        inset 0 1px 0 rgba(255,255,255,0.45),
        inset 0 -1px 0 rgba(0,0,0,0.15) !important;
    position: relative !important;
    border: none !important;
    min-width: 8px !important;
    transition: width 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
}
/* Sheen sweep across the fill */
[data-testid="stProgress"] > div > div::after {
    content: "";
    position: absolute;
    inset: 0;
    background: linear-gradient(
        90deg,
        transparent 0%,
        rgba(255,255,255,0.55) 50%,
        transparent 100%
    );
    animation: progress-sheen 1.5s linear infinite;
    border-radius: 999px;
    pointer-events: none;
}
@keyframes progress-shimmer {
    0%   { background-position: 250% 0; }
    100% { background-position: -250% 0; }
}
@keyframes progress-glow {
    0%, 100% { filter: brightness(1) saturate(1); }
    50%      { filter: brightness(1.25) saturate(1.15); }
}
@keyframes progress-sheen {
    0%   { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}

/* === Status / "processing" info banner — make it pulse === */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertInfoIcon"]) {
    animation: status-pulse 2.5s ease-in-out infinite;
}
@keyframes status-pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(99,102,241,0.0); }
    50%      { box-shadow: 0 0 0 6px rgba(99,102,241,0.10); }
}

/* === Spinners — bigger, branded === */
[data-testid="stSpinner"] > div {
    border-color: rgba(168,85,247,0.2) !important;
    border-top-color: #a855f7 !important;
}

/* === Headings inside tabs === */
.stTabs h2, .stTabs h3 {
    color: #e0e7ff;
    font-weight: 600;
    letter-spacing: -0.01em;
}

/* === Dividers softer === */
hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,0.06) !important;
    margin: 1.2rem 0 !important;
}

/* === Slider — accent track === */
.stSlider [data-baseweb="slider"] [role="slider"] {
    background: linear-gradient(135deg, #818cf8, #c084fc) !important;
    box-shadow: 0 0 0 4px rgba(168,85,247,0.15);
}

/* === Radio buttons — premium tile-style selector === */
.stRadio [role="radiogroup"] {
    gap: 10px !important;
    display: flex;
    flex-wrap: wrap;
    width: 100%;
}
.stRadio [role="radiogroup"][aria-orientation="vertical"],
.stRadio [role="radiogroup"]:not([aria-orientation="horizontal"]) {
    flex-direction: column;
    gap: 10px !important;
}
/* HORIZONTAL: equal-width tiles share the row */
.stRadio [role="radiogroup"][aria-orientation="horizontal"] > label {
    flex: 1 1 0 !important;
    min-width: 140px;
}
/* VERTICAL: full-width stacked tiles */
.stRadio [role="radiogroup"]:not([aria-orientation="horizontal"]) > label {
    width: 100% !important;
}
.stRadio [role="radiogroup"] > label {
    position: relative;
    background: rgba(255,255,255,0.025);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    padding: 12px 16px !important;
    margin: 0 !important;
    transition: background 0.18s ease, border-color 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
    cursor: pointer;
    min-height: 48px;
    display: flex !important;
    align-items: center;
    gap: 12px;
    box-sizing: border-box;
    overflow: hidden;
}
.stRadio [role="radiogroup"] > label:hover {
    background: rgba(255,255,255,0.05);
    border-color: rgba(168,85,247,0.35);
    transform: translateY(-1px);
}
.stRadio [role="radiogroup"] > label:has(input:checked) {
    background: linear-gradient(135deg, rgba(99,102,241,0.18), rgba(168,85,247,0.12)) !important;
    border-color: rgba(168,85,247,0.55) !important;
    box-shadow: 0 4px 18px rgba(99,102,241,0.22), inset 0 0 0 1px rgba(168,85,247,0.18);
}
/* Active-tile left accent stripe */
.stRadio [role="radiogroup"] > label:has(input:checked)::after {
    content: "";
    position: absolute;
    left: 0; top: 8px; bottom: 8px;
    width: 3px;
    border-radius: 0 3px 3px 0;
    background: linear-gradient(180deg, #6366f1, #a855f7, #ec4899);
    box-shadow: 0 0 10px rgba(168,85,247,0.55);
}
/* Native radio dot — purple-themed (kills red default) */
.stRadio [role="radiogroup"] > label > div:first-child {
    margin: 0 !important;
    width: 18px !important;
    height: 18px !important;
    min-width: 18px !important;
    border: 2px solid rgba(255,255,255,0.25) !important;
    border-radius: 50% !important;
    background: rgba(255,255,255,0.03) !important;
    box-shadow: none !important;
    flex-shrink: 0;
    transition: all 0.18s ease;
    position: relative;
    display: flex !important;
    align-items: center;
    justify-content: center;
}
.stRadio [role="radiogroup"] > label:hover > div:first-child {
    border-color: rgba(168,85,247,0.55) !important;
}
.stRadio [role="radiogroup"] > label:has(input:checked) > div:first-child {
    border-color: #a855f7 !important;
    background: rgba(168,85,247,0.10) !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.18) !important;
}
/* Hide Streamlit's painted inner dot/svg, draw our own gradient dot */
.stRadio [role="radiogroup"] > label > div:first-child > div,
.stRadio [role="radiogroup"] > label > div:first-child svg {
    display: none !important;
}
.stRadio [role="radiogroup"] > label:has(input:checked) > div:first-child::after {
    content: "";
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: linear-gradient(135deg, #818cf8, #c084fc);
    box-shadow: 0 0 6px rgba(168,85,247,0.7);
}
/* Label text — consistent weight & color */
.stRadio [role="radiogroup"] > label > div:last-child,
.stRadio [role="radiogroup"] > label p {
    font-size: 0.92rem !important;
    font-weight: 500 !important;
    color: rgba(240,244,255,0.85) !important;
    line-height: 1.3 !important;
    margin: 0 !important;
}
.stRadio [role="radiogroup"] > label:has(input:checked) > div:last-child,
.stRadio [role="radiogroup"] > label:has(input:checked) p {
    color: #ffffff !important;
    font-weight: 600 !important;
}

/* === Checkboxes — sibling of the radio style === */
.stCheckbox > label {
    padding: 8px 12px !important;
    border-radius: 10px;
    transition: background 0.18s ease;
    min-height: 40px;
    display: flex !important;
    align-items: center;
    gap: 10px;
}
.stCheckbox > label:hover {
    background: rgba(168,85,247,0.06);
}
.stCheckbox > label > div:first-child {
    border-radius: 6px !important;
    border-color: rgba(255,255,255,0.25) !important;
    background: rgba(255,255,255,0.03) !important;
    transition: all 0.18s ease;
}
.stCheckbox > label:hover > div:first-child {
    border-color: rgba(168,85,247,0.50) !important;
}
.stCheckbox > label:has(input:checked) > div:first-child {
    background: linear-gradient(135deg, #6366f1, #a855f7) !important;
    border-color: #a855f7 !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.18);
}
.stCheckbox > label p {
    font-size: 0.9rem !important;
    color: rgba(240,244,255,0.85) !important;
    margin: 0 !important;
}

/* === Text / Textarea / Number / Date inputs — unified glass === */
.stTextInput input,
.stTextArea textarea,
.stNumberInput input,
.stDateInput input {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 10px !important;
    color: #f0f4ff !important;
    transition: border-color 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
}
.stTextInput input:hover,
.stTextArea textarea:hover,
.stNumberInput input:hover,
.stDateInput input:hover {
    border-color: rgba(168,85,247,0.35) !important;
}
.stTextInput input:focus,
.stTextArea textarea:focus,
.stNumberInput input:focus,
.stDateInput input:focus {
    border-color: rgba(168,85,247,0.6) !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.18) !important;
    background: rgba(255,255,255,0.05) !important;
    outline: none !important;
}
.stTextInput input::placeholder,
.stTextArea textarea::placeholder,
.stNumberInput input::placeholder {
    color: rgba(255,255,255,0.32) !important;
}

/* === Multiselect chips — gradient pills === */
.stMultiSelect [data-baseweb="tag"] {
    background: linear-gradient(135deg, rgba(99,102,241,0.25), rgba(168,85,247,0.20)) !important;
    border: 1px solid rgba(168,85,247,0.40) !important;
    border-radius: 999px !important;
    color: #ffffff !important;
    font-weight: 500 !important;
    transition: all 0.15s ease;
}
.stMultiSelect [data-baseweb="tag"]:hover {
    border-color: rgba(168,85,247,0.65) !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.15);
}
.stMultiSelect [data-baseweb="tag"] [role="button"]:hover {
    color: #f472b6 !important;
}

/* === Code blocks === */
code, pre {
    background: rgba(255,255,255,0.04) !important;
    border-radius: 8px !important;
    border: 1px solid rgba(255,255,255,0.05);
}

/* === File uploader === */
[data-testid="stFileUploader"] section {
    border: 2px dashed rgba(168,85,247,0.3) !important;
    border-radius: 14px !important;
    background: rgba(168,85,247,0.04) !important;
    transition: all 0.2s ease;
}
[data-testid="stFileUploader"] section:hover {
    border-color: rgba(168,85,247,0.6) !important;
    background: rgba(168,85,247,0.08) !important;
}

/* === Subtle scrollbar === */
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: rgba(255,255,255,0.02); }
::-webkit-scrollbar-thumb {
    background: rgba(168,85,247,0.25);
    border-radius: 999px;
}
::-webkit-scrollbar-thumb:hover { background: rgba(168,85,247,0.5); }

/* === Caption color === */
[data-testid="stCaptionContainer"] {
    color: rgba(255,255,255,0.55) !important;
}

/* === Smooth fade-in for elements === */
.element-container {
    animation: fadeIn 0.3s ease-out;
}
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ===================================================== */
/* === POLISH LAYER — final visual upgrades         === */
/* ===================================================== */

/* --- Slider: full purple gradient track + glow thumb --- */
.stSlider [data-baseweb="slider"] > div > div {
    background: rgba(255,255,255,0.06) !important;
    height: 6px !important;
    border-radius: 999px !important;
}
.stSlider [data-baseweb="slider"] > div > div > div {
    background: linear-gradient(90deg, #6366f1, #a855f7, #ec4899) !important;
    border-radius: 999px !important;
    height: 6px !important;
}
.stSlider [role="slider"] {
    background: #ffffff !important;
    border: 3px solid #a855f7 !important;
    box-shadow: 0 0 0 6px rgba(168,85,247,0.18), 0 4px 12px rgba(168,85,247,0.4) !important;
    width: 18px !important;
    height: 18px !important;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.stSlider [role="slider"]:hover {
    transform: scale(1.15);
    box-shadow: 0 0 0 8px rgba(168,85,247,0.25), 0 6px 16px rgba(168,85,247,0.55) !important;
}
/* Slider value pill */
.stSlider [data-baseweb="slider"] [data-testid="stTickBarMin"],
.stSlider [data-baseweb="slider"] [data-testid="stTickBarMax"] {
    color: rgba(255,255,255,0.4) !important;
    font-size: 0.75rem;
}

/* --- Subheader (st.subheader) — gradient accent bar --- */
.stTabs h3, [data-testid="stHeading"] h3, h3 {
    position: relative;
    padding-left: 14px !important;
    font-weight: 700 !important;
    color: #f0f4ff !important;
    letter-spacing: -0.01em;
}
.stTabs h3::before, [data-testid="stHeading"] h3::before, h3::before {
    content: "";
    position: absolute;
    left: 0;
    top: 6px;
    bottom: 6px;
    width: 4px;
    background: linear-gradient(180deg, #6366f1, #a855f7, #ec4899);
    border-radius: 999px;
    box-shadow: 0 0 8px rgba(168,85,247,0.5);
}

/* --- H2 / "Data Source" markdown heading --- */
h2 {
    font-weight: 700 !important;
    color: #e0e7ff !important;
    background: linear-gradient(135deg, #c7d2fe 0%, #ddd6fe 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: -0.015em;
}

/* --- Number input — themed +/- buttons --- */
.stNumberInput button {
    background: rgba(168,85,247,0.10) !important;
    border: 1px solid rgba(168,85,247,0.25) !important;
    color: #e0e7ff !important;
    border-radius: 8px !important;
    transition: all 0.15s ease;
}
.stNumberInput button:hover {
    background: rgba(168,85,247,0.25) !important;
    border-color: rgba(168,85,247,0.5) !important;
    color: #ffffff !important;
}

/* --- Selectbox: rounded glassy with purple chevron --- */
.stSelectbox [data-baseweb="select"] {
    border-radius: 10px !important;
}
.stSelectbox [data-baseweb="select"] > div {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 10px !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
}
.stSelectbox [data-baseweb="select"] > div:hover {
    border-color: rgba(168,85,247,0.4) !important;
}
.stSelectbox [data-baseweb="select"]:focus-within > div {
    border-color: rgba(168,85,247,0.6) !important;
    box-shadow: 0 0 0 3px rgba(168,85,247,0.15) !important;
}
/* Dropdown popup */
[data-baseweb="popover"] [role="listbox"] {
    background: rgba(15,19,32,0.97) !important;
    backdrop-filter: blur(20px);
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 12px !important;
    box-shadow: 0 20px 60px rgba(0,0,0,0.5) !important;
    padding: 4px !important;
}
[data-baseweb="popover"] [role="option"] {
    border-radius: 8px !important;
    margin: 2px 0;
    transition: background 0.15s ease;
}
[data-baseweb="popover"] [role="option"]:hover {
    background: linear-gradient(135deg, rgba(99,102,241,0.20), rgba(168,85,247,0.15)) !important;
}
[data-baseweb="popover"] [role="option"][aria-selected="true"] {
    background: linear-gradient(135deg, rgba(99,102,241,0.30), rgba(168,85,247,0.25)) !important;
    color: #ffffff !important;
}

/* --- Sidebar inputs slightly smaller for density --- */
section[data-testid="stSidebar"] .stTextInput input,
section[data-testid="stSidebar"] .stSelectbox > div > div {
    font-size: 0.9rem !important;
}
/* Sidebar headings */
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    font-size: 1.05rem !important;
    margin-top: 0.5rem !important;
    padding-left: 0 !important;
}
section[data-testid="stSidebar"] h2::before,
section[data-testid="stSidebar"] h3::before {
    display: none !important;
}
/* Sidebar primary button (Connect) — full-width gradient */
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    width: 100% !important;
    padding: 10px 16px !important;
    font-size: 0.95rem !important;
    border-radius: 12px !important;
}

/* --- Container card pattern: wrap blocks via st.container(border=True) --- */
[data-testid="stVerticalBlockBorderWrapper"] {
    background: rgba(255,255,255,0.025) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 16px !important;
    padding: 18px !important;
    backdrop-filter: blur(8px);
    transition: border-color 0.25s ease;
}
[data-testid="stVerticalBlockBorderWrapper"]:hover {
    border-color: rgba(168,85,247,0.20) !important;
}

/* --- Markdown body text comfort --- */
.stMarkdown p, .stMarkdown li {
    color: rgba(255,255,255,0.82);
    line-height: 1.6;
}
.stMarkdown strong {
    color: #f0f4ff;
}

/* --- Toast / popups --- */
[data-testid="stToast"] {
    background: rgba(15,19,32,0.95) !important;
    backdrop-filter: blur(20px);
    border: 1px solid rgba(168,85,247,0.3) !important;
    border-radius: 12px !important;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4) !important;
}

/* --- Tooltip / help icon --- */
[data-testid="stTooltipHoverTarget"] svg {
    color: rgba(168,85,247,0.65) !important;
    transition: color 0.2s ease;
}
[data-testid="stTooltipHoverTarget"]:hover svg {
    color: #c084fc !important;
}

/* --- Title polish: animated subtle glow + lightning bolt --- */
h1:first-of-type {
    text-shadow: 0 0 30px rgba(168,85,247,0.2);
    animation: title-glow 6s ease-in-out infinite;
}
@keyframes title-glow {
    0%, 100% { filter: drop-shadow(0 0 10px rgba(99,102,241,0.0)); }
    50%      { filter: drop-shadow(0 0 18px rgba(168,85,247,0.25)); }
}

/* --- Tab bottom indicator hide (we use full pill highlight) --- */
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"] { display: none !important; }

/* --- Empty / placeholder selectbox text softer --- */
.stSelectbox [data-baseweb="select"] [aria-disabled="true"] {
    color: rgba(255,255,255,0.4) !important;
    font-style: italic;
}

/* --- Section dividers with subtle gradient --- */
hr {
    background: linear-gradient(90deg,
        transparent 0%,
        rgba(168,85,247,0.25) 50%,
        transparent 100%
    ) !important;
    height: 1px !important;
    border: none !important;
}

/* --- Smooth transitions on everything --- */
* {
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* === Hide Streamlit's default header / toolbar / footer (removes black bar at top) === */
[data-testid="stHeader"] {
    background: transparent !important;
    pointer-events: none;
}
[data-testid="stHeader"] button,
[data-testid="stSidebarCollapsedControl"],
[data-testid="stSidebarCollapseButton"] {
    visibility: visible !important;
    pointer-events: auto !important;
}
[data-testid="stToolbar"] {
    display: contents !important;
    pointer-events: none !important;
}
[data-testid="stExpandSidebarButton"] {
    position: fixed;
    top: 12px;
    left: 18px;
    width: 36px;
    height: 36px;
    background: var(--sf-surface) !important;
    color: var(--sf-text) !important;
    border: 1px solid var(--sf-border) !important;
    border-radius: 6px;
    pointer-events: auto !important;
}
[data-testid="stExpandSidebarButton"] span {
    color: inherit !important;
    -webkit-text-fill-color: currentColor !important;
}
[data-testid="stToolbarActions"],
[data-testid="stStatusWidget"] { display: none !important; }
#MainMenu, [data-testid="stMainMenu"] { display: none !important; }
footer[data-testid="stFooter"] { display: none !important; }
.stAppDeployButton { display: none !important; }
/* Remove the default Streamlit top-padding that was reserved for the header */
[data-testid="stAppViewContainer"] { padding-top: 0 !important; }

/* --- Make the main content padding feel airier --- */
[data-testid="stAppViewContainer"] > .main .block-container {
    padding-top: 0.75rem !important;
    padding-bottom: 4rem !important;
    max-width: 1400px;
}

/* App-level fade-in on load to prevent flash of unstyled content */
.stApp {
    animation: sf-app-fadein 0.4s ease-out;
}
@keyframes sf-app-fadein {
    from { opacity: 0; }
    to   { opacity: 1; }
}

/* ===================================================== */
/* === EXTRA FLAIR — animations & shine             === */
/* ===================================================== */

/* --- Info banner: sweeping shimmer light across it --- */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertInfoIcon"]) {
    position: relative;
    overflow: hidden;
    isolation: isolate;
}
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertInfoIcon"])::before {
    content: "";
    position: absolute;
    top: 0;
    left: -100%;
    width: 60%;
    height: 100%;
    background: linear-gradient(
        90deg,
        transparent,
        rgba(255,255,255,0.10),
        rgba(99,102,241,0.18),
        rgba(255,255,255,0.10),
        transparent
    );
    transform: skewX(-20deg);
    animation: banner-sweep 3.5s ease-in-out infinite;
    z-index: 0;
    pointer-events: none;
}
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertInfoIcon"]) > * {
    position: relative;
    z-index: 1;
}
@keyframes banner-sweep {
    0%   { left: -100%; }
    60%  { left: 120%; }
    100% { left: 120%; }
}

/* --- Primary button: shiny gloss sweep on hover --- */
.stButton > button[kind="primary"] {
    position: relative;
    overflow: hidden;
    isolation: isolate;
}
.stButton > button[kind="primary"]::before {
    content: "";
    position: absolute;
    top: 0;
    left: -100%;
    width: 50%;
    height: 100%;
    background: linear-gradient(
        90deg,
        transparent,
        rgba(255,255,255,0.35),
        transparent
    );
    transform: skewX(-25deg);
    transition: left 0.6s ease;
    z-index: 0;
    pointer-events: none;
}
.stButton > button[kind="primary"]:hover::before {
    left: 150%;
}
.stButton > button[kind="primary"] > * {
    position: relative;
    z-index: 1;
}

/* --- Tab pill: subtle hover lift + active glow pulse --- */
.stTabs [aria-selected="true"] {
    animation: tab-glow 3s ease-in-out infinite;
}
@keyframes tab-glow {
    0%, 100% { box-shadow: 0 4px 14px rgba(99,102,241,0.25); }
    50%      { box-shadow: 0 4px 20px rgba(168,85,247,0.45); }
}

/* --- Container border-wrapper: animated gradient border --- */
[data-testid="stVerticalBlockBorderWrapper"] {
    position: relative;
    isolation: isolate;
}
[data-testid="stVerticalBlockBorderWrapper"]::before {
    content: "";
    position: absolute;
    inset: -1px;
    padding: 1px;
    background: linear-gradient(
        135deg,
        rgba(168,85,247,0.0),
        rgba(168,85,247,0.0),
        rgba(168,85,247,0.0)
    );
    border-radius: 16px;
    -webkit-mask:
        linear-gradient(#fff 0 0) content-box,
        linear-gradient(#fff 0 0);
    -webkit-mask-composite: xor;
            mask-composite: exclude;
    transition: background 0.4s ease;
    pointer-events: none;
}
[data-testid="stVerticalBlockBorderWrapper"]:hover::before {
    background: linear-gradient(
        135deg,
        rgba(99,102,241,0.5),
        rgba(168,85,247,0.5),
        rgba(236,72,153,0.5)
    );
    animation: border-rotate 4s linear infinite;
}
@keyframes border-rotate {
    0%   { filter: hue-rotate(0deg); }
    100% { filter: hue-rotate(360deg); }
}

/* --- Success badge: gentle scale-in pulse on appear --- */
div[data-testid="stAlertContainer"]:has(svg[data-testid="stAlertSuccessIcon"]) {
    animation: success-pop 0.5s cubic-bezier(0.34, 1.56, 0.64, 1);
}
@keyframes success-pop {
    0%   { opacity: 0; transform: scale(0.92); }
    100% { opacity: 1; transform: scale(1); }
}

/* --- Sidebar logo title (Salesforce Connection) gets gradient bar --- */
section[data-testid="stSidebar"] h1 {
    position: relative;
    padding-bottom: 0.5rem !important;
    margin-bottom: 1rem !important;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}

/* --- Floating ambient blobs (decorative, subtle) --- */
.stApp::before {
    content: "";
    position: fixed;
    top: -200px;
    left: 30%;
    width: 600px;
    height: 600px;
    background: radial-gradient(circle, rgba(99,102,241,0.10) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
    animation: blob-float-1 18s ease-in-out infinite;
    filter: blur(40px);
}
.stApp::after {
    content: "";
    position: fixed;
    bottom: -250px;
    right: 10%;
    width: 700px;
    height: 700px;
    background: radial-gradient(circle, rgba(236,72,153,0.08) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
    animation: blob-float-2 22s ease-in-out infinite;
    filter: blur(50px);
}
@keyframes blob-float-1 {
    0%, 100% { transform: translate(0, 0) scale(1); }
    50%      { transform: translate(120px, 80px) scale(1.15); }
}
@keyframes blob-float-2 {
    0%, 100% { transform: translate(0, 0) scale(1); }
    50%      { transform: translate(-150px, -100px) scale(1.10); }
}
/* Keep main content above the blobs */
[data-testid="stAppViewContainer"] > .main {
    position: relative;
    z-index: 1;
}

/* --- Caption upgrade: subtle indent + better color --- */
[data-testid="stCaptionContainer"] {
    color: rgba(255,255,255,0.6) !important;
    font-size: 0.85rem;
    line-height: 1.5;
}

/* --- Data preview table header pop --- */
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] th {
    background: linear-gradient(180deg, rgba(168,85,247,0.12), rgba(99,102,241,0.08)) !important;
    color: #e0e7ff !important;
    font-weight: 600 !important;
    border-bottom: 1px solid rgba(168,85,247,0.25) !important;
}

/* ===================================================== */
/* === WAVE 3 — TABS / DATAFRAMES / LOADERS POLISH  === */
/* ===================================================== */

/* --- Tab content fade-in on switch --- */
.stTabs [data-baseweb="tab-panel"] {
    animation: tab-fade-in 0.22s ease-out;
}
@keyframes tab-fade-in {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* --- Active tab: gradient underline accent --- */
.stTabs [aria-selected="true"]::after {
    content: "";
    position: absolute;
    left: 14px;
    right: 14px;
    bottom: 4px;
    height: 2px;
    border-radius: 999px;
    background: linear-gradient(90deg, #6366f1, #a855f7, #ec4899);
    box-shadow: 0 0 8px rgba(168,85,247,0.55);
    animation: tab-underline-in 0.3s ease-out;
}
@keyframes tab-underline-in {
    from { transform: scaleX(0); opacity: 0; }
    to   { transform: scaleX(1); opacity: 1; }
}
.stTabs [data-baseweb="tab"] { position: relative; }
/* Tab icons consistent sizing */
.stTabs [data-baseweb="tab"] p {
    font-size: 0.92rem !important;
}

/* --- Dataframes: sticky header + row hover + zebra stripes --- */
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] thead,
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] th {
    position: sticky !important;
    top: 0 !important;
    z-index: 5 !important;
}
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] tbody tr {
    transition: background 0.15s ease;
}
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] tbody tr:nth-child(even) {
    background: rgba(255,255,255,0.015);
}
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] tbody tr:hover {
    background: linear-gradient(90deg, rgba(168,85,247,0.10), rgba(99,102,241,0.06)) !important;
}
/* Numeric cells get monospace for clarity */
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] td[class*="numeric"],
[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] td[class*="number"] {
    font-family: 'JetBrains Mono', 'Menlo', 'Consolas', monospace !important;
    text-align: right !important;
}

/* --- Loading spinner upgrade: 3-dot pulse --- */
[data-testid="stSpinner"] {
    display: inline-flex;
    align-items: center;
    gap: 10px;
}
[data-testid="stSpinner"] > div {
    width: 28px !important;
    height: 28px !important;
    border-width: 3px !important;
    border-color: rgba(168,85,247,0.15) !important;
    border-top-color: #a855f7 !important;
    border-right-color: #ec4899 !important;
    border-radius: 50% !important;
    animation: sf-spinner-rotate 0.9s linear infinite !important;
}
@keyframes sf-spinner-rotate {
    to { transform: rotate(360deg); }
}
[data-testid="stSpinner"] + div, [data-testid="stSpinner"] > div + * {
    color: #c7d2fe !important;
    font-weight: 500 !important;
    font-size: 0.92rem !important;
}

/* --- Skeleton shimmer utility class (for use in HTML helpers) --- */
.sf-skeleton {
    background: linear-gradient(
        90deg,
        rgba(255,255,255,0.04) 0%,
        rgba(168,85,247,0.10) 50%,
        rgba(255,255,255,0.04) 100%
    );
    background-size: 200% 100%;
    animation: sf-skeleton-shimmer 1.4s ease-in-out infinite;
    border-radius: 8px;
}
@keyframes sf-skeleton-shimmer {
    0%   { background-position: 200% 0; }
    100% { background-position: -200% 0; }
}

/* --- Toast: gradient left border per kind --- */
[data-testid="stToast"] {
    border-left: 4px solid #a855f7 !important;
    padding-left: 14px !important;
}
[data-testid="stToast"]:has(svg[data-testid*="Success"]) {
    border-left-color: #22c55e !important;
}
[data-testid="stToast"]:has(svg[data-testid*="Error"]) {
    border-left-color: #ef4444 !important;
}
[data-testid="stToast"]:has(svg[data-testid*="Warning"]) {
    border-left-color: #f59e0b !important;
}

/* ===================================================== */
/* === WAVE 4 — MICRO-INTERACTIONS                  === */
/* ===================================================== */

/* KPI card gentle hover tilt (2deg max) */
.sf-kpi-card {
    transform-style: preserve-3d;
    perspective: 600px;
}
.sf-kpi-card:hover {
    transform: translateY(-3px) rotateX(2deg) !important;
}

/* Connect-button ripple on click */
.stButton > button[kind="primary"] {
    position: relative;
}
.stButton > button[kind="primary"]:active::after {
    content: "";
    position: absolute;
    inset: 0;
    border-radius: inherit;
    background: radial-gradient(circle at center, rgba(255,255,255,0.25), transparent 60%);
    animation: sf-ripple 0.5s ease-out;
}
@keyframes sf-ripple {
    from { opacity: 1; transform: scale(0.4); }
    to   { opacity: 0; transform: scale(1.4); }
}

/* ===================================================== */
/* === WAVE 5 — PREMIUM v2.1 ENHANCEMENTS           === */
/* ===================================================== */

/* --- Hero card: rotating gradient glow ring via pseudo-element --- */
.sf-hero { position: relative; z-index: 0; overflow: visible !important; }
.sf-hero::after {
    content: "";
    position: absolute;
    inset: -2px;
    border-radius: 22px;
    background: linear-gradient(130deg, #6366f1 0%, #a855f7 35%, #ec4899 65%, #f472b6 80%, #6366f1 100%);
    background-size: 300% 300%;
    z-index: -1;
    opacity: 0.45;
    animation: sf-hero-border-shift 5s ease infinite;
    filter: blur(4px);
    pointer-events: none;
}
@keyframes sf-hero-border-shift {
    0%   { background-position: 0% 50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

/* --- Section icon circle --- */
.sf-icon-circle {
    display: inline-flex; align-items: center; justify-content: center;
    width: 54px; height: 54px; border-radius: 17px;
    font-size: 1.65rem; line-height: 1; flex-shrink: 0;
    transition: transform 0.25s ease, box-shadow 0.25s ease;
}
.sf-icon-circle:hover { transform: scale(1.10) rotate(-5deg); }
.sf-icon-circle-purple {
    background: linear-gradient(135deg, rgba(99,102,241,0.28), rgba(168,85,247,0.22));
    border: 1px solid rgba(168,85,247,0.42);
    box-shadow: 0 4px 20px rgba(168,85,247,0.28), inset 0 1px 0 rgba(255,255,255,0.10);
}
.sf-icon-circle-green {
    background: linear-gradient(135deg, rgba(16,185,129,0.22), rgba(34,197,94,0.16));
    border: 1px solid rgba(34,197,94,0.38);
    box-shadow: 0 4px 20px rgba(34,197,94,0.22), inset 0 1px 0 rgba(255,255,255,0.10);
}
.sf-icon-circle-pink {
    background: linear-gradient(135deg, rgba(219,39,119,0.22), rgba(236,72,153,0.16));
    border: 1px solid rgba(236,72,153,0.38);
    box-shadow: 0 4px 20px rgba(236,72,153,0.22), inset 0 1px 0 rgba(255,255,255,0.10);
}
.sf-icon-circle-cyan {
    background: linear-gradient(135deg, rgba(8,145,178,0.22), rgba(6,182,212,0.16));
    border: 1px solid rgba(6,182,212,0.38);
    box-shadow: 0 4px 20px rgba(6,182,212,0.22), inset 0 1px 0 rgba(255,255,255,0.10);
}
.sf-icon-circle-indigo {
    background: linear-gradient(135deg, rgba(79,70,229,0.24), rgba(99,102,241,0.18));
    border: 1px solid rgba(99,102,241,0.40);
    box-shadow: 0 4px 20px rgba(99,102,241,0.24), inset 0 1px 0 rgba(255,255,255,0.10);
}

/* --- KPI card stagger pop-in animation --- */
@keyframes sf-kpi-pop {
    from { opacity: 0; transform: translateY(14px) scale(0.94); }
    to   { opacity: 1; transform: translateY(0) scale(1); }
}
.sf-kpi-card { animation: sf-kpi-pop 0.42s cubic-bezier(0.34, 1.56, 0.64, 1) both; }
.sf-kpi-row .sf-kpi-card:nth-child(1) { animation-delay: 0ms; }
.sf-kpi-row .sf-kpi-card:nth-child(2) { animation-delay: 65ms; }
.sf-kpi-row .sf-kpi-card:nth-child(3) { animation-delay: 130ms; }
.sf-kpi-row .sf-kpi-card:nth-child(4) { animation-delay: 195ms; }
.sf-kpi-row .sf-kpi-card:nth-child(5) { animation-delay: 260ms; }
.sf-kpi-row .sf-kpi-card:nth-child(6) { animation-delay: 325ms; }

/* --- Connection card: active pulsing green ring --- */
@keyframes sf-conn-ring {
    0%   { box-shadow: 0 0 0 0   rgba(34,197,94,0.60), 0 4px 16px rgba(34,197,94,0.12); }
    70%  { box-shadow: 0 0 0 10px rgba(34,197,94,0.00), 0 4px 16px rgba(34,197,94,0.12); }
    100% { box-shadow: 0 0 0 0   rgba(34,197,94,0.00), 0 4px 16px rgba(34,197,94,0.12); }
}
.sf-conn-card.active {
    animation: sf-conn-ring 2.2s ease-out infinite !important;
    border-color: rgba(34,197,94,0.50) !important;
    background: linear-gradient(135deg, rgba(34,197,94,0.10), rgba(99,102,241,0.07)) !important;
}

/* --- Sidebar section card wrapper --- */
.sf-sidebar-section {
    background: rgba(255,255,255,0.025);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 14px;
    padding: 12px 14px 12px 18px;
    margin: 8px 0 12px 0;
    position: relative;
    overflow: hidden;
    transition: border-color 0.25s ease;
}
.sf-sidebar-section:hover { border-color: rgba(168,85,247,0.20); }
.sf-sidebar-section::before {
    content: "";
    position: absolute;
    left: 0; top: 10px; bottom: 10px;
    width: 3px; border-radius: 0 3px 3px 0;
    background: linear-gradient(180deg, #6366f1, #a855f7, #ec4899);
    box-shadow: 0 0 10px rgba(168,85,247,0.45);
}
.sf-sidebar-section-title {
    font-size: 0.67rem; font-weight: 700; letter-spacing: 0.09em;
    text-transform: uppercase; color: rgba(255,255,255,0.38); margin-bottom: 10px;
}

/* --- Widget label gradient text --- */
[data-testid="stWidgetLabel"] p {
    background: linear-gradient(90deg, #c7d2fe 0%, #ddd6fe 55%, #f0abfc 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: 500 !important;
}

/* --- Premium gradient divider --- */
.sf-divider {
    display: flex; align-items: center; gap: 14px; margin: 22px 0;
}
.sf-divider::before, .sf-divider::after {
    content: ""; flex: 1; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(168,85,247,0.28), transparent);
}
.sf-divider-label {
    font-size: 0.69rem; font-weight: 700; letter-spacing: 0.09em;
    text-transform: uppercase; color: rgba(255,255,255,0.42); white-space: nowrap;
    padding: 3px 12px;
    background: rgba(168,85,247,0.08);
    border: 1px solid rgba(168,85,247,0.18); border-radius: 999px;
}

/* --- Tip card --- */
.sf-tip-card {
    display: flex; align-items: flex-start; gap: 13px;
    padding: 13px 16px;
    background: linear-gradient(135deg, rgba(99,102,241,0.08), rgba(168,85,247,0.06));
    border: 1px solid rgba(168,85,247,0.22); border-left: 4px solid #a855f7;
    border-radius: 0 12px 12px 0; margin: 10px 0;
    animation: sf-fade-in 0.35s ease;
}
.sf-tip-card-icon { font-size: 1.18rem; flex-shrink: 0; margin-top: 2px; filter: drop-shadow(0 0 8px rgba(168,85,247,0.5)); }
.sf-tip-card-body { font-size: 0.87rem; color: rgba(240,244,255,0.86); line-height: 1.55; }
.sf-tip-card-body strong { color: #f0f4ff; }

/* --- Scrollbar full gradient upgrade --- */
::-webkit-scrollbar-thumb {
    background: linear-gradient(180deg, #6366f1 0%, #a855f7 50%, #ec4899 100%) !important;
    border-radius: 999px !important;
    border: 2px solid transparent !important;
    background-clip: padding-box !important;
}

/* --- Tooltip glass card --- */
[data-testid="stTooltipContent"] {
    background: rgba(15,19,32,0.97) !important;
    backdrop-filter: blur(22px) !important;
    border: 1px solid rgba(168,85,247,0.30) !important;
    border-radius: 12px !important;
    box-shadow: 0 14px 36px rgba(0,0,0,0.48) !important;
    color: rgba(240,244,255,0.90) !important;
    font-size: 0.84rem !important;
    padding: 10px 14px !important;
}

/* --- Sidebar version badge upgrade --- */
.sf-sidebar-version {
    background: linear-gradient(135deg, rgba(99,102,241,0.22), rgba(168,85,247,0.16)) !important;
    border: 1px solid rgba(168,85,247,0.38) !important;
    color: #c084fc !important;
    font-weight: 600 !important;
    padding: 3px 10px !important;
    letter-spacing: 0.05em;
}

/* --- st.metric delta colors --- */
[data-testid="stMetricDelta"] { font-weight: 600 !important; font-size: 0.82rem !important; }
[data-testid="stMetricDeltaIcon-Up"]   + span { color: #4ade80 !important; }
[data-testid="stMetricDeltaIcon-Down"] + span { color: #f87171 !important; }

</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
html[data-sf-theme="light"] .stApp {
    background: var(--sf-page-wash) !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme="light"] section[data-testid="stSidebar"] {
    background: var(--sf-sidebar-wash, var(--sf-panel-wash)) !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme="light"] .stTabs [data-baseweb="tab-list"] {
    background: rgba(15, 118, 110, 0.06) !important;
}
html[data-sf-theme="light"] .stTabs [data-baseweb="tab"] {
    color: var(--sf-muted) !important;
}
html[data-sf-theme="light"] .stTabs [aria-selected="true"] {
    background: var(--sf-selected) !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme="light"] [data-testid="stFileUploaderDropzone"],
html[data-sf-theme="light"] [data-testid="stExpander"] details,
html[data-sf-theme="light"] [data-baseweb="popover"] > div {
    background: var(--sf-surface) !important;
    color: var(--sf-text) !important;
    border-color: var(--sf-border) !important;
}
</style>
""", unsafe_allow_html=True)

# ====================================================================
# 🎨 PREMIUM UI HELPER COMPONENTS (Phase 1 — reusable building blocks)
# ====================================================================
# All helpers emit HTML via st.markdown(unsafe_allow_html=True).
# Theme-aware controls and reusable building blocks.

render_theme_control()

def _ui(html):
    """Shortcut: render arbitrary HTML in Streamlit.
    Collapses newlines/indentation before passing to markdown — mistune (Streamlit's
    markdown parser) falls out of HTML block mode on blank or indented lines, which
    causes nested div content to render as raw escaped text instead of styled HTML.
    """
    st.markdown(' '.join(html.split()), unsafe_allow_html=True)

class InlineProgressBar:
    """Single custom progress bar used where native st.progress caused double bars."""
    def __init__(self, container, label='Working'):
        self.container = container
        self.label = label
        self.progress(0.0)

    def progress(self, value, text=None):
        pct = max(0, min(float(value or 0), 1.0))
        label = text or self.label
        color = '#22c55e' if pct >= 1 else '#38bdf8'
        html = (
            f'<div style="background:linear-gradient(135deg,rgba(14,165,233,0.12),rgba(168,85,247,0.08));'
            f'border:1px solid rgba(56,189,248,0.28);border-radius:12px;padding:10px 12px;margin:8px 0;">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:7px;">'
            f'<div style="font-size:0.78rem;color:#7dd3fc;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{label}</div>'
            f'<div style="font-size:0.72rem;color:rgba(255,255,255,0.58);font-weight:700;">{pct*100:.0f}%</div>'
            f'</div>'
            f'<div style="height:7px;border-radius:999px;background:rgba(255,255,255,0.08);overflow:hidden;">'
            f'<div style="height:100%;width:{pct*100:.1f}%;border-radius:999px;background:linear-gradient(90deg,{color},#a855f7);'
            f'box-shadow:0 0 18px {color}66;transition:width 0.25s ease;"></div>'
            f'</div></div>'
        )
        self.container.markdown(' '.join(html.split()), unsafe_allow_html=True)

def render_bulk_jobs_quota_panel(sf=None):
    """Render Salesforce Bulk Data Load Jobs quota, matching Setup's batch quota view."""
    if sf is None:
        sf = st.session_state.get('sf')
    if not sf:
        return

    used = None
    limit = 15000
    remaining = None
    source = 'Salesforce Bulk Data Load Jobs'

    try:
        base_url = f'https://{sf.sf_instance}/services/data/v{sf.sf_version}'
        resp = requests.get(
            f'{base_url}/limits',
            headers={'Authorization': f'Bearer {sf.session_id}'},
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            bulk_limit = data.get('DailyBulkApiBatches') or data.get('DailyBulkV2QueryJobs')
            if bulk_limit:
                remaining = int(bulk_limit.get('Remaining', 0))
                limit = int(bulk_limit.get('Max', limit) or limit)
                used = max(limit - remaining, 0)
    except Exception:
        pass

    if used is None:
        try:
            from sf_bulk_loader import get_quota_tracker
            tracker = get_quota_tracker()
            tracker._check_reset()
            used = int(tracker._state.get('bulk_jobs', 0))
            remaining = max(limit - used, 0)
            source = 'Local Bulk Jobs Tracker'
        except Exception:
            used = 0
            remaining = limit

    pct = (used / max(limit, 1)) * 100
    color = '#22c55e' if pct < 70 else ('#f59e0b' if pct < 90 else '#ef4444')
    _ui(
        f'<div style="display:grid;grid-template-columns:auto 1fr auto;gap:14px;align-items:center;'
        f'background:linear-gradient(135deg,rgba(14,165,233,0.13),rgba(168,85,247,0.08));'
        f'border:1px solid rgba(56,189,248,0.28);border-radius:12px;padding:12px 14px;margin:8px 0 14px 0;">'
        f'<div style="font-size:1.35rem;">⚙️</div>'
        f'<div>'
        f'<div style="font-size:0.70rem;text-transform:uppercase;letter-spacing:0.08em;color:#7dd3fc;font-weight:800;">{source}</div>'
        f'<div style="font-size:0.92rem;color:#f0f4ff;font-weight:800;margin-top:2px;">Bulk Data Load Jobs quota</div>'
        f'<div style="height:6px;border-radius:999px;background:rgba(255,255,255,0.08);overflow:hidden;margin-top:8px;">'
        f'<div style="height:100%;width:{min(pct,100):.1f}%;background:linear-gradient(90deg,{color},#38bdf8);border-radius:999px;"></div>'
        f'</div></div>'
        f'<div style="text-align:right;white-space:nowrap;">'
        f'<div style="font-size:1.05rem;color:{color};font-weight:900;">{used:,}/{limit:,}</div>'
        f'<div style="font-size:0.70rem;color:rgba(255,255,255,0.54);">{pct:.1f}% used · {remaining:,} remaining</div>'
        f'</div></div>'
    )

def status_pill(label, state='idle', pulse=False, size='sm'):
    """Render a status pill with optional pulsing dot.
    state: 'active' (green), 'idle' (gray), 'error' (red), 'warn' (amber), 'info' (purple)
    """
    palettes = {
        'active': ('#22c55e', 'rgba(34,197,94,0.15)', 'rgba(34,197,94,0.4)'),
        'idle':   ('#94a3b8', 'rgba(148,163,184,0.12)', 'rgba(148,163,184,0.3)'),
        'error':  ('#ef4444', 'rgba(239,68,68,0.15)', 'rgba(239,68,68,0.4)'),
        'warn':   ('#f59e0b', 'rgba(245,158,11,0.15)', 'rgba(245,158,11,0.4)'),
        'info':   ('#a855f7', 'rgba(168,85,247,0.15)', 'rgba(168,85,247,0.4)'),
    }
    dot_color, bg, border = palettes.get(state, palettes['idle'])
    pulse_cls = ' sf-pulse' if pulse else ''
    font_size = '0.72rem' if size == 'xs' else ('0.80rem' if size == 'sm' else '0.95rem')
    pad = '4px 10px' if size != 'lg' else '7px 14px'
    return f"""<span class="sf-status-pill" style="
        display:inline-flex;align-items:center;gap:6px;
        padding:{pad};border-radius:999px;
        background:{bg};border:1px solid {border};
        font-size:{font_size};font-weight:500;color:#f0f4ff;
        white-space:nowrap;">
        <span class="sf-dot{pulse_cls}" style="
            width:7px;height:7px;border-radius:50%;background:{dot_color};
            box-shadow:0 0 8px {dot_color};"></span>
        {label}
    </span>"""

def kpi_card(label, value, icon='', color='purple', sub=None, trend=None, sparkline=None):
    """Render a premium animated KPI card.
    color: 'purple' | 'green' | 'red' | 'indigo' | 'pink' | 'slate' | 'amber' | 'cyan'
    sparkline: list of up to 8 floats (0.0-1.0) for a mini bar chart, or None
    """
    palettes = {
        'purple': ('#a855f7', '#c084fc', 'rgba(168,85,247,0.10)', 'rgba(168,85,247,0.30)'),
        'green':  ('#22c55e', '#4ade80', 'rgba(34,197,94,0.10)',  'rgba(34,197,94,0.30)'),
        'red':    ('#ef4444', '#f87171', 'rgba(239,68,68,0.10)',  'rgba(239,68,68,0.30)'),
        'indigo': ('#6366f1', '#818cf8', 'rgba(99,102,241,0.10)', 'rgba(99,102,241,0.30)'),
        'pink':   ('#ec4899', '#f472b6', 'rgba(236,72,153,0.10)', 'rgba(236,72,153,0.30)'),
        'slate':  ('#64748b', '#94a3b8', 'rgba(100,116,139,0.10)','rgba(100,116,139,0.30)'),
        'amber':  ('#f59e0b', '#fbbf24', 'rgba(245,158,11,0.10)', 'rgba(245,158,11,0.30)'),
        'cyan':   ('#06b6d4', '#22d3ee', 'rgba(6,182,212,0.10)',  'rgba(6,182,212,0.30)'),
    }
    c1, c2, bg, border = palettes.get(color, palettes['purple'])
    trend_html = ''
    if trend:
        t_color = '#22c55e' if trend.startswith('+') else '#ef4444' if trend.startswith('-') else '#94a3b8'
        t_arrow = '▲' if trend.startswith('+') else '▼' if trend.startswith('-') else ''
        trend_html = f'<span style="font-size:0.70rem;font-weight:700;color:{t_color};padding:2px 7px;border-radius:999px;background:{t_color}22;">{t_arrow} {trend}</span>'
    sub_html = f'<div style="font-size:0.70rem;color:rgba(255,255,255,0.45);margin-top:5px;letter-spacing:0.01em;">{sub}</div>' if sub else ''
    # Mini sparkline bars (pure CSS, no JS)
    spark_html = ''
    if sparkline and len(sparkline) >= 2:
        vals = [max(0.0, min(1.0, float(v))) for v in sparkline[-8:]]
        bars = ''.join(
            f'<div style="flex:1;border-radius:2px 2px 0 0;min-height:3px;'
            f'height:{max(int(v * 28), 3)}px;'
            f'background:linear-gradient(180deg,{c2},{c1});opacity:{0.4 + v*0.6:.2f};"></div>'
            for v in vals
        )
        spark_html = f'<div style="display:flex;align-items:flex-end;gap:2px;height:32px;margin-top:8px;padding:0 2px;">{bars}</div>'
    return f"""<div class="sf-kpi-card" style="
        background:linear-gradient(135deg, {bg}, rgba(255,255,255,0.015));
        border:1px solid {border};border-radius:16px;
        padding:16px 16px 12px 16px;position:relative;overflow:hidden;
        backdrop-filter:blur(10px);transition:all 0.25s ease;">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:8px;">
            <span style="font-size:0.68rem;font-weight:700;color:rgba(255,255,255,0.55);
                text-transform:uppercase;letter-spacing:0.07em;line-height:1.3;">{icon}&nbsp;{label}</span>
            {trend_html}
        </div>
        <div style="font-size:1.65rem;font-weight:800;line-height:1.0;
            background:linear-gradient(135deg, {c2}, {c1});
            -webkit-background-clip:text;-webkit-text-fill-color:transparent;
            background-clip:text;letter-spacing:-0.02em;">{value}</div>
        {sub_html}
        {spark_html}
        <div style="position:absolute;top:-10px;right:-10px;width:90px;height:90px;
            background:radial-gradient(circle, {c1}44 0%, transparent 70%);
            pointer-events:none;"></div>
    </div>"""

def render_kpi_row(cards):
    """Render a row of KPI cards from a list of dicts."""
    html = '<div class="sf-kpi-row" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:8px 0 16px 0;">'
    for c in cards:
        html += kpi_card(**c)
    html += '</div>'
    _ui(html)

def step_chip(n, label, state='pending'):
    """Render a numbered step indicator.
    state: 'done' (green check), 'active' (purple glow), 'pending' (muted)
    """
    if state == 'done':
        bg, border, num_bg, txt = 'rgba(34,197,94,0.10)', 'rgba(34,197,94,0.30)', '#22c55e', '#f0f4ff'
        num = '✓'
    elif state == 'active':
        bg, border, num_bg, txt = 'rgba(168,85,247,0.15)', 'rgba(168,85,247,0.45)', '#a855f7', '#ffffff'
        num = str(n)
    else:
        bg, border, num_bg, txt = 'rgba(255,255,255,0.03)', 'rgba(255,255,255,0.08)', 'rgba(255,255,255,0.10)', 'rgba(255,255,255,0.45)'
        num = str(n)
    glow = 'box-shadow:0 0 12px rgba(168,85,247,0.30);' if state == 'active' else ''
    return f"""<div style="display:inline-flex;align-items:center;gap:8px;
        padding:7px 14px 7px 7px;border-radius:999px;
        background:{bg};border:1px solid {border};{glow}">
        <span style="display:inline-flex;align-items:center;justify-content:center;
            width:22px;height:22px;border-radius:50%;background:{num_bg};
            color:#fff;font-size:0.75rem;font-weight:700;">{num}</span>
        <span style="font-size:0.82rem;font-weight:500;color:{txt};">{label}</span>
    </div>"""

def render_steps(steps, active_index=None):
    """Render a horizontal step indicator row.
    steps: list of label strings
    active_index: which step is currently active (0-based). All prior are 'done'.
    """
    html = '<div class="sf-steps" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:4px 0 18px 0;">'
    for i, label in enumerate(steps):
        if active_index is None:
            state = 'pending'
        elif i < active_index:
            state = 'done'
        elif i == active_index:
            state = 'active'
        else:
            state = 'pending'
        html += step_chip(i + 1, label, state)
        if i < len(steps) - 1:
            html += '<span style="color:rgba(255,255,255,0.20);font-size:0.8rem;">→</span>'
    html += '</div>'
    _ui(html)

def section_header(icon, title, subtitle=None, accent='purple'):
    """Render a section header with gradient icon circle + accent bar.
    accent: 'purple' | 'green' | 'indigo' | 'pink' | 'cyan'
    """
    accents = {
        'purple': ('linear-gradient(180deg, #6366f1, #a855f7, #ec4899)', 'sf-icon-circle-purple', 'rgba(168,85,247,0.45)'),
        'green':  ('linear-gradient(180deg, #10b981, #22c55e, #4ade80)', 'sf-icon-circle-green',  'rgba(34,197,94,0.40)'),
        'indigo': ('linear-gradient(180deg, #4f46e5, #6366f1, #818cf8)', 'sf-icon-circle-indigo', 'rgba(99,102,241,0.40)'),
        'pink':   ('linear-gradient(180deg, #db2777, #ec4899, #f472b6)', 'sf-icon-circle-pink',   'rgba(236,72,153,0.40)'),
        'cyan':   ('linear-gradient(180deg, #0891b2, #06b6d4, #22d3ee)', 'sf-icon-circle-cyan',   'rgba(6,182,212,0.40)'),
    }
    bar, circle_cls, glow = accents.get(accent, accents['purple'])
    sub_html = f'<div style="font-size:0.87rem;color:rgba(255,255,255,0.55);margin-top:5px;line-height:1.55;">{subtitle}</div>' if subtitle else ''
    return _ui(f"""<div style="display:flex;align-items:flex-start;gap:16px;margin:8px 0 20px 0;padding:16px 18px;
        background:linear-gradient(135deg,rgba(255,255,255,0.025),rgba(255,255,255,0.01));
        border:1px solid rgba(255,255,255,0.06);border-radius:18px;
        backdrop-filter:blur(8px);position:relative;overflow:hidden;">
        <div style="position:absolute;left:0;top:0;bottom:0;width:4px;border-radius:4px 0 0 4px;
            background:{bar};box-shadow:0 0 16px {glow};"></div>
        <div class="sf-icon-circle {circle_cls}" style="margin-left:8px;">{icon}</div>
        <div style="flex:1;">
            <h2 style="margin:0 !important;padding:0 !important;font-size:1.50rem !important;
                font-weight:800 !important;letter-spacing:-0.018em;
                background:linear-gradient(135deg, #f0f4ff 0%, #c7d2fe 100%);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                background-clip:text;border:none !important;line-height:1.15;">{title}</h2>
            {sub_html}
        </div>
    </div>""")

def event_log_row(time_str, kind, message):
    """Render a single event-log row (for the live event feed)."""
    palettes = {
        'info':    ('#6366f1', 'rgba(99,102,241,0.08)'),
        'success': ('#22c55e', 'rgba(34,197,94,0.08)'),
        'warn':    ('#f59e0b', 'rgba(245,158,11,0.08)'),
        'error':   ('#ef4444', 'rgba(239,68,68,0.08)'),
        'api':     ('#a855f7', 'rgba(168,85,247,0.08)'),
    }
    color, bg = palettes.get(kind, palettes['info'])
    return f"""<div class="sf-event-row" style="
        display:flex;align-items:flex-start;gap:10px;padding:6px 10px;
        border-left:3px solid {color};background:{bg};
        border-radius:0 8px 8px 0;margin-bottom:4px;
        animation:sf-fade-in 0.4s ease;">
        <span style="font-family:monospace;font-size:0.72rem;color:rgba(255,255,255,0.40);
            min-width:55px;padding-top:1px;">{time_str}</span>
        <span style="font-size:0.85rem;color:rgba(255,255,255,0.85);line-height:1.4;flex:1;">{message}</span>
    </div>"""

def empty_state(icon, title, hint):
    """Render an illustrated empty state card."""
    return _ui(f"""<div style="text-align:center;padding:44px 20px;
        background:linear-gradient(135deg, rgba(168,85,247,0.05), rgba(99,102,241,0.03));
        border:1px dashed rgba(168,85,247,0.22);border-radius:18px;margin:12px 0;
        backdrop-filter:blur(6px);">
        <div style="font-size:3.8rem;line-height:1;margin-bottom:14px;
            filter:drop-shadow(0 0 22px rgba(168,85,247,0.40));">{icon}</div>
        <div style="font-size:1.08rem;font-weight:700;color:#f0f4ff;margin-bottom:8px;
            letter-spacing:-0.01em;">{title}</div>
        <div style="font-size:0.85rem;color:rgba(255,255,255,0.50);max-width:380px;
            margin:0 auto;line-height:1.55;">{hint}</div>
    </div>""")

def divider(title=None):
    """Render a premium gradient ruled divider with optional centered title badge."""
    if title:
        _ui(f'<div class="sf-divider"><span class="sf-divider-label">{title}</span></div>')
    else:
        _ui('<div class="sf-divider"></div>')

def tip_card(body, icon='💡', kind='info'):
    """Render a glassmorphic tip / info callout card.
    kind: 'info' (purple) | 'success' (green) | 'warn' (amber) | 'error' (red)
    """
    palettes = {
        'info':    ('#a855f7', 'rgba(99,102,241,0.08)', 'rgba(168,85,247,0.22)'),
        'success': ('#22c55e', 'rgba(34,197,94,0.08)',  'rgba(34,197,94,0.22)'),
        'warn':    ('#f59e0b', 'rgba(245,158,11,0.08)', 'rgba(245,158,11,0.22)'),
        'error':   ('#ef4444', 'rgba(239,68,68,0.08)',  'rgba(239,68,68,0.22)'),
    }
    stripe, bg, border = palettes.get(kind, palettes['info'])
    _ui(f"""<div class="sf-tip-card" style="border-left-color:{stripe};background:{bg};border-color:{border};">
        <span class="sf-tip-card-icon">{icon}</span>
        <span class="sf-tip-card-body">{body}</span>
    </div>""")

# Extra CSS for components (pulse animation + hover states)
st.markdown("""<style>
@keyframes sf-pulse-dot {
    0%, 100% { transform: scale(1); opacity: 1; }
    50%      { transform: scale(1.35); opacity: 0.65; }
}
.sf-dot.sf-pulse { animation: sf-pulse-dot 1.8s ease-in-out infinite; }

@keyframes sf-fade-in {
    from { opacity: 0; transform: translateY(-4px); }
    to   { opacity: 1; transform: translateY(0); }
}

.sf-kpi-card:hover {
    transform: translateY(-3px);
    border-color: rgba(168,85,247,0.50) !important;
    box-shadow: 0 8px 24px rgba(168,85,247,0.15);
}

.sf-hero {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
    padding: 18px 24px;
    margin: 0 0 22px 0;
    background:
        radial-gradient(circle at 0% 50%, rgba(99,102,241,0.10) 0%, transparent 60%),
        radial-gradient(circle at 100% 50%, rgba(236,72,153,0.08) 0%, transparent 60%),
        linear-gradient(135deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 20px;
    backdrop-filter: blur(10px);
    position: relative;
    overflow: hidden;
}
.sf-hero::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #6366f1, #a855f7, #ec4899, #f472b6);
    background-size: 200% 100%;
    animation: sf-shimmer 4s linear infinite;
}
@keyframes sf-shimmer {
    0%   { background-position: 200% 0; }
    100% { background-position: -200% 0; }
}
.sf-hero-logo {
    font-size: 2.6rem;
    line-height: 1;
    filter: drop-shadow(0 0 18px rgba(168,85,247,0.55));
    animation: sf-logo-bob 4s ease-in-out infinite;
}
@keyframes sf-logo-bob {
    0%, 100% { transform: translateY(0) rotate(0); }
    50%      { transform: translateY(-3px) rotate(-3deg); }
}
.sf-hero-title {
    font-size: 1.85rem;
    font-weight: 800;
    letter-spacing: -0.025em;
    background: linear-gradient(135deg, #818cf8 0%, #c084fc 50%, #f472b6 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1.1;
    margin: 0;
}
.sf-hero-tagline {
    font-size: 0.82rem;
    color: rgba(255,255,255,0.55);
    margin-top: 4px;
    letter-spacing: 0.01em;
}
.sf-hero-status {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: flex-end;
}
.sf-sidebar-brand {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 4px 16px 4px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    margin-bottom: 12px;
}
.sf-sidebar-brand-logo {
    font-size: 1.55rem;
    filter: drop-shadow(0 0 14px rgba(168,85,247,0.65));
}
.sf-sidebar-brand-name {
    font-size: 1.05rem;
    font-weight: 700;
    background: linear-gradient(135deg, #c084fc, #f472b6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: -0.01em;
}
.sf-sidebar-version {
    font-size: 0.68rem;
    color: rgba(255,255,255,0.40);
    background: rgba(255,255,255,0.04);
    padding: 2px 8px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,0.06);
}
.sf-conn-card {
    padding: 14px;
    border-radius: 14px;
    background: rgba(255,255,255,0.025);
    border: 1px solid rgba(255,255,255,0.06);
    transition: border-color 0.3s ease, background 0.3s ease;
    margin-bottom: 8px;
}
/* .sf-conn-card.active is handled by Wave 5 CSS with pulsing ring animation */
.sf-conn-icon {
    font-size: 1.4rem;
    margin-right: 6px;
}
.sf-event-feed {
    max-height: 240px;
    overflow-y: auto;
    padding-right: 6px;
}
.sf-event-feed::-webkit-scrollbar { width: 6px; }
.sf-event-feed::-webkit-scrollbar-thumb {
    background: rgba(168,85,247,0.25);
    border-radius: 999px;
}
.sf-livebar {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 12px 18px;
    border-radius: 14px;
    background: linear-gradient(135deg, rgba(239,68,68,0.12), rgba(168,85,247,0.10));
    border: 1px solid rgba(239,68,68,0.30);
    margin-bottom: 14px;
    position: relative;
    overflow: hidden;
}
.sf-livebar::before {
    content: "";
    position: absolute;
    inset: 0;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.05), transparent);
    animation: sf-livebar-sweep 3s linear infinite;
}
@keyframes sf-livebar-sweep {
    0%   { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}
.sf-live-dot {
    width: 10px; height: 10px; border-radius: 50%;
    background: #ef4444;
    box-shadow: 0 0 12px #ef4444, 0 0 24px rgba(239,68,68,0.5);
    animation: sf-pulse-dot 1.2s ease-in-out infinite;
}
.sf-api-row {
    display: flex; gap: 8px; flex-wrap: wrap;
    margin: 10px 0;
}
.sf-api-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 5px 10px; border-radius: 999px;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    font-size: 0.78rem; color: rgba(255,255,255,0.75);
}
.sf-api-chip.active {
    background: linear-gradient(135deg, rgba(99,102,241,0.18), rgba(168,85,247,0.12));
    border-color: rgba(168,85,247,0.45);
    color: #fff;
}
</style>""", unsafe_allow_html=True)

# ====================================================================
# 🚀 HERO HEADER (Phase 2) — replaces plain st.title
# ====================================================================
# Render placeholder; the dynamic status pills get injected after
# session state is initialized (a few lines below).
_hero_placeholder = st.empty()

def _render_hero():
    """Render the hero header. Called once after session state is set up."""
    sf_connected = bool(st.session_state.get('sf'))
    snow_connected = bool(st.session_state.get('sf_conn'))
    sf_pill = status_pill(
        'Salesforce',
        state='active' if sf_connected else 'idle',
        pulse=sf_connected, size='sm'
    )
    snow_pill = status_pill(
        'Snowflake',
        state='active' if snow_connected else 'idle',
        pulse=snow_connected, size='sm'
    )
    session_rows = st.session_state.get('_session_total_rows', 0)
    rows_pill = status_pill(
        f'{session_rows:,} rows this session',
        state='info' if session_rows else 'idle',
        pulse=False, size='sm'
    )
    _hero_placeholder.markdown(f"""
    <div class="sf-hero">
        <div style="display:flex;align-items:center;gap:16px;">
            <div class="sf-hero-logo">⚡️</div>
            <div>
                <div class="sf-hero-title">TAVANT MIGRATION APP</div>
                <div class="sf-hero-tagline">Bulk · Multi-API · Streaming · Snowflake-native</div>
            </div>
        </div>
        <div class="sf-hero-status">
            {sf_pill}{snow_pill}{rows_pill}
            <span onclick="(window.__sfOpenCmdk || window.top.__sfOpenCmdk || (window.parent && window.parent.__sfOpenCmdk) || function(){{}})();" 
                style="display:inline-flex;align-items:center;gap:6px;padding:5px 11px;
                border-radius:999px;background:rgba(168,85,247,0.10);
                border:1px solid rgba(168,85,247,0.30);
                font-size:0.74rem;color:rgba(240,244,255,0.85);
                cursor:pointer;transition:all 0.18s ease;font-weight:500;"
                onmouseover="this.style.background='rgba(168,85,247,0.18)';this.style.borderColor='rgba(168,85,247,0.55)';"
                onmouseout="this.style.background='rgba(168,85,247,0.10)';this.style.borderColor='rgba(168,85,247,0.30)';"
                title="Open command palette">
                <span style="font-size:0.78rem;">🔍</span>
                <kbd style="background:rgba(255,255,255,0.08);border:1px solid rgba(255,255,255,0.12);
                    padding:1px 5px;border-radius:4px;font-size:0.66rem;
                    font-family:Inter, monospace;color:#f0f4ff;">Ctrl+/</kbd>
                <span style="color:rgba(255,255,255,0.55);">Search</span>
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# -------------------------------------------------

# Initialize Session State (must be before any access)

# -------------------------------------------------

if 'sf' not in st.session_state:

    st.session_state.sf = None

if 'sf_conn' not in st.session_state:

    st.session_state.sf_conn = None

# Load credentials from disk ONCE on startup; reuse in-memory on every rerun

if '_saved_creds' not in st.session_state:

    st.session_state._saved_creds = load_saved_credentials()

if '_saved_sf_creds' not in st.session_state:

    st.session_state._saved_sf_creds = load_saved_snowflake_credentials()

if '_saved_sf_configs' not in st.session_state:

    _cfg_path = os.path.join(os.path.dirname(__file__), 'saved_sf_to_snowflake_configs.json')

    st.session_state._saved_sf_configs = json.load(open(_cfg_path)) if os.path.exists(_cfg_path) else {}

# Session-level rolling counters (used by the hero header)
if '_session_total_rows' not in st.session_state:
    st.session_state._session_total_rows = 0

# Show one acknowledgement dialog for the most recent saved-item action.
render_action_feedback()

# Render the premium hero header now that session state exists
_render_hero()

# -------------------------------------------------

# Sidebar — Premium Brand Header + Connection Mode Selector

# -------------------------------------------------

# Brand lockup at top of sidebar
st.sidebar.markdown("""
<div class="sf-sidebar-brand">
    <span class="sf-sidebar-brand-logo" style="animation:sf-logo-bob 4s ease-in-out infinite;">⚡️</span>
    <span class="sf-sidebar-brand-name">TAVANT MIGRATION APP</span>
    <span class="sf-sidebar-version" style="margin-left:auto;">v2.1</span>
</div>
""", unsafe_allow_html=True)

# Active-connection summary tiles
_sf_active = bool(st.session_state.get('sf'))
_snow_active = bool(st.session_state.get('sf_conn'))
st.sidebar.markdown(f"""
<div class="sf-sidebar-section">
    <div class="sf-sidebar-section-title">Connection Status</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
        <div class="sf-conn-card {'active' if _sf_active else ''}" style="padding:11px 10px;text-align:center;">
            <div style="font-size:1.4rem;line-height:1;filter:{'drop-shadow(0 0 10px rgba(34,197,94,0.6))' if _sf_active else 'none'}">🔌</div>
            <div style="font-size:0.70rem;font-weight:700;color:rgba(255,255,255,0.80);margin-top:5px;letter-spacing:0.02em;">Salesforce</div>
            <div style="font-size:0.62rem;font-weight:700;margin-top:3px;
                color:{'#4ade80' if _sf_active else 'rgba(255,255,255,0.35)'};">
                {'● LIVE' if _sf_active else '○ OFFLINE'}
            </div>
        </div>
        <div class="sf-conn-card {'active' if _snow_active else ''}" style="padding:11px 10px;text-align:center;">
            <div style="font-size:1.4rem;line-height:1;filter:{'drop-shadow(0 0 10px rgba(34,197,94,0.6))' if _snow_active else 'none'}">❄️</div>
            <div style="font-size:0.70rem;font-weight:700;color:rgba(255,255,255,0.80);margin-top:5px;letter-spacing:0.02em;">Snowflake</div>
            <div style="font-size:0.62rem;font-weight:700;margin-top:3px;
                color:{'#4ade80' if _snow_active else 'rgba(255,255,255,0.35)'};">
                {'● LIVE' if _snow_active else '○ OFFLINE'}
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

sidebar_mode = st.sidebar.radio(

    'Connection Type',

    ['🔌 Salesforce', '❄️ Snowflake'],

    key='sidebar_mode',

    horizontal=False

)

st.sidebar.markdown('---')

# -------------------------------------------------

# Sidebar — Salesforce Connection

# -------------------------------------------------

if sidebar_mode == '🔌 Salesforce':

    st.sidebar.header('Salesforce Connection')

    # Use in-memory cache (loaded once at startup)

    saved_creds = st.session_state._saved_creds

    cred_names = list(saved_creds.keys())

    # Credential selector

    selected_cred = st.sidebar.selectbox(

        '📋 Saved Credentials',

        ['-- New --'] + cred_names,

        key='cred_selector'

    )

    # When the selection changes, push the new values into session state so the
    # text_input widgets (which are keyed and ignore `value=` after first render)
    # actually reflect the chosen credential.
    if st.session_state.get('_prev_cred_selector') != selected_cred:

        st.session_state['_prev_cred_selector'] = selected_cred

        if selected_cred != '-- New --' and selected_cred in saved_creds:

            cred = saved_creds[selected_cred]

            st.session_state['cred_name_input'] = selected_cred

            st.session_state['sf_user']   = cred.get('username', '')

            st.session_state['sf_pass']   = cred.get('password', '')

            st.session_state['sf_token']  = cred.get('security_token', '')

            st.session_state['sf_domain'] = cred.get('domain', 'test')

        else:

            st.session_state['cred_name_input'] = ''

            st.session_state['sf_user']   = ''

            st.session_state['sf_pass']   = ''

            st.session_state['sf_token']  = ''

            st.session_state['sf_domain'] = 'test'

    sf_username = st.sidebar.text_input('Username', key='sf_user')

    sf_password = st.sidebar.text_input('Password', type='password', key='sf_pass')

    sf_token    = st.sidebar.text_input('Security Token', type='password', key='sf_token')

    _domain_opts = ['test', 'login']

    _domain_val  = st.session_state.get('sf_domain', 'test')

    sf_domain   = st.sidebar.selectbox('Domain', _domain_opts,
                                        index=_domain_opts.index(_domain_val) if _domain_val in _domain_opts else 0,
                                        key='sf_domain')

    # Save / Delete buttons

    st.sidebar.markdown('---')

    cred_name = st.sidebar.text_input('Credential Name', placeholder='e.g. DTNA UAT', key='cred_name_input')

    save_col, edit_col, del_col = st.sidebar.columns(3)

    with save_col:

        if st.button('💾 Save', key='save_cred', use_container_width=True):

            if cred_name.strip():

                saved_creds[cred_name.strip()] = {

                    'username': sf_username,

                    'password': sf_password,

                    'security_token': sf_token,

                    'domain': sf_domain

                }

                save_credentials(saved_creds)

                st.session_state._saved_creds = saved_creds

                st.session_state['cred_msg'] = f'✅ Saved: {cred_name.strip()}'
                queue_action_feedback('Salesforce credentials saved', f'Saved credential "{cred_name.strip()}".')

                st.rerun()

            else:

                st.toast('Enter a name')

    with edit_col:

        if st.button('✏️ Edit', key='edit_cred', use_container_width=True,
                     disabled=selected_cred == '-- New --'):

            saved_creds[selected_cred] = {

                'username': sf_username,

                'password': sf_password,

                'security_token': sf_token,

                'domain': sf_domain

            }

            save_credentials(saved_creds)

            st.session_state._saved_creds = saved_creds

            st.session_state['cred_msg'] = f'✏️ Updated: {selected_cred}'
            queue_action_feedback('Salesforce credentials updated', f'Updated credential "{selected_cred}".')

            st.rerun()

    with del_col:

        if selected_cred != '-- New --':

            st.write('')  # spacer

            st.write('')  # spacer

            if st.button(f'🗑️ Delete', key='del_cred', use_container_width=True):
                st.session_state['_cred_del_confirm'] = selected_cred
                st.rerun()

    if st.session_state.get('_cred_del_confirm'):
        _cred_to_del = st.session_state['_cred_del_confirm']
        st.sidebar.warning(f'⚠️ Delete credential **{_cred_to_del}**?')
        _cdc1, _cdc2 = st.sidebar.columns(2)
        with _cdc1:
            if st.button('✅ Yes', key='cred_del_yes', use_container_width=True):
                del saved_creds[_cred_to_del]
                save_credentials(saved_creds)
                st.session_state._saved_creds = saved_creds
                st.session_state.pop('_cred_del_confirm', None)
                st.session_state['cred_msg'] = f'🗑️ Deleted: {_cred_to_del}'
                queue_action_feedback('Salesforce credential deleted', f'Deleted credential "{_cred_to_del}".')
                st.rerun()
        with _cdc2:
            if st.button('❌ No', key='cred_del_no', use_container_width=True):
                st.session_state.pop('_cred_del_confirm', None)
                st.rerun()

    # Show save/delete message after rerun

    if 'cred_msg' in st.session_state:

        _cred_msg = st.session_state.pop('cred_msg')

        show_feedback('success', _cred_msg, success_fn=st.sidebar.success)

    st.sidebar.markdown('---')

    if 'sf' not in st.session_state:

        st.session_state.sf = None

    if st.sidebar.button('⚡️ Connect', type='primary', key='sf_connect_sidebar'):

        try:

            from simple_salesforce import Salesforce  # deferred: only imported on click

            st.session_state.sf = Salesforce(

                username=sf_username,

                password=sf_password,

                security_token=sf_token,

                domain=sf_domain

            )

            st.sidebar.success(f'Connected to {st.session_state.sf.sf_instance}')

        except Exception as e:

            st.sidebar.error(f'Connection failed: {e}')

    if st.session_state.sf:

        st.sidebar.success(f'Active: {st.session_state.sf.sf_instance}')

# -------------------------------------------------

# Sidebar — Snowflake Connection

# -------------------------------------------------

else:  # sidebar_mode == '❄️ Snowflake'

    if not HAS_SNOWFLAKE:

        st.sidebar.error("❌ Snowflake not installed")

        st.sidebar.info("Install with:\n```pip install snowflake-connector-python cryptography```")

    else:

        st.sidebar.header('Snowflake Connection')

        

        # Use in-memory cache (loaded once at startup)

        saved_sf_creds = st.session_state._saved_sf_creds

        sf_cred_names = list(saved_sf_creds.keys())

        

        # Credential selector

        selected_sf_cred = st.sidebar.selectbox(

            '📋 Saved Credentials',

            ['-- New --'] + sf_cred_names,

            key='sf_cred_selector_sidebar'

        )

        # When selection changes, push values into session state so keyed widgets update

        if st.session_state.get('_prev_sf_cred_selector') != selected_sf_cred:

            st.session_state['_prev_sf_cred_selector'] = selected_sf_cred

            if selected_sf_cred != '-- New --' and selected_sf_cred in saved_sf_creds:

                cred = saved_sf_creds[selected_sf_cred]

                st.session_state['sf_cred_name_sidebar'] = selected_sf_cred

                st.session_state['sf_user_sidebar']      = cred.get('user', 'NEXTGEN_DEV_SA')

                st.session_state['sf_account_sidebar']   = cred.get('account', 'PEB93217')

                st.session_state['sf_warehouse_sidebar'] = cred.get('warehouse', 'ssz_nextgen_adhoc_wh')

                st.session_state['sf_database_sidebar']  = cred.get('database', 'SSZ_NEXTGEN_DB')

                st.session_state['sf_schema_sidebar']    = cred.get('schema', 'RAW_OWL_QA')

                st.session_state['sf_role_sidebar']      = cred.get('role', 'SSZ_NEXTGEN_FR')

                st.session_state['sf_auth_sidebar']      = cred.get('auth_method', 'Private Key')

                st.session_state['sf_key_path_sidebar']  = cred.get('private_key_path', '')

            else:

                st.session_state['sf_cred_name_sidebar'] = ''

                st.session_state['sf_user_sidebar']      = 'NEXTGEN_DEV_SA'

                st.session_state['sf_account_sidebar']   = 'PEB93217'

                st.session_state['sf_warehouse_sidebar'] = 'ssz_nextgen_adhoc_wh'

                st.session_state['sf_database_sidebar']  = 'SSZ_NEXTGEN_DB'

                st.session_state['sf_schema_sidebar']    = 'RAW_OWL_QA'

                st.session_state['sf_role_sidebar']      = 'SSZ_NEXTGEN_FR'

                st.session_state['sf_auth_sidebar']      = 'Private Key'

                st.session_state['sf_key_path_sidebar']  = ''

        sf_user_sidebar      = st.sidebar.text_input('User', key='sf_user_sidebar')

        sf_account_sidebar   = st.sidebar.text_input('Account', key='sf_account_sidebar')

        sf_warehouse_sidebar = st.sidebar.text_input('Warehouse', key='sf_warehouse_sidebar')

        sf_database_sidebar  = st.sidebar.text_input('Database', key='sf_database_sidebar')

        sf_schema_sidebar    = st.sidebar.text_input('Schema', key='sf_schema_sidebar')

        sf_role_sidebar      = st.sidebar.text_input('Role', key='sf_role_sidebar')

        

        # Authentication

        _auth_opts = ['Private Key', 'Password']

        _auth_val  = st.session_state.get('sf_auth_sidebar', 'Private Key')

        auth_method_sidebar = st.sidebar.radio(

            'Auth Method',

            _auth_opts,

            index=_auth_opts.index(_auth_val) if _auth_val in _auth_opts else 0,

            key='sf_auth_sidebar'

        )

        

        if auth_method_sidebar == 'Private Key':

            private_key_path_sidebar = st.sidebar.text_input(

                'Private Key Path',

                key='sf_key_path_sidebar'

            )

            sf_password_sidebar = None

        else:

            sf_password_sidebar = st.sidebar.text_input('Password', type='password', key='sf_password_sidebar')

            private_key_path_sidebar = None

        

        # Save credentials

        st.sidebar.markdown('---')

        sf_cred_name_sidebar = st.sidebar.text_input('Credential Name', placeholder='e.g. UAT', key='sf_cred_name_sidebar')

        

        sf_save_col, sf_edit_col, sf_del_col = st.sidebar.columns(3)

        with sf_save_col:

            if st.button('💾 Save', key='save_sf_cred_sidebar', use_container_width=True):

                if sf_cred_name_sidebar.strip():

                    saved_sf_creds[sf_cred_name_sidebar.strip()] = {

                        'user': sf_user_sidebar,

                        'account': sf_account_sidebar,

                        'warehouse': sf_warehouse_sidebar,

                        'database': sf_database_sidebar,

                        'schema': sf_schema_sidebar,

                        'role': sf_role_sidebar,

                        'auth_method': auth_method_sidebar,

                        'private_key_path': private_key_path_sidebar if auth_method_sidebar == 'Private Key' else ''

                    }

                    save_snowflake_credentials(saved_sf_creds)

                    st.session_state._saved_sf_creds = saved_sf_creds

                    st.session_state['sf_cred_msg_sidebar'] = f'✅ Saved: {sf_cred_name_sidebar.strip()}'
                    queue_action_feedback('Snowflake credentials saved', f'Saved credential "{sf_cred_name_sidebar.strip()}".')

                    st.rerun()

                else:

                    st.toast('Enter a credential name first')

        with sf_edit_col:

            if st.button('✏️ Edit', key='edit_sf_cred_sidebar', use_container_width=True,
                         disabled=selected_sf_cred == '-- New --'):

                saved_sf_creds[selected_sf_cred] = {

                    'user': sf_user_sidebar,

                    'account': sf_account_sidebar,

                    'warehouse': sf_warehouse_sidebar,

                    'database': sf_database_sidebar,

                    'schema': sf_schema_sidebar,

                    'role': sf_role_sidebar,

                    'auth_method': auth_method_sidebar,

                    'private_key_path': private_key_path_sidebar if auth_method_sidebar == 'Private Key' else ''

                }

                save_snowflake_credentials(saved_sf_creds)

                st.session_state._saved_sf_creds = saved_sf_creds

                st.session_state['sf_cred_msg_sidebar'] = f'✏️ Updated: {selected_sf_cred}'
                queue_action_feedback('Snowflake credentials updated', f'Updated credential "{selected_sf_cred}".')

                st.rerun()

        with sf_del_col:

            if selected_sf_cred != '-- New --':

                if st.sidebar.button('🗑️ Delete', key='del_sf_cred_sidebar', use_container_width=True):

                    del saved_sf_creds[selected_sf_cred]

                    save_snowflake_credentials(saved_sf_creds)

                    st.session_state._saved_sf_creds = saved_sf_creds

                    st.session_state['sf_cred_msg_sidebar'] = f'🗑️ Deleted: {selected_sf_cred}'
                    queue_action_feedback('Snowflake credential deleted', f'Deleted credential "{selected_sf_cred}".')

                    st.rerun()

        

        if 'sf_cred_msg_sidebar' in st.session_state:

            _sf_cred_msg = st.session_state.pop('sf_cred_msg_sidebar')

            show_feedback('success', _sf_cred_msg, success_fn=st.sidebar.success)

        

        st.sidebar.markdown('---')

        

        # Initialize connection

        if 'sf_conn' not in st.session_state:

            st.session_state.sf_conn = None

        

        if st.sidebar.button('❄️ Connect', type='primary', key='sf_connect_btn_sidebar'):

            try:

                with st.spinner('Connecting...'):

                    import snowflake.connector  # deferred: only imported on click

                    if auth_method_sidebar == 'Private Key':

                        private_key = load_private_key(private_key_path_sidebar)

                        if private_key:

                            st.session_state.sf_conn = snowflake.connector.connect(

                                user=sf_user_sidebar,

                                account=sf_account_sidebar,

                                private_key=private_key,

                                warehouse=sf_warehouse_sidebar,

                                database=sf_database_sidebar,

                                schema=sf_schema_sidebar,

                                role=sf_role_sidebar

                            )

                            st.sidebar.success(f'✅ Connected: {sf_database_sidebar}.{sf_schema_sidebar}')

                    else:

                        st.session_state.sf_conn = snowflake.connector.connect(

                            user=sf_user_sidebar,

                            password=sf_password_sidebar,

                            account=sf_account_sidebar,

                            warehouse=sf_warehouse_sidebar,

                            database=sf_database_sidebar,

                            schema=sf_schema_sidebar,

                            role=sf_role_sidebar

                        )

                        st.sidebar.success(f'✅ Connected: {sf_database_sidebar}.{sf_schema_sidebar}')

            except Exception as e:

                st.sidebar.error(f'❌ Failed: {str(e)[:100]}')

        

        if st.session_state.sf_conn:

            try:

                st.sidebar.success(f'✅ Active: {sf_database_sidebar}.{sf_schema_sidebar}')

            except:

                st.sidebar.success('✅ Snowflake Connected')

# -------------------------------------------------
# Sidebar — Job History (premium recent-runs panel)
# -------------------------------------------------
_history = load_job_history()
import json as _json
_history_json  = _json.dumps(list(reversed(_history[-20:]))) if _history else '[]'
_today = time.strftime('%Y-%m-%d')
_today_runs_json = _json.dumps([r for r in _history if r.get('ts','').startswith(_today)]) if _history else '[]'

# Single components.html for both Today's Activity + Recent Runs
# (one iframe = consistent dark background, no visibility/transparency issues)
with st.sidebar:
    components.html(f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
html,body{{margin:0;padding:0;background:#0d1117;font-family:Inter,system-ui,sans-serif;color:#f0f4ff;}}

/* ---- Today's Activity ---- */
.sfa{{
    background:rgba(255,255,255,0.04);
    border:1px solid rgba(255,255,255,0.09);
    border-radius:12px;padding:12px 14px;margin-bottom:10px;
}}
.sec-title{{font-size:0.67rem;font-weight:700;color:rgba(255,255,255,0.42);
    text-transform:uppercase;letter-spacing:0.08em;margin-bottom:9px;}}
.sfa-nums{{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:8px;}}
.sfa-big{{font-size:1.4rem;font-weight:800;line-height:1;letter-spacing:-0.02em;
    background:linear-gradient(135deg,#c084fc,#f472b6);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}}
.sfa-sub{{font-size:0.60rem;color:rgba(255,255,255,0.38);margin-top:3px;}}
.sfa-right{{text-align:right;}}
.sfa-right-num{{font-size:0.95rem;font-weight:700;color:#818cf8;}}
.sfa-right-lbl{{font-size:0.60rem;color:rgba(255,255,255,0.38);margin-top:3px;}}
.sfa-pills{{display:flex;flex-wrap:wrap;gap:4px;}}
.sfa-pill{{display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;
    font-size:0.61rem;font-weight:700;white-space:nowrap;}}
.sfa-none{{font-size:0.66rem;color:rgba(255,255,255,0.28);}}

/* ---- Recent Runs ---- */
.sfr-hdr{{display:flex;align-items:center;justify-content:space-between;margin-bottom:7px;}}
.sfr-badge{{font-size:0.60rem;padding:2px 9px;border-radius:999px;font-weight:700;
    background:rgba(168,85,247,0.22);border:1px solid rgba(168,85,247,0.42);color:#c084fc;}}
.sfr-card{{border-radius:10px;background:rgba(255,255,255,0.03);
    border:1px solid rgba(255,255,255,0.07);margin-bottom:5px;cursor:pointer;
    transition:border-color 0.15s,background 0.15s;overflow:hidden;}}
.sfr-card:hover{{background:rgba(168,85,247,0.07);border-color:rgba(168,85,247,0.28);}}
.sfr-card.open{{border-color:rgba(168,85,247,0.38);background:rgba(168,85,247,0.06);}}
.sfr-sum{{padding:8px 10px;display:flex;flex-direction:column;gap:2px;}}
.sfr-r1{{display:flex;align-items:center;gap:6px;}}
.sfr-dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0;}}
.sfr-op{{font-size:0.75rem;font-weight:700;color:#f0f4ff;}}
.sfr-ts{{font-size:0.63rem;color:rgba(255,255,255,0.36);margin-left:auto;font-family:monospace;}}
.sfr-chev{{font-size:0.58rem;color:rgba(255,255,255,0.22);margin-left:4px;transition:transform 0.2s;}}
.sfr-card.open .sfr-chev{{transform:rotate(180deg);}}
.sfr-obj{{font-size:0.69rem;color:rgba(255,255,255,0.52);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.sfr-meta{{font-size:0.63rem;color:rgba(255,255,255,0.32);}}
.sfr-det{{display:none;padding:0 10px 10px;border-top:1px solid rgba(255,255,255,0.06);}}
.sfr-card.open .sfr-det{{display:block;}}
.sfr-grid{{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-top:8px;}}
.sfr-kpi{{padding:7px 9px;border-radius:8px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.07);}}
.sfr-kpi.full{{grid-column:1/-1;}}
.sfr-klbl{{font-size:0.57rem;color:rgba(255,255,255,0.36);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:3px;}}
.sfr-kval{{font-size:0.90rem;font-weight:800;line-height:1;}}
.sfr-bar{{height:3px;border-radius:999px;background:rgba(255,255,255,0.08);margin-top:5px;}}
.sfr-fill{{height:3px;border-radius:999px;}}
.sfr-irow{{display:flex;align-items:center;gap:5px;font-size:0.61rem;color:rgba(255,255,255,0.32);
    margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,0.05);flex-wrap:wrap;}}
.sfr-ival{{color:rgba(255,255,255,0.52);font-weight:600;}}
.sfr-empty{{text-align:center;padding:18px 10px;color:rgba(255,255,255,0.32);font-size:0.73rem;line-height:1.5;}}
html[data-sf-theme] {{background:var(--activity-bg);color:var(--activity-text);}}
html[data-sf-theme] body {{background:var(--activity-bg);color:var(--activity-text);}}
html[data-sf-theme] :is(.sfa,.sfr-card,.sfr-kpi) {{background:var(--activity-panel);border-color:var(--activity-border);box-shadow:0 4px 14px rgba(0,0,0,0.06);}}
html[data-sf-theme] :is(.sec-title,.sfa-sub,.sfa-right-lbl,.sfa-none,.sfr-ts,.sfr-chev,.sfr-obj,.sfr-meta,.sfr-klbl,.sfr-irow,.sfr-ival,.sfr-empty) {{color:var(--activity-muted);}}
html[data-sf-theme] :is(.sfr-op,.sfr-kval,.sfa-big,.sfa-right-num,.sfa-pill,.sfr-badge) {{color:var(--activity-text)!important;-webkit-text-fill-color:currentColor;background-clip:border-box;}}
html[data-sf-theme] .sfa-big {{background:var(--activity-accent);background-clip:text;-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
html[data-sf-theme] .sfr-badge {{background:var(--sf-selected);border-color:var(--activity-border);}}
html[data-sf-theme] .sfr-card:is(:hover,.open) {{background:var(--activity-panel);border-color:var(--sf-accent);}}
</style>
</head>
<body>
<div id="sfa-root" class="sfa"></div>
<div id="sfr-root"></div>
<script>
(function(){{
    const ALL   = {_history_json};
    const TODAY = {_today_runs_json};
    const parentRoot = window.parent.document.documentElement;
    function syncActivityTheme() {{
        const theme = parentRoot.dataset.sfTheme || 'dark';
        const root = document.documentElement;
        const parentStyle = window.parent.getComputedStyle(parentRoot);
        ['--sf-surface','--sf-field','--sf-text','--sf-muted','--sf-border','--sf-accent',
         '--sf-selected','--sf-panel-wash','--sf-title-wash'].forEach(property => {{
            const value = parentStyle.getPropertyValue(property).trim();
            if (value) root.style.setProperty(property, value);
            else root.style.removeProperty(property);
        }});
        root.dataset.sfTheme = theme;
        root.style.setProperty('--activity-bg', 'transparent');
        root.style.setProperty('--activity-panel', 'var(--sf-panel-wash, #15232b)');
        root.style.setProperty('--activity-accent', 'var(--sf-title-wash, linear-gradient(#5eead4, #5eead4))');
        root.style.setProperty('--activity-text', 'var(--sf-text, #f0f4ff)');
        root.style.setProperty('--activity-muted', 'var(--sf-muted, #b2bdcd)');
        root.style.setProperty('--activity-border', 'var(--sf-border, #414b60)');
    }}
    syncActivityTheme();
    const themeObserver = new MutationObserver(syncActivityTheme);
    themeObserver.observe(parentRoot, {{attributes:true, attributeFilter:['data-sf-theme','data-sf-custom-color','style']}});
    window.addEventListener('pagehide', () => themeObserver.disconnect(), {{once:true}});
    const TAB_OPS  = ['Insert','Update','Upsert','Delete','Multi-Object','Snowflake','SF\u2192Snowflake','TestCase'];
    const REC_OPS  = ['insert','update','delete','multi-object','snowflake','sf\u2192snowflake','upsert'];
    const OP_COL   = {{
        'insert':'#22c55e','update':'#60a5fa','upsert':'#a78bfa','delete':'#f87171',
        'multi-object':'#c084fc','snowflake':'#22d3ee','sf\u2192snowflake':'#f472b6'
    }};

    function normOp(op){{
        const raw=(op||'').toLowerCase();
        let compact=raw.replace(/\\s+/g,'').replace(/->/g,'→').replace(/[-–—]/g,'→');
        if(compact==='sf→snowflake'||compact==='salesforce→snowflake'||raw==='sf to snowflake') return 'sf→snowflake';
        return raw;
    }}
    function tabAliases(tabKey){{
        return [tabKey];
    }}
    function oc(op){{ return OP_COL[normOp(op)]||'#94a3b8'; }}
    function fmt(s){{
        if(!s) return '0s'; s=parseFloat(s);
        if(s<60) return s.toFixed(1)+'s';
        const m=Math.floor(s/60),r=Math.round(s%60);
        return m+'m'+(r?' '+r+'s':'');
    }}
    function pct(ok,fail){{ const t=ok+fail; return t?(ok/t)*100:100; }}
    function sc(p){{ return p>=99.5?'#22c55e':p>=90?'#f59e0b':'#ef4444'; }}
    function spd(ok,s){{
        if(!s||s<0.01) return '\u2014';
        const r=ok/s; return r>=1000?(r/1000).toFixed(1)+'k/s':r.toFixed(1)+'/s';
    }}
    function resolveActiveTabOp(){{
        /* Walk up to window.top — sidebar runs in a nested iframe so
           window.parent is only one level up and may not reach the Streamlit tabs. */
        const frames = [window.top, window.parent, window];
        for(let i=0;i<frames.length;i++){{
            try{{
                const doc = frames[i].document;
                const tabs = Array.from(doc.querySelectorAll('[role="tab"]'));
                const activeIdx = tabs.findIndex(t => t.getAttribute('aria-selected') === 'true');
                if(activeIdx >= 0){{
                    const op = TAB_OPS[activeIdx] || 'Insert';
                    try{{ window.top.__sfActiveTabOp = op; }}catch(e){{}}
                    return op;
                }}
            }}catch(e){{}}
        }}
        /* Only use cached value if it is one of the known tab operations */
        try{{
            const cached = window.top.__sfActiveTabOp;
            if(cached && TAB_OPS.includes(cached)) return cached;
        }}catch(e){{}}
        return 'Insert';
    }}

    /* ---- Today's Activity ---- */
    function renderActivity(tabOp){{
        const tabKey = normOp(tabOp);
        const isRecording = REC_OPS.includes(tabKey);
        const aliases = tabAliases(tabKey);
        /* Tabs that don't record runs (TestCase, RecordTest) show zero — not all ops */
        const runs = isRecording
            ? TODAY.filter(r=>aliases.includes(normOp(r.op)))
            : [];
        const totalRows = runs.reduce(function(s,r){{return s+(r.success||0);}},0);
        const totalRuns = runs.length;
        const counts={{}};
        runs.forEach(function(r){{ const o=r.op||'Other'; counts[o]=(counts[o]||0)+1; }});
        const label = isRecording ? tabOp : tabOp+' (no history)';
        let pills='';
        Object.keys(counts).forEach(function(op){{
            const c=oc(op);
            pills+='<span class="sfa-pill" style="background:'+c+'1a;border:1px solid '+c+'44;color:'+c+';">'+op+'\u00b7'+counts[op]+'</span>';
        }});
        if(!pills) pills='<span class="sfa-none">'+(isRecording?'No activity yet':'No job history for this tab')+'</span>';
        document.getElementById('sfa-root').innerHTML=
            '<div class="sec-title">\U0001F4CA Today\u2019s Activity \u2014 '+label+'</div>'+
            '<div class="sfa-nums">'+
              '<div><div class="sfa-big">'+totalRows.toLocaleString()+'</div>'+
              '<div class="sfa-sub">rows loaded</div></div>'+
              '<div class="sfa-right"><div class="sfa-right-num">'+totalRuns+'</div>'+
              '<div class="sfa-right-lbl">run'+(totalRuns!==1?'s':'')+'</div></div>'+
            '</div>'+
            '<div class="sfa-pills">'+pills+'</div>';
    }}

    /* ---- Recent Runs ---- */
    function buildCard(r,i){{
        const ok=r.success||0, fail=r.failed||0;
        const p=pct(ok,fail), scolor=sc(p), ocolor=oc(r.op||'');
        const ts=(r.ts||'').substring(5,16), dur=fmt(r.elapsed||0);
        const sp=spd(ok,r.elapsed||0), api=r.api||'', obj=r.obj||'\u2014';
        const src=r.source?(r.source.split('/').pop().split('\\\\').pop()):'';
        const subOp=r.sub_op||'';
        const opLabel=subOp?(r.op+'\u00b7'+subOp.toUpperCase()):(r.op||'\u2014');
        return(
            '<div class="sfr-card" id="sfr-c'+i+'" onclick="sfT('+i+')">'+
            '<div class="sfr-sum">'+
              '<div class="sfr-r1">'+
                '<span class="sfr-dot" style="background:'+ocolor+';box-shadow:0 0 5px '+ocolor+'88;"></span>'+
                '<span class="sfr-op">'+opLabel+'</span>'+
                '<span class="sfr-ts">'+ts+'</span>'+
                '<span class="sfr-chev">\u25be</span>'+
              '</div>'+
              '<div class="sfr-obj">'+obj+'</div>'+
              (src?'<div style="font-size:0.65rem;color:rgba(255,255,255,0.35);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">\u2744\ufe0f '+src+'</div>':'')+
              '<div class="sfr-meta">'+ok.toLocaleString()+' ok \u00b7 '+dur+'</div>'+
            '</div>'+
            '<div class="sfr-det">'+
              '<div class="sfr-grid">'+
                '<div class="sfr-kpi" style="border-color:rgba(34,197,94,.2);background:rgba(34,197,94,.07);">'+
                  '<div class="sfr-klbl">\u2705 Success</div>'+
                  '<div class="sfr-kval" style="color:#4ade80;">'+ok.toLocaleString()+'</div></div>'+
                '<div class="sfr-kpi" style="border-color:'+(fail>0?'rgba(239,68,68,.2)':'rgba(255,255,255,.07)')+';background:'+(fail>0?'rgba(239,68,68,.07)':'rgba(255,255,255,.04)')+';">'+
                  '<div class="sfr-klbl">\u274c Failed</div>'+
                  '<div class="sfr-kval" style="color:'+(fail>0?'#f87171':'#64748b')+';">'+fail.toLocaleString()+'</div></div>'+
                '<div class="sfr-kpi" style="border-color:rgba(99,102,241,.2);background:rgba(99,102,241,.07);">'+
                  '<div class="sfr-klbl">\u23f1 Duration</div>'+
                  '<div class="sfr-kval" style="color:#818cf8;">'+dur+'</div></div>'+
                '<div class="sfr-kpi" style="border-color:rgba(168,85,247,.2);background:rgba(168,85,247,.07);">'+
                  '<div class="sfr-klbl">\u26a1 Speed</div>'+
                  '<div class="sfr-kval" style="color:#c084fc;">'+sp+'</div></div>'+
                '<div class="sfr-kpi full" style="border-color:'+scolor+'33;background:'+scolor+'0d;">'+
                  '<div class="sfr-klbl">\U0001F3C6 Success Rate</div>'+
                  '<div style="display:flex;align-items:baseline;gap:6px;">'+
                    '<div class="sfr-kval" style="color:'+scolor+';">'+p.toFixed(1)+'%</div>'+
                    '<div style="font-size:.60rem;color:rgba(255,255,255,.30);">('+ok+' of '+(ok+fail)+')</div></div>'+
                  '<div class="sfr-bar"><div class="sfr-fill" style="background:linear-gradient(90deg,'+scolor+','+scolor+'88);width:'+Math.min(p,100)+'%;"></div></div>'+
                '</div>'+
              '</div>'+
              ((api||src)?
                '<div class="sfr-irow">'+
                  (api?'\U0001F517 <span class="sfr-ival">'+api+'</span>':'')+
                  (api&&src?' <span style="color:rgba(255,255,255,.14);">\u00b7</span> ':'')+
                  (src?'\U0001F4C4 <span class="sfr-ival">'+src+'</span>':'')+
                '</div>':'')+
            '</div></div>'
        );
    }}

    function renderRuns(tabOp){{
        const tabIdx=TAB_OPS.indexOf(tabOp);
        const isAll=tabIdx<0;
        const tabKey=normOp(tabOp);
        const aliases = tabAliases(tabKey);
        const filtered=isAll?ALL:ALL.filter(r=>aliases.includes(normOp(r.op)));
        const show=filtered.slice(0,5);
        let html=
            '<div class="sfr-hdr">'+
              '<span class="sec-title">\u23f1 Recent Runs</span>'+
              '<span class="sfr-badge">'+(isAll?'All':tabOp)+'</span>'+
            '</div>';
        if(show.length===0){{
            html+='<div class="sfr-empty">No <b>'+tabOp+'</b> runs yet</div>';
        }} else {{
            show.forEach(function(r,i){{ html+=buildCard(r,i); }});
        }}
        document.getElementById('sfr-root').innerHTML=html;
    }}

    function render(){{
        const tabOp = resolveActiveTabOp();
        renderActivity(tabOp);
        renderRuns(tabOp);
    }}

    window.sfT=function(i){{
        const c=document.getElementById('sfr-c'+i);
        if(!c) return;
        const wasOpen=c.classList.contains('open');
        document.querySelectorAll('.sfr-card').forEach(function(x){{x.classList.remove('open');}});
        if(!wasOpen) c.classList.add('open');
    }};

    render();
    let _last='';
    setInterval(function(){{
        const cur = resolveActiveTabOp();
        if(cur!==_last){{_last=cur;render();}}
    }},400);
}})();
</script>
</body>
</html>
""", height=520, scrolling=True)

# Keyboard hint pill at the very bottom of sidebar
st.sidebar.markdown("""
<div class="sf-sidebar-section" style="margin-top:18px;text-align:center;
    background:rgba(168,85,247,0.04);border-color:rgba(168,85,247,0.14);">
    <div style="font-size:0.70rem;color:rgba(255,255,255,0.50);line-height:1.7;">
        Quick-search any tab<br/>
        <kbd style="background:rgba(168,85,247,0.22);
            border:1px solid rgba(168,85,247,0.40);
            padding:2px 8px;border-radius:6px;font-size:0.68rem;
            color:#f0f4ff;font-family:Inter, monospace;">Ctrl+/</kbd>
        &nbsp;or&nbsp;
        <kbd style="background:rgba(168,85,247,0.22);
            border:1px solid rgba(168,85,247,0.40);
            padding:2px 8px;border-radius:6px;font-size:0.68rem;
            color:#f0f4ff;font-family:Inter, monospace;">/</kbd>
    </div>
</div>
""", unsafe_allow_html=True)

# -------------------------------------------------

# Main Area

# -------------------------------------------------

tab_insert, tab_update, tab_upsert, tab_delete, tab_multi, tab_snowflake, tab_sf_to_snowflake, tab_testcase = st.tabs(['📥 Insert', '🔄 Update', '🔀 Upsert', '🗑️ Delete', '🚀 Multi-Object', '❄️ Snowflake', '🔄❄️ SF → Snowflake', '🧪 Test Case Generator'])

# Persist active tab across reruns — auto-restores on every Streamlit rerun

install_tab_persistence()

# Auto-tag danger buttons (STOP/Delete/Disconnect) so CSS styles them red

install_button_retagger()

# Inject the ⌘K / Ctrl+K command palette overlay (one-time per session)

install_command_palette()

# --- Test Case Generator Tab ---

def _tab_testcase():

    section_header('🧪', 'Test Case Generator', 'Generate and execute migration test cases comparing <b>Source Table vs Target Table</b> (both in Snowflake). Uses Salesforce metadata for smart validation.', accent='pink')
    render_steps(['Connect', 'Select Tables', 'Map Columns', 'Generate Cases', 'Run Validation'])

    # --- Persistent save/update/delete notification popup ---
    if st.session_state.get('_tc_save_msg'):
        _msg_type, _msg_text = st.session_state.pop('_tc_save_msg')
        show_feedback(_msg_type, _msg_text, success_fn=st.success, warning_fn=st.warning, error_fn=st.error)

    # --- Load Saved Config ---

    _tc_configs = load_testcase_configs()

    _tc_config_names = list(_tc_configs.keys())

    _tc_selected = st.selectbox(

        "📂 Load Saved Config",

        options=["-- New --"] + _tc_config_names,

        key="tc_load_select",

        help="Select a previously saved configuration to auto-fill all fields"

    )

    # If a saved config is selected, populate session state — but ONLY when

    # the selection actually changes. Otherwise we'd overwrite user edits on

    # every rerun.

    _tc_last_loaded_key = '_tc_last_loaded_config'

    if st.session_state.get(_tc_last_loaded_key) != _tc_selected:

        st.session_state[_tc_last_loaded_key] = _tc_selected

        if _tc_selected != "-- New --" and _tc_selected in _tc_configs:

            _loaded = _tc_configs[_tc_selected]

            st.session_state['testcase_snowflake_source_table'] = _loaded.get('source_table', '')

            st.session_state['testcase_snowflake_table'] = _loaded.get('target_table', '')

            st.session_state['testcase_sf_object'] = _loaded.get('sf_object', '')

            st.session_state['testcase_where_filter'] = _loaded.get('where_filter', '')

            st.session_state['_tc_saved_mapping'] = _loaded.get('column_mapping', {})

            st.session_state['_tc_saved_key_field'] = _loaded.get('key_field', '')

            # Load per-object lookup ref table map (new format); ignore old single-string 'lookup_ref_table'
            st.session_state['_tc_saved_lookup_ref_map'] = _loaded.get('lookup_ref_tables_map', {})

    st.markdown("---")

    # --- Fetch Snowflake table list for searchable dropdowns ---
    _snow_tables = []
    if st.session_state.get('sf_conn'):
        _db  = (st.session_state.get('sf_database_sidebar') or '').strip()
        _sc  = (st.session_state.get('sf_schema_sidebar') or '').strip()
        _cache_key = f"_tc_snow_tables_{_db}_{_sc}"
        _refresh_col, _info_col = st.columns([1, 5])
        with _refresh_col:
            if st.button("🔄 Refresh Tables", key="tc_refresh_tables", help="Reload table list from Snowflake"):
                if _cache_key in st.session_state:
                    del st.session_state[_cache_key]
        if _cache_key not in st.session_state:
            with st.spinner("Loading table list from Snowflake…"):
                try:
                    _cur = st.session_state['sf_conn'].cursor()
                    if _db and _sc:
                        _cur.execute(f'SHOW TERSE TABLES IN SCHEMA "{_db}"."{_sc}"')
                    elif _sc:
                        _cur.execute(f'SHOW TERSE TABLES IN SCHEMA "{_sc}"')
                    else:
                        _cur.execute('SHOW TERSE TABLES')
                    _rows = _cur.fetchall()
                    _cur.close()
                    st.session_state[_cache_key] = sorted({r[1] for r in _rows if r[1]})
                except Exception as _e:
                    st.session_state[_cache_key] = []
                    with _info_col:
                        st.warning(f"Could not load table list: {_e}")
        _snow_tables = st.session_state.get(_cache_key, [])
        with _info_col:
            if _snow_tables:
                st.caption(f"❄️ {len(_snow_tables)} tables available in **{_db}.{_sc}** — type to search in the dropdowns below.")

    _table_opts = [''] + _snow_tables   # blank = "not selected"

    # --- Input Fields ---

    col_src, col_tgt = st.columns(2)

    with col_src:
        _src_default_idx = 0
        _src_saved = st.session_state.get('testcase_snowflake_source_table', '')
        if _src_saved and _src_saved in _table_opts:
            _src_default_idx = _table_opts.index(_src_saved)
        snowflake_source_table = st.selectbox(
            "Source Snowflake Table",
            options=_table_opts,
            index=_src_default_idx,
            key="testcase_snowflake_source_table",
            help="Type to search. Tables are fetched from Snowflake based on your connected schema."
        )

    with col_tgt:
        _tgt_default_idx = 0
        _tgt_saved = st.session_state.get('testcase_snowflake_table', '')
        if _tgt_saved and _tgt_saved in _table_opts:
            _tgt_default_idx = _table_opts.index(_tgt_saved)
        snowflake_table = st.selectbox(
            "Target Snowflake Table",
            options=_table_opts,
            index=_tgt_default_idx,
            key="testcase_snowflake_table",
            help="Type to search. Tables are fetched from Snowflake based on your connected schema."
        )

    if not _snow_tables and st.session_state.get('sf_conn'):
        st.caption("⚠️ Could not load table list. Check your schema/role permissions. You can still type a table name manually by selecting the blank entry — columns will be fetched when tables are entered.")

    col_sf, = st.columns(1)

    with col_sf:

        sf_object = st.text_input(

            "SF Object API Name (metadata only)",

            placeholder="e.g. Account, WOD_2__Warehouse__c",

            key="testcase_sf_object",

            help="Used ONLY to fetch field metadata (picklist values, lookup references, required fields). No SF data is queried."

        )

    # lookup_ref_tables_map is built dynamically below after column mapping is shown
    lookup_ref_tables_map = {}

    where_filter = st.text_input(

        "WHERE Filter (Optional)",

        placeholder="e.g. STATUS = 'Active' AND CREATED_DATE > '2024-01-01'",

        key="testcase_where_filter",

        help="Filter records to test only a subset"

    )

    # Key Field Selection

    key_field_option = None

    source_columns = []

    target_columns = []

    ref_table_columns = []   # columns fetched from the lookup reference table

    auto_mapping = {}

    mapping_result = {}

    # --- SF Metadata Fetch ---

    sf_field_types = {}

    sf_field_ref = {}

    sf_picklist_values = {}

    sf_required_fields = set()

    sf_field_length = {}

    if sf_object and st.session_state.get('sf'):

        try:

            sf = st.session_state['sf']

            desc = getattr(sf, sf_object).describe()

            for f in desc.get('fields', []):

                fname_lower = f['name'].lower()

                sf_field_types[fname_lower] = f.get('type', '')

                sf_field_length[fname_lower] = f.get('length', 0)

                if f.get('type') == 'reference':

                    sf_field_ref[fname_lower] = f.get('referenceTo', [])

                if f.get('type') in ('picklist', 'multipicklist'):

                    valid_vals = [pv['value'] for pv in f.get('picklistValues', []) if pv.get('active', True)]

                    sf_picklist_values[fname_lower] = valid_vals

                if not f.get('nillable', True):

                    sf_required_fields.add(fname_lower)

            st.success(f"✅ SF metadata loaded: {len(sf_field_types)} fields | {len(sf_picklist_values)} picklists | {len(sf_field_ref)} lookups | {len(sf_required_fields)} required")

        except Exception as e:

            st.warning(f"Could not fetch SF metadata: {e}. Tests will run without field-type classification.")

    elif sf_object and not st.session_state.get('sf'):

        st.warning("Connect to Salesforce in the sidebar to load field metadata (picklist, lookup, required info).")

    # --- Helper: qualify table name with db.schema if not already qualified ---
    def _fq_table(tbl):
        """Prepend database.schema. when the user typed a bare table name (no dots)."""
        if not tbl or tbl.strip() == '' or '.' in tbl:
            return tbl   # already qualified or empty
        _db = (st.session_state.get('sf_database_sidebar') or '').strip()
        _sc = (st.session_state.get('sf_schema_sidebar') or '').strip()
        if _db and _sc:
            return f"{_db}.{_sc}.{tbl}"
        if _sc:
            return f"{_sc}.{tbl}"
        return tbl   # no default db/schema — leave as-is (connection may have it set)

    # Fetch columns from both Snowflake tables

    if snowflake_source_table and snowflake_table:

        if not st.session_state.get('sf_conn'):

            st.warning("Connect to Snowflake in the sidebar.")

        else:

            conn = st.session_state['sf_conn']

            # Fetch Source table columns

            try:

                cursor = conn.cursor()

                cursor.execute(f'DESCRIBE TABLE {_fq_table(snowflake_source_table)}')

                rows = cursor.fetchall()

                source_columns = [row[0] for row in rows if row[0] and row[0] != '']

                cursor.close()

            except Exception as e:

                st.error(f"Failed to fetch Source table columns: {e}")

            # Fetch Target table columns

            try:

                cursor = conn.cursor()

                cursor.execute(f'DESCRIBE TABLE {_fq_table(snowflake_table)}')

                rows = cursor.fetchall()

                target_columns = [row[0] for row in rows if row[0] and row[0] != '']

                cursor.close()

            except Exception as e:

                st.error(f"Failed to fetch Target table columns: {e}")

            # Lookup ref table schemas are fetched per-object at run time (see pre-flight section)

        # Show key field selector

        if source_columns:

            _default_idx = 0

            _saved_key = st.session_state.get('_tc_saved_key_field', '')

            for i, col in enumerate(source_columns):

                cl = col.upper()

                if _saved_key and col.lower() == _saved_key.lower():

                    _default_idx = i

                    break

                elif cl == 'ID':

                    _default_idx = i

                    break

                elif 'EXTERNAL_ID' in cl:

                    _default_idx = i

                    break

            key_field_option = st.selectbox(

                "🔑 Key Field for Record Matching (pick from your source table columns)",

                options=source_columns,

                index=_default_idx,

                key="testcase_key_field_select",

                help="This column must exist in BOTH source and target tables. Used to match records."

            )

        # Auto-map: use saved mapping first, then fall back to name match

        _saved_mapping = st.session_state.get('_tc_saved_mapping', {})

        for col in source_columns:

            saved_tgt = _saved_mapping.get(col)

            if saved_tgt and saved_tgt in target_columns:

                auto_mapping[col] = saved_tgt

            else:

                match = next((f for f in target_columns if f.lower() == col.lower()), None)

                auto_mapping[col] = match

        # Show mapping UI

        mapped = 0

        if source_columns and target_columns:

            st.subheader("Column Mapping (Auto-mapped by name)")

            for col in source_columns:

                default = auto_mapping[col] if auto_mapping[col] else None

                options = ["-- Select --"] + target_columns

                index = options.index(default) if default in options else 0

                mapping_result[col] = st.selectbox(

                    f"Map source column '{col}' to target column:",

                    options=options,

                    index=index,

                    key=f"mapping_{col}"

                )

            mapped = sum(1 for v in mapping_result.values() if v != "-- Select --")

            st.info(f"**{mapped}/{len(source_columns)} columns mapped.**")

        # --- Lookup Reference Table Mapping (per SF reference object) ---
        # Build this dynamically: find every unique SF referenceTo object among mapped
        # lookup fields, and let the user specify the matching Snowflake table for each.
        # If no table is given for a particular object ? that field's lookup validation is SKIPPED.
        if mapped > 0 and sf_field_ref:
            _lookup_ref_objs_needed = {}   # {sf_obj: [src_col, ...]}
            for src_col, tgt_col in mapping_result.items():
                if tgt_col and tgt_col != '-- Select --' and sf_field_types.get(src_col.lower()) == 'reference':
                    for _obj in sf_field_ref.get(src_col.lower(), []):
                        _lookup_ref_objs_needed.setdefault(_obj, []).append(src_col)

            if _lookup_ref_objs_needed:
                st.markdown("---")
                st.subheader("🔗 Lookup Reference Table Mapping")
                st.caption(
                    "Each lookup field references a specific Salesforce object. "
                    "Enter the **Snowflake table** where those IDs should exist. "
                    "**Leave blank to SKIP** lookup validation for that object — the test will tell you what's missing."
                )
                _saved_lkp_map = st.session_state.get('_tc_saved_lookup_ref_map', {})
                for _ref_obj in sorted(_lookup_ref_objs_needed.keys()):
                    _fields_using = ', '.join(_lookup_ref_objs_needed[_ref_obj])
                    _default_val  = _saved_lkp_map.get(_ref_obj, '')
                    _lkp_opts     = [''] + _snow_tables
                    _lkp_idx      = _lkp_opts.index(_default_val) if _default_val in _lkp_opts else 0
                    lookup_ref_tables_map[_ref_obj] = st.selectbox(
                        f"Snowflake table for SF object **{_ref_obj}**",
                        options=_lkp_opts,
                        index=_lkp_idx,
                        key=f"lookup_map_{_ref_obj}",
                        help=f"Used by field(s): {_fields_using}. Leave blank (first option) to SKIP lookup validation for this object."
                    )
                st.markdown("---")

        # Show test case preview (dynamic, based on metadata)

        if mapped > 0:

            st.subheader("Test Cases to be Generated")

            test_cases = [

                {"#": "1", "Test Case": "Source-to-Target Record Presence", "Description": "Every source record must exist in the target by the selected key; target-only records are ignored"},

                {"#": "2", "Test Case": "Null Count (per field)", "Description": "Source has value but target is NULL ? data lost"},

                {"#": "3", "Test Case": "Picklist (picklist fields only)", "Description": "Each source picklist value must match the target value for the same key"},

                {"#": "4", "Test Case": "Lookup (reference fields only)", "Description": "Gets IDs from target lookup field ? queries lookup reference table ? verifies IDs exist"},

                {"#": "5", "Test Case": "RecordType (RecordTypeId only)", "Description": "Source RecordTypeId must be exactly same in target"},

                {"#": "6", "Test Case": "Required Field (non-nillable fields)", "Description": "Mandatory fields must have data — checks both source and target for nulls"},

                {"#": "7", "Test Case": "Data Match (text/number/date fields)", "Description": "Source value must equal target value record-by-record (type-aware comparison)"},

            ]

            # Count how many of each type

            picklist_count = sum(1 for src, _ in mapping_result.items() if mapping_result[src] != '-- Select --' and sf_field_types.get(src.lower(), '') in ('picklist', 'multipicklist'))

            lookup_count = sum(1 for src, _ in mapping_result.items() if mapping_result[src] != '-- Select --' and sf_field_types.get(src.lower(), '') == 'reference')

            required_count = sum(1 for src, _ in mapping_result.items() if mapping_result[src] != '-- Select --' and src.lower() in sf_required_fields)

            st.caption(f"📊 Fields: {picklist_count} picklist | {lookup_count} lookup | {required_count} required | {mapped} total mapped")

            st.table(test_cases)

    # --- Save / Edit / Delete Config ---

    st.markdown("---")

    _sv_col1, _sv_col2, _sv_col3, _sv_col4 = st.columns([2, 1, 1, 1])

    with _sv_col1:

        _tc_save_name = st.text_input(

            "Config Name",

            value=_tc_selected if _tc_selected != "-- New --" else "",

            placeholder="Enter a name to save this config",

            key="tc_save_name"

        )

    with _sv_col2:

        if st.button("💾 Save", key="tc_save_btn", width='stretch'):

            if not _tc_save_name.strip():

                st.session_state['_tc_save_msg'] = ('warning', "Enter a config name.")

                st.rerun()

            elif not snowflake_source_table:

                st.error("Fill in Source Table first.")

            else:

                _tc_configs[_tc_save_name.strip()] = {

                    'source_table': snowflake_source_table,

                    'target_table': snowflake_table,

                    'sf_object': sf_object,

                    'lookup_ref_tables_map': {k: v for k, v in lookup_ref_tables_map.items() if v and v.strip()},

                    'where_filter': where_filter,

                    'key_field': key_field_option or '',

                    'column_mapping': {src: tgt for src, tgt in mapping_result.items() if tgt and tgt != '-- Select --'}

                }

                save_testcase_configs(_tc_configs)

                st.session_state['_tc_save_msg'] = ('success', f"✅ Config '{_tc_save_name.strip()}' saved successfully!")

                queue_action_feedback('Test case saved', f'Saved configuration "{_tc_save_name.strip()}".')
                st.rerun()

    with _sv_col3:

        if st.button("🔄 Update", key="tc_edit_btn", width='stretch'):

            if _tc_selected == "-- New --":

                st.session_state['_tc_save_msg'] = ('warning', "Select a saved config first.")

                st.rerun()

            elif not snowflake_source_table:

                st.error("Fill in Source Table first.")

            else:

                _tc_configs[_tc_selected] = {

                    'source_table': snowflake_source_table,

                    'target_table': snowflake_table,

                    'sf_object': sf_object,

                    'lookup_ref_tables_map': {k: v for k, v in lookup_ref_tables_map.items() if v and v.strip()},

                    'where_filter': where_filter,

                    'key_field': key_field_option or '',

                    'column_mapping': {src: tgt for src, tgt in mapping_result.items() if tgt and tgt != '-- Select --'}

                }

                save_testcase_configs(_tc_configs)

                st.session_state['_tc_save_msg'] = ('success', f"✅ Config '{_tc_selected}' updated successfully!")

                queue_action_feedback('Test case updated', f'Updated configuration "{_tc_selected}".')
                st.rerun()

    with _sv_col4:

        if st.button("🗑️ Delete", key="tc_del_btn", width='stretch'):

            if _tc_selected == "-- New --":

                st.session_state['_tc_save_msg'] = ('warning', "Select a config to delete.")

                st.rerun()

            else:
                st.session_state['_tc_del_confirm'] = _tc_selected
                st.rerun()

    if st.session_state.get('_tc_del_confirm'):
        _tc_to_del = st.session_state['_tc_del_confirm']
        st.warning(f"⚠️ Delete config **'{_tc_to_del}'**? This cannot be undone.")
        _tdc1, _tdc2 = st.columns(2)
        with _tdc1:
            if st.button('✅ Yes, Delete', key='tc_del_confirm_yes'):
                del _tc_configs[_tc_to_del]
                save_testcase_configs(_tc_configs)
                st.session_state.pop('_tc_del_confirm', None)
                st.session_state['_tc_save_msg'] = ('success', f"🗑️ Config '{_tc_to_del}' deleted.")
                queue_action_feedback('Test case deleted', f'Deleted configuration "{_tc_to_del}".')
                st.rerun()
        with _tdc2:
            if st.button('❌ Cancel', key='tc_del_confirm_no'):
                st.session_state.pop('_tc_del_confirm', None)
                st.rerun()

    st.markdown("---")

    # Generate & Run Button

    tc_run_col, tc_stop_col = st.columns([1, 1])

    with tc_run_col:

        testcase_run_clicked = st.button("🚀 Generate & Run Test Cases", key="testcase_run_btn", type="primary", width='stretch')

    with tc_stop_col:

        testcase_stop_clicked = st.button('🛑 STOP', key='testcase_stop_btn', help='Stop the ongoing process', width='stretch', on_click=set_stop_flag)

    if testcase_stop_clicked:

        set_stop_flag()

        st.warning('⚠️ Stop signal sent. Process will halt after the current step completes.')

    if testcase_run_clicked:

        clear_stop_flag()

        if not (snowflake_source_table and snowflake_table and source_columns and target_columns):

            st.error("Please provide both table names and ensure Snowflake is connected.")

        else:

            import numpy as np

            conn = st.session_state.get('sf_conn')

            results = []

            queries = []

            # --- Qualify all table names with db.schema. before any SQL is built ---
            # This ensures bare names like 'STG_GL_LEDGER_ASP_UPDATE' become
            # 'DB.SCHEMA.STG_GL_LEDGER_ASP_UPDATE' so Snowflake can resolve them.
            snowflake_source_table = _fq_table(snowflake_source_table)
            snowflake_table        = _fq_table(snowflake_table)
            lookup_ref_tables_map  = {
                obj: _fq_table(tbl)
                for obj, tbl in lookup_ref_tables_map.items()
                if tbl and tbl.strip()
            }

            # Build WHERE clause for filtering

            snow_where = f" WHERE {where_filter}" if where_filter else ""

            # Determine key field

            key_used = key_field_option

            if not key_used:

                st.error("Please select a Key Field for Record Matching.")

            else:

                # ==============================================================
                # PRE-FLIGHT: Re-fetch DESCRIBE TABLE for ALL tables at execution
                # time so queries are always built from live, verified metadata.
                # Never assume a column name — resolve it from DESCRIBE TABLE.
                # ==============================================================

                def _describe_table(conn, table_name):
                    """Return list of exact column names from Snowflake DESCRIBE TABLE."""
                    try:
                        cur = conn.cursor()
                        cur.execute(f"DESCRIBE TABLE {table_name}")
                        rows = cur.fetchall()
                        cur.close()
                        return [r[0] for r in rows if r[0]]
                    except Exception as exc:
                        return []   # caller will check emptiness and raise

                def _build_col_map(col_list):
                    """Build {UPPER_NAME: actual_stored_name} from DESCRIBE TABLE results."""
                    return {c.upper(): c for c in col_list}

                def _resolve_col(col_map, logical_name, table_label):
                    """
                    Find exact stored column name case-insensitively.
                    Returns (actual_name, quoted_name) or raises ValueError with a helpful message.
                    """
                    actual = col_map.get(logical_name.upper())
                    if actual is None:
                        available = ', '.join(list(col_map.values())[:12])
                        raise ValueError(
                            f"Column '{logical_name}' not found in table {table_label}. "
                            f"DESCRIBE TABLE returned: {available}"
                        )
                    return actual, f'"{actual}"'

                # Re-fetch all table schemas fresh at execution time
                with st.spinner("⚙️ Fetching live schema metadata for all tables…"):
                    live_src_cols  = _describe_table(conn, snowflake_source_table)
                    live_tgt_cols  = _describe_table(conn, snowflake_table)
                    # Fetch schema for each unique lookup ref table (one DESCRIBE per unique table)
                    _unique_ref_tables = {}  # {TABLE_NAME_UPPER: (actual_name, col_list, col_map)}
                    for _sf_obj, _tbl in lookup_ref_tables_map.items():
                        if _tbl and _tbl.strip():
                            _tbl_key = _tbl.strip().upper()
                            if _tbl_key not in _unique_ref_tables:
                                _cols = _describe_table(conn, _tbl.strip())
                                _unique_ref_tables[_tbl_key] = (_tbl.strip(), _cols, _build_col_map(_cols))

                pre_flight_errors = []
                pre_flight_info   = []

                if not live_src_cols:
                    pre_flight_errors.append(f"❌ Could not DESCRIBE TABLE {snowflake_source_table}")
                if not live_tgt_cols:
                    pre_flight_errors.append(f"❌ Could not DESCRIBE TABLE {snowflake_table}")
                for _sf_obj, _tbl in lookup_ref_tables_map.items():
                    if _tbl and _tbl.strip():
                        _tbl_key = _tbl.strip().upper()
                        if _tbl_key in _unique_ref_tables and not _unique_ref_tables[_tbl_key][1]:
                            pre_flight_errors.append(f"❌ Could not DESCRIBE TABLE {_tbl} (for SF object {_sf_obj})")

                if pre_flight_errors:
                    for e in pre_flight_errors:
                        st.error(e)
                    st.stop()

                # Build case-insensitive resolver maps
                src_col_map = _build_col_map(live_src_cols)
                tgt_col_map = _build_col_map(live_tgt_cols)

                # --- Resolve key column in source and target ---
                try:
                    key_actual_src, key_used_q   = _resolve_col(src_col_map, key_used, snowflake_source_table)
                    key_actual_tgt, target_key_q = _resolve_col(tgt_col_map, key_used, snowflake_table)
                    pre_flight_info.append(
                        f"🔑 Key field: source → **\"{key_actual_src}\"**  |  target ? **\"{key_actual_tgt}\"**"
                    )
                except ValueError as exc:
                    st.error(f"Key field error: {exc}")
                    st.stop()

                # --- Build per-SF-object ref table metadata ---
                # live_ref_tables_meta: {sf_obj: (table_name, col_list, col_map, id_col_actual, id_col_quoted)}
                live_ref_tables_meta = {}
                for _sf_obj, _tbl in lookup_ref_tables_map.items():
                    if not (_tbl and _tbl.strip()):
                        continue   # user left blank ? will SKIP at validation time
                    _tbl_key = _tbl.strip().upper()
                    _tbl_actual, _tbl_cols, _tbl_col_map = _unique_ref_tables[_tbl_key]
                    # Resolve ID column
                    try:
                        _id_actual, _id_q = _resolve_col(_tbl_col_map, 'ID', _tbl_actual)
                    except ValueError:
                        # Fall back to first column
                        _id_actual = _tbl_cols[0] if _tbl_cols else None
                        _id_q = f'"{_id_actual}"' if _id_actual else None
                        if _id_actual:
                            pre_flight_info.append(
                                f"🔗 Ref table **{_tbl_actual}** (for {_sf_obj}): no column named 'ID'. "
                                f"Falling back to first column **\"{_id_actual}\"**. "
                                f"Columns: {', '.join(_tbl_cols[:8])}"
                            )
                        else:
                            pre_flight_errors.append(f"❌ Ref table {_tbl_actual} for {_sf_obj}: no columns found.")
                            continue
                    live_ref_tables_meta[_sf_obj] = (_tbl_actual, _tbl_cols, _tbl_col_map, _id_actual, _id_q)
                    pre_flight_info.append(
                        f"🔗 **{_sf_obj}** → **{_tbl_actual}** | ID col: **\"{_id_actual}\"**"
                    )

                # --- Validate all mapped column pairs exist in their tables ---
                validated_mapped_fields = []
                for src_col, tgt_col in [(s, t) for s, t in mapping_result.items()
                                          if t and t != '-- Select --']:
                    src_ok = src_col_map.get(src_col.upper())
                    tgt_ok = tgt_col_map.get(tgt_col.upper())
                    if not src_ok:
                        pre_flight_errors.append(
                            f"❌ Mapped source column '{src_col}' not found in {snowflake_source_table}"
                        )
                    if not tgt_ok:
                        pre_flight_errors.append(
                            f"❌ Mapped target column '{tgt_col}' not found in {snowflake_table}"
                        )
                    if src_ok and tgt_ok:
                        # Use DESCRIBE TABLE resolved names (not user-typed values)
                        if tgt_ok.lower() != key_actual_tgt.lower():
                            validated_mapped_fields.append((src_ok, tgt_ok))

                # --- Show pre-flight metadata summary ---
                _ref_configured = len(live_ref_tables_meta)
                _ref_unconfigured = sum(
                    1 for s, t in mapping_result.items()
                    if t and t != '-- Select --'
                    and sf_field_types.get(s.lower()) == 'reference'
                    and not any(
                        obj in live_ref_tables_meta
                        for obj in sf_field_ref.get(s.lower(), [])
                    )
                )
                with st.expander("🔍 Pre-flight Metadata Check (expand to inspect)", expanded=bool(pre_flight_errors)):
                    for info_line in pre_flight_info:
                        st.markdown(info_line)
                    if _ref_unconfigured:
                        st.info(
                            f"⚠️ {_ref_unconfigured} lookup field(s) have no Snowflake ref table configured — "
                            f"those validations will be **SKIPPED** with instructions."
                        )
                    if pre_flight_errors:
                        for err in pre_flight_errors:
                            st.error(err)
                    else:
                        st.success(
                            f"✅ All metadata verified: "
                            f"{len(live_src_cols)} source cols, "
                            f"{len(live_tgt_cols)} target cols, "
                            f"{_ref_configured} lookup ref table(s) ready"
                            + f" | {len(validated_mapped_fields)} field pairs ready"
                        )
                        st.caption(
                            f"Source key: `{key_actual_src}` | Target key: `{key_actual_tgt}`"
                        )

                if pre_flight_errors:
                    st.error("🛑 Stopping: fix the errors above before running test cases.")
                    st.stop()

                # From here use ONLY the resolved names (exact from DESCRIBE TABLE)
                target_key_field = key_actual_tgt
                dq = '"'  # double-quote helper for f-strings

                mapped_fields = validated_mapped_fields
                total_fields  = len(mapped_fields)

                # ============================================================

                # SNOWFLAKE-ONLY VALIDATION ENGINE (with SF metadata classification)

                # ============================================================

                progress_bar = st.progress(0, text="Starting validation...")

                status_msg = st.empty()

                t_start = time.time()

                # Helper: run Snowflake query safely

                def _snow_query(sql):

                    """Run a Snowflake SQL and return rows."""

                    ensure_not_stopped()
                    cur = None
                    try:

                        cur = conn.cursor()

                        cur.execute(sql)

                        rows = cur.fetchall()

                        return rows

                    except Exception as e:

                        return [('ERROR', str(e))]

                    finally:
                        if cur is not None:
                            cur.close()

                # ==== AGGREGATE VALIDATIONS ====

                status_msg.info("⚙️ Running aggregate queries (100% data coverage)...")

                progress_bar.progress(0.05, text="Phase 1/4: Running aggregate validations...")

                # ---- TEST: RECORD COUNT ----

                def _count_validation():

                    src_count_sql = f"SELECT COUNT(*) FROM {snowflake_source_table}{snow_where}"

                    matched_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"WHERE EXISTS (SELECT 1 FROM {snowflake_table} t "

                        f"WHERE t.{target_key_q} = s.{key_used_q})"

                        f"{(' AND (' + where_filter + ')') if where_filter else ''}"

                    )

                    queries.append(f"Source: {src_count_sql}")

                    queries.append(f"Matched: {matched_sql}")

                    src_rows = _snow_query(src_count_sql)

                    src_count = src_rows[0][0] if src_rows and not isinstance(src_rows[0][0], str) else 0

                    matched_rows = _snow_query(matched_sql)

                    matched_count = matched_rows[0][0] if matched_rows and not isinstance(matched_rows[0][0], str) else 0

                    missing = src_count - matched_count

                    # Fetch up to 100 missing key IDs for details

                    missing_details = ""

                    if missing > 0:

                        missing_sql = (

                            f"SELECT s.{key_used_q} FROM {snowflake_source_table} s "

                            f"WHERE NOT EXISTS (SELECT 1 FROM {snowflake_table} t "

                            f"WHERE t.{target_key_q} = s.{key_used_q})"

                            f"{(' AND (' + where_filter + ')') if where_filter else ''}"

                            f" LIMIT 100"

                        )

                        queries.append(f"Missing: {missing_sql}")

                        miss_rows = _snow_query(missing_sql)

                        if miss_rows and not isinstance(miss_rows[0], str):

                            missing_ids = [str(r[0]) for r in miss_rows]

                            missing_details = ", ".join(missing_ids)

                            if missing > 100:

                                missing_details += f" ... and {missing - 100} more"

                    return {

                        "Test Case Name": f"Source-to-Target Record Presence Validation",

                        "Validation Type": "Count",

                        "Status": "PASS" if missing == 0 else "FAIL",

                        "Comparison Method": f"Source records matched in Target by key ({key_used})",

                        "Source Count": src_count,

                        "Target Count": matched_count,

                        "Failed Record Count": missing,

                        "Error Message": f"{missing:,} source records not found in target" if missing > 0 else "",

                        "Failed Record IDs/Details": missing_details,

                        "Source Query": src_count_sql,

                        "Target Query": matched_sql

                    }

                # ---- TEST: NULL COUNT per field ----

                def _null_validation(src_col, tgt_col):

                    # Count source records where field is NOT NULL

                    src_notnull_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE s.{dq}{src_col}{dq} IS NOT NULL"

                        f"{(' AND ' + where_filter) if where_filter else ''}"

                    )

                    # Of those, count where target also has a non-null value

                    tgt_notnull_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE s.{dq}{src_col}{dq} IS NOT NULL AND t.{dq}{tgt_col}{dq} IS NOT NULL"

                        f"{(' AND ' + where_filter) if where_filter else ''}"

                    )

                    src_notnull = _snow_query(src_notnull_sql)

                    src_notnull_count = src_notnull[0][0] if src_notnull and not isinstance(src_notnull[0][0], str) else 0

                    tgt_notnull = _snow_query(tgt_notnull_sql)

                    tgt_notnull_count = tgt_notnull[0][0] if tgt_notnull and not isinstance(tgt_notnull[0][0], str) else 0

                    data_loss = max(0, src_notnull_count - tgt_notnull_count)

                    return {

                        "Test Case Name": f"Null Count Validation ({src_col})",

                        "Validation Type": "Null Count",

                        "Status": "PASS" if data_loss == 0 else "FAIL",

                        "Comparison Method": f"Source NOT NULL vs Target NOT NULL (matched records by {key_used})",

                        "Source Count": f"{src_notnull_count:,} non-null",

                        "Target Count": f"{tgt_notnull_count:,} non-null",

                        "Failed Record Count": data_loss,

                        "Error Message": f"~{data_loss:,} records have value in source but NULL in target" if data_loss > 0 else "",

                        "Failed Record IDs/Details": "",

                        "Source Query": src_notnull_sql,

                        "Target Query": tgt_notnull_sql

                    }

                # ---- TEST: PICKLIST VALIDATION (record-by-record via join) ----

                def _picklist_validation(src_col, tgt_col):

                    # Compare source vs target values record-by-record using key join

                    mismatch_sql = (

                        f"SELECT s.{key_used_q}, s.{dq}{src_col}{dq} AS SRC_VAL, t.{dq}{tgt_col}{dq} AS TGT_VAL "

                        f"FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE s.{dq}{src_col}{dq} IS NOT NULL AND (t.{dq}{tgt_col}{dq} IS NULL OR LOWER(TRIM(s.{dq}{src_col}{dq})) != LOWER(TRIM(t.{dq}{tgt_col}{dq})))"

                    )

                    if where_filter:

                        mismatch_sql += f" AND {where_filter}"

                    count_sql = f"SELECT COUNT(*) FROM ({mismatch_sql})"

                    detail_sql = f"{mismatch_sql} LIMIT 30"

                    count_rows = _snow_query(count_sql)

                    mismatch_count = count_rows[0][0] if count_rows and not isinstance(count_rows[0][0], str) else 0

                    # Get sample mismatched records

                    details = []

                    if mismatch_count > 0:

                        detail_rows = _snow_query(detail_sql)

                        for r in detail_rows:

                            if not (isinstance(r[0], str) and r[0] == 'ERROR'):

                                details.append(f"{r[0]}: src='{r[1]}' vs tgt='{r[2]}'")

                    return {

                        "Test Case Name": f"Picklist Validation ({src_col})",

                        "Validation Type": "Picklist",

                        "Status": "PASS" if mismatch_count == 0 else "FAIL",

                        "Comparison Method": f"Source-to-target picklist comparison on matched records by {key_used}",

                        "Source Count": "Matched source values",

                        "Target Count": f"{mismatch_count:,} mismatches",

                        "Failed Record Count": mismatch_count,

                        "Error Message": f"{mismatch_count:,} records have different picklist values" if mismatch_count > 0 else "",

                        "Failed Record IDs/Details": '; '.join(details[:20]),

                        "Source Query": mismatch_sql,

                        "Target Query": count_sql

                    }

                # ---- TEST: LOOKUP VALIDATION (verify IDs exist in reference table) ----

                def _lookup_validation(src_col, tgt_col, ref_objects):
                    """
                    For each lookup field, find the matching Snowflake ref table from
                    live_ref_tables_meta (keyed by SF referenceTo object name).
                    If no table was configured for any of this field's reference objects,
                    SKIP with a clear instructional message — never blindly use a wrong table.
                    """
                    # Find the first ref object for which the user configured a Snowflake table
                    ref_entry     = None
                    matched_sf_obj = None
                    for ref_obj in ref_objects:
                        if ref_obj in live_ref_tables_meta:
                            ref_entry      = live_ref_tables_meta[ref_obj]
                            matched_sf_obj = ref_obj
                            break

                    if ref_entry is None:
                        # No table configured for any reference object this field points to
                        missing_objs = ', '.join(ref_objects) if ref_objects else 'Unknown'
                        configured   = ', '.join(sorted(live_ref_tables_meta.keys())) or 'none'
                        return {
                            "Test Case Name": f"Lookup Validation ({src_col})",
                            "Validation Type": "Lookup",
                            "Status": "SKIP",
                            "Comparison Method": "No Snowflake table configured for this lookup",
                            "Source Count": "",
                            "Target Count": "",
                            "Failed Record Count": 0,
                            "Error Message": (
                                f"Field '{src_col}' references SF object(s): [{missing_objs}]. "
                                f"No Snowflake table was provided for any of these objects. "
                                f"Objects currently configured: [{configured}]. "
                                f"Add the correct Snowflake table in the "
                                f"'Lookup Reference Table Mapping' section and re-run."
                            ),
                            "Failed Record IDs/Details": "",
                            "Source Query": "",
                            "Target Query": ""
                        }

                    ref_table_name, _, _, ref_id_col_actual, ref_id_q = ref_entry

                    if ref_id_q is None:
                        return {
                            "Test Case Name": f"Lookup Validation ({src_col} ? {matched_sf_obj})",
                            "Validation Type": "Lookup",
                            "Status": "SKIP",
                            "Comparison Method": f"Cannot resolve ID column in {ref_table_name}",
                            "Source Count": "",
                            "Target Count": "",
                            "Failed Record Count": 0,
                            "Error Message": f"No usable ID column found in ref table {ref_table_name}.",
                            "Failed Record IDs/Details": "",
                            "Source Query": "",
                            "Target Query": ""
                        }

                    # Get distinct IDs from target lookup field (only for matched source records)
                    # Exclude NULL and empty-string values — both would appear as "invalid" in the LEFT JOIN
                    tgt_ids_sql = (
                        f"SELECT DISTINCT t.{dq}{tgt_col}{dq} FROM {snowflake_source_table} s "
                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "
                        f"WHERE t.{dq}{tgt_col}{dq} IS NOT NULL AND TRIM(t.{dq}{tgt_col}{dq}) <> ''"
                        f"{(' AND ' + where_filter) if where_filter else ''}"
                    )

                    # Check which of those IDs don't exist in the correct reference table
                    invalid_sql = (
                        f"SELECT sub.{dq}{tgt_col}{dq} "
                        f"FROM ({tgt_ids_sql}) sub "
                        f"LEFT JOIN {ref_table_name} r ON sub.{dq}{tgt_col}{dq} = r.{ref_id_q} "
                        f"WHERE r.{ref_id_q} IS NULL"
                    )

                    count_sql  = f"SELECT COUNT(*) FROM ({invalid_sql})"
                    count_rows = _snow_query(count_sql)
                    invalid_count = count_rows[0][0] if count_rows and not isinstance(count_rows[0][0], str) else 0

                    # Get total distinct IDs (matched records only)
                    total_rows = _snow_query(f"SELECT COUNT(*) FROM ({tgt_ids_sql})")
                    total_ids  = total_rows[0][0] if total_rows and not isinstance(total_rows[0][0], str) else 0

                    # Get sample invalid IDs
                    invalid_details = []
                    if invalid_count > 0:
                        detail_rows = _snow_query(f"{invalid_sql} LIMIT 30")
                        invalid_details = [
                            str(r[0]) for r in detail_rows
                            if r[0] and not (isinstance(r[0], str) and r[0] == 'ERROR')
                        ]

                    return {
                        "Test Case Name": f"Lookup Validation ({src_col} ? {matched_sf_obj})",
                        "Validation Type": "Lookup",
                        "Status": "PASS" if invalid_count == 0 else "FAIL",
                        "Comparison Method": (
                            f"Field '{tgt_col}' IDs verified in {ref_table_name}.\"{ref_id_col_actual}\""
                        ),
                        "Source Count": f"{total_ids:,} distinct IDs",
                        "Target Count": f"{total_ids - invalid_count:,} valid / {invalid_count:,} invalid",
                        "Failed Record Count": invalid_count,
                        "Error Message": (
                            f"{invalid_count} IDs in '{tgt_col}' not found in {ref_table_name}.\"{ref_id_col_actual}\""
                            if invalid_count > 0 else ""
                        ),
                        "Failed Record IDs/Details": ', '.join(invalid_details[:30]),
                        "Source Query": tgt_ids_sql,
                        "Target Query": invalid_sql
                    }

                # ---- TEST: RECORDTYPE VALIDATION ----

                def _recordtype_validation(src_col, tgt_col):

                    # Record-by-record comparison of RecordTypeId

                    mismatch_sql = (

                        f"SELECT s.{key_used_q}, s.{dq}{src_col}{dq} AS SRC_VAL, t.{dq}{tgt_col}{dq} AS TGT_VAL "

                        f"FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE s.{dq}{src_col}{dq} IS NOT NULL AND (t.{dq}{tgt_col}{dq} IS NULL OR s.{dq}{src_col}{dq} != t.{dq}{tgt_col}{dq})"

                    )

                    if where_filter:

                        mismatch_sql += f" AND {where_filter}"

                    count_sql = f"SELECT COUNT(*) FROM ({mismatch_sql})"

                    count_rows = _snow_query(count_sql)

                    mismatch_count = count_rows[0][0] if count_rows and not isinstance(count_rows[0][0], str) else 0

                    # GROUP BY comparison (only matched records)

                    src_group_sql = (

                        f"SELECT s.{dq}{src_col}{dq}, COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q}"

                        f"{(' WHERE ' + where_filter) if where_filter else ''}"

                        f" GROUP BY s.{dq}{src_col}{dq}"

                    )

                    tgt_group_sql = (

                        f"SELECT t.{dq}{tgt_col}{dq}, COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q}"

                        f"{(' WHERE ' + where_filter) if where_filter else ''}"

                        f" GROUP BY t.{dq}{tgt_col}{dq}"

                    )

                    src_groups = _snow_query(src_group_sql)

                    tgt_groups = _snow_query(tgt_group_sql)

                    src_dist = {str(r[0]).strip(): r[1] for r in src_groups if r[0] and not (isinstance(r[0], str) and r[0] == 'ERROR')}

                    tgt_dist = {str(r[0]).strip(): r[1] for r in tgt_groups if r[0] and not (isinstance(r[0], str) and r[0] == 'ERROR')}

                    group_mismatches = []

                    for k, v in src_dist.items():

                        if k not in tgt_dist:

                            group_mismatches.append(f"{k}: in source({v}) but not target")

                        elif tgt_dist[k] != v:

                            group_mismatches.append(f"{k}: src={v} vs tgt={tgt_dist[k]}")

                    # Sample details

                    details = []

                    if mismatch_count > 0:

                        detail_rows = _snow_query(f"{mismatch_sql} LIMIT 20")

                        for r in detail_rows:

                            if not (isinstance(r[0], str) and r[0] == 'ERROR'):

                                details.append(f"{r[0]}: src='{r[1]}' vs tgt='{r[2]}'")

                    return {

                        "Test Case Name": f"RecordType Validation ({src_col})",

                        "Validation Type": "RecordType",

                        "Status": "PASS" if mismatch_count == 0 else "FAIL",

                        "Comparison Method": f"Record-by-record RecordTypeId comparison + GROUP BY distribution",

                        "Source Count": f"{len(src_dist)} distinct RecordTypes",

                        "Target Count": f"{len(tgt_dist)} distinct RecordTypes",

                        "Failed Record Count": mismatch_count,

                        "Error Message": f"{mismatch_count:,} records have different RecordTypeId. Distribution: {'; '.join(group_mismatches[:5])}" if mismatch_count > 0 else "",

                        "Failed Record IDs/Details": '; '.join(details[:20]),

                        "Source Query": src_group_sql,

                        "Target Query": mismatch_sql

                    }

                # ---- TEST: REQUIRED FIELD VALIDATION ----

                def _required_validation(src_col, tgt_col):

                    # Check NULL/empty only for matched source records in target

                    src_null_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE (s.{dq}{src_col}{dq} IS NULL OR TRIM(CAST(s.{dq}{src_col}{dq} AS VARCHAR)) = '')"

                        f"{(' AND ' + where_filter) if where_filter else ''}"

                    )

                    tgt_null_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE (t.{dq}{tgt_col}{dq} IS NULL OR TRIM(CAST(t.{dq}{tgt_col}{dq} AS VARCHAR)) = '')"

                        f"{(' AND ' + where_filter) if where_filter else ''}"

                    )

                    src_rows = _snow_query(src_null_sql)

                    src_null_count = src_rows[0][0] if src_rows and not isinstance(src_rows[0][0], str) else 0

                    tgt_rows = _snow_query(tgt_null_sql)

                    tgt_null_count = tgt_rows[0][0] if tgt_rows and not isinstance(tgt_rows[0][0], str) else 0

                    total_missing = src_null_count + tgt_null_count

                    return {

                        "Test Case Name": f"Required Field Validation ({src_col})",

                        "Validation Type": "Required Field",

                        "Status": "PASS" if total_missing == 0 else "FAIL",

                        "Comparison Method": f"NULL/empty check on matched records by {key_used} for mandatory field {tgt_col}",

                        "Source Count": f"{src_null_count:,} null/empty",

                        "Target Count": f"{tgt_null_count:,} null/empty",

                        "Failed Record Count": total_missing,

                        "Error Message": f"{src_null_count} null in source, {tgt_null_count} null in target (field is REQUIRED)" if total_missing > 0 else "",

                        "Failed Record IDs/Details": "",

                        "Source Query": src_null_sql,

                        "Target Query": tgt_null_sql

                    }

                # ---- TEST: DATA MATCH (text/number/date - record by record) ----

                def _data_match_validation(src_col, tgt_col, field_type):

                    # Build comparison based on type

                    mismatch_where_cond = ""

                    if field_type in ('double', 'currency', 'int', 'percent'):

                        compare_cond = f"TRY_CAST(s.{dq}{src_col}{dq} AS FLOAT) != TRY_CAST(t.{dq}{tgt_col}{dq} AS FLOAT)"

                    elif field_type in ('date', 'datetime'):

                        compare_cond = f"TO_DATE(s.{dq}{src_col}{dq}) != TO_DATE(t.{dq}{tgt_col}{dq})"

                    elif field_type == 'boolean':

                        compare_cond = (

                            f"(CASE WHEN LOWER(s.{dq}{src_col}{dq}) IN ('true','1','yes') THEN 'true' ELSE 'false' END) != "

                            f"(CASE WHEN LOWER(t.{dq}{tgt_col}{dq}) IN ('true','1','yes') THEN 'true' ELSE 'false' END)"

                        )

                    else:

                        # String comparison (case-insensitive, trimmed).
                        # Treat literal 'None' and empty strings as equivalent because SF often stores blank instead of 'None'.

                        src_norm = (

                            f"COALESCE(NULLIF(NULLIF(LOWER(TRIM(COALESCE(CAST(s.{dq}{src_col}{dq} AS VARCHAR), ''))), 'none'), ''), '')"

                        )

                        tgt_norm = (

                            f"COALESCE(NULLIF(NULLIF(LOWER(TRIM(COALESCE(CAST(t.{dq}{tgt_col}{dq} AS VARCHAR), ''))), 'none'), ''), '')"

                        )

                        compare_cond = f"{src_norm} != {tgt_norm}"

                        # For strings, compare normalized values directly so NULL, '', and 'None' are treated as equal.

                        mismatch_where_cond = compare_cond

                    if not mismatch_where_cond:

                        mismatch_where_cond = (

                            f"(s.{dq}{src_col}{dq} IS NOT NULL OR t.{dq}{tgt_col}{dq} IS NOT NULL) AND "

                            f"(s.{dq}{src_col}{dq} IS NULL OR t.{dq}{tgt_col}{dq} IS NULL OR {compare_cond})"

                        )

                    mismatch_sql = (

                        f"SELECT s.{key_used_q}, s.{dq}{src_col}{dq} AS SRC_VAL, t.{dq}{tgt_col}{dq} AS TGT_VAL "

                        f"FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q} "

                        f"WHERE {mismatch_where_cond}"

                    )

                    if where_filter:

                        mismatch_sql += f" AND {where_filter}"

                    count_sql = f"SELECT COUNT(*) FROM ({mismatch_sql})"

                    count_rows = _snow_query(count_sql)

                    mismatch_count = count_rows[0][0] if count_rows and not isinstance(count_rows[0][0], str) else 0

                    # Get total matched records

                    total_sql = (

                        f"SELECT COUNT(*) FROM {snowflake_source_table} s "

                        f"JOIN {snowflake_table} t ON s.{key_used_q} = t.{target_key_q}"

                    )

                    if where_filter:

                        total_sql += f" WHERE {where_filter}"

                    total_rows = _snow_query(total_sql)

                    total_records = total_rows[0][0] if total_rows and not isinstance(total_rows[0][0], str) else 0

                    # Sample details

                    details = []

                    if mismatch_count > 0:

                        detail_rows = _snow_query(f"{mismatch_sql} LIMIT 10")

                        for r in detail_rows:

                            if not (isinstance(r[0], str) and r[0] == 'ERROR'):

                                details.append(f"{r[0]}: '{r[1]}' vs '{r[2]}'")

                    return {

                        "Test Case Name": f"Data Match ({src_col})",

                        "Validation Type": "Data Match",

                        "Status": "PASS" if mismatch_count == 0 else "FAIL",

                        "Comparison Method": f"Record-by-record JOIN comparison (type={field_type or 'string'})",

                        "Source Count": f"{total_records:,} records",

                        "Target Count": f"{total_records:,} records",

                        "Failed Record Count": mismatch_count,

                        "Error Message": f"{mismatch_count:,} mismatches in {total_records:,} records" if mismatch_count > 0 else "",

                        "Failed Record IDs/Details": '; '.join(details),

                        "Source Query": mismatch_sql,

                        "Target Query": count_sql

                    }

                # ============================================================

                # EXECUTE ALL VALIDATIONS IN PARALLEL

                # ============================================================

                from concurrent.futures import ThreadPoolExecutor, as_completed

                import concurrent.futures

                progress_bar.progress(0.10, text="Phase 2/4: Running parallel validations...")

                status_msg.info("⚙️ Running all validations in parallel (15 threads)...")

                futures_map = {}

                with ThreadPoolExecutor(max_workers=15) as executor:

                    # Submit Record Count

                    futures_map[executor.submit(_count_validation)] = "Record Count"

                    # Submit per-field validations based on SF metadata

                    for src_col, tgt_col in mapped_fields:

                        field_type = sf_field_types.get(src_col.lower(), '')

                        # Null Count for EVERY field

                        futures_map[executor.submit(_null_validation, src_col, tgt_col)] = f"Null({src_col})"

                        # Picklist validation (picklist/multipicklist fields)

                        if field_type in ('picklist', 'multipicklist'):

                            futures_map[executor.submit(_picklist_validation, src_col, tgt_col)] = f"Picklist({src_col})"

                        # Lookup validation (reference fields)

                        elif field_type == 'reference':

                            ref_objs = sf_field_ref.get(src_col.lower(), [])

                            futures_map[executor.submit(_lookup_validation, src_col, tgt_col, ref_objs)] = f"Lookup({src_col})"

                        # RecordType validation

                        if src_col.lower() == 'recordtypeid' or tgt_col.lower() == 'recordtypeid':

                            futures_map[executor.submit(_recordtype_validation, src_col, tgt_col)] = f"RecordType({src_col})"

                        # Required Field validation (non-nillable fields)

                        if src_col.lower() in sf_required_fields:

                            futures_map[executor.submit(_required_validation, src_col, tgt_col)] = f"Required({src_col})"

                        # Data Match for EVERY mapped field (including picklist and lookup)

                        dm_type = field_type if field_type else ''

                        # Keep lookup comparison as string-oriented for robust text normalization

                        if field_type == 'reference':

                            dm_type = 'string'

                        futures_map[executor.submit(_data_match_validation, src_col, tgt_col, dm_type)] = f"DataMatch({src_col})"

                    # Collect results as they complete

                    done_count = 0

                    total_tasks = len(futures_map)

                    for fut in as_completed(futures_map):

                        if is_stopped():

                            status_msg.warning('⚠️ Stop signal received. Halting test-case generation...')

                            st.stop()

                        done_count += 1

                        label = futures_map[fut]

                        try:

                            result = fut.result()

                            results.append(result)

                        except Exception as e:

                            results.append({

                                "Test Case Name": f"ERROR: {label}",

                                "Validation Type": "Error",

                                "Status": "ERROR",

                                "Comparison Method": "",

                                "Source Count": "",

                                "Target Count": "",

                                "Failed Record Count": 0,

                                "Error Message": str(e)[:200],

                                "Failed Record IDs/Details": "",

                                "Source Query": "",

                                "Target Query": ""

                            })

                        progress_bar.progress(0.10 + 0.80 * (done_count / total_tasks),

                                              text=f"Validations: {done_count}/{total_tasks} complete ({label})")

                # RESULTS: Build DataFrame + Excel Report

                # ============================================================

                t_total = time.time() - t_start

                progress_bar.progress(0.95, text="Generating report...")

                results_df = pd.DataFrame(results)

                if results_df.empty:

                    st.warning("No test results generated. Check your column mapping.")

                else:

                    category_order = ["Count", "Null Count", "Picklist", "Lookup", "RecordType", "Required Field", "Data Match", "Error"]

                    results_df["_sort_key"] = results_df["Validation Type"].apply(

                        lambda x: category_order.index(x) if x in category_order else 99

                    )

                    results_df = results_df.sort_values("_sort_key").drop(columns=["_sort_key"]).reset_index(drop=True)

                    query_purposes = {
                        "Count": (
                            "Counts all source records in scope",
                            "Counts source records that exist in target by key",
                        ),
                        "Null Count": (
                            "Counts matched records with a non-null source value",
                            "Counts those source records with a non-null target value",
                        ),
                        "Picklist": (
                            "Shows source-to-target picklist value mismatches",
                            "Counts the source-to-target picklist mismatches",
                        ),
                        "Lookup": (
                            "Gets lookup IDs from target rows matched to source",
                            "Shows lookup IDs missing from the reference table",
                        ),
                        "RecordType": (
                            "Shows source RecordType value distribution",
                            "Shows source-to-target RecordType mismatches",
                        ),
                        "Required Field": (
                            "Counts null or empty source values",
                            "Counts null or empty target values for matched source records",
                        ),
                        "Data Match": (
                            "Shows source-to-target field value mismatches",
                            "Counts the source-to-target field mismatches",
                        ),
                    }
                    results_df["Query 1 Purpose"] = results_df["Validation Type"].map(
                        lambda validation_type: query_purposes.get(validation_type, ("", ""))[0]
                    )
                    results_df["Query 2 Purpose"] = results_df["Validation Type"].map(
                        lambda validation_type: query_purposes.get(validation_type, ("", ""))[1]
                    )
                    results_df = results_df.rename(columns={
                        "Source Query": "Query 1",
                        "Target Query": "Query 2",
                    })

                    st.session_state["testcase_queries"] = queries

                    # Excel report

                    output = io.BytesIO()

                    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

                    from openpyxl.utils import get_column_letter

                    with pd.ExcelWriter(output, engine='openpyxl') as writer:

                        results_df.to_excel(writer, index=False, sheet_name='Test Results')

                        wb = writer.book

                        ws = wb['Test Results']

                        # Summary sheet

                        ws_summary = wb.create_sheet("Summary", 0)

                        total_tests = len(results_df)

                        pass_count = len(results_df[results_df["Status"] == "PASS"])

                        fail_count = len(results_df[results_df["Status"] == "FAIL"])

                        error_count = len(results_df[results_df["Status"] == "ERROR"])

                        ws_summary["A1"] = "Migration Test Report"

                        ws_summary["A1"].font = Font(size=18, bold=True, color="1F4E79")

                        ws_summary["A3"] = "Source Table:"

                        ws_summary["B3"] = snowflake_source_table

                        ws_summary["A4"] = "Target Table:"

                        ws_summary["B4"] = snowflake_table

                        ws_summary["A5"] = "SF Object (metadata):"

                        ws_summary["B5"] = sf_object or "N/A"

                        ws_summary["A6"] = "Lookup Ref Tables:"

                        ws_summary["B6"] = ', '.join(
                            f"{obj}?{tbl}" for obj, tbl in lookup_ref_tables_map.items() if tbl and tbl.strip()
                        ) or "N/A"

                        ws_summary["A7"] = "Run Date:"

                        ws_summary["B7"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

                        ws_summary["A8"] = "Total Time:"

                        ws_summary["B8"] = f"{t_total:.1f}s"

                        ws_summary["A9"] = "Mode:"

                        ws_summary["B9"] = "Full (100% data - Snowflake queries + SF metadata)"

                        ws_summary["A11"] = "Total Test Cases:"

                        ws_summary["B11"] = total_tests

                        ws_summary["A12"] = "PASS:"

                        ws_summary["B12"] = pass_count

                        ws_summary["B12"].font = Font(bold=True, color="006100")

                        ws_summary["A13"] = "FAIL:"

                        ws_summary["B13"] = fail_count

                        ws_summary["B13"].font = Font(bold=True, color="9C0006")

                        ws_summary["A14"] = "ERROR:"

                        ws_summary["B14"] = error_count

                        # Category-wise summary (shows what validation types were checked)

                        cat_summary = results_df.groupby(["Validation Type", "Status"]).size().unstack(fill_value=0)

                        ws_summary["A16"] = "Validation Category Summary"

                        ws_summary["A16"].font = Font(size=13, bold=True, color="1F4E79")

                        ws_summary["A17"] = "Category"

                        ws_summary["B17"] = "Total"

                        ws_summary["C17"] = "PASS"

                        ws_summary["D17"] = "FAIL"

                        ws_summary["E17"] = "ERROR"

                        cat_header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")

                        cat_header_font = Font(bold=True, color="FFFFFF")

                        for col in ["A", "B", "C", "D", "E"]:

                            c = ws_summary[f"{col}17"]

                            c.fill = cat_header_fill

                            c.font = cat_header_font

                            c.alignment = Alignment(horizontal='center', vertical='center')

                        row = 18

                        for cat in category_order:

                            if cat not in cat_summary.index:

                                continue

                            p = int(cat_summary.at[cat, "PASS"]) if "PASS" in cat_summary.columns else 0

                            f = int(cat_summary.at[cat, "FAIL"]) if "FAIL" in cat_summary.columns else 0

                            e = int(cat_summary.at[cat, "ERROR"]) if "ERROR" in cat_summary.columns else 0

                            ws_summary[f"A{row}"] = cat

                            ws_summary[f"B{row}"] = p + f + e

                            ws_summary[f"C{row}"] = p

                            ws_summary[f"D{row}"] = f

                            ws_summary[f"E{row}"] = e

                            ws_summary[f"C{row}"].font = Font(color="006100", bold=True)

                            ws_summary[f"D{row}"].font = Font(color="9C0006", bold=True)

                            ws_summary[f"E{row}"].font = Font(color="9C6500", bold=True)

                            row += 1

                        for row in range(3, 15):

                            ws_summary[f"A{row}"].font = Font(bold=True)

                        ws_summary.column_dimensions["A"].width = 22

                        ws_summary.column_dimensions["B"].width = 50

                        # Format Test Results sheet

                        header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")

                        header_font = Font(bold=True, color="FFFFFF", size=11)

                        pass_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")

                        pass_font = Font(color="006100", bold=True)

                        fail_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

                        fail_font = Font(color="9C0006", bold=True)

                        error_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")

                        error_font = Font(color="9C6500", bold=True)

                        alt_row_fill = PatternFill(start_color="F8FBFF", end_color="F8FBFF", fill_type="solid")

                        # Category row fills (applied to entire row)
                        category_fills = {
                            "Count":          PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid"),
                            "Null Count":     PatternFill(start_color="E2F0D9", end_color="E2F0D9", fill_type="solid"),
                            "Picklist":       PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid"),
                            "Lookup":         PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"),
                            "RecordType":     PatternFill(start_color="E4DFEC", end_color="E4DFEC", fill_type="solid"),
                            "Required Field": PatternFill(start_color="D9F2E6", end_color="D9F2E6", fill_type="solid"),
                            "Data Match":     PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid"),
                            "Error":          PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid"),
                        }
                        # Darker shade for the Validation Type label cell itself
                        category_label_fills = {
                            "Count":          PatternFill(start_color="B8D0F0", end_color="B8D0F0", fill_type="solid"),
                            "Null Count":     PatternFill(start_color="A9D18E", end_color="A9D18E", fill_type="solid"),
                            "Picklist":       PatternFill(start_color="F4B183", end_color="F4B183", fill_type="solid"),
                            "Lookup":         PatternFill(start_color="FFD966", end_color="FFD966", fill_type="solid"),
                            "RecordType":     PatternFill(start_color="C5A0D8", end_color="C5A0D8", fill_type="solid"),
                            "Required Field": PatternFill(start_color="70AD47", end_color="70AD47", fill_type="solid"),
                            "Data Match":     PatternFill(start_color="9DC3E6", end_color="9DC3E6", fill_type="solid"),
                            "Error":          PatternFill(start_color="F4A460", end_color="F4A460", fill_type="solid"),
                        }

                        for col_idx in range(1, len(results_df.columns) + 1):

                            cell = ws.cell(row=1, column=col_idx)

                            cell.fill = header_fill

                            cell.font = header_font

                            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

                        status_col_idx = list(results_df.columns).index("Status") + 1

                        valtype_col_idx = list(results_df.columns).index("Validation Type") + 1

                        for row_idx in range(2, len(results_df) + 2):

                            valtype = ws.cell(row=row_idx, column=valtype_col_idx).value

                            # Apply category colour to entire row first
                            row_fill = category_fills.get(valtype, alt_row_fill)
                            for col_idx in range(1, len(results_df.columns) + 1):
                                ws.cell(row=row_idx, column=col_idx).fill = row_fill

                            # Darker accent on the Validation Type label cell
                            if valtype in category_label_fills:
                                lbl_cell = ws.cell(row=row_idx, column=valtype_col_idx)
                                lbl_cell.fill = category_label_fills[valtype]
                                lbl_cell.font = Font(bold=True, size=10)

                            # Override Status cell with PASS/FAIL/ERROR colour
                            status_val = ws.cell(row=row_idx, column=status_col_idx).value

                            status_cell = ws.cell(row=row_idx, column=status_col_idx)

                            if status_val == "PASS":

                                status_cell.fill = pass_fill

                                status_cell.font = pass_font

                            elif status_val == "FAIL":

                                status_cell.fill = fail_fill

                                status_cell.font = fail_font

                            elif status_val == "ERROR":

                                status_cell.fill = error_fill

                                status_cell.font = error_font

                        col_widths = {

                            "Test Case Name": 35, "Validation Type": 16, "Status": 10,

                            "Comparison Method": 55, "Source Count": 18, "Target Count": 18,

                            "Failed Record Count": 18, "Error Message": 50,

                            "Failed Record IDs/Details": 60,

                            "Query 1 Purpose": 52, "Query 1": 60,

                            "Query 2 Purpose": 52, "Query 2": 60

                        }

                        for col_idx, col_name in enumerate(results_df.columns, 1):

                            ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col_name, 20)

                        ws_summary.column_dimensions["C"].width = 12

                        ws_summary.column_dimensions["D"].width = 12

                        ws_summary.column_dimensions["E"].width = 12

                        ws.freeze_panes = "A2"

                    st.session_state["testcase_results"] = results_df

                    st.session_state["testcase_report_bytes"] = output.getvalue()

                    # Final status

                    progress_bar.progress(1.0, text=f"✅ Done in {t_total:.1f}s!")

                    status_msg.success(

                        f"✅ Validation complete in **{t_total:.1f}s** | "

                        f"{pass_count} PASS / {fail_count} FAIL / {error_count} ERROR | "

                        f"{total_tests} test cases"

                    )

    # Show results and download button if available

    results_df = st.session_state.get("testcase_results")

    if results_df is not None and not results_df.empty:

        # Display grouped by category

        for cat in results_df["Validation Type"].unique():

            cat_df = results_df[results_df["Validation Type"] == cat]

            pass_count = len(cat_df[cat_df["Status"] == "PASS"])

            fail_count = len(cat_df[cat_df["Status"] == "FAIL"])

            with st.expander(f"📊 {cat} ({pass_count} Pass / {fail_count} Fail)", expanded=(fail_count > 0)):

                st.dataframe(cat_df, width='stretch')

        st.download_button(

            label="📥 Download Excel Report",

            data=st.session_state["testcase_report_bytes"],

            file_name="migration_test_report.xlsx",

            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        )

        # Show queries in UI

        queries = st.session_state.get("testcase_queries", [])

        if queries:

            st.subheader("Queries Used")

            for idx, q in enumerate(queries):

                with st.expander(f"Query for Test Case {idx+1}"):

                    st.code(q, language="sql" if "SELECT" in q else "text")

def render_operation_tab(operation_name, operation_key):

    _op_icons = {'Insert': '📥', 'Update': '🔄', 'Upsert': '🔀'}
    _op_accents = {'Insert': 'indigo', 'Update': 'cyan', 'Upsert': 'cyan'}
    _op_icon = _op_icons.get(operation_name, '⚙️')
    _op_accent = _op_accents.get(operation_name, 'cyan')
    section_header(_op_icon, f'{operation_name} Records into Salesforce', f'Stream CSV data into any Salesforce object via Bulk API 2.0 with chunked uploads, mapping, and live progress.', accent=_op_accent)
    render_steps(['Data Source', 'Object', 'Mapping', 'Configure', 'Run'])
    render_bulk_jobs_quota_panel()

    # Data Source Selection

    st.markdown('### 📊 Data Source')

    data_source = st.radio(

        'Select Data Source',

        ['📄 File (CSV/Excel)', '❄️ Snowflake Table'],

        key=f'{operation_key}_data_source',

        horizontal=True,

        help='Load from file or directly from Snowflake table'

    )

    # ---- Fast source-switch: purge stale widget state immediately ----

    # When user switches data source, all mapping selectboxes from the

    # previous source are in session_state. Clean them up so the new

    # source renders fresh. Do NOT rerun — data_source already has the

    # correct new value, so the rest of the function renders correctly.

    _ds_track_key = f'{operation_key}_active_source'

    if st.session_state.get(_ds_track_key) != data_source:

        _stale = [

            f'{operation_key}_cached_mapping',

            f'{operation_key}_cached_mapping_src',

            f'{operation_key}_sf_preview_data',

            f'{operation_key}_sf_preview_table',

        ] + [k for k in st.session_state if k.startswith(f'{operation_key}_map_')]

        for _k in _stale:

            st.session_state.pop(_k, None)

        st.session_state[_ds_track_key] = data_source

        # No rerun needed — fall through and render the correct UI in this same pass

    col1, col2 = st.columns(2)

    with col1:

        object_name = st.text_input(

            'Salesforce Object API Name',

            value='',

            placeholder='e.g. Asset, Account, WOD_2__Warranty_Coverages__c',

            key=f'{operation_key}_object'

        )

    # ---- Invalidate cached mapping when object name changes ----

    _obj_track_key = f'{operation_key}_active_object'

    if st.session_state.get(_obj_track_key) != object_name.strip():

        _stale_obj = [

            f'{operation_key}_cached_mapping',

            f'{operation_key}_cached_mapping_src',

        ] + [k for k in st.session_state if k.startswith(f'{operation_key}_map_')]

        for _k in _stale_obj:

            st.session_state.pop(_k, None)

        st.session_state[_obj_track_key] = object_name.strip()

    with col2:

        chunk_size = st.number_input(

            f'Chunk Size (rows per SF API job)',

            min_value=MIN_CHUNK_SIZE, max_value=150000, value=MAX_CHUNK_SIZE, step=5000,

            key=f'{operation_key}_chunk',

            help=(

                f'💡 Tavant Loader optimal: 25,000 rows/job. '

                f'SF processes 25K in ~5s. '

                f'With 32 parallel threads = 800K records in-flight: 30M in ~12 min. '

                f'Larger chunks = slower SF processing per record. '

                f'Keep at 25,000 for maximum speed.'

            )

        )

    col3, col4 = st.columns(2)

    with col3:

        num_parallel = st.slider(

            f'Parallel Jobs (Max: {MAX_PARALLEL_JOBS})', 

            min_value=1, max_value=MAX_PARALLEL_JOBS, value=min(MAX_PARALLEL_JOBS, 32),

            key=f'{operation_key}_parallel',

            help=f'Tavant Loader uses 32 threads. More = faster pipeline. Auto-scales down on rate-limit errors.'

        )

        if num_parallel > MAX_PARALLEL_JOBS:

            st.warning(f'⚠️ Parallel jobs will be reduced to {MAX_PARALLEL_JOBS} to avoid API limits')

    external_id_field = None

    if operation_key == 'upsert':

        st.markdown('---')

        st.markdown('#### 🔑 Upsert Key Field')

        st.caption('Select the field Salesforce will use to match existing records for update.')

        if st.session_state.sf and object_name and object_name.strip():

            # Use cached version to avoid repeated API calls

            upsert_fields = get_cached_upsert_fields(st.session_state.sf, object_name.strip())

            if not upsert_fields:

                st.error('No upsert matching fields are available for this Salesforce object.')

                return

            field_options = [f"{fld['name']}  ({fld['tags']})" for fld in upsert_fields]

            field_names = [fld['name'] for fld in upsert_fields]

            selected_idx = st.selectbox(

                'Upsert Key Field (fetched from Salesforce)',

                range(len(field_options)),

                format_func=lambda i: field_options[i],

                key='upsert_external_id'

            )

            external_id_field = field_names[selected_idx]

            st.success(f'Will upsert on: **{external_id_field}**')

        else:

            if not object_name.strip():

                st.info('💡 Enter a Salesforce Object API Name above to auto-load upsert key fields.')

            else:

                st.warning('🔌 Connect to Salesforce first (sidebar) to auto-load upsert key fields.')

            external_id_field = st.text_input(

                'Or enter External ID Field manually',

                value='',

                placeholder='e.g. External_Id__c',

                key='upsert_external_id_manual'

            )

    # File or Snowflake Table Selection

    st.markdown('---')

    

    csv_path = None

    df_preview = None

    snowflake_table = None

    

    if data_source == '📄 File (CSV/Excel)':

        # File selection — support CSV, Excel, TSV, TXT

        supported_extensions = ('.csv', '.xlsx', '.xls', '.tsv', '.txt')

        # Use cached file list to avoid repeated os.listdir calls

        data_files = get_file_list(DATA_DIR, supported_extensions)

        selected_file = st.selectbox(

            'Select Data File', data_files if data_files else ['No data files found'],

            key=f'{operation_key}_file'

        )

        uploaded_file = st.file_uploader(

            'Or upload a file', type=['csv', 'xlsx', 'xls', 'tsv', 'txt'],

            key=f'{operation_key}_upload'

        )

        # Preview & Column Mapping

        csv_path = None

        df_preview = None

        def read_file_chunks(filepath, chunksize):

            ext = os.path.splitext(filepath)[1].lower()

            if ext in ('.xlsx', '.xls'):

                # Excel doesn't support chunked reading, read all then split

                df = pd.read_excel(filepath)

                for i in range(0, len(df), chunksize):

                    yield df.iloc[i:i + chunksize]

            elif ext == '.tsv':

                yield from pd.read_csv(filepath, sep='\t', chunksize=chunksize, low_memory=False)

            elif ext == '.txt':

                try:

                    sample = pd.read_csv(filepath, sep='\t', nrows=2, low_memory=False)

                    sep = '\t' if len(sample.columns) > 1 else ','

                except Exception:

                    sep = ','

                yield from pd.read_csv(filepath, sep=sep, chunksize=chunksize, low_memory=False)

            else:

                yield from pd.read_csv(filepath, chunksize=chunksize, low_memory=False)

        if uploaded_file is not None:

            csv_path = os.path.join(DATA_DIR, uploaded_file.name)

            with open(csv_path, 'wb') as f:

                f.write(uploaded_file.getbuffer())

            df_preview = read_file_preview_cached(csv_path)

        elif selected_file and selected_file != 'No data files found':

            csv_path = os.path.join(DATA_DIR, selected_file)

            df_preview = read_file_preview_cached(csv_path)

    

    else:  # Snowflake Table source

        st.markdown('#### ❄️ Snowflake Table Selection')

        

        if not st.session_state.sf_conn:

            st.warning('❌ Not connected to Snowflake. Please connect via sidebar (select ❄️ Snowflake mode).')

            st.info('💡 Go to sidebar → Select "❄️ Snowflake" → Enter credentials → Connect')

        else:

            st.success('✅ Connected to Snowflake')

            

            sf_table_input_col1, sf_table_input_col2 = st.columns([3, 1])

            with sf_table_input_col1:

                snowflake_table = st.text_input(

                    'Snowflake Table Name',

                    placeholder='DATABASE.SCHEMA.TABLE or SCHEMA.TABLE or TABLE',

                    key=f'{operation_key}_sf_table',

                    help='Enter full table name. Will use current database/schema if not specified.'

                )

            

            with sf_table_input_col2:

                st.write('')  # Spacer

                st.write('')  # Spacer

                load_preview = st.button('🔍 Preview', key=f'{operation_key}_sf_preview')

            

            # Persist preview in session state so it survives reruns

            preview_key = f'{operation_key}_sf_preview_data'

            preview_table_key = f'{operation_key}_sf_preview_table'

            

            if snowflake_table and load_preview:

                try:

                    cursor = st.session_state.sf_conn.cursor()

                    # Fetch only 5 sample rows — fast regardless of table size

                    cursor.execute(f"SELECT * FROM {snowflake_table} LIMIT 5")

                    df_preview = cursor.fetch_pandas_all()

                    cursor.close()

                    if not df_preview.empty:

                        st.session_state[preview_key] = df_preview

                        st.session_state[preview_table_key] = snowflake_table

                        st.success(f'✅ Found table with {len(df_preview.columns)} columns')

                        # Get row count via COUNT(*) — accurate and near-instant

                        # in Snowflake (uses metadata when possible).

                        # NOTE: INFORMATION_SCHEMA.ROW_COUNT is NOT real-time and

                        # can show 0 for several minutes after a fresh load.

                        try:

                            cursor2 = st.session_state.sf_conn.cursor()

                            cursor2.execute(f"SELECT COUNT(*) FROM {snowflake_table}")

                            row = cursor2.fetchone()

                            cursor2.close()

                            row_count = row[0] if row else None

                            if row_count is not None:

                                st.info(f'📊 Total rows in table: {int(row_count):,}')

                            else:

                                st.info('⚠️ Row count unavailable')

                        except Exception as _rc_err:

                            st.info(f'⚠️ Row count unavailable: {str(_rc_err)[:80]}')

                    else:

                        st.warning('⚠️ Table is empty')

                        if preview_key in st.session_state:

                            del st.session_state[preview_key]

                except Exception as e:

                    st.error(f'❌ Could not load table: {e}')

                    df_preview = None

                    if preview_key in st.session_state:

                        del st.session_state[preview_key]

            elif preview_key in st.session_state and st.session_state.get(preview_table_key) == snowflake_table:

                # Restore preview from session state

                df_preview = st.session_state[preview_key]

                st.success(f'✅ Table loaded with {len(df_preview.columns)} columns')

    if df_preview is not None:

        # Show row count next to preview — crucial so user knows actual file size before running

        _row_count_str = ''

        if csv_path:

            _rc = count_file_rows(csv_path)

            if _rc is not None:

                _row_count_str = f' — **{_rc:,} total rows** in file'

        elif snowflake_table:

            pass  # SF table row count already shown above

        st.write(f'**Data Preview (first 5 rows){_row_count_str}:**')

        st.dataframe(df_preview, width='stretch')

        # Fetch SF fields for smart mapping — only when user has typed an object name

        sf_fields = []

        sf_field_names = ['-- Skip --']

        if st.session_state.sf and object_name and object_name.strip():

            _all_sf_fields = get_cached_object_fields(st.session_state.sf, object_name.strip())

            # Filter to only writable / external-id fields — drops read-only computed fields

            if operation_key == 'insert':

                sf_fields = [f for f in _all_sf_fields if f.get('createable') or f.get('externalId')]

            elif operation_key == 'update':

                sf_fields = [f for f in _all_sf_fields if f.get('updateable') or f.get('name') == 'Id']

            else:

                sf_fields = [f for f in _all_sf_fields if f.get('updateable') or f.get('createable') or f.get('externalId') or f.get('idLookup')]

            # Exclude compound fields if Bulk API is selected

            use_rest_api = st.session_state.get('use_rest_api', False)

            from sf_bulk_loader import is_compound_field

            if not use_rest_api:

                orig_count = len(sf_fields)

                sf_fields = [f for f in sf_fields if not is_compound_field(f)]

                excluded_count = orig_count - len(sf_fields)

                if excluded_count > 0:

                    st.warning(f'⚠️ {excluded_count} compound field(s) excluded due to Bulk API limitations. Switch to REST API to use all fields.')

            sf_field_names = ['-- Skip --'] + [f['name'] for f in sf_fields]

        elif not object_name.strip():

            st.info('💡 Enter a Salesforce Object API Name above to enable field auto-mapping.')

        csv_columns = list(df_preview.columns)

        # Auto-match CSV columns to SF fields (skip .1 duplicates)

        auto_matches = {}

        if sf_fields:

            auto_matches = auto_match_csv_to_sf(csv_columns, sf_fields)

            for col in csv_columns:

                if re.search(r'\.\d+$', col):

                    auto_matches[col] = None

        st.markdown('---')

        st.markdown('**Column Mapping** (CSV Column → Salesforce Field)')

        # O(1) index lookup for selectbox defaults

        _sf_field_index = {name: i + 1 for i, name in enumerate(f['name'] for f in sf_fields)}

        column_mapping = {}

        if sf_fields:

            _matched_count = sum(1 for col in csv_columns

                                 if auto_matches.get(col) is not None

                                 and not re.search(r'\.\d+$', col))

            _total_mappable = sum(1 for col in csv_columns if not re.search(r'\.\d+$', col))

            st.caption(

                f'**{object_name}** — {len(sf_fields)} fields available. '

                f'Auto-matched **{_matched_count}/{_total_mappable}** columns. '

                f'Set to "-- Skip --" to exclude a column.'

            )

            cols_grid = st.columns(2)

            for i, col in enumerate(csv_columns):

                if re.search(r'\.\d+$', col):

                    continue  # skip .1 duplicate columns silently

                with cols_grid[i % 2]:

                    matched = auto_matches.get(col)

                    default_idx = _sf_field_index.get(matched, 0) if matched else 0

                    selected = st.selectbox(

                        col,

                        sf_field_names,

                        index=default_idx,

                        key=f'{operation_key}_map_{col}',

                    )

                    if selected != '-- Skip --':

                        column_mapping[col] = selected

            _skipped = len(csv_columns) - len(column_mapping)

            st.caption(f'✅ **{len(column_mapping)}** columns mapped, {_skipped} skipped')

        else:

            st.warning('🔌 Connect to Salesforce to auto-detect fields. Using manual text input.')

            cols_grid = st.columns(2)

            for i, col in enumerate(csv_columns):

                with cols_grid[i % 2]:

                    sf_field = st.text_input(

                        col, value=col,

                        key=f'{operation_key}_map_{col}',

                    )

                    if sf_field.strip():

                        column_mapping[col] = sf_field.strip()

        

        required_columns = list(column_mapping.values())

        # Date columns

        date_cols_input = st.text_input(

            'Date Columns (comma-separated, if any extra)',

            key=f'{operation_key}_datecols',

            placeholder='e.g. Start_Date__c, End_Date__c'

        )

        extra_date_cols = [c.strip() for c in date_cols_input.split(',') if c.strip()] if date_cols_input else None

        st.markdown('---')

        

        # Create button columns for Run and Stop

        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 3])

        with btn_col1:

            run_clicked = st.button(f'🚀 Run {operation_name}', type='primary', key=f'{operation_key}_run')

        with btn_col2:

            stop_clicked = st.button('🛑 STOP', key=f'{operation_key}_stop', help='Stop the ongoing operation', on_click=set_stop_flag)

        

        if stop_clicked:

            set_stop_flag()

            st.warning('⚠️ Stop signal sent. Operation will halt after current chunk completes.')

        

        if run_clicked:

            if not st.session_state.sf:

                st.error('Connect to Salesforce first (use sidebar).')

                return

            # Build error file path at run time (accurate timestamp, object_name guaranteed set)
            import datetime as _dt2, re as _re2
            _fail_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'failed', operation_key)
            os.makedirs(_fail_dir, exist_ok=True)
            _ts_now = _dt2.datetime.now().strftime('%Y%m%d_%H%M%S')
            if csv_path:
                _src_tag = _re2.sub(r'[^\w]', '_', os.path.splitext(os.path.basename(csv_path))[0])[:50]
            else:
                _src_tag = _re2.sub(r'[^\w]', '_', snowflake_table or 'snowflake')[:50]
            error_file = os.path.join(_fail_dir, f'failed_{object_name}_{_src_tag}_{_ts_now}.csv')

            sf_operation = operation_key
            object_api_name = normalize_sf_api_name(object_name)
            upsert_key_field = normalize_sf_api_name(external_id_field) if sf_operation == 'upsert' else None

            if not object_api_name:

                st.error('❌ Enter a valid Salesforce Object API Name.')

                return

            if sf_operation == 'upsert' and not upsert_key_field:

                st.error('❌ Upsert requires a valid External ID field. Select one or enter the API name only.')

                return

            if sf_operation == 'update' and 'Id' not in required_columns:

                st.error('Update requires a source column mapped to Salesforce Id.')

                return

            if sf_operation == 'upsert' and upsert_key_field not in required_columns:

                st.error(f'Upsert requires a source column mapped to Salesforce {upsert_key_field}.')

                return

            _timing_extra_steps = []  # extra (label, secs) steps measured outside bulk_load_v2

            # --- Tavant Loader-style Live Dashboard ---
            clear_stop_flag()  # reset any leftover stop flag from a previous run

            dashboard = LiveDashboard(

                st, total_records=0, operation=operation_name, object_name=object_name

            )

            # Handle data source

            if data_source == '📄 File (CSV/Excel)':

                # File source

                if not csv_path:

                    st.error('❌ No file selected')

                    return

                dashboard.on_status(f'📄 Reading file and submitting to Salesforce Bulk API…')

                # For non-CSV files, pass a chunk iterator

                file_ext = os.path.splitext(csv_path)[1].lower()

                chunks = None

                if file_ext in ('.xlsx', '.xls', '.tsv', '.txt'):

                    chunks = read_file_chunks(csv_path, chunk_size)

                result = bulk_load_v2(

                    manage_stop_flag=False,

                    csv_file_path=csv_path,

                    object_name=object_api_name,

                    sf=st.session_state.sf,

                    required_columns=required_columns,

                    chunk_size=chunk_size,

                    operation=sf_operation,

                    external_id_field=upsert_key_field,

                    column_mapping=column_mapping,

                    error_file=error_file,

                    num_parallel_chunks=num_parallel,

                    date_columns=extra_date_cols,

                    on_progress=dashboard.on_progress,

                    on_status=dashboard.on_status,

                    on_error=dashboard.on_error,

                    chunk_iterator=chunks

                )

            else:

                # Snowflake table source

                if not snowflake_table:

                    st.error('❌ No Snowflake table specified')

                    return

                

                if not st.session_state.sf_conn:

                    st.error('❌ Not connected to Snowflake. Connect via sidebar.')

                    return

                

                # Live status panel — shows fetch + load progress in real time

                _time_sf = time

                phase_box   = st.empty()   # current phase header

                detail_box  = st.empty()   # scrolling detail line

                metrics_box = st.empty()   # live row counters — single text line (no flicker)

                def _show_metrics(fetched, sent, success, failed, elapsed):

                    metrics_box.info(

                        f'⬇️ Fetched: **{fetched:,}** | '

                        f'📤 Sent: **{sent:,}** | '

                        f'✅ Success: **{success:,}** | '

                        f'❌ Failed: **{failed:,}** | '

                        f'⏱️ {elapsed:.0f}s'

                    )

                try:

                    sf_load_start = _time_sf.time()

                    phase_box.info('**Phase 1 / 2 — Connecting to Snowflake & executing query...**')

                    cursor = st.session_state.sf_conn.cursor()

                    if column_mapping:
                        validate_snowflake_column_mapping(column_mapping)

                        selected_cols = ', '.join([f'"{k}" as "{v}"' for k, v in column_mapping.items()])

                        query = f"SELECT {selected_cols} FROM {snowflake_table}"

                    else:

                        query = f"SELECT * FROM {snowflake_table}"

                    cursor.execute(query)

                    query_time = _time_sf.time() - sf_load_start

                    _timing_extra_steps.append(('Snowflake Query Execution', round(query_time, 2)))

                    detail_box.caption(f'Query executed in {query_time:.1f}s — fetching result batches...')

                    # --- TRUE STREAMING via get_result_batches() ---

                    try:

                        result_batches = cursor.get_result_batches()

                        cursor.close()

                        use_streaming = True

                    except Exception:

                        use_streaming = False

                    # Shared live counters updated by the stream generator

                    _live = {'fetched': 0, 'sent': 0}

                    # Cumulative time spent inside batch.to_pandas() across all batches

                    _dl_total_s = {'v': 0.0}

                    if use_streaming and result_batches:

                        total_batches = len(result_batches)

                        phase_box.info(

                            f'**Phase 2 / 2 — Streaming {total_batches} batch(es) from Snowflake ? Salesforce '

                            f'(parallel pipeline mode)**'

                        )

                        _show_metrics(0, 0, 0, 0, 0)

                        # --- Parallel prefetch: download next N batches while

                        # current batches are being uploaded to Salesforce ---

                        _PREFETCH = min(4, total_batches)  # download up to 4 ahead

                        def _snowflake_stream(batches, target_size):

                            """Download Snowflake batches in background thread;

                            yield SF-sized chunks with minimal wait.

                            Uses a list buffer to avoid O(n²) pd.concat."""

                            batch_q = queue.Queue(maxsize=_PREFETCH)

                            def _downloader():

                                for b_idx, batch in enumerate(batches, 1):

                                    _t0 = _time_sf.time()

                                    try:

                                        part = batch.to_pandas()

                                    except Exception:

                                        part = pd.DataFrame()

                                    _dl_total_s['v'] += _time_sf.time() - _t0

                                    batch_q.put((b_idx, part))

                                batch_q.put(None)  # sentinel

                            dl_thread = threading.Thread(

                                target=_downloader, daemon=True

                            )

                            dl_thread.start()

                            # Accumulate parts in a list; concat only when yielding

                            # This is O(n) instead of O(n²) vs repeated pd.concat

                            buf_parts = []

                            buf_len   = 0

                            while True:

                                item = batch_q.get()

                                if item is None:

                                    break

                                b_idx, part = item

                                if part.empty:

                                    continue

                                _live['fetched'] += len(part)

                                detail_box.caption(

                                    f'⬇️ Fetched batch {b_idx}/{total_batches} '

                                    f'(+{len(part):,} rows) | total fetched: {_live["fetched"]:,}'

                                )

                                buf_parts.append(part)

                                buf_len += len(part)

                                while buf_len >= target_size:

                                    combined   = pd.concat(buf_parts, ignore_index=True)

                                    chunk_out  = combined.iloc[:target_size].copy()

                                    remainder  = combined.iloc[target_size:].reset_index(drop=True)

                                    buf_parts  = [remainder] if not remainder.empty else []

                                    buf_len    = len(remainder)

                                    _live['sent'] += len(chunk_out)

                                    _show_metrics(

                                        _live['fetched'], _live['sent'], 0, 0,

                                        _time_sf.time() - sf_load_start

                                    )

                                    yield chunk_out

                            # Yield any remaining rows

                            if buf_parts:

                                combined = pd.concat(buf_parts, ignore_index=True)

                                _live['sent'] += len(combined)

                                yield combined

                        def _on_status_live(msg, level='info'):

                            dashboard.on_status(msg, level=level)

                            elapsed = time.time() - sf_load_start

                            m = re.search(r'(\d[\d,]*)\s+success.*?(\d[\d,]*)\s+fail', msg, re.I)

                            if m:

                                suc = int(m.group(1).replace(',', ''))

                                fai = int(m.group(2).replace(',', ''))

                                _show_metrics(_live['fetched'], _live['sent'], suc, fai, elapsed)

                            detail_box.caption(msg)

                        def _on_error_live(msg):

                            dashboard.on_error(msg)

                            detail_box.caption(f'❌ {msg}')

                        result = bulk_load_v2(

                            manage_stop_flag=False,

                            csv_file_path=None,

                            object_name=object_api_name,

                            sf=st.session_state.sf,

                            required_columns=required_columns,

                            chunk_size=chunk_size,

                            operation=sf_operation,

                            external_id_field=upsert_key_field,

                            column_mapping=None,

                            error_file=error_file,

                            num_parallel_chunks=num_parallel,

                            date_columns=extra_date_cols,

                            on_progress=dashboard.on_progress,

                            on_status=_on_status_live,

                            on_error=_on_error_live,

                            chunk_iterator=_snowflake_stream(result_batches, chunk_size)

                        )

                    else:

                        phase_box.info('**Phase 2 / 2 — Downloading full result set from Snowflake...**')

                        _t_dl = _time_sf.time()

                        df_full = cursor.fetch_pandas_all()

                        _dl_total_s['v'] = _time_sf.time() - _t_dl

                        cursor.close()

                        if df_full.empty:

                            st.error('⚠️ Snowflake table is empty')

                            return

                        fetch_time = time.time() - sf_load_start

                        _live['fetched'] = len(df_full)

                        phase_box.info(

                            f'**Phase 2 / 2 — Uploading {len(df_full):,} rows ? Salesforce**'

                        )

                        detail_box.caption(

                            f'⬇️ Downloaded {len(df_full):,} rows × {len(df_full.columns)} cols in {fetch_time:.1f}s'

                        )

                        _show_metrics(len(df_full), 0, 0, 0, fetch_time)

                        def _df_iter(df, size):

                            for i in range(0, len(df), size):

                                chunk_out = df.iloc[i:i + size].copy()

                                _live['sent'] += len(chunk_out)

                                yield chunk_out

                        def _on_status_live(msg, level='info'):

                            dashboard.on_status(msg, level=level)

                            elapsed = time.time() - sf_load_start

                            m = re.search(r'(\d[\d,]*)\s+success.*?(\d[\d,]*)\s+fail', msg, re.I)

                            if m:

                                suc = int(m.group(1).replace(',', ''))

                                fai = int(m.group(2).replace(',', ''))

                                _show_metrics(_live['fetched'], _live['sent'], suc, fai, elapsed)

                            detail_box.caption(msg)

                        def _on_error_live(msg):

                            dashboard.on_error(msg)

                            detail_box.caption(f'❌ {msg}')

                        result = bulk_load_v2(

                            csv_file_path=None,

                            object_name=object_api_name,

                            sf=st.session_state.sf,

                            required_columns=required_columns,

                            chunk_size=chunk_size,

                            operation=sf_operation,

                            external_id_field=upsert_key_field,

                            column_mapping=None,

                            error_file=error_file,

                            num_parallel_chunks=num_parallel,

                            date_columns=extra_date_cols,

                            on_progress=dashboard.on_progress,

                            on_status=_on_status_live,

                            on_error=_on_error_live,

                            chunk_iterator=_df_iter(df_full, chunk_size),
                            manage_stop_flag=False,

                        )

                    # Final summary in the metrics panel

                    if result:

                        elapsed = _time_sf.time() - sf_load_start

                        _show_metrics(

                            _live['fetched'], _live['sent'],

                            result['total_success'], result['total_failed'], elapsed

                        )

                        phase_box.success('✅ Done!')

                except Exception as e:

                    st.error(f'❌ Error loading from Snowflake: {e}')

                    return

                # Record actual Snowflake ? DataFrame fetch time as its own step

                if _dl_total_s['v'] > 0:

                    _timing_extra_steps.append((

                        'Snowflake → DataFrame Fetch',

                        round(_dl_total_s['v'], 2),

                    ))

            if result:

                dashboard.finalize(result)

                col_r1, col_r2, col_r3, col_r4 = st.columns(4)

                col_r1.metric('Total Processed', f"{result['total_processed']:,}")

                col_r2.metric('Success', f"{result['total_success']:,}")

                col_r3.metric('Failed', f"{result['total_failed']:,}")

                col_r4.metric('Time', f"{result['elapsed']:.1f}s")

                if result['error_file']:

                    st.warning(f"Failed records saved to: {result['error_file']}")

                    failed_df = pd.read_csv(result['error_file'])

                    st.dataframe(failed_df.head(20), width='stretch')

                else:

                    st.success('All records loaded successfully!')

                st.toast(f'✅ {operation_name} completed: {result["total_success"]:,} records processed.')

                # Step-by-step timing report

                _show_timing_report(

                    result,

                    operation_name=operation_name,

                    extra_steps=_timing_extra_steps if _timing_extra_steps else None,

                )


with tab_testcase:

    _tab_testcase()

def _tab_insert():

    render_operation_tab('Insert', 'insert')

def _tab_update():

    render_operation_tab('Update', 'update')

def _tab_upsert():

    render_operation_tab('Upsert', 'upsert')

with tab_insert:

    _tab_insert()

with tab_update:

    _tab_update()

with tab_upsert:

    _tab_upsert()

def _tab_delete():

    section_header('🗑️', 'Delete Records from Salesforce', 'Bulk-delete records by Id with optional safety checks and dry-run preview.', accent='pink')
    render_steps(['Data Source', 'Object', 'Confirm', 'Run'])
    render_bulk_jobs_quota_panel()

    st.caption('Upload a CSV/Excel file containing Salesforce record IDs to delete.')

    del_col1, del_col2 = st.columns(2)

    with del_col1:

        del_object_name = st.text_input(

            'Salesforce Object API Name',

            value='',

            key='delete_object'

        )

    with del_col2:

        del_chunk_size = st.number_input(

            f'Chunk Size (rows per SF API job)', 

            min_value=MIN_CHUNK_SIZE, max_value=150000, value=MAX_CHUNK_SIZE, step=5000,

            key='delete_chunk',

            help='💡 10,000 rows/job = optimal. SF processes small jobs in 2-3s for max throughput.'

        )

        if del_chunk_size > MAX_CHUNK_SIZE:

            st.warning(f'⚠️ Chunk size will be reduced to {MAX_CHUNK_SIZE:,}')

    del_col3, del_col4 = st.columns(2)

    with del_col3:

        del_parallel = st.slider(

            f'Parallel Jobs (Max: {MAX_PARALLEL_JOBS})', 

            min_value=1, max_value=MAX_PARALLEL_JOBS, value=min(MAX_PARALLEL_JOBS, 32),

            key='delete_parallel',

            help=f'Tavant Loader uses 32 threads. Auto-scales down on errors.'

        )

        if del_parallel > MAX_PARALLEL_JOBS:

            st.warning(f'⚠️ Parallel jobs will be reduced to {MAX_PARALLEL_JOBS}')

    # Data Source Selection
    st.markdown('---')
    del_source_mode = st.radio(
        'Data Source',
        ['📄 File (CSV/Excel)', '🔍 SOQL Query', '❄️ Snowflake Table'],
        key='delete_source_mode',
        horizontal=True,
        help='Choose a file with IDs, write a SOQL query, or load IDs from a Snowflake table'
    )

    supported_extensions = ('.csv', '.xlsx', '.xls', '.tsv', '.txt')

    del_data_files = get_file_list(DATA_DIR, supported_extensions)

    if del_source_mode == '📄 File (CSV/Excel)':
        del_selected_file = st.selectbox(

            'Select Data File', del_data_files if del_data_files else ['No data files found'],

            key='delete_file'

        )
    else:
        del_selected_file = None

    if st.session_state.get('_del_mode') == 'soql' and del_source_mode != '🔍 SOQL Query':
        st.session_state.pop('_del_confirm_pending', None)
        st.session_state.pop('_del_pending_count', None)


    del_csv_path = None
    del_df_preview = None
    del_id_column = 'Id'
    del_soql_ids_df = None

    def del_read_chunks(filepath, chunksize):

        ext = os.path.splitext(filepath)[1].lower()

        if ext in ('.xlsx', '.xls'):

            df = pd.read_excel(filepath)

            for i in range(0, len(df), chunksize):

                yield df.iloc[i:i + chunksize]

        elif ext == '.tsv':

            yield from pd.read_csv(filepath, sep='\t', chunksize=chunksize, low_memory=False)

        elif ext == '.txt':

            try:

                sample = pd.read_csv(filepath, sep='\t', nrows=2, low_memory=False)

                sep = '\t' if len(sample.columns) > 1 else ','

            except Exception:

                sep = ','

            yield from pd.read_csv(filepath, sep=sep, chunksize=chunksize, low_memory=False)

        else:

            yield from pd.read_csv(filepath, chunksize=chunksize, low_memory=False)

    if del_source_mode == '📄 File (CSV/Excel)':
        del_uploaded_file = st.file_uploader(

            'Or upload a file', type=['csv', 'xlsx', 'xls', 'tsv', 'txt'],

            key='delete_upload'

        )

        if del_uploaded_file is not None:

            del_csv_path = os.path.join(DATA_DIR, del_uploaded_file.name)

            with open(del_csv_path, 'wb') as f:

                f.write(del_uploaded_file.getbuffer())

            del_df_preview = read_file_preview_cached(del_csv_path)

        elif del_selected_file and del_selected_file != 'No data files found':

            del_csv_path = os.path.join(DATA_DIR, del_selected_file)

            del_df_preview = read_file_preview_cached(del_csv_path)

    elif del_source_mode == '🔍 SOQL Query':  # SOQL Query mode
        if not st.session_state.sf:
            for _state_key in ('_del_soql_preview_df', '_del_soql_total', '_del_soql_q', '_del_soql_object', '_del_soql_context'):
                st.session_state.pop(_state_key, None)
            if st.session_state.get('_del_mode') == 'soql':
                st.session_state.pop('_del_confirm_pending', None)
                st.session_state.pop('_del_pending_count', None)
            st.warning('🔌 Connect to Salesforce first (sidebar) to use SOQL query mode.')
        else:
            st.markdown('#### 🔍 SOQL Query')
            st.caption('Write a SOQL SELECT query — the **Id** field will be used for deletion.')
            del_soql_query = st.text_area(
                'SOQL Query',
                value=f'SELECT Id FROM {del_object_name} WHERE IsDeleted = false LIMIT 200',
                height=100,
                key='delete_soql_query',
                help='Query must return the Id field. Results will be fetched then deleted.'
            )
            _soql_context = (del_soql_query.strip().rstrip(';'), del_object_name.strip(), id(st.session_state.sf))
            if st.session_state.get('_del_soql_context') != _soql_context:
                for _state_key in ('_del_soql_preview_df', '_del_soql_total', '_del_soql_q', '_del_soql_object'):
                    st.session_state.pop(_state_key, None)
                if st.session_state.get('_del_mode') == 'soql':
                    st.session_state.pop('_del_confirm_pending', None)
                    st.session_state.pop('_del_pending_count', None)
            _soql_col1, _soql_col2 = st.columns(2)
            with _soql_col1:
                soql_count_clicked = st.button('📊 Count / Preview Records', key='delete_soql_count')
            if soql_count_clicked or st.session_state.get('_del_soql_preview_df') is not None:
                if soql_count_clicked:
                    st.session_state.pop('_del_confirm_pending', None)
                    st.session_state.pop('_del_pending_count', None)
                    try:
                        _count_q = del_soql_query.strip().rstrip(';')
                        if not _count_q:
                            raise ValueError('Enter a SOQL SELECT query containing Id.')
                        with st.spinner('Counting and previewing matching records...'):
                            _prev_r = st.session_state.sf.query(_count_q)
                        _total_count = _prev_r.get('totalSize')
                        _prev_records = _prev_r.get('records', [])
                        if _prev_records:
                            _prev_df = pd.DataFrame(_prev_records[:10]).drop(columns=['attributes'], errors='ignore')
                            if 'Id' not in _prev_df.columns or _prev_df['Id'].isna().any():
                                raise ValueError('Query must SELECT the record Id field for deletion.')
                            _query_object = _prev_records[0].get('attributes', {}).get('type', '')
                            if not _query_object:
                                raise ValueError('Cannot determine the Salesforce object from the query results.')
                            if del_object_name.strip() and del_object_name.strip().lower() != _query_object.lower():
                                raise ValueError(f'Query returns {_query_object}, but the selected object is {del_object_name.strip()}.')
                            st.session_state['_del_soql_preview_df'] = _prev_df
                            st.session_state['_del_soql_total'] = _total_count
                            st.session_state['_del_soql_q'] = _count_q
                            st.session_state['_del_soql_object'] = _query_object
                            st.session_state['_del_soql_context'] = _soql_context
                        else:
                            st.warning('⚠️ Query returned no records.')
                            st.session_state.pop('_del_soql_preview_df', None)
                    except Exception as _soql_err:
                        st.error(f'❌ SOQL error: {_soql_err}')
                        st.session_state.pop('_del_soql_preview_df', None)

                if st.session_state.get('_del_soql_preview_df') is not None:
                    _prev_df = st.session_state['_del_soql_preview_df']
                    _total = st.session_state.get('_del_soql_total')
                    _total_str = f'{_total:,}' if _total is not None else 'unknown'
                    st.info(f'📊 **Total matching records: {_total_str}** — preview of first {len(_prev_df)} shown below')
                    st.dataframe(_prev_df, width='stretch')
                    del_soql_ids_df = _prev_df

                    st.markdown('---')
                    _sq_btn1, _sq_btn2, _ = st.columns([1, 1, 3])
                    with _sq_btn1:
                        soql_run_clicked = st.button('🗑️ Run Delete', type='primary', key='delete_soql_run')
                    with _sq_btn2:
                        soql_stop_clicked = st.button('🛑 STOP', key='delete_soql_stop', help='Stop the ongoing operation', on_click=set_stop_flag)
                    if soql_stop_clicked:
                        set_stop_flag()
                        st.warning('⚠️ Stop signal sent.')
                    if soql_run_clicked:
                        _stotal = st.session_state.get('_del_soql_total')
                        if not st.session_state.get('_del_confirm_pending'):
                            st.session_state['_del_confirm_pending'] = True
                            st.session_state['_del_pending_count'] = _stotal
                            st.session_state['_del_mode'] = 'soql'
                            st.session_state['_del_object_name'] = st.session_state['_del_soql_object']
                            st.session_state['_del_chunk_size'] = del_chunk_size
                            st.session_state['_del_parallel'] = del_parallel
                            st.rerun()

    elif del_source_mode == '❄️ Snowflake Table':
        st.markdown('#### ❄️ Snowflake Table Selection')
        st.caption('Fetch record IDs from a Snowflake table — the **Id** column will be used for deletion.')
        if not st.session_state.get('sf_conn'):
            st.warning('❌ Not connected to Snowflake. Please connect via sidebar (select ❄️ Snowflake mode).')
            st.info('💡 Go to sidebar → Select "❄️ Snowflake" → Enter credentials → Connect')
        else:
            st.success('✅ Connected to Snowflake')
            _del_snow_col1, _del_snow_col2 = st.columns([3, 1])
            with _del_snow_col1:
                del_snow_table = st.text_input(
                    'Snowflake Table Name',
                    placeholder='DATABASE.SCHEMA.TABLE or SCHEMA.TABLE or TABLE',
                    key='delete_sf_table',
                    help='Enter full table name. Must contain an Id column with Salesforce Record IDs.'
                )
            with _del_snow_col2:
                st.write('')
                st.write('')
                del_snow_preview_clicked = st.button('🔍 Preview', key='delete_sf_preview')

            _del_snow_preview_key = 'delete_sf_preview_data'
            _del_snow_table_key = 'delete_sf_preview_table'

            if del_snow_table and del_snow_preview_clicked:
                try:
                    _cursor = st.session_state.sf_conn.cursor()
                    _cursor.execute(f"SELECT * FROM {del_snow_table} LIMIT 5")
                    del_df_preview = _cursor.fetch_pandas_all()
                    _cursor.close()
                    if not del_df_preview.empty:
                        st.session_state[_del_snow_preview_key] = del_df_preview
                        st.session_state[_del_snow_table_key] = del_snow_table
                        st.success(f'✅ Found table with {len(del_df_preview.columns)} columns')
                        try:
                            _cursor2 = st.session_state.sf_conn.cursor()
                            _cursor2.execute(f"SELECT COUNT(*) FROM {del_snow_table}")
                            _row = _cursor2.fetchone()
                            _cursor2.close()
                            if _row:
                                st.info(f'📊 Total rows in table: {int(_row[0]):,}')
                        except Exception:
                            pass
                    else:
                        st.warning('⚠️ Table is empty')
                        st.session_state.pop(_del_snow_preview_key, None)
                except Exception as _e:
                    st.error(f'❌ Could not load table: {_e}')
                    del_df_preview = None
                    st.session_state.pop(_del_snow_preview_key, None)
            elif st.session_state.get(_del_snow_preview_key) is not None and st.session_state.get(_del_snow_table_key) == del_snow_table:
                del_df_preview = st.session_state[_del_snow_preview_key]
                st.success(f'✅ Table loaded with {len(del_df_preview.columns)} columns')

            if del_df_preview is not None:
                st.write('**Data Preview (first 5 rows):**')
                st.dataframe(del_df_preview, width='stretch')
                del_columns = list(del_df_preview.columns)
                del_id_column = st.selectbox(
                    '🔑 Select the column containing Salesforce Record IDs',
                    del_columns,
                    index=del_columns.index('Id') if 'Id' in del_columns else 0,
                    key='delete_snow_id_col'
                )
                st.markdown('---')
                _del_snow_btn1, _del_snow_btn2, _ = st.columns([1, 1, 3])
                with _del_snow_btn1:
                    del_snow_run_clicked = st.button('🗑️ Run Delete', type='primary', key='delete_snow_run')
                with _del_snow_btn2:
                    del_snow_stop_clicked = st.button('🛑 STOP', key='delete_snow_stop', help='Stop the ongoing operation', on_click=set_stop_flag)
                if del_snow_stop_clicked:
                    set_stop_flag()
                    st.warning('⚠️ Stop signal sent.')
                if del_snow_run_clicked:
                    if not st.session_state.sf:
                        st.error('Connect to Salesforce first (use sidebar).')
                    elif not del_snow_table:
                        st.error('Enter a Snowflake table name above.')
                    elif not st.session_state.get('_del_confirm_pending'):
                        st.session_state['_del_confirm_pending'] = True
                        st.session_state['_del_pending_count'] = None
                        st.session_state['_del_mode'] = 'snowflake'
                        st.session_state['_del_snow_table'] = del_snow_table
                        st.session_state['_del_snow_id_col'] = del_id_column
                        st.rerun()

    if del_source_mode == '📄 File (CSV/Excel)' and del_df_preview is not None:

        st.write('**Data Preview (first 5 rows):**')

        st.dataframe(del_df_preview, width='stretch')

    

        del_columns = list(del_df_preview.columns)

        del_id_column = st.selectbox(

            '🔑 Select the column containing Salesforce Record IDs',

            del_columns,

            index=del_columns.index('Id') if 'Id' in del_columns else 0,

            key='delete_id_col'

        )

    

        st.markdown('---')

        

        # Create button columns for Run and Stop

        del_btn_col1, del_btn_col2, del_btn_col3 = st.columns([1, 1, 3])

        with del_btn_col1:

            del_run_clicked = st.button('🗑️ Run Delete', type='primary', key='delete_run')

        with del_btn_col2:

            del_stop_clicked = st.button('🛑 STOP', key='delete_stop', help='Stop the ongoing operation', on_click=set_stop_flag)

        

        if del_stop_clicked:

            set_stop_flag()

            st.warning('⚠️ Stop signal sent. Operation will halt after current chunk completes.')

        if del_run_clicked:

            if not st.session_state.sf:

                st.error('Connect to Salesforce first (use sidebar).')

            else:

                # Count records before deleting so user sees how many will be affected
                try:
                    _del_ext = os.path.splitext(del_csv_path)[1].lower()
                    if _del_ext in ('.xlsx', '.xls'):
                        _del_count = len(pd.read_excel(del_csv_path))
                    else:
                        _del_count = sum(1 for _ in open(del_csv_path, 'rb')) - 1
                except Exception:
                    _del_count = None

                _del_count_str = f'{_del_count:,}' if _del_count is not None else 'unknown number of'
                if not st.session_state.get('_del_confirm_pending'):
                    st.session_state['_del_confirm_pending'] = True
                    st.session_state['_del_pending_count'] = _del_count
                    st.session_state['_del_mode'] = 'file'
                    st.session_state['_del_file_path'] = del_csv_path
                    st.session_state['_del_id_column'] = del_id_column
                    st.session_state['_del_object_name'] = del_object_name
                    st.session_state['_del_chunk_size'] = del_chunk_size
                    st.session_state['_del_parallel'] = del_parallel
                    st.rerun()

    if st.session_state.get('_del_confirm_pending'):
        _pcount = st.session_state.get('_del_pending_count')
        _pcount_str = f'{_pcount:,}' if _pcount is not None else 'unknown number of'
        _confirm_object = st.session_state.get('_del_object_name', del_object_name)
        st.warning(f'⚠️ You are about to delete **{_pcount_str} records** from **{_confirm_object}** in Salesforce.')
        _dc1, _dc2 = st.columns(2)
        with _dc1:
            _del_confirmed = st.button('✅ Yes, Delete Now', key='del_confirm_yes', type='primary')
        with _dc2:
            _del_cancelled = st.button('❌ Cancel', key='del_confirm_no')
        if _del_cancelled:
            st.session_state.pop('_del_confirm_pending', None)
            st.session_state.pop('_del_pending_count', None)
            st.info('Delete cancelled.')
        elif _del_confirmed:
            st.session_state.pop('_del_confirm_pending', None)
            st.session_state.pop('_del_pending_count', None)
            clear_stop_flag()

            _del_mode_now = st.session_state.get('_del_mode', 'file')
            _del_object_name_now = st.session_state.get('_del_object_name', del_object_name)
            _del_chunk_size_now = st.session_state.get('_del_chunk_size', del_chunk_size)
            _del_parallel_now = st.session_state.get('_del_parallel', del_parallel)
            if _del_mode_now == 'soql':
                _soql_q_saved = st.session_state.get('_del_soql_q', '')
                if not _soql_q_saved:
                    st.error('❌ No SOQL query saved. Please run Count/Preview first.')
                    st.stop()
                _export_status = st.empty()
                _export_status.info('Submitting Bulk API 2.0 query export...')
                try:
                    del_csv_path, _export_count = bulk2_query_ids_to_csv(
                        st.session_state.sf, _soql_q_saved, on_progress=_export_status.info
                    )
                    if not _export_count:
                        os.remove(del_csv_path)
                        st.warning('⚠️ SOQL query returned no records to delete.')
                        st.stop()
                    del_id_column = 'Id'
                except InterruptedError as _soql_stop:
                    st.warning(str(_soql_stop))
                    st.stop()
                except Exception as _soql_fetch_err:
                    st.error(f'❌ Bulk query export failed; deletion was not started: {_soql_fetch_err}')
                    st.stop()

            elif _del_mode_now == 'snowflake':
                _snow_tbl = st.session_state.get('_del_snow_table', '')
                _snow_id_col = st.session_state.get('_del_snow_id_col', 'Id')
                if not _snow_tbl:
                    st.error('❌ No Snowflake table saved. Please preview the table first.')
                    st.stop()
                st.info(f'⏳ Fetching all record IDs from Snowflake table {_snow_tbl}...')
                try:
                    _cursor_del = st.session_state.sf_conn.cursor()
                    _cursor_del.execute(f'SELECT "{_snow_id_col}" FROM {_snow_tbl}')
                    _ids_df_snow = _cursor_del.fetch_pandas_all()
                    _cursor_del.close()
                    if _ids_df_snow.empty:
                        st.warning('⚠️ Snowflake table returned no records to delete.')
                        st.stop()
                    _tmp_snow = tempfile.NamedTemporaryFile(suffix='.csv', delete=False, prefix='snow_del_', mode='w', newline='', encoding='utf-8')
                    _ids_df_snow.to_csv(_tmp_snow.name, index=False)
                    _tmp_snow.close()
                    del_csv_path = _tmp_snow.name
                    del_id_column = _snow_id_col
                    _del_object_name_now = st.session_state.get('_del_object_name', del_object_name)
                    st.success(f'✅ Fetched {len(_ids_df_snow):,} record IDs from Snowflake for deletion.')
                except Exception as _snow_fetch_err:
                    st.error(f'❌ Failed to fetch IDs from Snowflake: {_snow_fetch_err}')
                    st.stop()
            else:
                del_csv_path = st.session_state.get('_del_file_path')
                del_id_column = st.session_state.get('_del_id_column', del_id_column)
                if not del_csv_path:
                    st.error('❌ Delete file context was lost. Please select the file again and retry.')
                    st.stop()

            del_dashboard = LiveDashboard(
                st, total_records=_export_count if _del_mode_now == 'soql' else 0,
                operation='Delete', object_name=_del_object_name_now
            )

            import datetime as _dt3, re as _re3
            _del_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'failed', 'delete')
            os.makedirs(_del_dir, exist_ok=True)
            _del_ts = _dt3.datetime.now().strftime('%Y%m%d_%H%M%S')
            _del_src = _re3.sub(r'[^\w]', '_', os.path.splitext(os.path.basename(del_csv_path))[0])[:50]
            del_error_file = os.path.join(_del_dir, f'failed_{_del_object_name_now}_{_del_src}_{_del_ts}.csv')

            file_ext = os.path.splitext(del_csv_path)[1].lower()
            del_chunks = None
            if file_ext in ('.xlsx', '.xls', '.tsv', '.txt'):
                del_chunks = del_read_chunks(del_csv_path, _del_chunk_size_now)

            try:
                result = bulk_delete_v2(
                    manage_stop_flag=False,
                    csv_file_path=del_csv_path,
                    object_name=_del_object_name_now,
                    sf=st.session_state.sf,
                    id_column=del_id_column,
                    chunk_size=_del_chunk_size_now,
                    error_file=del_error_file,
                    num_parallel_chunks=_del_parallel_now,
                    on_progress=del_dashboard.on_progress,
                    on_status=del_dashboard.on_status,
                    on_error=del_dashboard.on_error,
                    chunk_iterator=del_chunks
                )
            finally:
                if _del_mode_now == 'soql' and del_csv_path and os.path.exists(del_csv_path):
                    os.remove(del_csv_path)

            if result is None:
                st.error('❌ Delete operation failed unexpectedly. Check the dashboard events above for details.')
            elif result:
                del_dashboard.finalize(result)
                col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                col_r1.metric('Total Processed', f"{result['total_processed']:,}")
                col_r2.metric('Deleted', f"{result['total_success']:,}")
                col_r3.metric('Failed', f"{result['total_failed']:,}")
                col_r4.metric('Time', f"{result['elapsed']:.1f}s")

                if result['error_file']:
                    st.error(f"❌ {result['total_failed']:,} record(s) failed to delete. Details below:")
                    try:
                        failed_df = pd.read_csv(result['error_file'], keep_default_na=False)
                        # Show sf__Error first so the reason is immediately visible
                        _err_cols = ['sf__Id', 'sf__Error'] + [c for c in failed_df.columns if c not in ('sf__Id', 'sf__Error')]
                        _err_cols = [c for c in _err_cols if c in failed_df.columns]
                        st.dataframe(failed_df[_err_cols].head(50), use_container_width=True)
                        st.caption(f'Full error log saved to: `{result["error_file"]}`')
                    except Exception as _read_err:
                        st.warning(f'Could not read error file: {_read_err}')
                        st.caption(f'Error file path: `{result["error_file"]}`')
                elif result['total_failed'] > 0:
                    st.error(f"❌ {result['total_failed']:,} record(s) failed but error details could not be saved. Check dashboard events above.")
                else:
                    st.success('✅ All records deleted successfully!')
                _toast_icon = '✅' if result['total_failed'] == 0 else '⚠️'
                st.toast(f"{_toast_icon} Delete completed: {result['total_success']:,} deleted, {result['total_failed']:,} failed.")
                queue_action_feedback(
                    'Delete completed' if result['total_failed'] == 0 else 'Delete partially completed',
                    f"{result['total_success']:,} record(s) deleted; {result['total_failed']:,} failed.",
                    tone='success' if result['total_failed'] == 0 else 'warning',
                )
                render_action_feedback()
                _show_timing_report(result, operation_name='Delete')

with tab_delete:

    _tab_delete()

# -------------------------------------------------

# Multi-Object Parallel Tab

# -------------------------------------------------

def _tab_multi_object():

    section_header('🚀', 'Multi-Object Parallel Load', 'Run inserts/updates across multiple Salesforce objects in parallel with per-object mapping and a unified live dashboard.', accent='cyan')
    render_steps(['Add Objects', 'Configure Each', 'Run All', 'Monitor'])

    st.caption(

        'Queue multiple Salesforce objects for parallel processing. '

        'Each object runs in its own thread with the 10-thread pool split across objects. '

        'Uses AUTO mode (Bulk v2 ? v1 ? REST chain).'

    )

    if not st.session_state.sf:

        st.warning('Connect to Salesforce first (use sidebar).')

        return

    render_bulk_jobs_quota_panel()

    # Object queue management

    if 'multi_queue' not in st.session_state:

        st.session_state.multi_queue = []

    st.markdown('### Add Objects to Queue')

    # Data source selection
    mq_data_source = st.radio(
        'Data Source',
        ['📄 CSV File', '❄️ Snowflake Table'],
        key='mq_data_source',
        horizontal=True,
        help='Load data from a local CSV file or directly from a Snowflake table'
    )

    mq_col1, mq_col2, mq_col3, mq_col4 = st.columns(4)

    with mq_col1:

        mq_object = st.text_input('SF Object API Name', key='mq_object')

    with mq_col2:

        mq_operation = st.selectbox('Operation', ['insert', 'upsert', 'update', 'delete'], key='mq_op')

    with mq_col3:
        if mq_data_source == '📄 CSV File':
            mq_file = st.text_input('CSV File Path', key='mq_file')
            mq_snow_table = None
        else:
            mq_snow_table = st.text_input('Snowflake Table (DB.SCHEMA.TABLE)', key='mq_snow_table')
            mq_file = None

    with mq_col4:

        mq_ext_id = st.text_input('External ID Field (upsert)', key='mq_extid')

    # Optional WHERE clause for Snowflake source
    if mq_data_source == '❄️ Snowflake Table':
        mq_where = st.text_input(
            'WHERE clause (optional, without WHERE keyword)',
            key='mq_snow_where',
            placeholder='e.g. STATUS = \'Active\' AND CREATED_DATE > \'2024-01-01\''
        )
    else:
        mq_where = None

    # --- Column Mapping Section ---
    _mq_can_fetch = bool(mq_object and (mq_file or mq_snow_table))
    if st.button('⬇️ Load Columns for Mapping', key='mq_fetch_cols', disabled=not _mq_can_fetch,
                 help='Fetch source columns and SF fields to configure column mapping'):
        with st.spinner('Fetching columns…'):
            # Fetch SF writable fields — for INSERT exclude Id (idLookup only), for others include idLookup
            if mq_object and st.session_state.sf:
                try:
                    from sf_bulk_loader import is_compound_field
                    _sf_raw = get_cached_object_fields(st.session_state.sf, mq_object.strip())
                    if mq_operation == 'insert':
                        # INSERT: only createable fields + externalId; NEVER include Id (idLookup-only)
                        _sf_write = [f for f in _sf_raw if f.get('createable') or f.get('externalId')]
                    else:
                        # UPDATE / UPSERT: include updateable, createable, externalId, idLookup
                        _sf_write = [f for f in _sf_raw if f.get('updateable') or f.get('createable') or f.get('externalId') or f.get('idLookup')]
                    _sf_write = [f for f in _sf_write if not is_compound_field(f)]
                    st.session_state['mq_sf_fields'] = [f['name'] for f in _sf_write]
                    st.session_state['mq_mapping_op'] = mq_operation  # remember which op fields were fetched for
                except Exception as _e:
                    st.error(f'Could not fetch SF fields: {_e}')
                    st.session_state.pop('mq_sf_fields', None)
            # Fetch source columns
            if mq_data_source == '📄 CSV File' and mq_file:
                try:
                    _prev = pd.read_csv(mq_file.strip(), nrows=1)
                    st.session_state['mq_src_columns'] = list(_prev.columns)
                    # Count rows without loading full file
                    with open(mq_file.strip(), 'r', encoding='utf-8-sig', errors='replace') as _f:
                        _row_count = sum(1 for _ in _f) - 1  # subtract header
                    st.session_state['mq_src_row_count'] = max(_row_count, 0)
                except Exception as _e:
                    st.error(f'Could not read CSV: {_e}')
                    st.session_state.pop('mq_src_columns', None)
                    st.session_state.pop('mq_src_row_count', None)
            elif mq_data_source == '❄️ Snowflake Table' and mq_snow_table and st.session_state.get('sf_conn'):
                try:
                    _cur = st.session_state.sf_conn.cursor()
                    _cur.execute(f'DESCRIBE TABLE {mq_snow_table.strip()}')
                    st.session_state['mq_src_columns'] = [r[0] for r in _cur.fetchall()]
                    # Also fetch row count
                    _where_clause = f' WHERE {mq_where.strip()}' if mq_where and mq_where.strip() else ''
                    _cur.execute(f'SELECT COUNT(*) FROM {mq_snow_table.strip()}{_where_clause}')
                    st.session_state['mq_src_row_count'] = _cur.fetchone()[0]
                    _cur.close()
                except Exception as _e:
                    st.error(f'Could not fetch table columns: {_e}')
                    st.session_state.pop('mq_src_columns', None)
                    st.session_state.pop('mq_src_row_count', None)
            # Clear any previously confirmed mapping when re-loading columns
            st.session_state.pop('mq_mapping_result', None)
            st.session_state.pop('mq_mapping_confirmed', None)
            # NOTE: do NOT clear mq_src_row_count here — it was just populated above

    # Show mapping UI when columns are loaded
    mq_column_mapping = {}
    if st.session_state.get('mq_src_columns') and st.session_state.get('mq_sf_fields'):
        _src_cols = st.session_state['mq_src_columns']
        _sf_fields = st.session_state['mq_sf_fields']
        _fetched_op = st.session_state.get('mq_mapping_op', mq_operation)
        st.markdown('#### 🗂️ Column Mapping')
        # --- Source row count banner ---
        _row_count = st.session_state.get('mq_src_row_count')
        if _row_count is not None:
            _src_label = mq_snow_table.strip() if mq_data_source == '❄️ Snowflake Table' else (mq_file.strip().split('/')[-1].split('\\')[-1] if mq_file else 'source')
            _where_note = f' (filtered by WHERE)' if mq_where and mq_where.strip() else ''
            _ui(f''
                f'<div style="display:flex;align-items:center;gap:14px;padding:10px 16px;'
                f'background:linear-gradient(90deg,rgba(34,197,94,0.10),rgba(6,182,212,0.07));'
                f'border:1px solid rgba(34,197,94,0.25);border-radius:10px;margin-bottom:10px;">'
                f'<span style="font-size:1.45rem;">📊</span>'
                f'<div>'
                f'<div style="font-size:0.72rem;color:rgba(255,255,255,0.50);text-transform:uppercase;letter-spacing:0.06em;">Source Records{_where_note}</div>'
                f'<div style="font-size:1.5rem;font-weight:800;color:#4ade80;letter-spacing:-0.02em;">{_row_count:,}</div>'
                f'<div style="font-size:0.70rem;color:rgba(255,255,255,0.40);">{_src_label}</div>'
                f'</div></div>'
            )
        st.caption(
            f'Map each source column ? Salesforce field. **-- Skip --** excludes a column. '
            f'({len(_src_cols)} source columns | {len(_sf_fields)} SF fields | operation: **{_fetched_op.upper()}**)'
            + (' — 🔑 Id field excluded for INSERT' if _fetched_op == 'insert' else '')
        )
        _sf_opts = ['-- Skip --'] + _sf_fields
        for _col in _src_cols:
            _auto = next((f for f in _sf_fields if f.lower() == _col.lower()), '-- Skip --')
            _idx = _sf_opts.index(_auto) if _auto in _sf_opts else 0
            _chosen = st.selectbox(f'`{_col}` ?', _sf_opts, index=_idx, key=f'mq_map_{_col}')
            if _chosen != '-- Skip --':
                mq_column_mapping[_col] = _chosen
        st.session_state['mq_mapping_result'] = mq_column_mapping

        # -- Confirm Mapping button ------------------------------------------
        _mc = len(mq_column_mapping)
        _btn_col, _info_col = st.columns([2, 5])
        if _btn_col.button('✅ Confirm Mapping', key='mq_confirm_map', type='primary',
                           disabled=(_mc == 0)):
            st.session_state['mq_mapping_confirmed'] = dict(mq_column_mapping)
            st.success(f'✅ Mapping confirmed: {_mc} of {len(_src_cols)} columns locked in for this object.')
        if st.session_state.get('mq_mapping_confirmed') is not None:
            _conf = st.session_state['mq_mapping_confirmed']
            _info_col.success(f'✅ **{len(_conf)} columns confirmed** — ready to Add to Queue')
        elif _mc == 0:
            st.warning('⚠️ No columns mapped — confirm mapping or columns will be sent as-is (names must match SF field API names)')
        else:
            st.info(f'✅ {_mc} columns selected. Click **Confirm Mapping** to lock them in before adding to queue.')
    elif 'mq_src_columns' in st.session_state or 'mq_sf_fields' in st.session_state:
        if not st.session_state.get('mq_src_columns'):
            st.warning('❌ Could not load source columns. Check file path / Snowflake connection.')
        if not st.session_state.get('mq_sf_fields'):
            st.warning('❌ Could not load SF fields. Check SF connection and object name.')

    if st.button('➕ Add to Queue', key='mq_add'):
        mq_object_api_name = normalize_sf_api_name(mq_object)
        mq_external_id = normalize_sf_api_name(mq_ext_id)
        if mq_operation == 'upsert' and not mq_external_id:
            st.error('❌ Upsert queue items require a valid External ID field.')
            st.stop()
        # Use only the CONFIRMED mapping (empty dict if user never confirmed)
        _confirmed_map = dict(st.session_state.get('mq_mapping_confirmed', {}))
        if mq_data_source == '📄 CSV File':
            if mq_object and mq_file:
                st.session_state.multi_queue.append({
                    'object': mq_object_api_name,
                    'operation': mq_operation,
                    'source': 'file',
                    'file': mq_file.strip(),
                    'external_id': mq_external_id or None,
                    'column_mapping': _confirmed_map,
                })
                st.session_state.pop('mq_src_columns', None)
                st.session_state.pop('mq_sf_fields', None)
                st.session_state.pop('mq_mapping_result', None)
                st.session_state.pop('mq_mapping_confirmed', None)
                st.session_state.pop('mq_mapping_op', None)
                st.rerun(scope='fragment')
            else:
                st.error('Object name and CSV file path are required.')
        else:
            if mq_object and mq_snow_table:
                if not st.session_state.get('sf_conn'):
                    st.error('❌ Connect to Snowflake first (use sidebar).')
                else:
                    st.session_state.multi_queue.append({
                        'object': mq_object_api_name,
                        'operation': mq_operation,
                        'source': 'snowflake',
                        'file': mq_snow_table.strip(),
                        'snow_where': mq_where.strip() if mq_where else '',
                        'external_id': mq_external_id or None,
                        'column_mapping': _confirmed_map,
                    })
                    st.session_state.pop('mq_src_columns', None)
                    st.session_state.pop('mq_sf_fields', None)
                    st.session_state.pop('mq_mapping_result', None)
                    st.session_state.pop('mq_mapping_confirmed', None)
                    st.session_state.pop('mq_mapping_op', None)
                    st.rerun(scope='fragment')
            else:
                st.error('Object name and Snowflake table are required.')

    # Show current queue

    if st.session_state.multi_queue:

        st.markdown('### Current Queue')

        for i, item in enumerate(st.session_state.multi_queue):

            col_a, col_b = st.columns([4, 1])
            src_icon = '❄️' if item.get('source') == 'snowflake' else '📄'
            where_info = f' WHERE {item["snow_where"]}' if item.get('snow_where') else ''
            _map_info = f' | **{len(item.get("column_mapping", {}))} cols mapped**' if item.get('column_mapping') else ' | *no mapping (as-is)*'
            col_a.write(f'**{i+1}.** {src_icon} {item["operation"].upper()} ? `{item["object"]}` | `{item["file"]}{where_info}`{_map_info}')

            if col_b.button('🗑️', key=f'mq_del_{i}'):

                st.session_state.multi_queue.pop(i)

                st.rerun(scope='fragment')

        st.markdown('---')

        mq_threads_total = st.slider(

            'Total parallel threads (split across objects)',

            min_value=2, max_value=32, value=32, key='mq_threads'

        )

        mq_chunk_size = st.number_input(

            'Chunk Size per job', min_value=1000, max_value=150000,

            value=25000, step=5000, key='mq_chunk'

        )

        mq_run_col, mq_stop_col = st.columns([1, 1])

        with mq_run_col:

            mq_run_clicked = st.button('🚀 Run All (Parallel)', key='mq_run', type='primary')

        with mq_stop_col:

            mq_stop_clicked = st.button('🛑 STOP', key='mq_stop', help='Stop the ongoing operation', on_click=set_stop_flag)

        if mq_stop_clicked:

            set_stop_flag()

            st.warning('⚠️ Stop signal sent. Operation will halt after current chunk completes.')

        if mq_run_clicked:

            clear_stop_flag()

            queue = st.session_state.multi_queue

            n_objects = len(queue)

            threads_per_obj = max(1, mq_threads_total // n_objects)

            shared_capacity = SharedLoadCapacity(mq_threads_total, should_stop=is_stopped) if all(
                item['operation'] != 'delete' for item in queue
            ) else None
            if shared_capacity is not None:
                threads_per_obj = mq_threads_total
                st.info(f'Running {n_objects} datasets concurrently with {mq_threads_total} shared upload slots.')
            else:
                st.info(f'Running {n_objects} objects with {threads_per_obj} threads each…')

            results = {}

            dashboards = {}
            load_events = LoadEventBus()

            # Create a dashboard for each object — use index key so same SF object
            # with different sources never overwrites each other in the dict
            for _mq_i, item in enumerate(queue):

                _mq_key = f"{item['object']}___{_mq_i}"  # unique per slot
                item['_mq_key'] = _mq_key  # store back so _run_one can use it
                obj_name = item['object']
                _src_icon = '❄️' if item.get('source') == 'snowflake' else '📄'
                _src_lbl = f"{_src_icon} {item.get('file', '')}"
                if item.get('snow_where'):
                    _src_lbl += f" WHERE {item['snow_where']}"
                # Single source of truth: store the resolved label back on the item so
                # the dashboard, failed-file naming and history record can never desync.
                item['_src_lbl'] = _src_lbl

                dashboards[_mq_key] = LiveDashboard(

                    st, total_records=0,

                    operation=item['operation'].capitalize(),

                    object_name=obj_name,

                    source_label=_src_lbl,

                    record_op='Multi-Object',  # always log as Multi-Object in history

                )

            # Capture session objects on main thread — background threads cannot access st.session_state
            _sf_client = st.session_state.sf
            _snow_conn = st.session_state.get('sf_conn')

            # Run objects in parallel using thread pool

            def _run_one(item):

                _mq_key = item['_mq_key']
                obj_name = item['object']
                on_status = load_events.callback(_mq_key, 'status')
                on_progress = load_events.callback(_mq_key, 'progress')
                on_error = load_events.callback(_mq_key, 'error')

                # Authoritative source label built from THIS item, inside the worker —
                # this is the literal table/file the job queries, so it can never drift
                # from another slot's value. Stamped into every return path below.
                _src_icon_w = '❄️' if item.get('source') == 'snowflake' else '📄'
                _src_lbl_w = f"{_src_icon_w} {item.get('file', '')}"
                if item.get('snow_where'):
                    _src_lbl_w += f" WHERE {item['snow_where']}"

                chunk_iter = None
                try:

                    import datetime as _dt, re as _re
                    from sf_bulk_loader import bulk_load_v2, bulk_delete_v2

                    # Failed records folder: failed/multi-object/<operation>/
                    _ts = _dt.datetime.now().strftime('%Y%m%d_%H%M%S')
                    _raw_src = os.path.splitext(os.path.basename(item.get('file', 'unknown')))[0]
                    _src_safe = _re.sub(r'[^\w]', '_', _raw_src)[:50]
                    _op_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'failed', 'multi-object', item['operation'])
                    os.makedirs(_op_dir, exist_ok=True)
                    _err_file = os.path.join(_op_dir, f'failed_{obj_name}_{_src_safe}_{_ts}.csv')
                    chunk_iter = None
                    file_path = item.get('file', '')
                    if item.get('source') == 'snowflake':
                        # Fetch from Snowflake and create chunk iterator
                        if not _snow_conn:
                            raise RuntimeError('Snowflake connection is not active. Connect to Snowflake before running.')
                        snow_table = item['file']
                        snow_where = item.get('snow_where', '')
                        query = f"SELECT * FROM {snow_table}"
                        if snow_where:
                            query += f" WHERE {snow_where}"
                        on_status(f'❄️ Fetching data from Snowflake: {snow_table}...')
                        _item_col_map = item.get('column_mapping', {})
                        chunk_iter = stream_snowflake_batches(
                            _snow_conn, query, min(mq_chunk_size, MAX_CHUNK_SIZE),
                            column_mapping=_item_col_map, should_stop=is_stopped,
                            on_rows=lambda count: on_status(
                                f'Snowflake: {count:,} source rows read; uploads running.'
                            ),
                        )
                        file_path = None  # not used when chunk_iter is provided

                    if item['operation'] == 'delete':

                        result = bulk_delete_v2(

                            csv_file_path=file_path,

                            object_name=obj_name,

                            sf=_sf_client,

                            id_column='Id',

                            chunk_size=mq_chunk_size,

                            error_file=_err_file,

                            num_parallel_chunks=threads_per_obj,

                            on_progress=on_progress,

                            on_status=on_status,

                            on_error=on_error,
                            chunk_iterator=chunk_iter,
                            manage_stop_flag=False,

                        )

                    else:

                        # For CSV source: pass column_mapping so bulk_load_v2 renames; for Snowflake: already renamed above
                        _csv_col_map = item.get('column_mapping', {}) if not chunk_iter else {}

                        result = bulk_load_v2(

                            csv_file_path=file_path,

                            object_name=obj_name,

                            sf=_sf_client,

                            required_columns=list(_csv_col_map.values()) if _csv_col_map else [],

                            chunk_size=mq_chunk_size,

                            operation=item['operation'],

                            external_id_field=item['external_id'],

                            column_mapping=_csv_col_map if _csv_col_map else None,

                            error_file=_err_file,

                            num_parallel_chunks=threads_per_obj,

                            on_progress=on_progress,

                            on_status=on_status,

                            on_error=on_error,
                            chunk_iterator=chunk_iter,
                            manage_stop_flag=False,
                            upload_capacity=shared_capacity,

                        )

                    # Stamp the authoritative source onto the result so the finalize
                    # card / history record always reflect the table THIS job ran.
                    if isinstance(result, dict):
                        result['_source_label'] = _src_lbl_w
                        if item.get('source') == 'snowflake' and result.get('total_processed') == 0 and not is_stopped():
                            result['skipped'] = True
                            result['skip_reason'] = 'Source returned no loadable rows'
                    return _mq_key, result

                except Exception as e:
                    import traceback as _tb
                    _err_msg = str(e) or repr(e) or type(e).__name__
                    _err_tb = _tb.format_exc()
                    return _mq_key, {'error': _err_msg, 'traceback': _err_tb, '_source_label': _src_lbl_w}
                finally:
                    if chunk_iter is not None:
                        chunk_iter.close()

            def _completed_loads(futures):
                waiting = set(futures)
                while waiting:
                    done, waiting = concurrent.futures.wait(
                        waiting, timeout=0.25, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    load_events.drain(dashboards)
                    yield from done

            with concurrent.futures.ThreadPoolExecutor(max_workers=n_objects) as executor:

                futures = [executor.submit(_run_one, item) for item in queue]

                # Authoritative lookup of the exact item each result belongs to.
                _queue_lookup = {item['_mq_key']: item for item in queue}

                for f in _completed_loads(futures):

                    _mq_key, result = f.result()

                    results[_mq_key] = result

                    if result and 'error' not in result:

                        # Bind labels from the EXACT job that ran. Source comes from the
                        # result itself (stamped inside the worker = literal table queried);
                        # object comes from the matching queue item. This makes it
                        # impossible for a card/history entry to show another table.
                        _ran_item = _queue_lookup.get(_mq_key, {})
                        if _ran_item:
                            dashboards[_mq_key].object_name = _ran_item.get('object', dashboards[_mq_key].object_name)
                        _authoritative_src = result.get('_source_label') if isinstance(result, dict) else None
                        if _authoritative_src:
                            dashboards[_mq_key].source_label = _authoritative_src
                        elif _ran_item:
                            dashboards[_mq_key].source_label = _ran_item.get('_src_lbl', dashboards[_mq_key].source_label)

                        dashboards[_mq_key].finalize(result)

            # Summary — build lookup from queue by unique key
            _queue_lookup = {item['_mq_key']: item for item in queue}

            st.markdown('### 🚀 Multi-Object Results')

            for _mq_key, result in results.items():
                _qitem = _queue_lookup.get(_mq_key, {})
                obj_name = _qitem.get('object', _mq_key.split('___')[0])
                _src_label = _qitem.get('file', '')
                _src_icon = '❄️' if _qitem.get('source') == 'snowflake' else '📄'
                _op_label = _qitem.get('operation', '').upper()
                _where = f" WHERE {_qitem['snow_where']}" if _qitem.get('snow_where') else ''
                _src_info = f"{_src_icon} `{_src_label}{_where}`" if _src_label else ''

                if isinstance(result, dict) and 'error' in result:
                    _emsg = result.get('error') or result.get('traceback') or 'Unknown error'
                    st.error(f'❌ **{obj_name}** ({_op_label} → {_src_info}): {_emsg}')
                    if result.get('traceback'):
                        with st.expander('⚠️ Full traceback'):
                            st.code(result['traceback'], language='python')

                elif result:
                    _succ = result.get('total_success', 0)
                    _fail = result.get('total_failed', 0)
                    _elapsed = result.get('elapsed', 0)
                    _err_file = result.get('error_file')
                    _summary = (
                        f'**{obj_name}** | {_op_label} → {_src_info} | '
                        f'{_succ:,} success, {_fail:,} failed, {_elapsed:.1f}s'
                    )
                    if _fail > 0:
                        st.warning(f'⚠️ {_summary}')
                    else:
                        st.success(f'✅ {_summary}')
                    if _err_file and os.path.exists(_err_file):
                        st.info(f'📄 Failed records saved to: `{_err_file}`')
                        try:
                            _failed_df = pd.read_csv(_err_file)
                            with st.expander(f'🔍 View failed records ({len(_failed_df):,} rows) — {obj_name} ← {_src_label}'):
                                st.dataframe(_failed_df, width='stretch')
                                st.download_button(
                                    label='📥 Download failed records CSV',
                                    data=_failed_df.to_csv(index=False).encode('utf-8'),
                                    file_name=os.path.basename(_err_file),
                                    mime='text/csv',
                                    key=f'mq_dl_failed_{_mq_key}',
                                )
                        except Exception as _re:
                            st.warning(f'Could not read failed file: {_re}')

            # Refresh the fragment so the results are visible; sidebar history
            # updates on the next natural full-page rerun.
            st.session_state['_mq_done_rerun'] = st.session_state.get('_mq_done_rerun', 0) + 1
            st.rerun(scope='fragment')

    else:

        st.info('Queue is empty. Add objects above to get started.')

with tab_multi:

    _tab_multi_object()

# -------------------------------------------------

# Snowflake Tab

# -------------------------------------------------

def _tab_snowflake():

    if not HAS_SNOWFLAKE:

        st.error("❌ Snowflake connector not installed. Install with: `pip install snowflake-connector-python cryptography`")

    else:

        section_header('❄️', 'Snowflake Table Creation & Data Loading', 'Auto-create staging tables from CSV and bulk-load data into Snowflake with intelligent type inference.', accent='cyan')
        render_steps(['Data Source', 'Create Table', 'Load Data', 'Verify'])

        st.caption('Create tables and load data into Snowflake from Excel/CSV files')

        # Connection status check

        if 'sf_conn' not in st.session_state:

            st.session_state.sf_conn = None

        if not st.session_state.sf_conn:

            st.warning('❌ Not connected to Snowflake. Use the **sidebar** (select ❄️ Snowflake mode) to connect.')

            st.info('💡 Switch to **❄️ Snowflake** in the sidebar to manage your Snowflake connection and credentials.')

            st.stop()

        

        st.success('✅ Connected to Snowflake')

        st.markdown('---')

        

        # File Selection & Configuration

        st.markdown('### 📄 Data File Selection')

        

        sf_data_col1, sf_data_col2 = st.columns(2)

        

        with sf_data_col1:

            supported_extensions = ('.csv', '.xlsx', '.xls')

            sf_data_files = get_file_list(DATA_DIR, supported_extensions)

            sf_selected_file = st.selectbox(

                'Select Data File',

                sf_data_files if sf_data_files else ['No data files found'],

                key='sf_datafile'

            )

        

        with sf_data_col2:

            sf_uploaded_file = st.file_uploader(

                'Or upload a file',

                type=['csv', 'xlsx', 'xls', 'tsv', 'txt'],

                key='sf_upload'

            )

        

        # Get file path

        sf_file_path = None

        if sf_uploaded_file is not None:

            sf_file_path = os.path.join(DATA_DIR, sf_uploaded_file.name)

            with open(sf_file_path, 'wb') as f:

                f.write(sf_uploaded_file.getbuffer())

        elif sf_selected_file and sf_selected_file != 'No data files found':

            sf_file_path = os.path.join(DATA_DIR, sf_selected_file)

        

        # Sheet name for Excel files

        sf_sheet_name = None

        if sf_file_path and sf_file_path.lower().endswith(('.xlsx', '.xls')):

            try:

                import openpyxl

                wb = openpyxl.load_workbook(sf_file_path, read_only=True)

                sheet_names = wb.sheetnames

                wb.close()

                if not sheet_names:

                    st.warning('❌ The selected Excel file has no worksheets to load.')

                    sf_sheet_name = None

                else:

                    sf_sheet_name = st.selectbox(

                        'Select Sheet',

                        sheet_names,

                        key='sf_sheet_name'

                    )

            except Exception as e:

                st.warning(f'Could not read sheets: {e}')

        

        if sf_file_path:

            st.markdown('---')

            st.markdown('### ⚙️ Data Configuration')

            

            # Table Name with Check Button

            table_col1, table_col2 = st.columns([3, 1])

            with table_col1:

                sf_table_name = st.text_input(

                    'Target Table Name',

                    value='MY_TABLE',

                    key='sf_table_name',

                    help='Table name will be sanitized (uppercase, no spaces)'

                )

                sf_table_name = sanitize_column(sf_table_name)

                st.caption(f'Sanitized table name: **{sf_table_name}**')

            

            with table_col2:

                st.write('')  # Spacer

                st.write('')  # Spacer

                check_table = st.button('🔍 Check Table', key='check_table_btn', help='Check if table exists in Snowflake')

            

            # Check if table exists — run DB query ONLY on button click,

            # restore cached result from session_state on every other render

            table_exists = None

            table_row_count = 0

            _tbl_name_key   = 'sf_tbl_checked_name'

            _tbl_exists_key = 'sf_tbl_exists'

            _tbl_count_key  = 'sf_tbl_row_count'

            if check_table and st.session_state.sf_conn:

                # Run query only when button is clicked

                st.session_state[_tbl_name_key] = sf_table_name

                try:

                    _cur = st.session_state.sf_conn.cursor()

                    _cur.execute(f"SELECT COUNT(*) FROM {sf_table_name}")

                    _res = _cur.fetchone()

                    _cur.close()

                    st.session_state[_tbl_exists_key] = True

                    st.session_state[_tbl_count_key]  = _res[0] if _res else 0

                except Exception as _e:

                    if 'does not exist' in str(_e).lower() or 'object does not exist' in str(_e).lower():

                        st.session_state[_tbl_exists_key] = False

                        st.session_state[_tbl_count_key]  = 0

                    else:

                        st.session_state[_tbl_exists_key] = None

                        st.session_state[_tbl_count_key]  = 0

                        st.warning(f'⚠️ Could not check table: {str(_e)[:100]}')

            # Restore result from session state (zero DB calls on rerenders)

            if st.session_state.get(_tbl_name_key) == sf_table_name and _tbl_exists_key in st.session_state:

                table_exists    = st.session_state[_tbl_exists_key]

                table_row_count = st.session_state.get(_tbl_count_key, 0)

                if table_exists is True:

                    st.success(f'✅ Table **{sf_table_name}** exists with **{table_row_count:,}** rows')

                elif table_exists is False:

                    st.info(f'ℹ️ Table **{sf_table_name}** does not exist - will be created')

            

            # Load Mode with smart recommendations

            st.markdown('---')

            

            if table_exists is True:

                st.warning(f'⚠️ Table **{sf_table_name}** already exists with {table_row_count:,} rows')

                st.markdown('**Choose what to do:**')

            elif table_exists is False:

                st.info(f'ℹ️ Table **{sf_table_name}** does not exist yet')

                st.markdown('**Choose operation:**')

            

            sf_load_mode = st.radio(

                'Load Mode',

                ['Create/Replace', 'Append'],

                key='sf_load_mode',

                horizontal=True,

                help='Create/Replace: Drop and recreate table | Append: Insert into existing table'

            )

            

            # Show warnings based on mode and table existence

            if sf_load_mode == 'Create/Replace' and table_exists is True:

                st.error(f'⚠️ **WARNING**: This will DELETE the existing table with {table_row_count:,} rows and create a new one!')

            elif sf_load_mode == 'Append' and table_exists is False:

                st.error(f'❌ **ERROR**: Cannot append to non-existent table. Use "Create/Replace" first.')

            elif sf_load_mode == 'Append' and table_exists is True:

                st.success(f'✅ Will append new rows to existing {table_row_count:,} rows')

            

            # Preview data

            st.markdown('---')

            with st.expander('🔍 Preview Data (first 5 rows)', expanded=False):

                try:

                    # Use cached fast-read (5 rows only) — avoids reading the whole file every render

                    preview_df = read_file_preview_with_sheet(sf_file_path, sf_sheet_name, nrows=5)

                    if preview_df is not None:

                        st.dataframe(preview_df, width='stretch')

                        st.caption(f'Columns: {preview_df.shape[1]}')

                except Exception as e:

                    st.error(f'Error reading file: {e}')

            

            # Zero Padding Configuration

            with st.expander('⚙️ Zero Padding Configuration (Optional)', expanded=False):

                st.caption('Add zero padding to numeric columns (e.g., pad "123" to "00123" with width 5)')

                sf_zero_pad_input = st.text_area(

                    'Enter column:width pairs (one per line)',

                    placeholder='COLUMN_NAME:5\nANOTHER_COLUMN:10',

                    key='sf_zero_pad',

                    height=100

                )

                

                zero_pad_config = {}

                if sf_zero_pad_input:

                    for line in sf_zero_pad_input.strip().split('\n'):

                        if ':' in line:

                            col, width = line.strip().split(':', 1)

                            try:

                                zero_pad_config[col.strip()] = int(width.strip())

                            except ValueError:

                                st.warning(f'Invalid width for {col}')

                

                if zero_pad_config:

                    st.success(f'✅ Zero padding configured for {len(zero_pad_config)} column(s)')

            

            # Date Columns Configuration

            with st.expander('📅 Date Columns (Optional)', expanded=False):

                st.caption('Select columns to normalize as dates (YYYY-MM-DD format)')

                sf_date_cols_input = st.text_input(

                    'Date Columns (comma-separated)',

                    placeholder='START_DATE, END_DATE, CREATED_DATE',

                    key='sf_date_cols'

                )

                date_columns = [c.strip() for c in sf_date_cols_input.split(',') if c.strip()] if sf_date_cols_input else []

                

                if date_columns:

                    st.success(f'✅ {len(date_columns)} date column(s) will be normalized')

            

            st.markdown('---')

            

            # Load Data Button with validation message

            sf_btn_col1, sf_btn_col2, sf_btn_col3 = st.columns([1, 1, 3])

            with sf_btn_col1:

                sf_load_clicked = st.button('⬆️ Load Data', type='primary', key='sf_load_btn')

            with sf_btn_col2:

                sf_stop_clicked = st.button('🛑 STOP', key='sf_load_stop_btn', help='Stop the ongoing load', on_click=set_stop_flag)

            with sf_btn_col3:

                if sf_load_mode == 'Append' and table_exists is False:

                    st.error('⚠️ Cannot load: Table does not exist')

            if sf_stop_clicked:

                set_stop_flag()

                st.warning('⚠️ Stop signal sent. Operation will halt after current stage completes.')

            

            if sf_load_clicked:

                clear_stop_flag()

                # Validation: Block append to non-existent table

                if sf_load_mode == 'Append' and table_exists is False:

                    st.error('❌ Cannot append to non-existent table!')

                    st.info('💡 Solution: Change mode to "Create/Replace" or create the table first.')

                    st.stop()

                

                if not st.session_state.sf_conn:

                    st.error('❌ Please connect to Snowflake first (use sidebar)')

                    st.stop()

                

                try:

                    with st.spinner('Loading data...'):

                        # Read data

                        status_msg = st.empty()

                        status_msg.info('📄 Reading data file...')

                        df = read_data_for_snowflake(sf_file_path, sf_sheet_name)

                        

                        if df is None or df.empty:

                            st.error('❌ No data to load')

                            st.stop()

                        else:

                                # Apply transformations

                                status_msg.info('⚙️ Applying zero padding...')

                                df = apply_zero_padding(df, zero_pad_config)

                                

                                status_msg.info('⚙️ Normalizing dates...')

                                df = normalize_dates(df, date_columns)

                                

                                status_msg.info('⚙️ Cleaning data...')

                                df = df.map(

                                    lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)

                                )

                                

                                st.info(f'📊 DataFrame shape: {df.shape[0]:,} rows × {df.shape[1]} columns')

                                

                                # Handle based on mode

                                if sf_load_mode == 'Create/Replace':

                                    # Get current database and schema for fully qualified name

                                    _ctx_cur = st.session_state.sf_conn.cursor()

                                    _ctx_cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")

                                    _ctx_r = _ctx_cur.fetchone()

                                    _cur_db = _ctx_r[0] if _ctx_r else 'UNKNOWN'

                                    _cur_sch = _ctx_r[1] if _ctx_r else 'UNKNOWN'

                                    _ctx_cur.close()

                                    fq_sf_table = f"{_cur_db}.{_cur_sch}.{sf_table_name}"

                                    

                                    # Drop table if exists

                                    status_msg.info(f'🗑️ Dropping table if exists...')

                                    cursor = st.session_state.sf_conn.cursor()

                                    cursor.execute(f"DROP TABLE IF EXISTS {fq_sf_table}")

                                    cursor.close()

                                    

                                    # Create table

                                    status_msg.info(f'🏗️ Creating table {fq_sf_table}...')

                                    cols = infer_column_lengths(df)

                                    ddl = ", ".join(f"{c} VARCHAR" for c, l in cols.items())

                                    sql = f"CREATE TABLE {fq_sf_table} ({ddl})"

                                    cursor = st.session_state.sf_conn.cursor()

                                    cursor.execute(sql)

                                    cursor.close()

                                    st.success(f'✅ Table **{fq_sf_table}** created')

                                else:

                                    # Append mode — table validated above; build fq name

                                    _ctx_cur = st.session_state.sf_conn.cursor()

                                    _ctx_cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")

                                    _ctx_r = _ctx_cur.fetchone()

                                    _ctx_cur.close()

                                    _cur_db = _ctx_r[0] if _ctx_r else 'UNKNOWN'

                                    _cur_sch = _ctx_r[1] if _ctx_r else 'UNKNOWN'

                                    fq_sf_table = f"{_cur_db}.{_cur_sch}.{sf_table_name}"

                                    st.info(f'📥 Appending to existing table {fq_sf_table}')

                                

                                # Load data — parallel compress + PUT + COPY INTO

                                import time as _snf_t

                                _snf_start = _snf_t.time()

                                sf_prog = InlineProgressBar(st.empty(), 'Snowflake load progress')

                                nrows_loaded, copy_time = _fast_snowflake_load(

                                    conn=st.session_state.sf_conn,

                                    df=df,

                                    fq_table_name=fq_sf_table,

                                    status_fn=lambda m: status_msg.info(m),

                                    progress_fn=lambda p: sf_prog.progress(p),

                                )

                                total_snf_time = _snf_t.time() - _snf_start

                                sf_prog.progress(1.0)

                                status_msg.empty()

                                record_job_run(
                                    operation='Snowflake',
                                    object_name=fq_sf_table,
                                    success=nrows_loaded,
                                    failed=0,
                                    elapsed=total_snf_time,
                                    api=sf_load_mode,
                                    source=os.path.basename(sf_file_path or ''),
                                )

                                st.success(f'✅ Loaded {nrows_loaded:,} rows into {fq_sf_table} in {total_snf_time:.1f}s '

                                           f'({nrows_loaded / max(total_snf_time, 0.1):,.0f} rows/s)')

                                # Show sample

                                with st.expander('🔍 View Loaded Data (first 10 rows)', expanded=True):

                                    cursor = st.session_state.sf_conn.cursor()

                                    cursor.execute(f"SELECT * FROM {fq_sf_table} LIMIT 10")

                                    result_df = cursor.fetch_pandas_all()

                                    cursor.close()

                                    st.dataframe(result_df, width='stretch')

                                # Update table check state so Check Table shows updated count after load

                                st.session_state['sf_tbl_checked_name'] = sf_table_name

                                st.session_state['sf_tbl_exists'] = True

                                

                except Exception as e:

                    if str(e) == STOP_REQUESTED_ERROR:

                        st.warning('⚠️ Operation stopped by user.')

                    else:

                        st.error(f'❌ Error: {e}')

                    with st.expander('⚠️ Error Details'):

                        st.code(traceback.format_exc())

with tab_snowflake:

    _tab_snowflake()

# -------------------------------------------------

# SF to Snowflake Tab - Extract from Salesforce & Load to Snowflake

# -------------------------------------------------

def _tab_sf_to_snowflake():

    section_header('🔄❄️', 'Salesforce → Snowflake Data Pipeline', 'End-to-end pipeline: SOQL query → CSV stream → Snowflake bulk load. Save reusable configs per pipeline.', accent='cyan')
    render_steps(['Source Query', 'Target Table', 'Mapping', 'Run Pipeline'])

    st.caption('Extract data from Salesforce using SOQL queries and load directly into Snowflake (optimized for speed)')

    

    # Important info about data preservation

    with st.expander('⚠️ Important: Data Preservation', expanded=False):

        st.markdown("""

        **What gets changed:**

        - ? **Table Name**: Converted to UPPERCASE (Snowflake standard)

        - ? Example: Table name input `product_data` ? `PRODUCT_DATA`

        

        **What stays EXACTLY the same:**

        - ? **Column Names**: Same case as Salesforce (e.g., `ProductCode`, `FirstName`)

        - ? **Data Values**: Preserved exactly as in Salesforce

        - ? **Case in Data**: "John" stays "John", not "JOHN"

        - ? **Spaces, Special Characters**: All preserved in data values

        - ? **Numbers, Dates**: No formatting changes

        

        **Example:**

        - Salesforce: Column `ProductCode = "iPH-001"`, Column `Name = "Apple iPhone"`

        - Snowflake: Column `ProductCode = "iPH-001"`, Column `Name = "Apple iPhone"`

        

        *Note: Only special characters in column names are replaced with underscores (e.g., `Account.Name` ? `Account_Name`)*

        """)

    

    # Check connections

    sf_connected = st.session_state.sf is not None

    

    if not HAS_SNOWFLAKE:

        st.error("❌ Snowflake connector not installed. Install with: `pip install snowflake-connector-python cryptography`")

    

    if 'sf_conn' not in st.session_state:

        st.session_state.sf_conn = None

    

    snowflake_connected = st.session_state.sf_conn is not None

    

    # Show connection status

    col_status1, col_status2 = st.columns(2)

    with col_status1:

        if sf_connected:

            st.success('✅ Salesforce Connected')

        else:

            st.warning('❌ Not connected to Salesforce')

            st.caption('Use the **sidebar** to connect')

    with col_status2:

        if snowflake_connected and HAS_SNOWFLAKE:

            st.success('✅ Snowflake Connected')

        else:

            st.warning('❌ Not connected to Snowflake')

            st.caption('Switch to **❄️ Snowflake** mode in sidebar')

    

    # Show connection requirement message

    if not sf_connected or not snowflake_connected:

        st.info('💡 **Both connections required:** Connect to Salesforce and Snowflake in the sidebar to use this feature.')

    

    st.markdown('---')

    

    # Disable features if not connected

    is_ready = sf_connected and snowflake_connected and HAS_SNOWFLAKE

    

    # Configuration file for saving queries

    SF_TO_SF_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'saved_sf_to_snowflake_configs.json')

    def save_sf_to_snowflake_configs(configs):

        try:

            with open(SF_TO_SF_CONFIG_FILE, 'w', encoding='utf-8') as f:

                json.dump(configs, f, indent=2)

            st.session_state._saved_sf_configs = configs  # keep in-memory cache in sync

            return True, ''

        except Exception as e:

            return False, str(e)

    # Use in-memory cache (loaded once at startup)

    saved_configs = st.session_state._saved_sf_configs

    

    # Configuration Management

    st.markdown('### ⚙️ Configuration Management')

    if st.session_state.get('_sf_to_sf_config_msg'):

        _cfg_msg_type, _cfg_msg_text = st.session_state.pop('_sf_to_sf_config_msg')

        show_feedback(_cfg_msg_type, _cfg_msg_text, success_fn=st.success, warning_fn=st.warning, error_fn=st.error)

    

    config_col1, config_col2 = st.columns([3, 1])

    

    with config_col1:

        config_options = ['-- New Configuration --'] + list(saved_configs.keys())

        _pending_sf_to_sf_select = st.session_state.pop('_sf_to_sf_pending_select', None)

        if _pending_sf_to_sf_select in config_options:

            st.session_state['sf_to_sf_config_select'] = _pending_sf_to_sf_select

        selected_config = st.selectbox(

            'Load Saved Configuration',

            config_options,

            key='sf_to_sf_config_select'

        )

    

    # Load selected configuration defaults

    if selected_config != '-- New Configuration --':

        config_data = saved_configs[selected_config]

        default_query = config_data.get('query', 'SELECT Id, Name, ProductCode, IsActive, Family, Description, CreatedDate FROM Product2')

        default_table = config_data.get('table_name', 'PRODUCT2_DATA')

        default_mode = config_data.get('mode', 'Create/Replace')

        default_batch_size = config_data.get('batch_size', 10000)

        st.success(f'✅ Loaded: "{selected_config}"')

        # Keep tab focused after config selection causes rerun — SF→Snowflake is index 6

        js_click_tab(6)

    else:

        default_query = 'SELECT Id, Name, ProductCode, IsActive, Family, Description, CreatedDate FROM Product2'

        default_table = 'PRODUCT2_DATA'

        default_mode = 'Create/Replace'

        default_batch_size = 10000

    

    # ---- CRITICAL: when selection changes, push loaded values into session_state

    #      BEFORE the widgets render. Otherwise widgets keep their old session

    #      state values and `value=default_X` is ignored (Streamlit gotcha). ----

    _last_cfg_key = '_sf_to_sf_last_loaded_config'

    if st.session_state.get(_last_cfg_key) != selected_config:

        st.session_state[_last_cfg_key] = selected_config

        st.session_state['sf_to_sf_soql_query']  = default_query

        st.session_state['sf_to_sf_table_name']  = default_table

        st.session_state['sf_to_sf_load_mode']   = default_mode

        st.session_state['sf_to_sf_batch_size']  = default_batch_size

    

    with config_col2:

        if selected_config != '-- New Configuration --':

            st.write('')  # spacer

            st.write('')  # spacer

            if st.button('🗑️ Delete', key='delete_config_btn', width='stretch'):

                st.session_state['_sf_to_sf_delete_confirm'] = selected_config
                st.rerun()

    if st.session_state.get('_sf_to_sf_delete_confirm'):
        _sf_to_sf_delete_name = st.session_state['_sf_to_sf_delete_confirm']
        st.warning(f'⚠️ Delete configuration **"{_sf_to_sf_delete_name}"**? This cannot be undone.')
        _sf_del_yes, _sf_del_no = st.columns(2)
        with _sf_del_yes:
            if st.button('✅ Yes, Delete', key='sf_to_sf_delete_confirm_yes', type='primary'):
                _next_saved_configs = dict(saved_configs)
                _next_saved_configs.pop(_sf_to_sf_delete_name, None)
                _ok, _err = save_sf_to_snowflake_configs(_next_saved_configs)
                if not _ok:
                    st.session_state['_sf_to_sf_config_msg'] = ('error', f'❌ Delete failed: {_err[:180]}')
                    st.session_state.pop('_sf_to_sf_delete_confirm', None)
                    st.rerun()
                saved_configs.clear()
                saved_configs.update(_next_saved_configs)
                st.session_state.pop('_sf_to_sf_delete_confirm', None)
                st.session_state['_sf_to_sf_pending_select'] = '-- New Configuration --'
                st.session_state['_sf_to_sf_last_loaded_config'] = '-- New Configuration --'
                st.session_state['_sf_to_sf_config_msg'] = ('success', f'🗑️ Deleted: "{_sf_to_sf_delete_name}"')
                queue_action_feedback('Configuration deleted', f'Deleted configuration "{_sf_to_sf_delete_name}".')
                st.rerun()
        with _sf_del_no:
            if st.button('❌ Cancel', key='sf_to_sf_delete_confirm_no'):
                st.session_state.pop('_sf_to_sf_delete_confirm', None)
                st.rerun()

    st.markdown('---')

    

    # SOQL Query Input

    st.markdown('### 📝 SOQL Query')

    

    col_tip1, col_tip2 = st.columns([1, 1])

    with col_tip1:

        st.caption('💡 List specific fields to extract ALL records (no LIMIT needed)')

    with col_tip2:

        st.caption('⚠️ FIELDS(ALL) requires LIMIT = 200 (not for large extractions)')

    

    # Helper for getting field names

    with st.expander('💡 Need help finding field names?', expanded=False):

        st.markdown("""

        **To see all available fields for an object:**

        

        1. **Workbench Method** (Recommended):

           - Go to: https://workbench.developerforce.com

           - Select your org and login

           - Navigate to: **Info** ? **Standard & Custom Objects**

           - Select your object (e.g., Product2, Account)

           - View all field names

        

        2. **Developer Console Method**:

           - In Salesforce ? Click gear icon ? Developer Console

           - Execute: `Schema.SObjectType.Product2.fields.getMap().keySet()`

           - See all field API names

        

        3. **Common Product2 Fields**:

           ```

           Id, Name, ProductCode, IsActive, Description, Family, 

           ExternalId, CreatedDate, CreatedById, LastModifiedDate

           ```

        

        4. **Common Account Fields**:

           ```

           Id, Name, AccountNumber, BillingCity, BillingState, 

           Industry, Phone, Website, Type, CreatedDate

           ```

        """)

    

    soql_query = st.text_area(

        'SOQL Query',

        value=default_query,

        height=120,

        key='sf_to_sf_soql_query',

        help='Example: SELECT Id, Name, Email FROM Contact WHERE CreatedDate = TODAY',

        disabled=not is_ready

    )

    

    # Quick validation hints

    if soql_query and is_ready:

        query_upper = soql_query.upper().strip()

        if 'SELECT' in query_upper and 'FROM' not in query_upper:

            st.warning('⚠️ Query looks incomplete - did you forget the FROM clause? Example: `SELECT Id, Name FROM Product2`')

        if 'SELECT *' in query_upper or 'SELECT  *' in query_upper:

            st.error('? **SOQL does NOT support `SELECT *`**. You must list specific fields: `SELECT Id, Name, ProductCode FROM Product2`')

        if 'FIELDS(ALL)' in query_upper or 'FIELDS(STANDARD)' in query_upper or 'FIELDS(CUSTOM)' in query_upper:

            if 'LIMIT' not in query_upper:

                st.error('⚠️ **FIELDS(ALL) requires LIMIT = 200**')

                st.info('💡 **To extract all data:** List specific fields instead: `SELECT Id, Name, Field1, Field2, ... FROM Object`')

            else:

                # Check if LIMIT is > 200

                limit_match = re.search(r'LIMIT\s+(\d+)', query_upper)

                if limit_match:

                    limit_val = int(limit_match.group(1))

                    if limit_val > 200:

                        st.error(f'⚠️ **FIELDS(ALL) requires LIMIT = 200** (you have {limit_val})')

                        st.info('💡 **For large extractions:** Use explicit field names without LIMIT')

    

    # Query Examples

    with st.expander('📝 SOQL Query Examples - Click to see correct syntax', expanded=False):

        st.markdown('**Copy and modify these examples:**')

        st.code("""

-- Extract ALL Product2 records with specific fields (NO LIMIT - gets all data)

SELECT Id, Name, ProductCode, IsActive, Family, Description FROM Product2

-- All Accounts with key fields (extracts everything)

SELECT Id, Name, BillingCity, BillingState, Industry, Phone, AccountNumber FROM Account

-- Accounts with filters

SELECT Id, Name, BillingCity, Industry, Phone FROM Account WHERE Industry = 'Technology'

-- Contacts with related Account (all records)

SELECT Id, FirstName, LastName, Email, Phone, Account.Name, Account.Id FROM Contact

-- With Date filters (only records matching filter)

SELECT Id, Name, CreatedDate, CloseDate FROM Opportunity WHERE CloseDate >= 2026-01-01

-- Testing with limited results

SELECT Id, Name, ProductCode FROM Product2 LIMIT 100

-- FIELDS(ALL) - Only for small datasets (MAX 200 records!)

SELECT FIELDS(ALL) FROM Product2 LIMIT 200

        """, language='sql')

        

        st.markdown('---')

        st.markdown('### 📊 How to Extract ALL Data (No Limits)')

        st.success("""

**Best Practice for Large Extractions:**

1. ? List the specific fields you need (no LIMIT clause)

2. ? Query will automatically fetch ALL records

3. ? Example: `SELECT Id, Name, ProductCode, IsActive FROM Product2`

   ? This gets ALL Product2 records!

        """)

        

        st.error('🚫 **AVOID:** `SELECT FIELDS(ALL)` without LIMIT or LIMIT > 200')

        st.info('💡 **Tip:** Use Salesforce Workbench or Developer Console to see available field names for an object')

        st.warning('⚠️ **Common mistakes**: Using `SELECT *` or `FIELDS(ALL)` without proper LIMIT')

    

    st.markdown('---')

    

    # Snowflake Configuration

    st.markdown('### ❄️ Snowflake Target Configuration')

    

    sf_col1, sf_col2 = st.columns(2)

    

    with sf_col1:

        target_table_input = st.text_input(

            'Target Table Name',

            value=default_table,

            key='sf_to_sf_table_name',

            help='Table name in Snowflake (will be sanitized)',

            disabled=not is_ready

        )

        target_table_name = sanitize_table_name(target_table_input)

        st.caption(f'Sanitized: **{target_table_name}**')

    

    with sf_col2:

        load_mode = st.radio(

            'Load Mode',

            ['Create/Replace', 'Append'],

            index=0 if default_mode == 'Create/Replace' else 1,

            key='sf_to_sf_load_mode',

            horizontal=True,

            help='Create/Replace: Drop and recreate table | Append: Insert into existing',

            disabled=not is_ready

        )

    

    # Batch size for extraction

    batch_size = st.number_input(

        'Batch Size for Extraction',

        min_value=1000,

        max_value=50000,

        value=default_batch_size,

        step=1000,

        key='sf_to_sf_batch_size',

        help='Number of records to fetch per batch (larger = faster but more memory)',

        disabled=not is_ready

    )

    

    # API Selection

    api_col1, api_col2 = st.columns([1, 3])

    with api_col1:

        api_choice = st.radio(

            '⚙️ API Method',

            ['Bulk API 2.0', 'REST API'],

            index=0,

            key='sf_to_sf_api_choice',

            horizontal=True,

            disabled=not is_ready

        )

    with api_col2:

        if api_choice == 'Bulk API 2.0':

            st.caption('⚡️ **Fastest** — best for large datasets. Auto-strips compound fields (BillingAddress, etc.) since individual components are already included.')

        else:

            st.caption('📊 **Supports all fields** including compound fields (BillingAddress, ShippingAddress). Slower for large datasets.')

    

    st.markdown('---')

    

    # Save Configuration Section

    save_msg_placeholder = st.empty()

    

    with st.expander('💾 Save This Configuration', expanded=False):

        config_name = st.text_input(

            'Configuration Name',

            placeholder='e.g., Product2_Extract, Account_Daily_Load',

            key='sf_to_sf_config_name',

            disabled=not is_ready

        )

        

        save_button_clicked = st.button('💾 Save Configuration', key='save_sf_to_sf_config', disabled=not is_ready)

        

    # Show save messages outside expander (so they're always visible)

    if save_button_clicked:

        if config_name and config_name.strip():

            _next_saved_configs = dict(saved_configs)
            _next_saved_configs[config_name.strip()] = {

                'query': soql_query,

                'table_name': target_table_name,

                'mode': load_mode,

                'batch_size': batch_size,

                'saved_at': datetime.datetime.now().isoformat()

            }

            _ok, _err = save_sf_to_snowflake_configs(_next_saved_configs)

            if not _ok:

                st.session_state['_sf_to_sf_config_msg'] = ('error', f'❌ Save failed: {_err[:180]}')

                st.rerun()

            saved_configs.clear()
            saved_configs.update(_next_saved_configs)
            st.session_state['_sf_to_sf_pending_select'] = config_name.strip()

            st.session_state['_sf_to_sf_last_loaded_config'] = config_name.strip()

            st.session_state['_sf_to_sf_config_msg'] = (

                'success',

                f'✅ Configuration "{config_name.strip()}" saved successfully!'

            )
            queue_action_feedback('Configuration saved', f'Saved configuration "{config_name.strip()}".')

            st.rerun()

        else:

            st.session_state['_sf_to_sf_config_msg'] = ('warning', '⚠️ Please enter a configuration name first')

            st.rerun()

    

    st.markdown('---')

    

    # Validate Query Button

    validate_col1, validate_col2 = st.columns([1, 3])

    

    with validate_col1:

        validate_clicked = st.button('🔍 Validate Query', key='validate_query_btn', disabled=not is_ready)

    

    if validate_clicked:

        with st.spinner('Validating SOQL query...'):

            try:

                # Test query with LIMIT 1

                test_query = soql_query.strip()

                if 'LIMIT' not in test_query.upper():

                    test_query = f"{test_query} LIMIT 1"

                

                result = st.session_state.sf.query(test_query)

                

                if result['totalSize'] >= 0:

                    st.success(f"✅ Query is valid! Estimated records: {result.get('totalSize', 'Unknown')}")

                    

                    # Show field names

                    if result['records']:

                        fields = list(result['records'][0].keys())

                        fields = [f for f in fields if f != 'attributes']

                        st.info(f"📊 Fields to extract ({len(fields)}): {', '.join(fields[:10])}{'...' if len(fields) > 10 else ''}")

            except Exception as e:

                st.error(f'❌ Query validation failed: {str(e)[:200]}')

    

    st.markdown('---')

    

    # Run Extraction Button

    run_col1, run_col2, run_col3 = st.columns([1, 1, 3])

    

    with run_col1:

        run_extract_clicked = st.button('🚀 Extract & Load', type='primary', key='run_extract_btn', disabled=not is_ready)

    with run_col2:

        stop_extract_clicked = st.button('🛑 STOP', key='run_extract_stop_btn', help='Stop the ongoing pipeline', disabled=not is_ready, on_click=set_stop_flag)

    

    if stop_extract_clicked:

        set_stop_flag()

        st.warning('⚠️ Stop signal sent. Pipeline will halt after the current step completes.')

    if run_extract_clicked:

        clear_stop_flag()

        if not is_ready:

            st.error('❌ Please connect to both Salesforce and Snowflake first')

            st.stop()

        

        if not soql_query.strip():

            st.error('❌ Please enter a SOQL query')

            st.stop()

        

        # Basic query validation

        query_upper = soql_query.upper().strip()

        if 'SELECT' not in query_upper:

            st.error('❌ Query must contain SELECT statement')

            st.stop()

        if 'FROM' not in query_upper:

            st.error('❌ Query must contain FROM clause. Example: `SELECT Id, Name FROM Product2`')

            st.stop()

        if 'SELECT *' in query_upper or 'SELECT  *' in query_upper:

            st.error('? **SOQL does NOT support `SELECT *`**. You must explicitly list field names.')

            st.info('💡 **Correct syntax:** `SELECT Id, Name, ProductCode FROM Product2`')

            st.stop()

        

        # Check FIELDS(ALL) usage

        if 'FIELDS(ALL)' in query_upper or 'FIELDS(STANDARD)' in query_upper or 'FIELDS(CUSTOM)' in query_upper:

            if 'LIMIT' not in query_upper:

                st.error('⚠️ **FIELDS(ALL) requires a LIMIT clause (max 200)**')

                st.info('💡 **To extract ALL data:** Use explicit field names instead')

                st.code('SELECT Id, Name, ProductCode, IsActive FROM Product2', language='sql')

                st.stop()

            else:

                limit_match = re.search(r'LIMIT\s+(\d+)', query_upper)

                if limit_match:

                    limit_val = int(limit_match.group(1))

                    if limit_val > 200:

                        st.error(f'⚠️ **FIELDS(ALL) requires LIMIT = 200** (you have LIMIT {limit_val})')

                        st.info('💡 **For unlimited extraction:** Remove FIELDS(ALL) and list specific fields')

                        st.code('SELECT Id, Name, ProductCode FROM Product2  -- No LIMIT needed!', language='sql')

                        st.stop()

        

        if not target_table_name:

            st.error('❌ Please enter a target table name')

            st.stop()

        

        try:

            _start_time = time.time()

            

            progress_bar = InlineProgressBar(st.empty(), 'SF → Snowflake pipeline progress')

            status_msg = st.empty()

            timer_msg = st.empty()

            

            def _elapsed():

                return f"{time.time() - _start_time:.1f}s"

            

            # ==============================================

            # OPTIMIZED: Use selected API method

            # ==============================================

            sf = st.session_state.sf

            base_url = f"https://{sf.sf_instance}/services/data/v{sf.sf_version}"

            headers = {'Authorization': f'Bearer {sf.session_id}', 'Content-Type': 'application/json'}

            

            df = None

            used_api = api_choice

            

            if api_choice == 'Bulk API 2.0':

                # ==============================================

                # BULK API 2.0 — Fast, auto-strips compound fields

                # ==============================================

                sanitized_query = soql_query.strip()

                # Auto-strip compound fields (address/location/name) — not supported by Bulk API

                try:

                    # Known compound field names to always exclude (do NOT exclude 'Name')

                    known_compound_fields = {"ADDRESS", "LOCATION"}

                    from_match = re.search(r'\bFROM\s+(\w+)', sanitized_query, re.IGNORECASE)

                    if from_match:

                        sf_object_name = from_match.group(1)

                        status_msg.info(f'⚙️ Checking field compatibility for {sf_object_name}... [{_elapsed()}]')

                        describe_url = f"{base_url}/sobjects/{sf_object_name}/describe"

                        desc_resp = requests.get(describe_url, headers=headers)

                        compound_fields = set(known_compound_fields)

                        if desc_resp.status_code == 200:

                            desc_data = desc_resp.json()

                            for f in desc_data.get('fields', []):

                                if f.get('type', '') in ('address', 'location'):

                                    compound_fields.add(f['name'].upper())

                        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sanitized_query, re.IGNORECASE | re.DOTALL)

                        if select_match:

                            fields_list = [f.strip() for f in select_match.group(1).split(',')]

                            removed = [f for f in fields_list if f.strip().upper() in compound_fields]

                            clean = [f for f in fields_list if f.strip().upper() not in compound_fields]

                            if removed:

                                new_fields_str = ', '.join(clean)

                                sanitized_query = sanitized_query[:select_match.start(1)] + new_fields_str + sanitized_query[select_match.end(1):]

                                st.info(f'ℹ️ Auto-removed {len(removed)} compound field(s): **{", ".join(r.strip() for r in removed)}** (individual components already included)')

                except Exception as e:

                    # Enhanced error message: show which columns may have caused the issue

                    error_msg = str(e)

                    # Try to extract the field list from the query for user help

                    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sanitized_query, re.IGNORECASE | re.DOTALL)

                    if select_match:

                        fields_list = [f.strip() for f in select_match.group(1).split(',')]

                        st.error(f"Compound field auto-removal failed: {error_msg}\nColumns in your query: {fields_list}\nIf you see a casing issue, try matching the field names' case exactly as in Salesforce.")

                    else:

                        st.error(f"Compound field auto-removal failed: {error_msg}\nCould not parse field list from your query. Please check your SOQL syntax.")

                except Exception as e:

                    st.warning(f"Compound field auto-removal failed: {e}")

                # Always use sanitized_query for Bulk API 2.0

                soql_query = sanitized_query

                status_msg.info(f'⚡️ [Bulk API 2.0] Creating Bulk Query Job... [{_elapsed()}]')

                timer_msg.caption('Using Salesforce Bulk API 2.0 for maximum speed')

                query_job_url = f"{base_url}/jobs/query"

                _sf_query_session = requests.Session()
                _sf_query_adapter = HTTPAdapter(
                    max_retries=Retry(
                        total=5,
                        connect=5,
                        read=5,
                        backoff_factor=1,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=['GET', 'POST'],
                    )
                )
                _sf_query_session.mount('https://', _sf_query_adapter)
                _sf_query_session.mount('http://', _sf_query_adapter)

                def _sf_bulk_query_request(method, url, retry_label, **kwargs):

                    last_error = None

                    for attempt in range(1, 5):

                        ensure_not_stopped()

                        try:

                            resp = _sf_query_session.request(
                                method,
                                url,
                                timeout=(30, 300),
                                **kwargs,
                            )

                            if resp.status_code not in (429, 500, 502, 503, 504):

                                return resp

                            last_error = RuntimeError(f'HTTP {resp.status_code}: {resp.text[:200]}')

                        except requests.exceptions.RequestException as exc:

                            last_error = exc

                        if attempt < 4:

                            wait_s = min(2 ** (attempt - 1), 8)

                            status_msg.info(
                                f'🔁 [Bulk API 2.0] Transient Salesforce connection issue during {retry_label}; '
                                f'retrying in {wait_s}s [{_elapsed()}]'
                            )

                            time.sleep(wait_s)

                    raise RuntimeError(
                        f'Salesforce request failed during {retry_label}: {last_error}'
                    )

                job_payload = {

                    "operation": "query",

                    "query": soql_query

                }

                job_response = _sf_bulk_query_request(
                    'POST',
                    query_job_url,
                    'job creation',
                    headers=headers,
                    json=job_payload,
                )

                

                if job_response.status_code not in (200, 201):

                    st.error(f'❌ Bulk Query failed: {job_response.text[:300]}')

                    st.info('💡 **Tip:** Switch to **REST API** using the API Method selector above and try again.')

                    st.stop()

                

                job_info = job_response.json()

                job_id = job_info['id']

                

                status_msg.info(f'⚡️ [Bulk API 2.0] Job created (ID: {job_id[:12]}...) - Processing... [{_elapsed()}]')

                progress_bar.progress(0.05)

                

                job_status_url = f"{query_job_url}/{job_id}"

                poll_count = 0

                

                while True:

                    ensure_not_stopped()

                    poll_count += 1

                    status_response = _sf_bulk_query_request(
                        'GET',
                        job_status_url,
                        'job status poll',
                        headers=headers,
                    )

                    status_data = status_response.json()

                    state = status_data.get('state', '')

                    records_processed = status_data.get('numberRecordsProcessed', 0)

                    

                    if state == 'JobComplete':

                        progress_bar.progress(0.3)

                        status_msg.success(f'✅ [Bulk API 2.0] Query complete! {records_processed:,} records ready [{_elapsed()}]')

                        break

                    elif state in ('Failed', 'Aborted'):

                        err_detail = status_data.get('errorMessage', state)

                        st.error(f'❌ Bulk Query job {state}: {err_detail}')

                        st.info('💡 **Tip:** Switch to **REST API** using the API Method selector above and try again.')

                        st.stop()

                    else:

                        pct = min(0.05 + (poll_count * 0.02), 0.25)

                        progress_bar.progress(pct)

                        status_msg.info(f'⏳ [Bulk API 2.0] Processing... (state: {state}, records: {records_processed:,}, poll #{poll_count}) [{_elapsed()}]')

                        if poll_count < 5:

                            time.sleep(1)

                        elif poll_count < 15:

                            time.sleep(2)

                        else:

                            time.sleep(3)

                

                # ============================================================

                # STREAMING PIPELINE: HTTP page ? gzip file ? Snowflake stage

                # No intermediate DataFrame — handles 100M+ rows with low RAM

                # ============================================================

                status_msg.info(f'⚡️ [Bulk API 2.0] Starting streaming pipeline... [{_elapsed()}]')

                progress_bar.progress(0.35)

                # Get fully-qualified table name now (needed for DDL below)

                _ctx_cursor = st.session_state.sf_conn.cursor()

                _ctx_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")

                _ctx_row = _ctx_cursor.fetchone()

                _current_db  = _ctx_row[0] if _ctx_row else 'UNKNOWN'

                _current_schema = _ctx_row[1] if _ctx_row else 'UNKNOWN'

                fq_table_name = f"{_current_db}.{_current_schema}.{target_table_name}"

                _ctx_cursor.close()

                st.info(f'❄️ Target: **{fq_table_name}**')

                STREAM_CHUNK_ROWS = 5_000_000   # rows per gzip file (counted by newlines)

                stage_name   = f'_SF_STREAM_{int(time.time())}'

                sf_cur       = st.session_state.sf_conn.cursor()

                # Stage created lazily inside _put_file_bg — no round-trip if 0 rows

                col_names_stream  = None   # sanitized column names

                preview_df_stream = None   # first 10 rows for display

                staged_files      = []     # (tmp_path, put_future)

                # 8 parallel PUT workers — more simultaneous stage uploads

                put_exec          = ThreadPoolExecutor(max_workers=8)

                total_rows_dl     = 0

                file_idx          = 0

                # Current gzip writer state — binary mode to write raw bytes

                _cur_tmp      = None

                _cur_gz       = None

                _rows_in_file = 0

                _header_bytes = None   # raw CSV header bytes (reused when rolling files)

                # Capture connection before entering threads (st.session_state not accessible in threads)

                _sf_conn_ref = st.session_state.sf_conn

                # Persistent HTTP session — reuses TCP connections (no handshake per page)

                _dl_session = requests.Session()

                _dl_session.headers.update({

                    'Authorization': f'Bearer {sf.session_id}',

                    'Accept': 'text/csv'

                })

                def _put_file_bg(path):

                    escaped = path.replace('\\', '/')

                    c = _sf_conn_ref.cursor()

                    # Create stage lazily on first PUT — IF NOT EXISTS is idempotent

                    # so concurrent calls from multiple PUT workers are safe.

                    c.execute(f'CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}')

                    c.execute(

                        f"PUT 'file://{escaped}' @{stage_name} "

                        f"AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE"

                    )

                    c.close()

                    return path

                def _open_new_file():

                    nonlocal _cur_tmp, _cur_gz, _rows_in_file

                    t = tempfile.NamedTemporaryFile(

                        suffix='.csv.gz', delete=False,

                        prefix=f'sf_stream_{file_idx}_'

                    )

                    t.close()

                    _cur_tmp = t

                    _cur_gz  = gzip.open(t.name, 'wb', compresslevel=1)

                    if _header_bytes is not None:

                        _cur_gz.write(_header_bytes)

                    _rows_in_file = 0

                def _close_and_stage_file():

                    nonlocal file_idx

                    _cur_gz.close()

                    fut = put_exec.submit(_put_file_bg, _cur_tmp.name)

                    staged_files.append((_cur_tmp.name, fut))

                    file_idx += 1

                _open_new_file()

                results_url    = f"{job_status_url}/results"

                chunk_num      = 0

                ITER_BYTES     = 16 * 1024 * 1024   # 16 MB iter_content chunk

                # -- TRUE STREAMING PIPELINE --------------------------------

                # Fetch result pages sequentially. Some orgs invalidate the

                # next locator if page N+1 is requested before page N has been

                # fully consumed, which surfaces as INVALID_QUERY_LOCATOR.

                def _do_get_stream(locator_val):

                    params = {'maxRecords': 500_000}

                    if locator_val:

                        params['locator'] = locator_val

                    r = _dl_session.get(results_url, params=params, stream=True)

                    if r.status_code != 200:

                        raise RuntimeError(

                            f"HTTP {r.status_code}: {r.text[:200]}"

                        )

                    return r   # headers available; body not yet read

                next_locator = None

                while True:

                    ensure_not_stopped()

                    chunk_num += 1

                    try:

                        result_response = _do_get_stream(next_locator)

                    except RuntimeError as _e:

                        st.error(f'❌ Failed to download results: {_e}')

                        st.stop()

                    sforce_locator = result_response.headers.get('Sforce-Locator', '')

                    has_more = bool(sforce_locator and sforce_locator not in ('null', ''))

                    next_locator = sforce_locator if has_more else None

                    # -- Stream body bytes directly into gzip --------------

                    # Salesforce guarantees every page contains COMPLETE rows.

                    # Therefore we ONLY roll the gzip file between pages, never

                    # inside the iter_content loop.  Rolling mid-chunk would

                    # split a CSV row across two files ? Snowflake parse error.

                    _skip_hdr    = True

                    _is_first_pg = (col_names_stream is None)

                    _hdr_buf     = b''

                    rows_this_pg = 0

                    for raw_chunk in result_response.iter_content(chunk_size=ITER_BYTES):

                        ensure_not_stopped()

                        if not raw_chunk:

                            continue

                        if _skip_hdr:

                            # Accumulate until we see the header-line newline

                            _hdr_buf += raw_chunk

                            nl = _hdr_buf.find(b'\n')

                            if nl == -1:

                                continue   # header spans multiple chunks (rare)

                            header_line = _hdr_buf[:nl + 1]

                            rest        = _hdr_buf[nl + 1:]

                            _hdr_buf    = b''

                            _skip_hdr   = False

                            if _is_first_pg:

                                # Parse schema once, build preview, create table

                                _header_bytes = header_line

                                header_str    = header_line.decode('utf-8').strip()

                                raw_col_names = next(csv.reader([header_str]))

                                san = [sanitize_column_preserve_case(c) for c in raw_col_names]

                                col_names_stream = make_unique_columns(san)

                                # Preview — first 10 data rows from this chunk

                                try:

                                    prev_lines = rest[:8192].split(b'\n')[:10]

                                    prev_rows  = [

                                        next(csv.reader([ln.decode('utf-8', errors='replace')]))

                                        for ln in prev_lines if ln.strip()

                                    ]

                                    preview_df_stream = pd.DataFrame(

                                        prev_rows,

                                        columns=col_names_stream

                                        if prev_rows and len(prev_rows[0]) == len(col_names_stream)

                                        else None

                                    )

                                except Exception:

                                    preview_df_stream = None

                                # Table creation deferred until after the

                                # download loop — only runs if rows > 0.

                                # Write header once (first file only)

                                _cur_gz.write(header_line)

                            # Write data bytes (header already stripped)

                            if rest:

                                _cur_gz.write(rest)

                                rows_this_pg += rest.count(b'\n')

                        else:

                            # Pure data bytes — write straight through, no rolling here

                            _cur_gz.write(raw_chunk)

                            rows_this_pg += raw_chunk.count(b'\n')

                    # -- Roll + PUT after EVERY page -----------------------

                    # Salesforce guarantees each page ends on a complete row.

                    # By closing and staging after each page, the Snowflake PUT

                    # upload of page N runs in background while page N+1 is

                    # being downloaded — download and upload fully overlap.

                    # Previously we waited for ALL pages before any PUT started.

                    _rows_in_file += rows_this_pg

                    total_rows_dl += rows_this_pg

                    if _rows_in_file > 0:

                        _close_and_stage_file()

                        _rows_in_file = 0

                        _cur_gz = None   # mark closed so flush knows

                        if has_more:

                            _open_new_file()

                    pct = min(0.35 + chunk_num * 0.005, 0.65)

                    progress_bar.progress(pct)

                    status_msg.info(

                        f'⬇️ Streaming... {total_rows_dl:,} rows downloaded, '

                        f'{len(staged_files)} file(s) uploading to stage [{_elapsed()}]'

                    )

                    if not has_more:

                        break

                # Flush last file — with roll-per-page it is already staged.

                # Only needed if the last page was empty (no rows written).

                if _rows_in_file > 0 and _cur_gz is not None:

                    _close_and_stage_file()

                elif _cur_gz is not None:

                    _cur_gz.close()

                    try:

                        os.unlink(_cur_tmp.name)

                    except Exception:

                        pass

                # -- Create / validate table (runs even with 0 data rows) ------

                # The Bulk API always returns a header row so col_names_stream

                # is always populated. We create the table regardless of row

                # count so the schema is always available in Snowflake.

                if col_names_stream:

                    if load_mode == 'Create/Replace':

                        status_msg.info(f'🏗️ Creating table {fq_table_name}... [{_elapsed()}]')

                        sf_cur.execute(f'DROP TABLE IF EXISTS {fq_table_name}')

                        col_defs = ', '.join([f'"{c}" VARCHAR' for c in col_names_stream])

                        sf_cur.execute(f'CREATE TABLE {fq_table_name} ({col_defs})')

                        st.success(f'✅ Table **{fq_table_name}** created ({len(col_names_stream)} columns)')

                    else:

                        try:

                            sf_cur.execute(f'SELECT COUNT(*) FROM {fq_table_name}')

                            st.info(f'📥 Appending to existing table {fq_table_name}')

                        except Exception:

                            st.error(

                                f'⚠️ Table {fq_table_name} does not exist. '

                                f'Use "Create/Replace" mode first.'

                            )

                            sf_cur.close()

                            put_exec.shutdown(wait=False)

                            st.stop()

                if total_rows_dl == 0:

                    # Table schema created but Salesforce returned 0 data rows.

                    progress_bar.progress(1.0)

                    status_msg.empty()

                    sf_cur.close()

                    put_exec.shutdown(wait=False)

                    total_time_0 = time.time() - _start_time

                    record_job_run(
                        operation='SF→Snowflake',
                        object_name=fq_table_name,
                        success=0,
                        failed=0,
                        elapsed=total_time_0,
                        api=used_api,
                        source=soql_query.strip(),
                    )

                    st.info(

                        f'⚠️ Salesforce returned **0 records** for this query. '

                        f'Empty table **{fq_table_name}** created with '

                        f'{len(col_names_stream) if col_names_stream else 0} columns in {total_time_0:.1f}s.'

                    )

                    st.stop()

                # Wait for all background PUT operations to complete

                status_msg.info(

                    f'⏳ Waiting for {len(staged_files)} file(s) to finish '

                    f'uploading to Snowflake stage... [{_elapsed()}]'

                )

                progress_bar.progress(0.7)

                for tmp_path, fut in staged_files:

                    ensure_not_stopped()

                    try:

                        fut.result()

                    except Exception as put_err:

                        st.warning(f'⚠️ PUT warning: {put_err}')

                put_exec.shutdown(wait=False)

                # COPY INTO

                status_msg.info(

                    f'❄️ COPY INTO {fq_table_name} ({len(staged_files)} file(s))... [{_elapsed()}]'

                )

                progress_bar.progress(0.8)

                quoted_cols = _snowflake_copy_columns(sf_cur, fq_table_name, col_names_stream)

                t_copy = time.time()

                copy_rows_result = sf_cur.execute(f"""

                    COPY INTO {fq_table_name} ({quoted_cols})

                    FROM @{stage_name}

                    FILE_FORMAT = (

                        TYPE = 'CSV'

                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'

                        SKIP_HEADER = 1

                        COMPRESSION = 'GZIP'

                        ENCODING = 'UTF8'

                        EMPTY_FIELD_AS_NULL = TRUE
            NULL_IF = ('', 'NULL', 'null')

                    )

                    PURGE = TRUE

                """).fetchall()

                copy_time_stream = time.time() - t_copy

                total_loaded_stream = (

                    sum(row[3] for row in copy_rows_result if len(row) >= 4)

                    or total_rows_dl

                )

                # Cleanup stage + temp files

                try:

                    sf_cur.execute(f'DROP STAGE IF EXISTS {stage_name}')

                except Exception:

                    pass

                sf_cur.close()

                for tmp_path, _ in staged_files:

                    ensure_not_stopped()

                    try:

                        os.unlink(tmp_path)

                    except Exception:

                        pass

                progress_bar.progress(1.0)

                status_msg.empty()

                timer_msg.empty()

                total_time_stream = time.time() - _start_time

                record_job_run(
                    operation='SF→Snowflake',
                    object_name=fq_table_name,
                    success=total_loaded_stream,
                    failed=0,
                    elapsed=total_time_stream,
                    api=used_api,
                    source=soql_query.strip(),
                )

                st.success(

                    f'✅ Done! {total_loaded_stream:,} rows loaded into '

                    f'{fq_table_name} in {total_time_stream:.1f}s'

                )

                m1, m2, m3, m4 = st.columns(4)

                m1.metric('Rows Loaded', f"{total_loaded_stream:,}")

                m2.metric('Columns', f"{len(col_names_stream)}")

                m3.metric('Total Time', f"{total_time_stream:.1f}s")

                m4.metric('Speed', f"{total_loaded_stream / max(total_time_stream, 0.1):,.0f} rows/s")

                st.caption(

                    f'❄️ Snowflake COPY INTO: {copy_time_stream:.1f}s | '

                    f'Extract + stage: {total_time_stream - copy_time_stream:.1f}s'

                )

                st.info(f'📊 {total_loaded_stream:,} rows × {len(col_names_stream)} columns')

                if preview_df_stream is not None:

                    with st.expander('🔍 Data Preview (first 10 rows)', expanded=False):

                        st.dataframe(preview_df_stream, width='stretch')

                with st.expander('🔍 Verify Loaded Data (first 10 rows from Snowflake)',

                                 expanded=True):

                    v_cur = st.session_state.sf_conn.cursor()

                    v_cur.execute(f"SELECT * FROM {fq_table_name} LIMIT 10")

                    st.dataframe(v_cur.fetch_pandas_all(), width='stretch')

                    v_cur.close()

                    v_cur2 = st.session_state.sf_conn.cursor()

                    v_cur2.execute(

                        f"SELECT COUNT(*) AS TOTAL_ROWS FROM {fq_table_name}"

                    )

                    cnt = v_cur2.fetchone()

                    v_cur2.close()

                    st.info(

                        f'📊 Total rows in {fq_table_name}: {cnt[0]:,}'

                    )

                st.stop()   # Bulk API path is fully handled — skip REST path below

            

            else:

                # ==============================================

                # REST API STREAMING — page-by-page gzip ? Snowflake stage

                # Handles millions of records without loading all into RAM.

                # None/null from Salesforce ? '' ? EMPTY_FIELD_AS_NULL ? NULL in Snowflake.

                # ==============================================

                status_msg.info(f'⚡️ [REST API] Starting streaming extraction... [{_elapsed()}]')

                timer_msg.caption('REST API — supports all field types including compound fields')

                progress_bar.progress(0.05)

                query_result_r = sf.query(soql_query.strip(), include_deleted=False)

                total_size_r   = query_result_r.get('totalSize', 0)

                if total_size_r == 0 and query_result_r.get('done', True) and not query_result_r.get('records'):

                    st.warning('⚠️ No records found for the query')

                    st.stop()

                # Fully-qualified target table

                _ctx_cur_r = st.session_state.sf_conn.cursor()

                _ctx_cur_r.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")

                _ctx_row_r = _ctx_cur_r.fetchone()

                _ctx_cur_r.close()

                fq_table_name = f"{_ctx_row_r[0]}.{_ctx_row_r[1]}.{target_table_name}"

                st.info(f'❄️ Target: **{fq_table_name}**')

                # --- Snowflake stage + streaming state ---

                stage_name_r   = f'_SF_REST_{int(time.time())}'

                sf_cur_r       = st.session_state.sf_conn.cursor()

                sf_cur_r.execute(f'CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name_r}')

                _sf_conn_ref_r = st.session_state.sf_conn

                staged_files_r = []

                put_exec_r     = ThreadPoolExecutor(max_workers=4)

                # 100K rows per file: PUT of file N starts in background while
                # pages N+1… are still downloading — true pipeline overlap.
                # At 5M (old value) all 8-lakh records went into ONE file and
                # the PUT only started after the entire download was done.
                CHUNK_R        = 100_000   # rows per gzip file

                file_idx_r     = 0

                total_rows_r   = 0

                col_names_r    = None

                _gz_file_r  = None

                _gz_tmp_r   = None

                _rows_cur_r = 0

                def _put_r(path):

                    escaped = path.replace('\\', '/')

                    c = _sf_conn_ref_r.cursor()

                    c.execute(

                        f"PUT 'file://{escaped}' @{stage_name_r} "

                        f"AUTO_COMPRESS=FALSE PARALLEL=4 OVERWRITE=TRUE"

                    )

                    c.close()

                def _open_gz_r():

                    nonlocal _gz_file_r, _gz_tmp_r, _rows_cur_r

                    t = tempfile.NamedTemporaryFile(

                        suffix='.csv.gz', delete=False,

                        prefix=f'sfrest_{file_idx_r}_'

                    )

                    t.close()

                    _gz_tmp_r  = t

                    _gz_file_r = gzip.open(t.name, 'wt', compresslevel=1, encoding='utf-8', newline='')

                    _rows_cur_r = 0

                def _flush_gz_r():

                    nonlocal file_idx_r

                    _gz_file_r.close()

                    fut = put_exec_r.submit(_put_r, _gz_tmp_r.name)

                    staged_files_r.append((_gz_tmp_r.name, fut))

                    file_idx_r += 1

                def _write_page_df(page_records):

                    """Convert a page of clean-dicts to DataFrame, apply null normalisation

                    (same as sf_bulk_loader._df_to_csv_bytes), write to the open gzip file."""

                    import numpy as _np

                    chunk_df = pd.DataFrame(page_records, columns=col_names_r)

                    # -- NULL FIX: same as sf_bulk_loader._df_to_csv_bytes --------------

                    # applymap converts None / '' / whitespace-only ? np.nan

                    # to_csv with na_rep='' writes np.nan as an unquoted empty CSV cell

                    # COPY INTO EMPTY_FIELD_AS_NULL=TRUE converts that cell ? NULL

                    chunk_df = chunk_df.applymap(

                        lambda x: _np.nan

                        if (pd.isna(x) or (isinstance(x, str) and x.strip() == ''))

                        else x

                    )

                    chunk_df.to_csv(

                        _gz_file_r,

                        index=False,

                        header=(_rows_cur_r == 0),  # header only at start of each file

                        lineterminator='\n',

                        na_rep=''

                    )

                # --- Page loop ---

                while True:

                    ensure_not_stopped()

                    page_records = []

                    for rec in query_result_r['records']:

                        ensure_not_stopped()

                        clean = {}

                        for k, v in rec.items():

                            if k == 'attributes':

                                continue

                            if isinstance(v, dict):

                                for nk, nv in v.items():

                                    if nk != 'attributes':

                                        clean[f'{k}_{nk}'] = nv

                            else:

                                clean[k] = v

                        if col_names_r is None:

                            # First record: determine schema, create table, open first file

                            col_names_r = make_unique_columns(

                                [sanitize_column_preserve_case(k) for k in clean.keys()]

                            )

                            if load_mode == 'Create/Replace':

                                status_msg.info(f'🏗️ Creating table {fq_table_name}... [{_elapsed()}]')

                                sf_cur_r.execute(f'DROP TABLE IF EXISTS {fq_table_name}')

                                col_defs_r = ', '.join([f'"{c}" VARCHAR' for c in col_names_r])

                                sf_cur_r.execute(f'CREATE TABLE {fq_table_name} ({col_defs_r})')

                                st.success(f'✅ Table **{fq_table_name}** created ({len(col_names_r)} columns)')

                            else:

                                try:

                                    sf_cur_r.execute(f'SELECT COUNT(*) FROM {fq_table_name}')

                                    st.info(f'📥 Appending to existing table {fq_table_name}')

                                except Exception:

                                    st.error(f'⚠️ Table {fq_table_name} does not exist. Use "Create/Replace" first.')

                                    st.stop()

                            _open_gz_r()

                        page_records.append(list(clean.values()))

                    if page_records:

                        _write_page_df(page_records)

                        _rows_cur_r  += len(page_records)

                        total_rows_r += len(page_records)

                        if _rows_cur_r >= CHUNK_R:

                            _flush_gz_r()

                            _open_gz_r()

                    pct = min(0.1 + (total_rows_r / max(total_size_r, 1)) * 0.55, 0.65)

                    progress_bar.progress(pct)

                    status_msg.info(

                        f'⬇️ Fetched {total_rows_r:,} / ~{total_size_r:,} records, '

                        f'{len(staged_files_r)} file(s) staged [{_elapsed()}]'

                    )

                    if query_result_r.get('done', True):

                        break

                    query_result_r = sf.query_more(

                        query_result_r['nextRecordsUrl'], identifier_is_url=True

                    )

                if total_rows_r == 0:

                    st.warning('⚠️ No records found for the query')

                    record_job_run(
                        operation='SF→Snowflake',
                        object_name=target_table_name,
                        success=0,
                        failed=0,
                        elapsed=time.time() - _start_time,
                        api=used_api,
                        source=soql_query.strip(),
                    )

                    st.stop()

                # Flush last file

                if _rows_cur_r > 0:

                    _flush_gz_r()

                elif _gz_file_r:

                    _gz_file_r.close()

                    try:

                        os.unlink(_gz_tmp_r.name)

                    except Exception:

                        pass

                # Wait for all background PUTs to finish

                status_msg.info(

                    f'⏳ Waiting for {len(staged_files_r)} file(s) to finish uploading... [{_elapsed()}]'

                )

                progress_bar.progress(0.7)

                for _, fut_r in staged_files_r:

                    ensure_not_stopped()

                    try:

                        fut_r.result()

                    except Exception as put_err_r:

                        st.warning(f'⚠️ PUT warning: {put_err_r}')

                put_exec_r.shutdown(wait=False)

                # COPY INTO — single call, Snowflake loads all staged files in parallel

                status_msg.info(f'❄️ COPY INTO {fq_table_name} ({len(staged_files_r)} file(s))... [{_elapsed()}]')

                progress_bar.progress(0.82)

                quoted_cols_r = _snowflake_copy_columns(sf_cur_r, fq_table_name, col_names_r)

                t0_copy_r = time.time()

                copy_result_r = sf_cur_r.execute(f"""

                    COPY INTO {fq_table_name} ({quoted_cols_r})

                    FROM @{stage_name_r}

                    FILE_FORMAT = (

                        TYPE = 'CSV'

                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'

                        SKIP_HEADER = 1

                        COMPRESSION = 'GZIP'

                        ENCODING = 'UTF8'

                        EMPTY_FIELD_AS_NULL = TRUE
            NULL_IF = ('', 'NULL', 'null')

                    )

                    PURGE = TRUE

                """).fetchall()

                copy_t_r = time.time() - t0_copy_r

                loaded_r = sum(row[3] for row in copy_result_r if len(row) >= 4) or total_rows_r

                # Cleanup stage + temp files

                try:

                    sf_cur_r.execute(f'DROP STAGE IF EXISTS {stage_name_r}')

                except Exception:

                    pass

                sf_cur_r.close()

                for p_r, _ in staged_files_r:

                    try:

                        os.unlink(p_r)

                    except Exception:

                        pass

                progress_bar.progress(1.0)

                status_msg.empty()

                timer_msg.empty()

                total_t_r = time.time() - _start_time

                record_job_run(
                    operation='SF→Snowflake',
                    object_name=fq_table_name,
                    success=loaded_r,
                    failed=0,
                    elapsed=total_t_r,
                    api=used_api,
                    source=soql_query.strip(),
                )

                st.success(f'✅ Done! {loaded_r:,} rows loaded into {fq_table_name} in {total_t_r:.1f}s')

                m1, m2, m3, m4 = st.columns(4)

                m1.metric('Rows Loaded', f'{loaded_r:,}')

                m2.metric('Columns', f'{len(col_names_r)}')

                m3.metric('Total Time', f'{total_t_r:.1f}s')

                m4.metric('Speed', f'{loaded_r / max(total_t_r, 0.1):,.0f} rows/s')

                st.caption(

                    f'❄️ COPY INTO: {copy_t_r:.1f}s | '

                    f'Extract + stage: {total_t_r - copy_t_r:.1f}s'

                )

                with st.expander('🔍 Verify Loaded Data (first 10 rows from Snowflake)', expanded=True):

                    v_cur_r = st.session_state.sf_conn.cursor()

                    v_cur_r.execute(f'SELECT * FROM {fq_table_name} LIMIT 10')

                    st.dataframe(v_cur_r.fetch_pandas_all(), width='stretch')

                    v_cur_r.close()

                    v2_r = st.session_state.sf_conn.cursor()

                    v2_r.execute(f'SELECT COUNT(*) AS TOTAL_ROWS FROM {fq_table_name}')

                    cnt_r = v2_r.fetchone()

                    v2_r.close()

                    st.info(f'📊 Total rows in {fq_table_name}: {cnt_r[0]:,}')

                st.stop()   # REST API path fully handled

        except Exception as e:

            if str(e) == STOP_REQUESTED_ERROR:

                st.warning('⚠️ Pipeline stopped by user.')

            else:

                st.error(f'❌ Error during extraction/loading: {e}')

            with st.expander('⚠️ Error Details'):

                st.code(traceback.format_exc())

with tab_sf_to_snowflake:

    _tab_sf_to_snowflake()

# -------------------------------------------------

# Record by Record Comparison Tab (Optimized for 10M+ records)

# -------------------------------------------------

def _tab_record_by_record():

    section_header('🔍', 'Record by Record Test', 'Surgical row-level validation: compare source vs target row-by-row with full field-level diff reports.', accent='green')
    render_steps(['Source / Target', 'Mapping', 'Sample', 'Validate'])

    st.caption('Compare every record & every column between Source Table and Target Table (both in Snowflake). Optimized for millions of records.')

    # Check Snowflake connection

    snowflake_connected = st.session_state.get('sf_conn') is not None

    if snowflake_connected:

        st.success('✅ Snowflake Connected')

    else:

        st.error('❌ Snowflake Not Connected')

        st.info('❄️ **Snowflake connection required.** Connect to Snowflake in the sidebar.')

        st.stop()

    st.markdown('---')

    # Inputs

    rr_col1, rr_col2 = st.columns(2)

    with rr_col1:

        rr_source_table = st.text_input(

            'Source Snowflake Table Name',

            value='',

            placeholder='e.g. FILE_Excelerator_WarrantyProduct_2026-05-07_1923',

            key='rr_source_table'

        )

    with rr_col2:

        rr_target_table = st.text_input(

            'Target Snowflake Table Name',

            value='',

            placeholder='e.g. PROD_WarrantyProduct_FINAL',

            key='rr_target_table'

        )

    # Optional WHERE filter

    rr_where_filter = st.text_input(

        'WHERE Filter (Optional)',

        value='',

        placeholder="e.g. STATUS = 'Active' AND CREATED_DATE > '2024-01-01'",

        key='rr_where_filter',

        help='Filter records to compare only a subset (applied to both source and target)'

    )

    # --- Column Mapping ---

    rr_source_columns = []

    rr_target_columns = []

    rr_auto_mapping = {}

    rr_mapping_result = {}

    key_field_option = None

    if rr_source_table and rr_target_table:

        conn = st.session_state.get('sf_conn')

        # Fetch Source table columns

        if conn:

            try:

                cursor = conn.cursor()

                cursor.execute(f'DESCRIBE TABLE {rr_source_table}')

                rows = cursor.fetchall()

                rr_source_columns = [row[0] for row in rows if row[0] and row[0] != '']

                cursor.close()

            except Exception as e:

                st.error(f"Failed to fetch Source table columns: {e}")

            # Fetch Target table columns

            try:

                cursor = conn.cursor()

                cursor.execute(f'DESCRIBE TABLE {rr_target_table}')

                rows = cursor.fetchall()

                rr_target_columns = [row[0] for row in rows if row[0] and row[0] != '']

                cursor.close()

            except Exception as e:

                st.error(f"Failed to fetch Target table columns: {e}")

        # Show key field selector from source columns

        if rr_source_columns:

            _default_idx = 0

            for i, col in enumerate(rr_source_columns):

                cl = col.upper()

                if cl == 'ID':

                    _default_idx = i

                    break

                elif 'EXTERNAL_ID' in cl:

                    _default_idx = i

                    break

            key_field_option = st.selectbox(

                '🔑 Key Field for Record Matching (from source table)',

                options=rr_source_columns,

                index=_default_idx,

                key='rr_key_field_select',

                help='This column must exist in BOTH source and target tables. Used to match records.'

            )

        # Auto-map by name (case-insensitive)

        for col in rr_source_columns:

            match = next((f for f in rr_target_columns if f.lower() == col.lower()), None)

            rr_auto_mapping[col] = match

        # Show mapping UI

        if rr_source_columns and rr_target_columns:

            st.subheader('Column Mapping (Auto-mapped by name)')

            for col in rr_source_columns:

                default = rr_auto_mapping[col] if rr_auto_mapping[col] else None

                options = ['-- Skip --'] + rr_target_columns

                index = options.index(default) if default in options else 0

                rr_mapping_result[col] = st.selectbox(

                    f"'{col}' ? Target column:",

                    options=options,

                    index=index,

                    key=f'rr_map_{col}'

                )

            mapped_count = sum(1 for v in rr_mapping_result.values() if v != '-- Skip --')

            st.info(f'**{mapped_count}/{len(rr_source_columns)} columns mapped.**')

    st.markdown('---')

    # Run button

    rr_run_col, rr_stop_col = st.columns([1, 1])

    with rr_run_col:

        rr_run_clicked = st.button('🔍 Run Record-by-Record Comparison', type='primary', key='rr_run_btn')

    with rr_stop_col:

        rr_stop_clicked = st.button('🛑 STOP', key='rr_stop_btn', help='Stop the ongoing comparison', on_click=set_stop_flag)

    if rr_stop_clicked:

        set_stop_flag()

        st.warning('⚠️ Stop signal sent. Comparison will halt after the current chunk completes.')

    if rr_run_clicked:

        clear_stop_flag()

        # Validate inputs

        mapped_cols = {src: tgt for src, tgt in rr_mapping_result.items() if tgt and tgt != '-- Skip --'}

        if not rr_source_table or not rr_target_table:

            st.error('Please provide both Source and Target Snowflake table names.')

            st.stop()

        if not mapped_cols:

            st.error('No columns mapped. Please map at least one column.')

            st.stop()

        conn = st.session_state.get('sf_conn')

        # --- Resolve key field ---

        snow_where = f" WHERE {rr_where_filter}" if rr_where_filter else ""

        key_used = key_field_option

        if not key_used:

            st.error("Please select a Key Field for Record Matching.")

            st.stop()

        # Resolve target key column name (case-insensitive match)

        target_key_field = next((c for c in rr_target_columns if c.lower() == key_used.lower()), key_used)

        # Get total record count

        total_record_count = 0

        try:

            cursor = conn.cursor()

            cursor.execute(f"SELECT COUNT(*) FROM {rr_source_table}{snow_where}")

            total_record_count = cursor.fetchone()[0]

            cursor.close()

        except Exception as e:

            st.error(f"Failed to get record count: {e}")

            st.stop()

        st.info(f'🔑 Key: **{key_used}** | Records: **{total_record_count:,}** | Columns: **{len(mapped_cols)}** | Total comparisons: **{total_record_count * len(mapped_cols):,}**')

        # --- Type-aware normalization function ---

        def _normalize(val, is_numeric=False, is_date=False):

            if val is None:

                return ''

            s = str(val).strip()

            if not s or s.lower() in ('none', 'nan', 'null'):

                return ''

            if is_numeric:

                try:

                    num = float(s)

                    if num == int(num):

                        return str(int(num))

                    return f"{num:.6f}".rstrip('0').rstrip('.')

                except (ValueError, TypeError):

                    return s.lower()

            if is_date:

                return s.split('T')[0].split(' ')[0]

            return s.lower()

        # =============================================================

        # OPTIMIZED CHUNKED PROCESSING (handles 10M+ records)

        # =============================================================

        CHUNK_SIZE = 50_000

        progress_bar = st.progress(0, text='Starting chunked comparison...')

        status_area = st.empty()

        # Build column lists

        src_key_col = key_used

        tgt_key_col = target_key_field

        snow_src_cols = [src_key_col] + list(mapped_cols.keys())

        snow_src_cols_str = ', '.join(snow_src_cols)

        snow_tgt_cols = [tgt_key_col] + list(mapped_cols.values())

        snow_tgt_cols_str = ', '.join(snow_tgt_cols)

        # Accumulators (only mismatches stored, not all data)

        total_comparisons = 0

        total_matches = 0

        total_mismatches_count = 0

        col_mismatch_counts = {src: 0 for src in mapped_cols.keys()}

        mismatch_buffer = []

        records_not_in_target = 0

        records_with_issues_set = set()

        failed_records_summary = []

        MAX_MISMATCH_ROWS = 1_000_000

        total_chunks = max(1, (total_record_count + CHUNK_SIZE - 1) // CHUNK_SIZE)

        t_start = time.time()

        # Pre-detect column types by sampling source table

        col_is_numeric = {}

        col_is_date = {}

        try:

            sample_sql = f"SELECT {snow_src_cols_str} FROM {rr_source_table}{snow_where} LIMIT 50"

            cursor = conn.cursor()

            cursor.execute(sample_sql)

            sample_rows = cursor.fetchall()

            cursor.close()

            for i, src_col in enumerate(mapped_cols.keys()):

                values = [str(row[i + 1]).strip() for row in sample_rows if row[i + 1] is not None and str(row[i + 1]).strip()]

                is_num = False

                is_dt = False

                if values:

                    try:

                        [float(v) for v in values[:20]]

                        is_num = True

                    except (ValueError, TypeError):

                        pass

                    if not is_num:

                        import re as _re

                        date_count = sum(1 for v in values[:20] if _re.match(r'^\d{4}-\d{2}-\d{2}', v))

                        if date_count > len(values[:20]) * 0.5:

                            is_dt = True

                col_is_numeric[src_col] = is_num

                col_is_date[src_col] = is_dt

        except Exception:

            for src_col in mapped_cols.keys():

                col_is_numeric[src_col] = False

                col_is_date[src_col] = False

        for chunk_idx in range(total_chunks):

            ensure_not_stopped()

            offset = chunk_idx * CHUNK_SIZE

            chunk_progress_base = chunk_idx / total_chunks

            status_area.text(f'📦 Chunk {chunk_idx + 1}/{total_chunks} '

                             f'(records {offset + 1:,}–{min(offset + CHUNK_SIZE, total_record_count):,})...')

            # --- Fetch chunk from Source table ---

            snow_chunk = {}

            try:

                cursor = conn.cursor()

                cursor.execute(

                    f"SELECT {snow_src_cols_str} FROM {rr_source_table}{snow_where} "

                    f"LIMIT {CHUNK_SIZE} OFFSET {offset}"

                )

                for row in cursor:

                    key_val = str(row[0]) if row[0] is not None else None

                    if key_val is None:

                        continue

                    record = {}

                    for i, src_col in enumerate(mapped_cols.keys()):

                        record[src_col] = row[i + 1]

                    snow_chunk[key_val] = record

                cursor.close()

            except Exception as e:

                st.error(f'Source fetch error at chunk {chunk_idx + 1}: {e}')

                st.stop()

            if not snow_chunk:

                break

            progress_bar.progress(

                chunk_progress_base + 0.3 / total_chunks,

                text=f'Chunk {chunk_idx + 1}/{total_chunks}: Fetched {len(snow_chunk):,} from source. Querying target...'

            )

            # --- Fetch matching records from Target table using key values ---

            tgt_chunk = {}

            chunk_keys = list(snow_chunk.keys())

            # Batch fetch from target (Snowflake IN clause limit ~16K values)

            TGT_BATCH_SIZE = 5000

            tgt_batches = [chunk_keys[i:i + TGT_BATCH_SIZE] for i in range(0, len(chunk_keys), TGT_BATCH_SIZE)]

            for batch in tgt_batches:

                ensure_not_stopped()

                in_clause = ','.join([f"'{k}'" for k in batch])

                tgt_sql = f"SELECT {snow_tgt_cols_str} FROM {rr_target_table} WHERE {tgt_key_col} IN ({in_clause})"

                try:

                    cursor = conn.cursor()

                    cursor.execute(tgt_sql)

                    for row in cursor:

                        key_val = str(row[0]) if row[0] is not None else None

                        if key_val is None:

                            continue

                        rec = {}

                        for i, tgt_col in enumerate(mapped_cols.values()):

                            rec[tgt_col] = row[i + 1]

                        tgt_chunk[key_val] = rec

                    cursor.close()

                except Exception as e:

                    st.warning(f'Target fetch warning: {e}')

            progress_bar.progress(

                chunk_progress_base + 0.6 / total_chunks,

                text=f'Chunk {chunk_idx + 1}/{total_chunks}: Comparing records...'

            )

            # --- Compare this chunk ---

            for key_val, src_record in snow_chunk.items():

                ensure_not_stopped()

                tgt_record = tgt_chunk.get(key_val, {})

                has_mismatch = False

                record_summary = {}

                if not tgt_record:

                    records_not_in_target += 1

                    for src_col in mapped_cols.keys():

                        total_comparisons += 1

                        total_mismatches_count += 1

                        col_mismatch_counts[src_col] += 1

                        record_summary[src_col] = '❌ NOT IN TARGET'

                        if len(mismatch_buffer) < MAX_MISMATCH_ROWS:

                            mismatch_buffer.append({

                                'Record_ID': key_val,

                                'Source_Column': src_col,

                                'Target_Column': mapped_cols[src_col],

                                'Source_Value': str(src_record.get(src_col, '')) if src_record.get(src_col) is not None else '',

                                'Target_Value': '❌ RECORD NOT FOUND',

                                'Detected_Type': 'N/A',

                            })

                    records_with_issues_set.add(key_val)

                    failed_records_summary.append((key_val, record_summary))

                    continue

                for src_col, tgt_col in mapped_cols.items():

                    src_val = src_record.get(src_col)

                    tgt_val = tgt_record.get(tgt_col)

                    is_num = col_is_numeric.get(src_col, False)

                    is_dt = col_is_date.get(src_col, False)

                    src_norm = _normalize(src_val, is_numeric=is_num, is_date=is_dt)

                    tgt_norm = _normalize(tgt_val, is_numeric=is_num, is_date=is_dt)

                    total_comparisons += 1

                    if src_norm == tgt_norm:

                        total_matches += 1

                        record_summary[src_col] = '✅'

                    else:

                        total_mismatches_count += 1

                        col_mismatch_counts[src_col] += 1

                        has_mismatch = True

                        record_summary[src_col] = '❌'

                        if len(mismatch_buffer) < MAX_MISMATCH_ROWS:

                            mismatch_buffer.append({

                                'Record_ID': key_val,

                                'Source_Column': src_col,

                                'Target_Column': tgt_col,

                                'Source_Value': str(src_val) if src_val is not None else '',

                                'Target_Value': str(tgt_val) if tgt_val is not None else '',

                                'Detected_Type': 'numeric' if is_num else ('date' if is_dt else 'string'),

                            })

                if has_mismatch:

                    records_with_issues_set.add(key_val)

                    failed_records_summary.append((key_val, record_summary))

            # Free chunk memory immediately

            del snow_chunk, tgt_chunk

            progress_bar.progress(

                (chunk_idx + 1) / total_chunks,

                text=f'Chunk {chunk_idx + 1}/{total_chunks} done. Mismatches so far: {total_mismatches_count:,}'

            )

        elapsed = time.time() - t_start

        progress_bar.progress(0.95, text='Generating Excel report...')

        # --- Display Summary Stats ---

        match_pct = (total_matches / max(total_comparisons, 1)) * 100

        records_with_issues = len(records_with_issues_set)

        st.markdown('---')

        st.markdown('### 📊 Comparison Results')

        m1, m2, m3, m4, m5 = st.columns(5)

        m1.metric('🔍 Records Compared', f'{total_record_count:,}')

        m2.metric('📊 Total Comparisons', f'{total_comparisons:,}')

        m3.metric('✅ Match %', f'{match_pct:.1f}%')

        m4.metric('❌ Mismatches', f'{total_mismatches_count:,}')

        m5.metric('⚠️ Records with Issues', f'{records_with_issues:,}')

        # Performance stats

        speed = int(total_comparisons / max(elapsed, 0.001))

        st.caption(f'✅ Completed in **{elapsed:.1f}s** | Speed: **{speed:,} comparisons/sec** | Chunks: {total_chunks}')

        if records_not_in_target > 0:

            st.warning(f'⚠️ {records_not_in_target:,} records exist in source but NOT found in target table.')

        # Top mismatched columns

        if total_mismatches_count > 0:

            st.markdown('#### 📊 Top Mismatched Columns')

            top_cols = sorted(col_mismatch_counts.items(), key=lambda x: x[1], reverse=True)[:15]

            top_cols_filtered = [(c, n, f'{n / max(total_record_count, 1) * 100:.1f}%') for c, n in top_cols if n > 0]

            if top_cols_filtered:

                top_df = pd.DataFrame(top_cols_filtered, columns=['Column', 'Mismatch Count', 'Fail Rate'])

                st.dataframe(top_df, width='stretch')

            # Show first N mismatches

            show_count = min(500, len(mismatch_buffer))

            with st.expander(f'🔍 Mismatch Details (showing first {show_count} of {total_mismatches_count:,})', expanded=True):

                mismatch_df = pd.DataFrame(mismatch_buffer[:500])

                st.dataframe(mismatch_df, width='stretch')

        else:

            st.success('✅ **Perfect match!** All records and columns match between source and target tables.')

        # --- Generate Excel Report (optimized for large data) ---

        status_area.text('📊 Writing Excel report...')

        output = io.BytesIO()

        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

        from openpyxl.utils import get_column_letter

        with pd.ExcelWriter(output, engine='openpyxl') as writer:

            # Sheet 1: Overview

            overview_data = {

                'Metric': [

                    'Source Table', 'Target Table', 'Key Field Used',

                    'Run Date', 'Total Records Compared', 'Records Found in Target',

                    'Records NOT in Target', 'Columns Compared', 'Total Cell Comparisons',

                    'Total Matches', 'Total Mismatches', 'Match Percentage',

                    'Records with Issues', 'Processing Time', 'Speed'

                ],

                'Value': [

                    rr_source_table, rr_target_table, key_used,

                    pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),

                    f'{total_record_count:,}', f'{total_record_count - records_not_in_target:,}',

                    f'{records_not_in_target:,}', len(mapped_cols), f'{total_comparisons:,}',

                    f'{total_matches:,}', f'{total_mismatches_count:,}', f'{match_pct:.2f}%',

                    f'{records_with_issues:,}', f'{elapsed:.1f} seconds',

                    f'{speed:,} comparisons/sec'

                ]

            }

            overview_df = pd.DataFrame(overview_data)

            overview_df.to_excel(writer, index=False, sheet_name='Overview')

            # Per-column breakdown in Overview

            if col_mismatch_counts:

                top_all = sorted(col_mismatch_counts.items(), key=lambda x: x[1], reverse=True)

                top_all_df = pd.DataFrame(top_all, columns=['Column', 'Mismatch_Count'])

                top_all_df['Mismatch_%'] = (top_all_df['Mismatch_Count'] / max(total_record_count, 1) * 100).round(2)

                top_all_df['Status'] = top_all_df['Mismatch_Count'].apply(lambda x: 'PASS' if x == 0 else 'FAIL')

                top_all_df.to_excel(writer, index=False, sheet_name='Overview', startrow=len(overview_data['Metric']) + 3)

            # Sheet 2: Mismatches (capped at MAX_MISMATCH_ROWS)

            if mismatch_buffer:

                mismatch_full_df = pd.DataFrame(mismatch_buffer)

                mismatch_full_df.to_excel(writer, index=False, sheet_name='Mismatches')

                if total_mismatches_count > MAX_MISMATCH_ROWS:

                    note_df = pd.DataFrame({'Note': [f'⚠️ Report truncated to {MAX_MISMATCH_ROWS:,} rows. Total mismatches: {total_mismatches_count:,}']})

                    note_df.to_excel(writer, index=False, sheet_name='Truncation Note')

            else:

                pd.DataFrame({'Message': ['No mismatches found — all records match perfectly!']}).to_excel(

                    writer, index=False, sheet_name='Mismatches')

            # Sheet 3: Failed Records Matrix — ONLY failed records (not all records)

            if failed_records_summary:

                MAX_SUMMARY_ROWS = 100_000

                summary_data = failed_records_summary[:MAX_SUMMARY_ROWS]

                summary_rows_out = []

                for key_val, col_results in summary_data:

                    row = {'Record_ID': key_val}

                    row.update(col_results)

                    summary_rows_out.append(row)

                summary_df = pd.DataFrame(summary_rows_out)

                summary_df.to_excel(writer, index=False, sheet_name='Failed Records Matrix')

                if len(failed_records_summary) > MAX_SUMMARY_ROWS:

                    pd.DataFrame({'Note': [f'⚠️ Showing first {MAX_SUMMARY_ROWS:,} of {len(failed_records_summary):,} failed records']}).to_excel(

                        writer, index=False, sheet_name='Matrix Note')

            else:

                pd.DataFrame({'Message': ['All records passed — no failed records to show']}).to_excel(

                    writer, index=False, sheet_name='Failed Records Matrix')

            # --- Formatting ---

            wb = writer.book

            # Format Overview sheet

            ws_ov = wb['Overview']

            header_fill = PatternFill(start_color='1F4E79', end_color='1F4E79', fill_type='solid')

            header_font = Font(bold=True, color='FFFFFF', size=11)

            for cell in ws_ov[1]:

                cell.fill = header_fill

                cell.font = header_font

            ws_ov.column_dimensions['A'].width = 30

            ws_ov.column_dimensions['B'].width = 50

            # Format Mismatches sheet

            ws_mm = wb['Mismatches']

            for cell in ws_mm[1]:

                cell.fill = header_fill

                cell.font = header_font

            for col_idx in range(1, ws_mm.max_column + 1):

                ws_mm.column_dimensions[get_column_letter(col_idx)].width = 25

            ws_mm.freeze_panes = 'A2'

            # Format Failed Records Matrix sheet

            ws_sm = wb['Failed Records Matrix']

            for cell in ws_sm[1]:

                cell.fill = header_fill

                cell.font = header_font

            ws_sm.freeze_panes = 'B2'

            pass_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')

            fail_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')

            warn_fill = PatternFill(start_color='FFEB9C', end_color='FFEB9C', fill_type='solid')

            # Only format up to 10K rows to avoid slow formatting on huge sheets

            max_format_rows = min(ws_sm.max_row, 10_001)

            for row_idx in range(2, max_format_rows + 1):

                for col_idx in range(2, ws_sm.max_column + 1):

                    cell = ws_sm.cell(row=row_idx, column=col_idx)

                    val = str(cell.value) if cell.value else ''

                    if val == '✅':

                        cell.fill = pass_fill

                    elif val == '❌':

                        cell.fill = fail_fill

                    elif '❌' in val or '⚠️' in val:

                        cell.fill = warn_fill

            for col_idx in range(1, ws_sm.max_column + 1):

                ws_sm.column_dimensions[get_column_letter(col_idx)].width = 18

        progress_bar.progress(1.0, text='✅ Done!')

        status_area.empty()

        # Store results in session

        st.session_state['rr_report_bytes'] = output.getvalue()

        st.session_state['rr_summary_stats'] = {

            'records': total_record_count,

            'comparisons': total_comparisons,

            'matches': total_matches,

            'mismatches': total_mismatches_count,

            'match_pct': match_pct,

        }

        st.success(f'✅ Comparison complete! {total_comparisons:,} comparisons across {total_record_count:,} records in {elapsed:.1f}s.')

    # Show download button if report exists

    if st.session_state.get('rr_report_bytes'):

        st.download_button(

            label='📥 Download Record-by-Record Excel Report',

            data=st.session_state['rr_report_bytes'],

            file_name=f'record_by_record_comparison_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.xlsx',

            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',

            key='rr_download_btn'

        )


