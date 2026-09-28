import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import sf_bulk_loader as loader
from ui_theme import render_theme_control


st.title('Cancellation Test')
render_theme_control()
st.text_input('Sample text', value='Readable text')
st.text_area('Sample query', value='SELECT Id FROM Account')
st.selectbox('Sample options', ['Account', 'Contact'])
st.button('STOP', on_click=loader.set_stop_flag, key='probe_stop')
if st.button('Run blocked task', key='probe_run'):
    loader.clear_stop_flag()
    st.info('Waiting for cancellation')
    cancelled = loader._stop_flag.wait(60)
    st.session_state['probe_cancelled'] = cancelled
if loader.is_stopped():
    st.success('Cancellation delivered')