import streamlit as st


THEME_CSS = """
:root {
    --sf-page: #0d1720;
    --sf-surface: #15232b;
    --sf-field: #1d3039;
    --sf-text: #f0f4ff;
    --sf-muted: #b2bdcd;
    --sf-border: #414b60;
    --sf-accent: #5eead4;
    --sf-selected: #164e63;
    --sf-primary: #0f766e;
    --sf-primary-text: #ffffff;
    --sf-page-wash: linear-gradient(135deg, #10232b 0%, #101a2a 48%, #211d27 100%);
    --sf-panel-wash: linear-gradient(135deg, #19323a 0%, #182936 65%, #2b2027 100%);
    --sf-control-wash: linear-gradient(135deg, #223a41, #1d3039);
    --sf-shadow: 0 8px 24px rgba(0, 0, 0, 0.16);
    --sf-title-wash: linear-gradient(100deg, #5eead4, #60a5fa 55%, #fb8b73);
}
:root[data-sf-theme="light"] {
    color-scheme: light;
    --sf-page: #ffffff;
    --sf-surface: #f3f5f7;
    --sf-field: #ffffff;
    --sf-text: #202632;
    --sf-muted: #526071;
    --sf-border: #bbc3d2;
    --sf-accent: #0f766e;
    --sf-selected: #d9f3f0;
    --sf-page-wash: linear-gradient(135deg, #edf9f7 0%, #ffffff 48%, #fff1ed 100%);
    --sf-panel-wash: linear-gradient(135deg, #e6f7f4 0%, #ffffff 65%, #fff1ed 100%);
    --sf-control-wash: linear-gradient(135deg, #ffffff, #f3f4fa);
    --sf-shadow: 0 8px 24px rgba(39, 45, 77, 0.07);
    --sf-title-wash: linear-gradient(100deg, #0f766e, #2563eb 55%, #c2412d);
}
:root[data-sf-theme="dark"] { color-scheme: dark; }
html[data-sf-theme] .stApp {
    background: var(--sf-page-wash) !important;
    background-attachment: fixed !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme] [data-testid="stSidebar"],
html[data-sf-theme] [data-testid="stExpander"] details,
html[data-sf-theme] [data-testid="stFileUploaderDropzone"],
html[data-sf-theme] [data-baseweb="popover"] > div,
html[data-sf-theme] [role="dialog"] {
    background: var(--sf-surface) !important;
    color: var(--sf-text) !important;
    border-color: var(--sf-border) !important;
}
html[data-sf-theme] :is(.stTextInput, .stTextArea, .stNumberInput, .stDateInput, .stTimeInput) :is(input, textarea, [data-baseweb="input"], [data-baseweb="base-input"], [data-baseweb="textarea"]),
html[data-sf-theme] [data-baseweb="select"] > div {
    background: var(--sf-field) !important;
    color: var(--sf-text) !important;
    -webkit-text-fill-color: var(--sf-text) !important;
    caret-color: var(--sf-text) !important;
    border-color: var(--sf-border) !important;
    transition: border-color 0.15s ease !important;
}
html[data-sf-theme] :is(input, textarea)::placeholder {
    color: var(--sf-muted) !important;
    -webkit-text-fill-color: var(--sf-muted) !important;
    opacity: 1 !important;
}
html[data-sf-theme] :is(input, textarea):disabled {
    color: var(--sf-muted) !important;
    -webkit-text-fill-color: var(--sf-muted) !important;
    opacity: 1 !important;
}
html[data-sf-theme] :is([data-baseweb="select"], [role="listbox"], [role="option"], [data-baseweb="calendar"]) {
    color: var(--sf-text) !important;
    background: var(--sf-field) !important;
}
html[data-sf-theme] [data-baseweb="select"] :is(input, span, div),
html[data-sf-theme] [role="option"] :is(span, div) {
    color: var(--sf-text) !important;
    -webkit-text-fill-color: var(--sf-text) !important;
}
html[data-sf-theme] [role="option"]:hover,
html[data-sf-theme] [role="option"][aria-selected="true"],
html[data-sf-theme] [data-baseweb="tag"] {
    background: var(--sf-selected) !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme] :is(.stRadio, .stCheckbox) label p,
html[data-sf-theme] [data-testid="stWidgetLabel"] p,
html[data-sf-theme] [data-testid="stMarkdownContainer"] > :is(p, ul, ol),
html[data-sf-theme] [data-testid="stMetricValue"],
html[data-sf-theme] [data-testid="stMetricLabel"],
html[data-sf-theme] :is(h2, h3, h4, h5, h6) {
    color: var(--sf-text) !important;
    -webkit-text-fill-color: var(--sf-text) !important;
}
html[data-sf-theme] :is([data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p) {
    color: var(--sf-muted) !important;
}
html[data-sf-theme] :is(.stButton button, .stDownloadButton button, .stNumberInput button, [data-testid="stFileUploader"] button) {
    background: var(--sf-surface) !important;
    color: var(--sf-text) !important;
    border-color: var(--sf-border) !important;
}
html[data-sf-theme] .stButton button[kind="primary"] {
    background: var(--sf-primary) !important;
    background-image: var(--sf-primary-wash, none) !important;
    color: var(--sf-primary-text) !important;
    box-shadow: 0 4px 14px color-mix(in srgb, var(--sf-primary) 24%, transparent) !important;
}
html[data-sf-theme] .stButton button[data-danger="true"] {
    background: linear-gradient(115deg, #ac2443, #c32d3f) !important;
    color: #ffffff !important;
    box-shadow: 0 4px 12px rgba(189, 36, 56, 0.18) !important;
}
html[data-sf-theme] :is(.stButton, .stDownloadButton) button :is(p, span, div) {
    color: inherit !important;
    -webkit-text-fill-color: currentColor !important;
}
html[data-sf-theme] :is(.stTextInput, .stNumberInput, .stDateInput, .stTimeInput) button {
    color: var(--sf-text) !important;
    background: var(--sf-field) !important;
}
html[data-sf-theme] :is(.stTextInput, .stNumberInput, .stDateInput, .stTimeInput, .stSelectbox, .stMultiSelect) svg {
    color: var(--sf-text) !important;
}
html[data-sf-theme] [data-testid="stFileUploaderDropzone"] :is(span, small, p, svg) {
    color: var(--sf-text) !important;
    -webkit-text-fill-color: currentColor !important;
    opacity: 1 !important;
}
html[data-sf-theme] .sf-conn-card > div {
    color: var(--sf-text) !important;
    -webkit-text-fill-color: currentColor !important;
}
html[data-sf-theme] .sf-conn-card > div:last-child {
    color: var(--sf-muted) !important;
}
html[data-sf-theme] .sf-conn-card.active > div:last-child {
    color: #218344 !important;
}
html[data-sf-theme="dark"] .sf-conn-card.active > div:last-child {
    color: #71df96 !important;
}
html[data-sf-theme] .sf-hero-title {
    background: var(--sf-title-wash) !important;
    background-clip: text !important;
    -webkit-background-clip: text !important;
    color: var(--sf-accent) !important;
    -webkit-text-fill-color: transparent !important;
}
html[data-sf-theme] .stTabs [data-baseweb="tab"] {
    color: var(--sf-muted) !important;
}
html[data-sf-theme] .stTabs [data-baseweb="tab"] p { color: inherit !important; }
html[data-sf-theme] .stTabs [aria-selected="true"] {
    color: var(--sf-text) !important;
    background: var(--sf-selected) !important;
}
html[data-sf-theme] :is(code, pre) {
    background: var(--sf-surface) !important;
    color: var(--sf-text) !important;
}
html[data-sf-theme="light"] h1 {
    background: none !important;
    color: var(--sf-accent) !important;
    -webkit-text-fill-color: var(--sf-accent) !important;
}
html[data-sf-theme="light"] :is(.sf-section-title, .sf-step-title, .sf-stat-value, .sf-kpi-value) {
    color: var(--sf-text) !important;
}
html[data-sf-theme="light"] :is([class^="sf-"], [class*=" sf-"]) {
    color: var(--sf-text);
}
html[data-sf-theme="light"] :is(.sf-hero, .sf-sidebar-section, .sf-conn-card, .sf-kpi-card, .sf-livebar, .sf-stepper, .sf-job-card, .sf-event-log) {
    background: var(--sf-surface) !important;
    border-color: var(--sf-border) !important;
}
html[data-sf-theme="light"] [class^="sf-"] :is(p, span, strong, small, label),
html[data-sf-theme="light"] [style*="color:rgba(255,255,255"],
html[data-sf-theme="light"] [style*="color: rgba(255,255,255"],
html[data-sf-theme="light"] [style*="color:#fff"],
html[data-sf-theme="light"] [style*="color:#f0f4ff"] {
    color: var(--sf-text) !important;
    -webkit-text-fill-color: currentColor !important;
}
html[data-sf-theme="light"] :is(.sf-hero, .stApp)::before,
html[data-sf-theme="light"] :is(.sf-hero, .stApp)::after {
    background: none !important;
}
html[data-sf-theme] [data-testid="stSidebar"] {
    background: var(--sf-sidebar-wash, var(--sf-panel-wash)) !important;
    border-right: 1px solid var(--sf-border) !important;
}
html[data-sf-theme] :is(.sf-hero, .sf-conn-card, .sf-kpi-card, .sf-job-card) {
    background: var(--sf-panel-wash) !important;
    border-color: var(--sf-border) !important;
    box-shadow: var(--sf-shadow) !important;
}
html[data-sf-theme] .sf-sidebar-section {
    background: transparent !important;
    border-color: transparent !important;
    box-shadow: none !important;
}
html[data-sf-theme] :is(.stButton button[kind="secondary"]:not([data-danger="true"]), .stDownloadButton button) {
    background: var(--sf-control-wash) !important;
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.06) !important;
}
html[data-sf-theme] :is(.stButton button, .stDownloadButton button):hover {
    border-color: var(--sf-accent) !important;
}
html[data-sf-theme] :is(.stTextInput, .stNumberInput, .stDateInput, .stTimeInput) [data-baseweb="input"] {
    border: 1px solid var(--sf-border) !important;
    border-radius: 8px !important;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.04);
}
html[data-sf-theme] :is(.stTextInput, .stNumberInput, .stDateInput, .stTimeInput) :is(input, [data-baseweb="base-input"]) {
    border: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
}
html[data-sf-theme] :is(.stTextInput, .stNumberInput, .stDateInput, .stTimeInput) [data-baseweb="input"]:focus-within {
    border-color: var(--sf-accent) !important;
    box-shadow: 0 0 0 3px rgba(139, 119, 220, 0.14) !important;
}
html[data-sf-theme] .stTabs [aria-selected="true"] {
    background: var(--sf-panel-wash) !important;
    border-color: var(--sf-accent) !important;
    box-shadow: 0 3px 10px rgba(112, 58, 174, 0.12) !important;
}
.sf-theme-control { position: fixed; top: 12px; right: 18px; z-index: 10000;
    display: flex; justify-content: flex-end; height: 38px; }
.sf-theme-template { display:none; }
.sf-theme-panel { position:fixed; inset:58px 12px auto auto; margin:0; padding:16px;
    width:280px; max-width:calc(100vw - 24px); box-sizing:border-box;
    max-height:calc(100dvh - 72px); overflow:auto; border:1px solid var(--sf-border);
    border-radius:8px; background:var(--sf-surface); color:var(--sf-text);
    box-shadow:var(--sf-shadow); font-family:inherit; }
.sf-theme-panel h2 { font-size:1rem; margin:0 0 12px; letter-spacing:0; }
.sf-theme-modes { display:flex; gap:8px; margin-bottom:16px; }
.sf-theme-panel button { font:inherit; cursor:pointer; color:var(--sf-text);
    background:var(--sf-field); border:1px solid var(--sf-border); border-radius:6px; }
.sf-theme-modes button { flex:1; padding:8px; }
.sf-theme-modes button[aria-pressed="true"] { background:var(--sf-selected); border-color:var(--sf-accent); }
.sf-theme-swatches { display:flex; gap:10px; margin:10px 0 16px; }
.sf-theme-panel .sf-theme-swatch { width:30px; height:30px; background:var(--swatch);
    border:2px solid var(--sf-border); border-radius:50%; }
.sf-theme-panel .sf-theme-swatch[aria-pressed="true"] { outline:2px solid var(--sf-text); outline-offset:2px; }
.sf-theme-palettes { display:grid; grid-template-columns:repeat(3, 1fr); gap:10px; margin:12px 0 16px; }
.sf-theme-panel .sf-palette-swatch { display:grid; grid-template-rows:repeat(4, 1fr);
    height:64px; width:100%; padding:0; overflow:hidden; border:2px solid var(--sf-border); }
.sf-palette-swatch span { display:block; width:100%; height:100%; }
.sf-theme-panel .sf-palette-swatch[aria-pressed="true"] { outline:2px solid var(--sf-text); outline-offset:2px; }
.sf-theme-custom { display:flex; align-items:center; justify-content:space-between; gap:12px; }
.sf-theme-custom + .sf-theme-custom { margin-top:8px; }
.sf-theme-custom input { width:44px; height:34px; padding:2px; cursor:pointer;
    background:var(--sf-field); border:1px solid var(--sf-border); }
.sf-theme-reset { padding:7px 12px; margin-top:16px; }
.sf-theme-panel :is(button, input):focus-visible { outline:2px solid var(--sf-accent); outline-offset:3px; }
.sf-theme-toggle {
    height: 36px; width: 36px; flex: 0 0 36px;
    display: grid; place-items: center; border-radius: 6px;
    background: var(--sf-surface); color: var(--sf-text);
    border: 1px solid var(--sf-border); cursor: pointer;
}
.sf-theme-toggle:hover { border-color: var(--sf-accent); }
.sf-theme-toggle:focus-visible { outline: 2px solid var(--sf-accent); outline-offset: 2px; }
.sf-theme-toggle span { font-family: 'Material Symbols Rounded'; font-size: 22px; }
html[data-sf-custom-color] :is(.sf-hero, .stApp)::after,
html[data-sf-custom-color] .stApp::before {
    background:none !important;
    animation:none !important;
    filter:none !important;
}
html[data-sf-custom-color] .sf-hero::before,
html[data-sf-custom-color] .sf-sidebar-section::before,
html[data-sf-custom-color] .stTabs [aria-selected="true"]::after {
    background:var(--sf-primary-wash) !important;
    box-shadow:none !important;
    animation:none !important;
}
html[data-sf-custom-color] :is(.sf-sidebar-brand-name, .sf-sidebar-section-title, .sf-hero-tagline) {
    color:var(--sf-text) !important;
    -webkit-text-fill-color:currentColor !important;
    background:none !important;
}
html[data-sf-custom-color] .sf-hero {
    background:var(--sf-header) !important;
    border-color:var(--sf-header) !important;
}
html[data-sf-custom-color] .sf-hero :is(.sf-hero-title, .sf-hero-tagline, .sf-hero-status, .sf-hero-status *) {
    color:var(--sf-header-text) !important;
    -webkit-text-fill-color:currentColor !important;
}
html[data-sf-custom-color] .sf-hero .sf-hero-title { background:none !important; }
html[data-sf-custom-color] :is(.sf-icon-circle-purple, .sf-icon-circle-indigo, .sf-sidebar-version) {
    background:var(--sf-control-wash) !important;
    border-color:var(--sf-border) !important;
    box-shadow:var(--sf-shadow) !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label {
    background:var(--sf-field) !important;
    border-color:var(--sf-border) !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label:has(input:checked) {
    background:var(--sf-selected) !important;
    border-color:var(--sf-accent) !important;
    box-shadow:0 2px 8px color-mix(in srgb, var(--sf-primary) 15%, transparent) !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label > div:first-child {
    background:var(--sf-field) !important;
    border-color:var(--sf-border) !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label:has(input:checked) > div:first-child {
    background:var(--sf-field) !important;
    border-color:var(--sf-accent) !important;
    box-shadow:none !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label:has(input:checked)::after,
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label:has(input:checked) > div:first-child::after {
    background:var(--sf-accent) !important;
    box-shadow:none !important;
}
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label p,
html[data-sf-custom-color] .stRadio [role="radiogroup"] > label:has(input:checked) p {
    color:var(--sf-text) !important;
    -webkit-text-fill-color:currentColor !important;
}
html[data-sf-custom-color] .stSlider [data-baseweb="slider"] > div > div {
    background:var(--sf-border) !important;
}
html[data-sf-custom-color] .stSlider [data-baseweb="slider"] > div > div > div {
    background:var(--sf-primary-wash) !important;
}
html[data-sf-custom-color] .stSlider [role="slider"] {
    background:var(--sf-field) !important;
    border-color:var(--sf-accent) !important;
    box-shadow:0 0 0 4px var(--sf-selected) !important;
}
html[data-sf-theme] [data-testid="stMarkdownContainer"] > div:has(> .sf-icon-circle) > div:last-child > div:last-child {
    color: var(--sf-muted) !important;
    -webkit-text-fill-color: currentColor !important;
}
@media (max-width: 640px) {
    html[data-sf-theme] .sf-hero {
        flex-direction: column !important;
        align-items: stretch !important;
        gap: 16px !important;
        padding: 18px !important;
    }
    html[data-sf-theme] .sf-hero > div { min-width: 0; }
    html[data-sf-theme] .sf-hero-status {
        flex-direction: row !important;
        flex-wrap: wrap !important;
        justify-content: flex-start !important;
        min-width: 0 !important;
    }
    html[data-sf-theme] .sf-hero-title { font-size: 1.25rem !important; }
    html[data-sf-theme] [data-testid="stMarkdownContainer"] > div:has(> .sf-icon-circle) {
        gap: 10px !important;
        padding: 14px 10px !important;
    }
    html[data-sf-theme] [data-testid="stMarkdownContainer"] > div:has(> .sf-icon-circle) h2 {
        font-size: 1.35rem !important;
    }
}
.sf-popup-overlay { position:fixed; inset:0; z-index:50000; display:grid; place-content:center;
    justify-items:center; background:rgba(0,0,0,0.6); backdrop-filter:blur(4px); opacity:1;
    transition:opacity 0.3s ease, visibility 0.3s ease; }
.sf-popup-overlay.is-hidden { opacity:0; visibility:hidden; pointer-events:none; }
.sf-popup-modal { background:white; border-radius:12px; padding:32px; max-width:500px;
    text-align:center; box-shadow:0 20px 60px rgba(0,0,0,0.3); gap:16px; display:flex;
    flex-direction:column; align-items:center; animation:slideUp 0.3s ease; }
.sf-popup-success { border-top:4px solid #10b981; }
.sf-popup-error { border-top:4px solid #ef4444; }
.sf-popup-icon { font-size:48px; margin-bottom:8px; }
.sf-popup-title { font-size:22px; font-weight:700; color:#1f2937; }
.sf-popup-message { font-size:16px; color:#6b7280; line-height:1.5; }
.sf-popup-close { margin-top:16px; background:#f3f4f6; border:none; border-radius:6px;
    padding:10px 24px; cursor:pointer; font-weight:600; color:#374151;
    transition:background 0.2s ease; }
.sf-popup-close:hover { background:#e5e7eb; }
@keyframes slideUp { from { transform:translateY(20px); opacity:0; }
    to { transform:translateY(0); opacity:1; } }
"""

THEME_JS = """
export default function(component) {
    const { parentElement, data } = component;
    const root = document.documentElement;
    const control = parentElement.querySelector('.sf-theme-control').cloneNode(true);
    control.classList.remove('sf-theme-template');
    document.body.appendChild(control);
    const button = control.querySelector('.sf-theme-toggle');
    const panel = control.querySelector('.sf-theme-panel');
    panel.id = 'sf-theme-panel';
    const modes = [...control.querySelectorAll('[data-theme]')];
    const colorInputs = [...control.querySelectorAll('[data-palette-role]')];
    const listeners = new AbortController();
    const listen = (element, event, handler) => element.addEventListener(event, handler, {signal:listeners.signal});
    const storageKey = 'sf_bulk_color_theme_v2';
    const accentKey = 'sf_bulk_accent_v1';
    const paletteKey = 'sf_bulk_palette_v1';
    const read = key => { try { return localStorage.getItem(key); } catch (error) { return null; } };
    const save = (key, value) => { try {
        if (value) localStorage.setItem(key, value); else localStorage.removeItem(key);
    } catch (error) {} };
    const presets = [
        {name:'Sky & Rose', background:'#75c1e8', panels:'#fff7dd', headers:'#425b9a', buttons:'#ff91a4'},
        {name:'Midnight & Cream', background:'#233b6c', panels:'#faf0ce', headers:'#010736', buttons:'#e4c66b'},
        {name:'Teal & Scarlet', background:'#2ca8a8', panels:'#f8e1a5', headers:'#6e1a1a', buttons:'#a92222'},
        {name:'Olive & Clay', background:'#91ad69', panels:'#fbecd7', headers:'#6b3513', buttons:'#587727'},
        {name:'Denim & Orange', background:'#455f8e', panels:'#f2f5fa', headers:'#253d6b', buttons:'#f58226'},
        {name:'Forest & Gold', background:'#296e60', panels:'#e8ddc5', headers:'#123e34', buttons:'#c19a43'},
    ];
    const roles = ['background', 'panels', 'headers', 'buttons'];
    const validColor = value => typeof value === 'string' && /^#[0-9a-f]{6}$/i.test(value);
    const colorsOf = source => ({
        ...Object.fromEntries(roles.map(role => [role, source[role]])),
        sidebar:validColor(source.sidebar) ? source.sidebar : source.panels,
    });
    let palette = null;
    try {
        const stored = JSON.parse(read(paletteKey));
        if (stored && roles.every(role => validColor(stored[role]))) palette = colorsOf(stored);
    } catch (error) {}
    const legacyAccent = read(accentKey);
    if (!palette && validColor(legacyAccent)) palette = {...colorsOf(presets[0]), buttons:legacyAccent};
    const swatches = presets.map(preset => {
        const swatch = document.createElement('button');
        swatch.type = 'button';
        swatch.className = 'sf-palette-swatch';
        swatch.title = preset.name;
        swatch.setAttribute('aria-label', preset.name);
        roles.forEach(role => {
            const stripe = document.createElement('span');
            stripe.style.backgroundColor = preset[role];
            stripe.setAttribute('aria-hidden', 'true');
            swatch.appendChild(stripe);
        });
        control.querySelector('.sf-theme-palettes').appendChild(swatch);
        return swatch;
    });
    const textOn = color => {
        const channels = [1, 3, 5].map(index => parseInt(color.slice(index, index + 2), 16) / 255)
            .map(value => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4);
        const luminance = channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
        return luminance > 0.179 ? '#000000' : '#ffffff';
    };
    const applyPalette = () => {
        const colors = palette || colorsOf(presets[0]);
        const primary = palette ? colors.buttons : '#0f766e';
        const primaryText = textOn(primary);
        root.style.setProperty('--sf-primary', primary);
        root.style.setProperty('--sf-primary-text', primaryText);
        const paletteProperties = ['--sf-accent', '--sf-selected', '--sf-title-wash',
            '--sf-page', '--sf-surface', '--sf-field', '--sf-border', '--sf-page-wash',
            '--sf-panel-wash', '--sf-control-wash', '--sf-primary-wash', '--sf-shadow',
            '--sf-header', '--sf-header-text', '--sf-sidebar-wash'];
        paletteProperties.forEach(property => root.style.removeProperty(property));
        root.toggleAttribute('data-sf-custom-color', Boolean(palette));
        if (palette) {
            const dark = root.dataset.sfTheme === 'dark';
            const mix = (color, amount, base) => `color-mix(in srgb, ${color} ${amount}%, ${base})`;
            const variables = {
                '--sf-page': mix(colors.background, dark ? 16 : 38, dark ? '#121619' : '#ffffff'),
                '--sf-surface': mix(colors.panels, dark ? 14 : 25, dark ? '#202629' : '#ffffff'),
                '--sf-field': mix(colors.panels, dark ? 10 : 18, dark ? '#292f33' : '#ffffff'),
                '--sf-border': mix(colors.headers, 32, dark ? '#647078' : '#b6bdc5'),
                '--sf-accent': mix(colors.buttons, dark ? 45 : 40, dark ? '#ffffff' : '#000000'),
                '--sf-selected': mix(colors.buttons, dark ? 30 : 20, 'var(--sf-surface)'),
                '--sf-page-wash': `linear-gradient(135deg, var(--sf-page), ${mix(colors.background, dark ? 10 : 22, dark ? '#121619' : '#ffffff')})`,
                '--sf-panel-wash': `linear-gradient(135deg, var(--sf-surface), ${mix(colors.panels, dark ? 20 : 35, dark ? '#202629' : '#ffffff')})`,
                '--sf-sidebar-wash': `linear-gradient(135deg, ${mix(colors.sidebar, dark ? 14 : 25, dark ? '#202629' : '#ffffff')}, ${mix(colors.sidebar, dark ? 20 : 35, dark ? '#202629' : '#ffffff')})`,
                '--sf-control-wash': `linear-gradient(135deg, var(--sf-field), ${mix(colors.buttons, dark ? 24 : 16, 'var(--sf-field)')})`,
                '--sf-primary-wash': `linear-gradient(115deg, ${primary}, ${mix(primary, 82, primaryText === '#000000' ? '#ffffff' : '#000000')})`,
                '--sf-title-wash': 'linear-gradient(100deg, var(--sf-accent), var(--sf-text))',
                '--sf-shadow': '0 8px 24px rgba(0, 0, 0, 0.12)',
                '--sf-header': colors.headers,
                '--sf-header-text': textOn(colors.headers),
            };
            Object.entries(variables).forEach(([property, value]) => root.style.setProperty(property, value));
        }
        colorInputs.forEach(input => { input.value = colors[input.dataset.paletteRole]; });
        swatches.forEach((swatch, index) => swatch.setAttribute('aria-pressed',
            String(Boolean(palette) && [...roles, 'sidebar'].every(role => palette[role] === colorsOf(presets[index])[role]))));
    };
    const apply = (theme, persist=false) => {
        root.dataset.sfTheme = theme;
        modes.forEach(mode => mode.setAttribute('aria-pressed', String(mode.dataset.theme === theme)));
        button.querySelector('span').textContent = theme === 'dark' ? 'light_mode' : 'dark_mode';
        applyPalette();
        if (persist) save(storageKey, theme);
        window.dispatchEvent(new CustomEvent('sf-theme-change', {detail:theme}));
    };
    const saved = read(storageKey);
    apply(saved === 'light' || saved === 'dark' ? saved : (data?.theme || 'dark'));
    button.popoverTargetElement = panel;
    button.popoverTargetAction = 'toggle';
    listen(panel, 'toggle', () => button.setAttribute('aria-expanded', String(panel.matches(':popover-open'))));
    modes.forEach(mode => listen(mode, 'click', () => apply(mode.dataset.theme, true)));
    const setPalette = colors => {
        palette = colors;
        save(paletteKey, palette ? JSON.stringify(palette) : null);
        save(accentKey, null);
        applyPalette();
    };
    swatches.forEach((swatch, index) => listen(swatch, 'click', () => setPalette(colorsOf(presets[index]))));
    colorInputs.forEach(input => listen(input, 'input', () => setPalette({
        ...(palette || colorsOf(presets[0])), [input.dataset.paletteRole]:input.value,
    })));
    listen(control.querySelector('.sf-theme-reset'), 'click', () => setPalette(null));
    return () => { listeners.abort(); control.remove(); };
}
"""

_theme_control = st.components.v2.component(
    'sf_color_theme',
    html='''<div class="sf-theme-control sf-theme-template">
        <button class="sf-theme-toggle" type="button" aria-label="Theme settings" title="Theme settings"
            aria-expanded="false" aria-controls="sf-theme-panel" aria-haspopup="dialog"><span aria-hidden="true"></span></button>
        <div class="sf-theme-panel" popover="auto" role="dialog" aria-label="Theme settings">
            <h2>Appearance</h2>
            <div class="sf-theme-modes" role="group" aria-label="Color mode">
                <button type="button" data-theme="light">Light</button>
                <button type="button" data-theme="dark">Dark</button>
            </div>
            <div>Color palettes</div>
            <div class="sf-theme-palettes" role="group" aria-label="Color palettes"></div>
            <label class="sf-theme-custom">Background <input type="color" data-palette-role="background" aria-label="Background color"></label>
            <label class="sf-theme-custom">Sidebar background <input type="color" data-palette-role="sidebar" aria-label="Sidebar background color"></label>
            <label class="sf-theme-custom">Panels <input type="color" data-palette-role="panels" aria-label="Panel color"></label>
            <label class="sf-theme-custom">Headers <input type="color" data-palette-role="headers" aria-label="Header color"></label>
            <label class="sf-theme-custom">Buttons <input type="color" data-palette-role="buttons" aria-label="Button color"></label>
            <button class="sf-theme-reset" type="button">Reset palette</button>
        </div>
    </div>''',
    css=THEME_CSS,
    js=THEME_JS,
    isolate_styles=False,
)


_theme_picker = st.components.v2.component(
    'sf_theme_picker',
    html='''<div class="sf-theme-picker" aria-label="Choose application theme">
        <div class="sf-theme-picker-label">Appearance</div>
        <div class="sf-theme-picker-actions">
            <button type="button" data-theme="light">Light</button>
            <button type="button" data-theme="dark">Dark</button>
        </div>
    </div>''',
    css='''
        .sf-theme-picker { display:grid; gap:7px; padding:9px 0 12px; }
        .sf-theme-picker-label { color:var(--sf-text); font-size:.72rem; font-weight:700; letter-spacing:.04em; }
        .sf-theme-picker-actions { display:grid; grid-template-columns:1fr 1fr; gap:6px; }
        .sf-theme-picker button { border:1px solid var(--sf-border); border-radius:7px; padding:7px 5px;
            background:var(--sf-field); color:var(--sf-text); cursor:pointer; font:inherit; font-size:.72rem; }
        .sf-theme-picker button[aria-pressed="true"] { background:var(--sf-selected); border-color:var(--sf-accent); font-weight:700; }
        .sf-theme-picker button:focus-visible { outline:2px solid var(--sf-accent); outline-offset:2px; }
    ''',
    js='''
export default function(component) {
    const root = component.parentElement;
    const buttons = [...root.querySelectorAll('button[data-theme]')];
    const storageKey = 'sf_bulk_color_theme_v2';
    const apply = (theme, persist=false) => {
        document.documentElement.dataset.sfTheme = theme;
        buttons.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.theme === theme)));
        if (persist) {
            try { localStorage.setItem(storageKey, theme); } catch (error) {}
            window.dispatchEvent(new CustomEvent('sf-theme-change', {detail: theme}));
        }
    };
    let saved = null;
    try { saved = localStorage.getItem(storageKey); } catch (error) {}
    apply(saved === 'dark' || saved === 'light' ? saved : (document.documentElement.dataset.sfTheme || 'light'));
    const handlers = buttons.map(button => {
        const handler = () => apply(button.dataset.theme, true);
        button.addEventListener('click', handler);
        return [button, handler];
    });
    const sync = event => apply(event.detail || document.documentElement.dataset.sfTheme || 'light');
    window.addEventListener('sf-theme-change', sync);
    return () => {
        handlers.forEach(([button, handler]) => button.removeEventListener('click', handler));
        window.removeEventListener('sf-theme-change', sync);
    };
}
''',
    isolate_styles=False,
)


def render_theme_control():
    _theme_control(data={'theme': 'light'}, key='sf_theme_control', height=38)


def render_theme_picker():
    _theme_picker(key='sf_theme_picker', height=74)


_startup_splash = st.components.v2.component(
    'tavant_migration_splash',
    html='''<dialog class="sf-splash" aria-labelledby="sf-splash-title">
        <button class="sf-splash-close" aria-label="Close" type="button">✕</button>
        <div class="sf-splash-mark" aria-hidden="true">T</div>
        <div class="sf-splash-title" id="sf-splash-title">TAVANT DATA MIGRATION</div>
        <div class="sf-splash-subtitle">Salesforce and Snowflake data operations</div>
    </dialog>''',
    css='''
        dialog.sf-splash { position:fixed; inset:0; margin:0; padding:24px; border:0;
            width:100vw; height:100dvh; max-width:none; max-height:none; box-sizing:border-box;
            background:#f4faf9 !important; color:#17212b; font-family:inherit; }
        dialog.sf-splash[open] { display:flex; flex-direction:column; align-items:center;
            justify-content:center; gap:16px; }
        dialog.sf-splash::backdrop { background:#f4faf9; }
        .sf-splash-close { position:absolute; top:20px; right:20px; width:40px; height:40px;
            border:none; background:rgba(0,0,0,0.1); border-radius:50%; cursor:pointer;
            font-size:24px; color:#17212b; display:grid; place-items:center;
            z-index:1; }
        .sf-splash-close:hover { background:rgba(0,0,0,0.2); }
        .sf-splash-close:focus-visible { outline:2px solid #0f766e; outline-offset:2px; }
        .sf-splash-mark { width:64px; height:64px; display:grid; place-items:center; border-radius:18px;
            background:linear-gradient(135deg,#5eead4,#60a5fa 55%,#fb8b73); color:#10232b;
            font-size:2.2rem; font-weight:900; box-shadow:0 14px 45px rgba(94,234,212,.25); }
        .sf-splash-title { max-width:100%; text-align:center; font-size:2.5rem;
            font-weight:900; letter-spacing:0; line-height:1.2; color:#0f766e;
            overflow-wrap:anywhere; }
        .sf-splash-subtitle { color:#526071; font-size:.9rem; text-align:center; }
        @media (max-width:640px) { .sf-splash-title { font-size:1.75rem; } }
    ''',
    js='''
export default function(component) {
    const template = component.parentElement.querySelector('.sf-splash');
    if (!template || window.__sfSplashDismissed) return;
    const root = template.cloneNode(true);
    root.setAttribute('aria-labelledby', 'sf-startup-overlay-title');
    root.querySelector('.sf-splash-title').id = 'sf-startup-overlay-title';
    document.body.appendChild(root);
    const closeBtn = root.querySelector('.sf-splash-close');
    const hide = () => {
        window.clearTimeout(timer);
        window.__sfSplashDismissed = true;
        root.close();
        root.remove();
    };
    const cancel = event => { event.preventDefault(); hide(); };
    root.showModal();
    const timer = window.setTimeout(hide, 2000);
    closeBtn.addEventListener('click', hide);
    root.addEventListener('cancel', cancel);
    return () => {
        window.clearTimeout(timer);
        closeBtn.removeEventListener('click', hide);
        root.removeEventListener('cancel', cancel);
        if (root.open) root.close();
        root.remove();
    };
}
''',
    isolate_styles=False,
)


def render_startup_splash():
    _startup_splash(key='tavant_migration_splash', height=1)


_popup_modal = st.components.v2.component(
    'sf_fullscreen_popup',
    html='''<div class="sf-popup-overlay" role="alertdialog" aria-modal="true">
        <div class="sf-popup-modal">
            <div class="sf-popup-icon" id="sf-popup-icon"></div>
            <div class="sf-popup-title" id="sf-popup-title"></div>
            <div class="sf-popup-message" id="sf-popup-message"></div>
            <button class="sf-popup-close" id="sf-popup-close" type="button">Close</button>
        </div>
    </div>''',
    css='',
    js='''
export default function(component) {
    const overlay = component.parentElement.querySelector('.sf-popup-overlay');
    const title = component.parentElement.querySelector('#sf-popup-title');
    const message = component.parentElement.querySelector('#sf-popup-message');
    const icon = component.parentElement.querySelector('#sf-popup-icon');
    const closeBtn = component.parentElement.querySelector('#sf-popup-close');
    const params = new URLSearchParams(window.location.search);
    
    if (window.Streamlit) {
        window.Streamlit.setComponentValue(true);
    }
    
    const hide = () => {
        overlay.classList.add('is-hidden');
    };
    closeBtn.addEventListener('click', hide);
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') hide();
    });
    return () => {
        closeBtn.removeEventListener('click', hide);
    };
}
''',
    isolate_styles=False,
)


def show_fullscreen_popup(message, status='success', title=None, icon=None, auto_close_ms=None):
    """Display a full-screen centered popup overlay.
    
    Args:
        message: Main message text to display
        status: 'success' (green) or 'error' (red)
        title: Optional title (defaults to 'Success' or 'Error')
        icon: Optional emoji/icon (defaults to ✓ or ✕)
        auto_close_ms: Optional auto-close delay in milliseconds
    """
    if title is None:
        title = 'Success!' if status == 'success' else 'Error'
    if icon is None:
        icon = '✓' if status == 'success' else '✕'
    
    # Inject CSS for the popup state
    status_class = 'sf-popup-success' if status == 'success' else 'sf-popup-error'
    css_inject = f'''
    <style>
        .sf-popup-modal {{ {status_class} }}
        #{icon} {{ color: {'#10b981' if status == 'success' else '#ef4444'}; }}
    </style>
    '''
    
    components.html(f'''
    {css_inject}
    <div class="sf-popup-overlay">
        <div class="sf-popup-modal {status_class}">
            <div class="sf-popup-icon">{icon}</div>
            <div class="sf-popup-title">{title}</div>
            <div class="sf-popup-message">{message}</div>
            <button class="sf-popup-close" onclick="this.closest('.sf-popup-overlay').classList.add('is-hidden')" type="button">Close</button>
        </div>
    </div>
    <script>
        (function() {{
            const overlay = document.querySelector('.sf-popup-overlay');
            if (overlay) {{
                const closeBtn = overlay.querySelector('.sf-popup-close');
                const hide = () => overlay.classList.add('is-hidden');
                closeBtn.addEventListener('click', hide);
                document.addEventListener('keydown', (e) => {{
                    if (e.key === 'Escape' && !overlay.classList.contains('is-hidden')) hide();
                }});
                {f'setTimeout(hide, {auto_close_ms});' if auto_close_ms else ''}
            }}
        }})();
    </script>
    ''', height=600, unsafe_allow_html=True)