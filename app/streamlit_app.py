"""
Lestrade demo UI — MiniLM retrieval + Ollama chat.

Run (from repo root, with [ui] extras and env set):

  export LESTRADE_CHROMA_PERSIST_DIR=./data/chroma
  # optional: same Supabase URI as Airflow
  export LESTRADE_DATABASE_URL="$AIRFLOW_CONN_LESTRADE_RDS"
  export LESTRADE_OLLAMA_MODEL=llama3.2
  uv run --extra ui streamlit run app/streamlit_app.py
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import streamlit as st

# Load .env from repo root if present (no dependency on python-dotenv).
_ROOT = Path(__file__).resolve().parents[1]
_env = _ROOT / ".env"
if _env.is_file():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

# Host Streamlit: remap container Chroma path to the Compose bind mount.
_host_chroma = _ROOT / "data" / "chroma"
_chroma_env = (os.environ.get("LESTRADE_CHROMA_PERSIST_DIR") or "").strip()
if _chroma_env in ("", "/opt/airflow/chroma") and _host_chroma.is_dir():
    os.environ["LESTRADE_CHROMA_PERSIST_DIR"] = str(_host_chroma)


st.set_page_config(
    page_title="Lestrade",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.5rem; max-width: 1100px; }
      h1 { font-family: "Iowan Old Style", "Palatino Linotype", Palatino, serif; letter-spacing: -0.02em; }
      .disclaimer { color: #5c5c5c; font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def _ollama_ok() -> bool:
    from lestrade_rag.ollama_client import ping

    return ping()


def _load_universe():
    from lestrade_rag.universe_ui import connect, list_trading_dates

    try:
        conn = connect()
    except Exception as e:
        return None, None, str(e)
    try:
        dates = list_trading_dates(conn)
        return conn, dates, None
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return None, None, str(e)


def _tickers_for_session(conn, trading_date, universe_group=None):
    from lestrade_rag.universe_ui import tickers_for_session

    return tickers_for_session(conn, trading_date, universe_group=universe_group)


st.title("Lestrade")
st.caption("Form 4 insider filings · highs/lows universe · MiniLM retrieval · Ollama chat")
st.markdown(
    '<p class="disclaimer">Research demo only — not investment advice. Answers are grounded in retrieved SEC Form 4 excerpts.</p>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Filters")
    chroma = (os.environ.get("LESTRADE_CHROMA_PERSIST_DIR") or "").strip()
    if not chroma:
        st.error("Set LESTRADE_CHROMA_PERSIST_DIR (e.g. ./data/chroma)")
        st.stop()
    st.caption(f"Chroma: `{chroma}`")

    ollama_model = st.text_input(
        "Ollama model",
        value=os.environ.get("LESTRADE_OLLAMA_MODEL") or "llama3.2",
    )
    if _ollama_ok():
        st.success("Ollama reachable")
    else:
        st.warning("Ollama not reachable at LESTRADE_OLLAMA_BASE_URL (default http://127.0.0.1:11434)")

    conn, dates, uni_err = _load_universe()
    trading_date: date | None = None
    ticker_choice = "(any)"
    group_choice = "(any)"
    if uni_err:
        st.info(f"Universe list unavailable (chat still works): {uni_err[:180]}")
    elif dates:
        date_labels = [d.isoformat() for d in dates]
        picked = st.selectbox("Universe session", date_labels, index=0)
        trading_date = date.fromisoformat(picked)
        group_choice = st.selectbox("List", ["(any)", "high", "low"], index=0)
        ug = None if group_choice == "(any)" else group_choice
        rows = _tickers_for_session(conn, trading_date, universe_group=ug) if conn else []
        tickers = sorted({t for t, _ in rows})
        ticker_choice = st.selectbox("Ticker", ["(any)"] + tickers, index=0)
    else:
        st.caption("No universe_snapshot rows yet.")

    side = st.selectbox("Buy / sell", ["(any)", "A (acquire)", "D (dispose)"], index=0)
    n_results = st.slider("Chunks to retrieve", 3, 12, 6)
    if st.button("Clear chat"):
        st.session_state.messages = []
        st.session_state.history = []

if "messages" not in st.session_state:
    st.session_state.messages = []
if "history" not in st.session_state:
    st.session_state.history = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sources"):
            with st.expander("Sources"):
                for s in msg["sources"]:
                    st.markdown(s)

prompt = st.chat_input("Ask about insider trades (e.g. CEO open-market purchases)")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    ticker = None if ticker_choice == "(any)" else ticker_choice
    ug = None if group_choice == "(any)" else group_choice
    ad = None
    if side.startswith("A"):
        ad = "A"
    elif side.startswith("D"):
        ad = "D"

    with st.chat_message("assistant"):
        with st.spinner("Retrieving filings and asking Ollama…"):
            try:
                from lestrade_rag.chat import answer_question

                result = answer_question(
                    prompt,
                    ticker=ticker,
                    acquired_disposed=ad,
                    universe_group=ug,
                    n_results=n_results,
                    ollama_model=ollama_model.strip() or None,
                    history=st.session_state.history,
                )
                answer = result.answer
                sources = []
                for ch in result.chunks:
                    meta = ch.metadata
                    line = (
                        f"- `{meta.get('accession_number')}` · "
                        f"{meta.get('issuer_ticker')} · "
                        f"{meta.get('insider_name') or 'insider'} · "
                        f"code {meta.get('transaction_code')}"
                    )
                    if ch.edgar_url:
                        line += f" · [EDGAR]({ch.edgar_url})"
                    line += f"\n  _{ch.text[:220]}{'…' if len(ch.text) > 220 else ''}_"
                    sources.append(line)
            except Exception as e:
                answer = f"Error: {e}"
                sources = []

        st.markdown(answer)
        if sources:
            with st.expander("Sources", expanded=True):
                for s in sources:
                    st.markdown(s)

    st.session_state.messages.append(
        {"role": "assistant", "content": answer, "sources": sources}
    )
    st.session_state.history.append({"role": "user", "content": prompt})
    st.session_state.history.append({"role": "assistant", "content": answer})

if conn is not None:
    try:
        conn.close()
    except Exception:
        pass
