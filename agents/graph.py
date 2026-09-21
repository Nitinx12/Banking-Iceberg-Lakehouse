"""agents/graph.py - LangGraph StateGraph on Gold serving layer (ADR 007).

Flow: classify -> generate_sql -> guard+execute -> synthesize
Uses ChatOpenAI (openai) via langchain-openai; falls back to clear error if OPENAI_API_KEY missing.
All SQL goes through agents/tools/sql_tool.py guard (serving.* read-only, HMAC masked).
"""

from __future__ import annotations

import os
from typing import TypedDict

import pandas as pd

from agents.config import OPENAI_API_KEY, OPENAI_MODEL
from agents.tools.sql_tool import exec_sql_guarded

SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "gold_system.md")


def _system_prompt() -> str:
    try:
        with open(SYSTEM_PROMPT_PATH, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "You are a helpful Gold layer analyst. Only use serving.* via tools."


class AgentState(TypedDict, total=False):
    question: str
    intent: str
    sql: str
    rows: list[dict]
    row_count: int
    error: str
    answer: str


def _get_llm():
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY missing - add it to .env (see .env.example: OPENAI_API_KEY=) and restart; .env is gitignored"
        )
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(model=OPENAI_MODEL, temperature=0, api_key=OPENAI_API_KEY)


# --- nodes ---


def classify(state: AgentState) -> AgentState:
    q = state["question"].lower()
    if "customer" in q and any(k in q for k in ["360", "profile", "cust"]):
        state["intent"] = "customer_360"
    elif any(k in q for k in ["dq", "quality", "freshness", "pipeline"]):
        state["intent"] = "ops"
    else:
        state["intent"] = "analytics"
    return state


def generate_sql(state: AgentState) -> AgentState:
    """LLM turns NL into guarded SELECT over serving.*"""
    llm = _get_llm()
    sys_prompt = _system_prompt()
    # few-shot guard in prompt already lists allowed tables
    user = (
        f"Question: {state['question']}\nIntent: {state['intent']}\n"
        "Return ONLY a single SELECT SQL (Postgres dialect) over allowed tables. No explanation."
    )
    resp = llm.invoke(
        [{"role": "system", "content": sys_prompt}, {"role": "user", "content": user}]
    )
    sql = resp.content.strip()
    # strip ```sql fences if model wraps
    if "```" in sql:
        sql = sql.split("```")[1]
        sql = sql.replace("sql", "", 1).strip()
    state["sql"] = sql.strip().rstrip(";")
    return state


def execute_sql(state: AgentState) -> AgentState:
    sql = state.get("sql", "")
    if not sql:
        state["error"] = "No SQL generated"
        return state
    try:
        df: pd.DataFrame = exec_sql_guarded(sql)
        state["rows"] = df.to_dict(orient="records")
        state["row_count"] = len(df)
        # keep preview small for LLM context
        state["_df_preview"] = df.head(20).to_csv(index=False)  # type: ignore
    except Exception as e:
        state["error"] = str(e)
        state["rows"] = []
        state["row_count"] = 0
    return state


def synthesize(state: AgentState) -> AgentState:
    """LLM synthesizes grounded answer from rows; if no LLM, return tabular."""
    if state.get("error"):
        state["answer"] = (
            f"Query blocked or failed: {state['error']}\nSQL was: {state.get('sql', '')}"
        )
        return state
    if not state.get("rows"):
        state["answer"] = f"No rows for that question. SQL: {state.get('sql', '')}"
        return state
    try:
        llm = _get_llm()
        preview = state.get("_df_preview", str(state["rows"][:5]))
        prompt = (
            f"Question: {state['question']}\nSQL: {state['sql']}\nRows (csv, max 20):\n{preview}\n"
            f"Total rows: {state['row_count']}\n"
            "Write a concise answer grounded in the rows. Cite the SQL. If data looks stale, mention it."
        )
        resp = llm.invoke(
            [{"role": "system", "content": _system_prompt()}, {"role": "user", "content": prompt}]
        )
        state["answer"] = resp.content
    except Exception:
        # fallback without LLM synthesis - still grounded
        state["answer"] = (
            f"Found {state['row_count']} rows. SQL: {state['sql']}\nPreview:\n{state.get('_df_preview', '')}"
        )
    return state


def build_graph():
    from langgraph.graph import END, StateGraph

    g = StateGraph(AgentState)
    g.add_node("classify", classify)
    g.add_node("generate_sql", generate_sql)
    g.add_node("execute_sql", execute_sql)
    g.add_node("synthesize", synthesize)
    g.set_entry_point("classify")
    g.add_edge("classify", "generate_sql")
    g.add_edge("generate_sql", "execute_sql")
    g.add_edge("execute_sql", "synthesize")
    g.add_edge("synthesize", END)
    return g.compile()


# convenience for streamlit / scripts
_graph = None


def ask(question: str) -> dict:
    """Run the graph for a single question. Returns {answer, sql, rows, row_count, error}."""
    global _graph
    if _graph is None:
        _graph = build_graph()
    result: AgentState = _graph.invoke({"question": question})
    return {
        "answer": result.get("answer", ""),
        "sql": result.get("sql", ""),
        "rows": result.get("rows", []),
        "row_count": result.get("row_count", 0),
        "error": result.get("error", ""),
    }
