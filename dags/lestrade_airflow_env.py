"""
Resolve Lestrade / EDGAR settings from process env first, then Airflow Variables.

MWAA often exposes only Airflow configuration keys on the environment; custom
values are commonly stored under Admin → Variables instead.
"""

from __future__ import annotations

import os


def _variable_get(key: str, default: str = "") -> str:
    try:
        from airflow.sdk import Variable
    except ImportError:
        from airflow.models.variable import Variable

        raw = Variable.get(key, default_var=default)
        if raw is None:
            return ""
        return str(raw).strip()

    raw = Variable.get(key, default=None)
    if raw is None:
        return default
    return str(raw).strip()


def str_from_env_or_variable(
    env_key: str,
    variable_key: str,
    *,
    default: str | None = None,
    use_airflow_variable: bool = True,
) -> str:
    v = (os.environ.get(env_key) or "").strip()
    if v:
        return v
    vv = ""
    if use_airflow_variable:
        vv = _variable_get(variable_key)
    if vv:
        return vv
    if default is not None:
        return default
    return ""


def optional_edgar_user_agent_override() -> str | None:
    """If set, replaces the default SEC User-Agent string (env or Variable)."""
    v = str_from_env_or_variable("EDGAR_USER_AGENT", "edgar_user_agent", default="")
    v = (v or "").strip()
    return v or None


def edgar_user_agent_from_env_or_variables() -> tuple[str, str]:
    app = str_from_env_or_variable("EDGAR_APP_NAME", "edgar_app_name")
    email = str_from_env_or_variable("EDGAR_CONTACT_EMAIL", "edgar_contact_email")
    if not app or not email:
        raise ValueError(
            "Set EDGAR_APP_NAME / EDGAR_CONTACT_EMAIL on workers, or Airflow Variables "
            "edgar_app_name / edgar_contact_email (Admin → Variables)."
        )
    return app, email


def postgres_conn_id_from_env_or_variable() -> str:
    """
    Airflow Connection id for PostgresHook.

    Uses env ``LESTRADE_PG_CONN`` only; defaults to ``lestrade_rds``.
    We intentionally do **not** fall back to an Airflow Variable here: on Airflow 3
    the Variable API logs ERROR (404) for missing keys even when a default exists,
    which spams worker logs on every task run.
    """
    cid = str_from_env_or_variable(
        "LESTRADE_PG_CONN",
        "lestrade_pg_conn",
        default="lestrade_rds",
        use_airflow_variable=False,
    )
    return cid or "lestrade_rds"
