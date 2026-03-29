"""
crm_backend.py - Backend selection and Postgres persistence helpers.
"""

from __future__ import annotations

import csv
import json
import os
import re
import atexit
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from crm_fields import ALL_COLUMNS


ROOT_DIR = Path(__file__).resolve().parent
CAMPAIGNS_DIR = ROOT_DIR / "campaigns"
REGISTRY_PATH = CAMPAIGNS_DIR / "registry.json"

_POSTGRES_BOOTSTRAP_CHECKED = False
_POSTGRES_SCHEMA_READY = False
_POSTGRES_STATE_LOCK = threading.RLock()
_POSTGRES_POOL_COND = threading.Condition(_POSTGRES_STATE_LOCK)
_POSTGRES_POOL: list[Any] = []
_POSTGRES_POOL_SIZE = 0
_POSTGRES_POOL_URL = ""
POSTGRES_SCHEMA_VERSION = 1

CAMPAIGN_COLUMNS = [
    "id",
    "label",
    "keyword",
    "location",
    "id_prefix",
    "rank_keyword_template",
    "price_default",
    "price_monthly",
    "turnaround_days",
    "sender_name",
    "sender_company",
    "sender_website",
    "sender_phone",
    "sender_email",
    "offer_summary",
    "example_intro",
    "service_singular",
    "service_plural",
    "config_version",
    "draft_config_version",
    "research_config_version",
    "last_scraped_at",
    "last_migrated_at",
    "last_enriched_at",
    "last_researched_at",
    "last_analyzed_at",
    "csv_path",
]

LEAD_QUEUE_FIELDS = {
    "Approved_At",
    "Scheduled_Send_At",
    "Scheduled_Send_Channel",
    "Scheduled_Send_Status",
    "Scheduled_Send_Error",
    "Scheduled_Send_Attempts",
    "Sent_At",
    "SMTP_Message_ID",
}


def backend_name() -> str:
    return (os.getenv("CRM_BACKEND") or "csv").strip().lower() or "csv"


def is_postgres_backend() -> bool:
    return backend_name() == "postgres"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _app_meta_value(row: dict[str, Any] | None) -> Any:
    if not row:
        return None
    value = row.get("value_json")
    if isinstance(value, dict):
        return value.get("value")
    return value


def _app_meta_int(row: dict[str, Any] | None, default: int = 0) -> int:
    value = _app_meta_value(row)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Postgres backend requires `psycopg[binary]`. Install dependencies from requirements.txt."
        ) from exc
    return psycopg, dict_row, Jsonb


def _database_url() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is required when CRM_BACKEND=postgres")
    return url


def _postgres_pool_max_size() -> int:
    raw = (os.getenv("CRM_POSTGRES_POOL_SIZE") or "").strip()
    if not raw:
        return 3
    try:
        return max(1, int(raw))
    except ValueError:
        return 3


def _close_postgres_connection(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _reset_postgres_pool_locked() -> None:
    global _POSTGRES_POOL_SIZE, _POSTGRES_POOL_URL
    while _POSTGRES_POOL:
        _close_postgres_connection(_POSTGRES_POOL.pop())
    _POSTGRES_POOL_SIZE = 0
    _POSTGRES_POOL_URL = ""


def _acquire_postgres_connection() -> tuple[Any, str]:
    global _POSTGRES_POOL_SIZE, _POSTGRES_POOL_URL

    database_url = _database_url()
    psycopg, dict_row, _ = _load_psycopg()
    with _POSTGRES_POOL_COND:
        if _POSTGRES_POOL_URL and _POSTGRES_POOL_URL != database_url:
            _reset_postgres_pool_locked()
        _POSTGRES_POOL_URL = database_url

        while True:
            while _POSTGRES_POOL:
                conn = _POSTGRES_POOL.pop()
                if getattr(conn, "closed", False):
                    _POSTGRES_POOL_SIZE = max(0, _POSTGRES_POOL_SIZE - 1)
                    continue
                return conn, database_url

            if _POSTGRES_POOL_SIZE < _postgres_pool_max_size():
                _POSTGRES_POOL_SIZE += 1
                break

            _POSTGRES_POOL_COND.wait(timeout=5.0)

    try:
        conn = psycopg.connect(database_url, row_factory=dict_row)
    except Exception:
        with _POSTGRES_POOL_COND:
            _POSTGRES_POOL_SIZE = max(0, _POSTGRES_POOL_SIZE - 1)
            _POSTGRES_POOL_COND.notify()
        raise
    return conn, database_url


def _release_postgres_connection(conn: Any, database_url: str, *, discard: bool = False) -> None:
    global _POSTGRES_POOL_SIZE

    if discard or getattr(conn, "closed", False):
        _close_postgres_connection(conn)
        with _POSTGRES_POOL_COND:
            _POSTGRES_POOL_SIZE = max(0, _POSTGRES_POOL_SIZE - 1)
            _POSTGRES_POOL_COND.notify()
        return

    with _POSTGRES_POOL_COND:
        if _POSTGRES_POOL_URL != database_url or len(_POSTGRES_POOL) >= _postgres_pool_max_size():
            _POSTGRES_POOL_SIZE = max(0, _POSTGRES_POOL_SIZE - 1)
            _POSTGRES_POOL_COND.notify()
            should_close = True
        else:
            _POSTGRES_POOL.append(conn)
            _POSTGRES_POOL_COND.notify()
            should_close = False

    if should_close:
        _close_postgres_connection(conn)


def _reset_postgres_pool() -> None:
    with _POSTGRES_POOL_COND:
        _reset_postgres_pool_locked()


atexit.register(_reset_postgres_pool)


@contextmanager
def postgres_connection() -> Iterator[Any]:
    conn, database_url = _acquire_postgres_connection()
    discard = False
    try:
        yield conn
        conn.commit()
    except Exception:
        discard = True
        try:
            conn.rollback()
        except Exception:
            discard = True
        raise
    finally:
        _release_postgres_connection(conn, database_url, discard=discard)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _slugify(value: str) -> str:
    text = (value or "").strip().lower()
    for old, new in [("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")]:
        text = text.replace(old, new)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _campaign_layout(campaign_id: str) -> dict[str, Path]:
    campaign_dir = CAMPAIGNS_DIR / campaign_id
    return {
        "campaign_dir": campaign_dir,
        "csv_path": campaign_dir / "leads.csv",
        "hooks_library_path": campaign_dir / "hooks_library.json",
        "template_overrides_path": campaign_dir / "template_overrides.json",
    }


def _campaign_defaults(config: dict[str, Any]) -> dict[str, Any]:
    campaign_id = config.get("id") or _slugify(f"{config.get('keyword', '')}_{config.get('location', '')}") or "campaign"
    layout = _campaign_layout(campaign_id)
    return {
        "id": campaign_id,
        "label": config.get("label") or f"{config.get('keyword', '')} {config.get('location', '')}".strip() or campaign_id,
        "keyword": str(config.get("keyword") or "").strip(),
        "location": str(config.get("location") or "").strip(),
        "id_prefix": str(config.get("id_prefix") or "LEAD").strip(),
        "rank_keyword_template": str(config.get("rank_keyword_template") or "{keyword} {plz}").strip(),
        "price_default": str(config.get("price_default") or "500").strip(),
        "price_monthly": str(config.get("price_monthly") or "25").strip(),
        "turnaround_days": _safe_int(config.get("turnaround_days"), 14) or 14,
        "sender_name": str(config.get("sender_name") or os.getenv("SENDER_NAME", "Linus")).strip(),
        "sender_company": str(config.get("sender_company") or os.getenv("SENDER_COMPANY", "Digitalagentur")).strip(),
        "sender_website": str(config.get("sender_website") or os.getenv("SENDER_WEBSITE", "")).strip(),
        "sender_phone": str(config.get("sender_phone") or os.getenv("SENDER_PHONE", "")).strip(),
        "sender_email": str(config.get("sender_email") or os.getenv("SENDER_EMAIL", "")).strip(),
        "offer_summary": str(config.get("offer_summary") or "").strip(),
        "example_intro": str(config.get("example_intro") or "").strip(),
        "service_singular": str(config.get("service_singular") or config.get("keyword") or "").strip(),
        "service_plural": str(config.get("service_plural") or "").strip(),
        "config_version": _safe_int(config.get("config_version"), 1) or 1,
        "draft_config_version": _safe_int(config.get("draft_config_version"), _safe_int(config.get("config_version"), 1)) or 1,
        "research_config_version": _safe_int(config.get("research_config_version"), _safe_int(config.get("config_version"), 1)) or 1,
        "last_scraped_at": str(config.get("last_scraped_at") or "").strip(),
        "last_migrated_at": str(config.get("last_migrated_at") or "").strip(),
        "last_enriched_at": str(config.get("last_enriched_at") or "").strip(),
        "last_researched_at": str(config.get("last_researched_at") or "").strip(),
        "last_analyzed_at": str(config.get("last_analyzed_at") or "").strip(),
        "csv_path": str(config.get("csv_path") or layout["csv_path"].relative_to(ROOT_DIR)),
        "hooks_library_json": config.get("hooks_library_json") if isinstance(config.get("hooks_library_json"), dict) else {},
        "template_overrides_json": config.get("template_overrides_json") if isinstance(config.get("template_overrides_json"), dict) else {},
        "hooks_library_path": str(config.get("hooks_library_path") or layout["hooks_library_path"].relative_to(ROOT_DIR)),
        "template_overrides_path": str(config.get("template_overrides_path") or layout["template_overrides_path"].relative_to(ROOT_DIR)),
    }


def _upsert_app_meta_value(cur: Any, key: str, value: Any, Jsonb: Any) -> None:
    cur.execute(
        """
        insert into app_meta (key, value_json, updated_at)
        values (%s, %s, now())
        on conflict (key) do update
        set value_json = excluded.value_json,
            updated_at = now()
        """,
        (key, Jsonb({"value": value})),
    )


def ensure_postgres_schema() -> None:
    global _POSTGRES_SCHEMA_READY
    if _POSTGRES_SCHEMA_READY:
        return

    with _POSTGRES_STATE_LOCK:
        if _POSTGRES_SCHEMA_READY:
            return
        _, _, Jsonb = _load_psycopg()
        with postgres_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists app_meta (
                    key text primary key,
                    value_json jsonb not null default '{}'::jsonb,
                    updated_at timestamptz not null default now()
                )
                """
            )
            cur.execute("select value_json from app_meta where key = 'schema_version'")
            current_version = _app_meta_int(cur.fetchone())
            if current_version < POSTGRES_SCHEMA_VERSION:
                cur.execute(
                    """
                    create table if not exists campaigns (
                        id text primary key,
                        label text not null default '',
                        keyword text not null default '',
                        location text not null default '',
                        id_prefix text not null default '',
                        rank_keyword_template text not null default '{keyword} {plz}',
                        price_default text not null default '',
                        price_monthly text not null default '',
                        turnaround_days integer not null default 14,
                        sender_name text not null default '',
                        sender_company text not null default '',
                        sender_website text not null default '',
                        sender_phone text not null default '',
                        sender_email text not null default '',
                        offer_summary text not null default '',
                        example_intro text not null default '',
                        service_singular text not null default '',
                        service_plural text not null default '',
                        config_version integer not null default 1,
                        draft_config_version integer not null default 1,
                        research_config_version integer not null default 1,
                        last_scraped_at text not null default '',
                        last_migrated_at text not null default '',
                        last_enriched_at text not null default '',
                        last_researched_at text not null default '',
                        last_analyzed_at text not null default '',
                        csv_path text not null default '',
                        hooks_library_json jsonb not null default '{}'::jsonb,
                        template_overrides_json jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now()
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists leads (
                        campaign_id text not null references campaigns(id) on delete cascade,
                        lead_id text not null,
                        payload jsonb not null default '{}'::jsonb,
                        status text not null default 'new',
                        priority integer not null default 5,
                        next_action_date date,
                        scheduled_send_at timestamptz,
                        scheduled_send_channel text not null default '',
                        scheduled_send_status text not null default '',
                        scheduled_send_error text not null default '',
                        scheduled_send_attempts integer not null default 0,
                        approved_at timestamptz,
                        sent_at timestamptz,
                        smtp_message_id text not null default '',
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now(),
                        primary key (campaign_id, lead_id)
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists contact_events (
                        id bigserial primary key,
                        campaign_id text not null references campaigns(id) on delete cascade,
                        lead_id text not null,
                        occurred_at timestamptz not null default now(),
                        channel text not null default '',
                        outcome text not null default '',
                        notes text not null default ''
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists scheduled_sends (
                        campaign_id text not null references campaigns(id) on delete cascade,
                        lead_id text not null,
                        channel text not null default 'email',
                        scheduled_at timestamptz not null,
                        status text not null default 'queued',
                        last_error text not null default '',
                        attempts integer not null default 0,
                        approved_at timestamptz,
                        sent_at timestamptz,
                        smtp_message_id text not null default '',
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now(),
                        primary key (campaign_id, lead_id, channel)
                    )
                    """
                )
                cur.execute("create index if not exists idx_leads_campaign_status on leads (campaign_id, status)")
                cur.execute("create index if not exists idx_leads_campaign_priority on leads (campaign_id, priority)")
                cur.execute("create index if not exists idx_leads_campaign_next_action_date on leads (campaign_id, next_action_date)")
                cur.execute("create index if not exists idx_leads_scheduled_status_at on leads (scheduled_send_status, scheduled_send_at)")
                cur.execute("create index if not exists idx_scheduled_sends_status_at on scheduled_sends (status, scheduled_at)")
                cur.execute("create index if not exists idx_contact_events_campaign_lead on contact_events (campaign_id, lead_id, occurred_at desc)")
                _upsert_app_meta_value(cur, "schema_version", POSTGRES_SCHEMA_VERSION, Jsonb)
            _POSTGRES_SCHEMA_READY = True


def postgres_campaign_count() -> int:
    ensure_postgres_schema()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute("select count(*) as count from campaigns")
        row = cur.fetchone()
        return int(row["count"] if row else 0)


def ensure_postgres_ready() -> None:
    global _POSTGRES_BOOTSTRAP_CHECKED
    ensure_postgres_schema()
    if _POSTGRES_BOOTSTRAP_CHECKED:
        return

    with _POSTGRES_STATE_LOCK:
        if _POSTGRES_BOOTSTRAP_CHECKED:
            return
        if postgres_campaign_count() == 0:
            bootstrap_postgres_from_files(force=False)
        _POSTGRES_BOOTSTRAP_CHECKED = True


def postgres_load_registry() -> dict[str, Any]:
    ensure_postgres_ready()
    campaigns = postgres_list_campaigns()
    active_id = postgres_get_active_campaign_id()
    if not active_id and campaigns:
        active_id = campaigns[0]["id"]
        postgres_set_active_campaign_id(active_id)
    return {
        "active_campaign_id": active_id,
        "campaigns": {
            campaign["id"]: {
                "label": campaign.get("label", campaign["id"]),
                "config_path": f"postgres://campaigns/{campaign['id']}",
            }
            for campaign in campaigns
        },
    }


def postgres_get_active_campaign_id() -> str:
    ensure_postgres_schema()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute("select value_json from app_meta where key = 'active_campaign_id'")
        return str(_app_meta_value(cur.fetchone()) or "").strip()


def postgres_set_active_campaign_id(campaign_id: str) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    with postgres_connection() as conn, conn.cursor() as cur:
        _upsert_app_meta_value(cur, "active_campaign_id", campaign_id, Jsonb)


def _campaign_row_to_config(row: dict[str, Any]) -> dict[str, Any]:
    config = {key: row.get(key) for key in CAMPAIGN_COLUMNS}
    config.update(
        {
            "hooks_library_json": row.get("hooks_library_json") or {},
            "template_overrides_json": row.get("template_overrides_json") or {},
            "hooks_library_path": f"postgres://campaigns/{row.get('id')}/hooks",
            "template_overrides_path": f"postgres://campaigns/{row.get('id')}/template-overrides",
        }
    )
    return _campaign_defaults(config)


def postgres_get_active_campaign() -> dict[str, Any]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select c.*
            from campaigns as c
            where c.id = coalesce(
                (select value_json ->> 'value' from app_meta where key = 'active_campaign_id'),
                (select id from campaigns order by id limit 1)
            )
            limit 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise KeyError("No campaigns available. Run `python crm.py bootstrap-postgres` first.")
        return _campaign_row_to_config(row)


def postgres_list_campaigns() -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute("select * from campaigns order by id")
        return [_campaign_row_to_config(row) for row in cur.fetchall()]


def postgres_get_campaign(campaign_id: str) -> dict[str, Any]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute("select * from campaigns where id = %s", (campaign_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Unknown campaign: {campaign_id}")
        return _campaign_row_to_config(row)


def postgres_save_campaign(config: dict[str, Any]) -> dict[str, Any]:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    normalized = _campaign_defaults(config)
    values = [normalized.get(column) for column in CAMPAIGN_COLUMNS]
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            insert into campaigns ({", ".join(CAMPAIGN_COLUMNS)}, hooks_library_json, template_overrides_json, updated_at)
            values ({", ".join(["%s"] * len(CAMPAIGN_COLUMNS))}, %s, %s, now())
            on conflict (id) do update set
                {", ".join(f"{column} = excluded.{column}" for column in CAMPAIGN_COLUMNS if column != "id")},
                hooks_library_json = excluded.hooks_library_json,
                template_overrides_json = excluded.template_overrides_json,
                updated_at = now()
            """,
            (
                *values,
                Jsonb(normalized.get("hooks_library_json") or {}),
                Jsonb(normalized.get("template_overrides_json") or {}),
            ),
        )
    return normalized


def _parse_schedule_timestamp(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _lead_payload_from_row(lead: dict[str, Any]) -> dict[str, Any]:
    payload = {column: str(lead.get(column, "") or "") for column in ALL_COLUMNS}
    payload.setdefault("Status", "new")
    payload.setdefault("Priority", "5")
    payload.setdefault("Drafts_Approved", "0")
    return payload


def _lead_sort_key(lead: dict[str, Any]) -> tuple[int, str]:
    lead_id = str(lead.get("ID") or "")
    suffix = lead_id.rsplit("-", 1)[-1]
    try:
        return (0, f"{int(suffix):08d}")
    except ValueError:
        return (1, lead_id)


def _lead_upsert_params(campaign_id: str, lead: dict[str, Any], Jsonb: Any) -> tuple[Any, ...]:
    scheduled_send_at = _parse_schedule_timestamp(lead.get("Scheduled_Send_At", ""))
    approved_at = _parse_schedule_timestamp(lead.get("Approved_At", ""))
    sent_at = _parse_schedule_timestamp(lead.get("Sent_At", ""))
    next_action_date = None
    next_action = (lead.get("Next_Action_Date") or "").strip()
    if next_action:
        try:
            next_action_date = datetime.fromisoformat(f"{next_action}T00:00:00").date()
        except ValueError:
            next_action_date = None
    return (
        campaign_id,
        lead["ID"],
        Jsonb(lead),
        lead.get("Status", "new"),
        _safe_int(lead.get("Priority"), 5),
        next_action_date,
        scheduled_send_at,
        lead.get("Scheduled_Send_Channel", ""),
        lead.get("Scheduled_Send_Status", ""),
        lead.get("Scheduled_Send_Error", ""),
        _safe_int(lead.get("Scheduled_Send_Attempts"), 0),
        approved_at,
        sent_at,
        lead.get("SMTP_Message_ID", ""),
    )


def _postgres_upsert_lead_row(cur: Any, campaign_id: str, lead: dict[str, Any], Jsonb: Any) -> None:
    cur.execute(
        """
        insert into leads (
            campaign_id, lead_id, payload, status, priority, next_action_date,
            scheduled_send_at, scheduled_send_channel, scheduled_send_status,
            scheduled_send_error, scheduled_send_attempts, approved_at, sent_at,
            smtp_message_id, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict (campaign_id, lead_id) do update set
            payload = excluded.payload,
            status = excluded.status,
            priority = excluded.priority,
            next_action_date = excluded.next_action_date,
            scheduled_send_at = excluded.scheduled_send_at,
            scheduled_send_channel = excluded.scheduled_send_channel,
            scheduled_send_status = excluded.scheduled_send_status,
            scheduled_send_error = excluded.scheduled_send_error,
            scheduled_send_attempts = excluded.scheduled_send_attempts,
            approved_at = excluded.approved_at,
            sent_at = excluded.sent_at,
            smtp_message_id = excluded.smtp_message_id,
            updated_at = now()
        """,
        _lead_upsert_params(campaign_id, lead, Jsonb),
    )
    _sync_scheduled_send_row(cur, campaign_id, lead)


def _lead_row_to_payload(row: dict[str, Any], *, include_campaign_id: bool = False) -> dict[str, Any]:
    payload = dict(row.get("payload") or {})
    for column in ALL_COLUMNS:
        payload.setdefault(column, "")
    payload["ID"] = row.get("lead_id") or payload.get("ID", "")
    payload["Status"] = row.get("status") or payload.get("Status", "new")
    priority = row.get("priority")
    payload["Priority"] = str(priority if priority is not None else payload.get("Priority", "5"))
    payload["Next_Action_Date"] = (
        row.get("next_action_date").isoformat()
        if row.get("next_action_date")
        else payload.get("Next_Action_Date", "")
    )
    payload["Scheduled_Send_At"] = (
        row.get("scheduled_send_at").isoformat()
        if row.get("scheduled_send_at")
        else payload.get("Scheduled_Send_At", "")
    )
    payload["Scheduled_Send_Channel"] = row.get("scheduled_send_channel") or payload.get("Scheduled_Send_Channel", "")
    payload["Scheduled_Send_Status"] = row.get("scheduled_send_status") or payload.get("Scheduled_Send_Status", "")
    payload["Scheduled_Send_Error"] = row.get("scheduled_send_error") or payload.get("Scheduled_Send_Error", "")
    payload["Scheduled_Send_Attempts"] = str(
        row.get("scheduled_send_attempts") or payload.get("Scheduled_Send_Attempts") or "0"
    )
    payload["Approved_At"] = row.get("approved_at").isoformat() if row.get("approved_at") else payload.get("Approved_At", "")
    payload["Sent_At"] = row.get("sent_at").isoformat() if row.get("sent_at") else payload.get("Sent_At", "")
    payload["SMTP_Message_ID"] = row.get("smtp_message_id") or payload.get("SMTP_Message_ID", "")
    if include_campaign_id:
        payload["Campaign_ID"] = row.get("campaign_id") or ""
    return payload


def _postgres_load_full_lead_rows(campaign_id: str, where_sql: str = "", params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    query = """
        select lead_id, payload, status, priority, next_action_date, scheduled_send_at,
               scheduled_send_channel, scheduled_send_status, scheduled_send_error,
               scheduled_send_attempts, approved_at, sent_at, smtp_message_id
        from leads
        where campaign_id = %s
    """
    if where_sql:
        query += f"\n          and {where_sql}"
    query += "\n        order by lead_id"
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (campaign_id, *params))
        return cur.fetchall()


def postgres_load_leads(campaign_id: str) -> list[dict[str, Any]]:
    rows = _postgres_load_full_lead_rows(campaign_id)
    leads = [_lead_row_to_payload(row) for row in rows]
    leads.sort(key=_lead_sort_key)
    return leads


def postgres_load_review_queue_summary(
    campaign_id: str,
    *,
    limit: int = 0,
    offset: int = 0,
) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    query = """
        select lead_id,
               coalesce(payload ->> 'Unternehmen', '') as company,
               priority,
               coalesce(payload ->> 'Analyzed_At', '') as analyzed_at,
               coalesce(payload ->> 'Research_Stale', '0') as research_stale
        from leads
        where campaign_id = %s
          and status = 'draft_ready'
          and coalesce(payload ->> 'Draft_Stale', '0') <> '1'
        order by priority asc, analyzed_at asc, lead_id asc
    """
    params: list[Any] = [campaign_id]
    if limit > 0:
        query += "\n        limit %s"
        params.append(limit)
    if offset > 0:
        query += "\n        offset %s"
        params.append(offset)
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return [
        {
            "ID": row.get("lead_id") or "",
            "Unternehmen": row.get("company") or "",
            "Priority": str(row.get("priority") or 5),
            "Analyzed_At": row.get("analyzed_at") or "",
            "Research_Stale": row.get("research_stale") or "0",
        }
        for row in rows
    ]


def postgres_load_outreach_leads(campaign_id: str) -> list[dict[str, Any]]:
    rows = _postgres_load_full_lead_rows(
        campaign_id,
        where_sql="""
            status = 'approved'
            and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
            and (
                coalesce(payload ->> 'Email_Draft', '') <> ''
                or coalesce(payload ->> 'WhatsApp_Draft', '') <> ''
                or coalesce(payload ->> 'Phone_Script', '') <> ''
            )
        """,
    )
    leads = [_lead_row_to_payload(row) for row in rows]
    leads.sort(key=lambda lead: (_safe_int(lead.get("Priority"), 5), lead.get("Unternehmen") or "", lead.get("ID") or ""))
    return leads


def postgres_load_outreach_summary(
    campaign_id: str,
    *,
    search: str = "",
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    ensure_postgres_ready()
    search_text = (search or "").strip().lower()
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or 100))
    offset = (page - 1) * page_size
    where_sql = """
        from leads
        where campaign_id = %s
          and status = 'approved'
          and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
          and (
                coalesce(payload ->> 'Email_Draft', '') <> ''
                or coalesce(payload ->> 'WhatsApp_Draft', '') <> ''
                or coalesce(payload ->> 'Phone_Script', '') <> ''
              )
    """
    params: list[Any] = [campaign_id]
    if search_text:
        like_value = f"%{search_text}%"
        where_sql += """
          and (
                lower(lead_id) like %s
                or lower(coalesce(payload ->> 'Unternehmen', '')) like %s
              )
        """
        params.extend([like_value, like_value])

    query = f"""
        select lead_id,
               coalesce(payload ->> 'Unternehmen', '') as company,
               status,
               priority,
               coalesce(payload ->> 'Draft_Stale', '0') as draft_stale,
               coalesce(payload ->> 'Email', '') as email,
               coalesce(payload ->> 'TelNr', '') as phone,
               coalesce(payload ->> 'Preferred_Channel', '') as preferred_channel,
               coalesce(payload ->> 'Next_Action_Type', '') as next_action_type,
               coalesce(payload ->> 'Channel_Used', '') as channel_used,
               coalesce(payload ->> 'Email_Draft', '') <> '' as has_email_draft,
               scheduled_send_at,
               scheduled_send_status,
               scheduled_send_error,
               sent_at
        {where_sql}
        order by priority asc, lead_id asc
        limit %s
        offset %s
    """
    count_query = f"select count(*) as count {where_sql}"
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(count_query, tuple(params))
        count_row = cur.fetchone() or {}
        cur.execute(query, tuple([*params, page_size, offset]))
        rows = cur.fetchall()
    items = [
        {
            "ID": row.get("lead_id") or "",
            "Unternehmen": row.get("company") or "",
            "Status": row.get("status") or "approved",
            "Priority": str(row.get("priority") or 5),
            "Draft_Stale": row.get("draft_stale") or "0",
            "Email": row.get("email") or "",
            "TelNr": row.get("phone") or "",
            "Preferred_Channel": row.get("preferred_channel") or "",
            "Next_Action_Type": row.get("next_action_type") or "",
            "Channel_Used": row.get("channel_used") or "",
            "Has_Email_Draft": bool(row.get("has_email_draft")),
            "Scheduled_Send_At": row.get("scheduled_send_at").isoformat() if row.get("scheduled_send_at") else "",
            "Scheduled_Send_Status": row.get("scheduled_send_status") or "",
            "Scheduled_Send_Error": row.get("scheduled_send_error") or "",
            "Sent_At": row.get("sent_at").isoformat() if row.get("sent_at") else "",
        }
        for row in rows
    ]
    return {
        "items": items,
        "total_count": int(count_row.get("count") or 0),
        "page": page,
        "page_size": page_size,
    }


def postgres_load_outreach_counts(campaign_id: str, today_iso: str) -> dict[str, int]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with approved as (
                select scheduled_send_at, scheduled_send_status, scheduled_send_error
                from leads
                where campaign_id = %s
                  and status = 'approved'
                  and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
                  and (
                        coalesce(payload ->> 'Email_Draft', '') <> ''
                        or coalesce(payload ->> 'WhatsApp_Draft', '') <> ''
                        or coalesce(payload ->> 'Phone_Script', '') <> ''
                      )
            )
            select
                count(*) as approved_total,
                count(*) filter (
                    where scheduled_send_status = 'queued'
                      and scheduled_send_at is not null
                      and timezone('Europe/Vienna', scheduled_send_at)::date = %s::date
                ) as queued_today,
                count(*) filter (
                    where scheduled_send_status = 'queued'
                      and (
                            scheduled_send_at is null
                            or timezone('Europe/Vienna', scheduled_send_at)::date <> %s::date
                          )
                ) as queued_later,
                count(*) filter (where coalesce(scheduled_send_error, '') <> '') as send_errors,
                (
                    select count(*)
                    from leads
                    where campaign_id = %s
                      and sent_at is not null
                      and timezone('Europe/Vienna', sent_at)::date = %s::date
                ) as sent_today
            from approved
            """,
            (campaign_id, today_iso, today_iso, campaign_id, today_iso),
        )
        row = cur.fetchone() or {}
    return {
        "approved_total": int(row.get("approved_total") or 0),
        "queued_today": int(row.get("queued_today") or 0),
        "queued_later": int(row.get("queued_later") or 0),
        "send_errors": int(row.get("send_errors") or 0),
        "sent_today": int(row.get("sent_today") or 0),
    }


def postgres_load_all_leads_summary(
    campaign_id: str,
    *,
    search: str = "",
    statuses: list[str] | None = None,
    channels: list[str] | None = None,
    priorities: list[str] | None = None,
    stale: str = "All",
    page: int = 1,
    page_size: int = 25,
) -> dict[str, Any]:
    ensure_postgres_ready()
    search_text = (search or "").strip().lower()
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or 25))
    offset = (page - 1) * page_size

    tel_clean_sql = "regexp_replace(coalesce(payload ->> 'TelNr', ''), '[^0-9+]', '', 'g')"
    has_email_sql = "coalesce(payload ->> 'Email', '') <> ''"
    has_phone_sql = "coalesce(payload ->> 'TelNr', '') <> ''"
    has_whatsapp_sql = f"({tel_clean_sql} like '+436%%' or {tel_clean_sql} like '06%%')"
    planned_channel_sql = f"""
        case
            when coalesce(payload ->> 'Next_Action_Type', '') = 'email' and {has_email_sql} then 'email'
            when coalesce(payload ->> 'Next_Action_Type', '') = 'whatsapp' and {has_whatsapp_sql} then 'whatsapp'
            when coalesce(payload ->> 'Next_Action_Type', '') = 'phone' and {has_phone_sql} then 'phone'
            when coalesce(payload ->> 'Preferred_Channel', '') = 'email' and {has_email_sql} then 'email'
            when coalesce(payload ->> 'Preferred_Channel', '') = 'whatsapp' and {has_whatsapp_sql} then 'whatsapp'
            when coalesce(payload ->> 'Preferred_Channel', '') = 'phone' and {has_phone_sql} then 'phone'
            when coalesce(payload ->> 'Channel_Used', '') = 'email' and {has_email_sql} then 'email'
            when coalesce(payload ->> 'Channel_Used', '') = 'whatsapp' and {has_whatsapp_sql} then 'whatsapp'
            when coalesce(payload ->> 'Channel_Used', '') = 'phone' and {has_phone_sql} then 'phone'
            when {has_email_sql} then 'email'
            when {has_whatsapp_sql} then 'whatsapp'
            when {has_phone_sql} then 'phone'
            else 'none'
        end
    """

    where_clauses = ["campaign_id = %s"]
    params: list[Any] = [campaign_id]

    if search_text:
        like_value = f"%{search_text}%"
        where_clauses.append(
            """
            (
                lower(lead_id) like %s
                or lower(coalesce(payload ->> 'Unternehmen', '')) like %s
                or lower(coalesce(payload ->> 'Adresse', '')) like %s
                or lower(coalesce(payload ->> 'Kontaktname', '')) like %s
                or lower(coalesce(payload ->> 'Email', '')) like %s
            )
            """
        )
        params.extend([like_value, like_value, like_value, like_value, like_value])

    status_values = [value for value in (statuses or []) if value]
    if status_values:
        where_clauses.append("status = any(%s)")
        params.append(status_values)

    channel_values = [value for value in (channels or []) if value in {"email", "whatsapp", "phone", "none"}]
    if channel_values:
        where_clauses.append(f"({planned_channel_sql}) = any(%s)")
        params.append(channel_values)

    priority_values = sorted({_safe_int(value, 0) for value in (priorities or []) if str(value).isdigit()})
    if priority_values:
        where_clauses.append("priority = any(%s)")
        params.append(priority_values)

    if stale == "Draft stale":
        where_clauses.append("coalesce(payload ->> 'Draft_Stale', '0') = '1'")
    elif stale == "Research stale":
        where_clauses.append("coalesce(payload ->> 'Research_Stale', '0') = '1'")
    elif stale == "Fresh only":
        where_clauses.append("coalesce(payload ->> 'Draft_Stale', '0') <> '1'")
        where_clauses.append("coalesce(payload ->> 'Research_Stale', '0') <> '1'")

    where_sql = " and ".join(where_clauses)
    count_query = f"select count(*) as count from leads where {where_sql}"
    query = f"""
        select lead_id,
               coalesce(payload ->> 'Unternehmen', '') as company,
               status,
               priority,
               coalesce(payload ->> 'Draft_Stale', '0') as draft_stale,
               coalesce(payload ->> 'Research_Stale', '0') as research_stale,
               coalesce(payload ->> 'Email', '') as email,
               coalesce(payload ->> 'TelNr', '') as phone,
               coalesce(payload ->> 'Kontaktname', '') as contact_name,
               coalesce(payload ->> 'Adresse', '') as address,
               {planned_channel_sql} as planned_channel
        from leads
        where {where_sql}
        order by priority asc, company asc, lead_id asc
        limit %s
        offset %s
    """
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(count_query, tuple(params))
        count_row = cur.fetchone() or {}
        cur.execute(query, tuple([*params, page_size, offset]))
        rows = cur.fetchall()

    items = [
        {
            "ID": row.get("lead_id") or "",
            "Unternehmen": row.get("company") or "",
            "Status": row.get("status") or "new",
            "Priority": str(row.get("priority") or 5),
            "Draft_Stale": row.get("draft_stale") or "0",
            "Research_Stale": row.get("research_stale") or "0",
            "Email": row.get("email") or "",
            "TelNr": row.get("phone") or "",
            "Kontaktname": row.get("contact_name") or "",
            "Adresse": row.get("address") or "",
            "Planned_Channel": row.get("planned_channel") or "none",
        }
        for row in rows
    ]
    return {
        "items": items,
        "total_count": int(count_row.get("count") or 0),
        "page": page,
        "page_size": page_size,
    }


def postgres_load_recontact_leads(campaign_id: str) -> list[dict[str, Any]]:
    rows = _postgres_load_full_lead_rows(
        campaign_id,
        where_sql="""
            status not in ('won', 'lost', 'done', 'blacklist', 'approved')
            and (
                coalesce(payload ->> 'Kontaktdatum', '') <> ''
                or coalesce(payload ->> 'Last_Contact_Date', '') <> ''
                or coalesce(payload ->> 'Contact_Log', '') <> ''
                or coalesce(payload ->> 'Contact_Count', '0') <> '0'
            )
            and (
                coalesce(payload ->> 'Email_Draft', '') <> ''
                or coalesce(payload ->> 'WhatsApp_Draft', '') <> ''
                or coalesce(payload ->> 'Phone_Script', '') <> ''
            )
        """,
    )
    return [_lead_row_to_payload(row) for row in rows]


def postgres_load_lead_metrics(campaign_id: str) -> dict[str, int]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select
                count(*) as total_leads,
                count(*) filter (where status = 'draft_ready') as draft_ready,
                count(*) filter (
                    where status = 'approved'
                      and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
                      and coalesce(payload ->> 'Draft_Stale', '0') <> '1'
                ) as approved_fresh,
                count(*) filter (where coalesce(payload ->> 'Draft_Stale', '0') = '1') as draft_stale,
                count(*) filter (where coalesce(payload ->> 'Research_Stale', '0') = '1') as research_stale
            from leads
            where campaign_id = %s
            """,
            (campaign_id,),
        )
        row = cur.fetchone() or {}
    return {
        "total_leads": int(row.get("total_leads") or 0),
        "draft_ready": int(row.get("draft_ready") or 0),
        "approved_fresh": int(row.get("approved_fresh") or 0),
        "draft_stale": int(row.get("draft_stale") or 0),
        "research_stale": int(row.get("research_stale") or 0),
    }


def postgres_load_dashboard_snapshot(campaign_id: str, today_iso: str) -> dict[str, Any]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select status, count(*) as count
            from leads
            where campaign_id = %s
            group by status
            """,
            (campaign_id,),
        )
        status_rows = cur.fetchall()

        cur.execute(
            """
            select lead_id, coalesce(payload ->> 'Unternehmen', '') as company
            from leads
            where campaign_id = %s
              and coalesce(payload ->> 'Website_Category', '') <> ''
              and (
                    coalesce(payload ->> 'Analyzed_At', '') = ''
                    or coalesce(payload ->> 'Draft_Stale', '0') = '1'
                  )
              and status not in ('done', 'won', 'lost', 'blacklist')
            order by priority asc, lead_id asc
            """,
            (campaign_id,),
        )
        pending_rows = cur.fetchall()

        cur.execute(
            """
            select lead_id,
                   coalesce(payload ->> 'Unternehmen', '') as company,
                   priority,
                   next_action_date,
                   coalesce(payload ->> 'Next_Action_Type', '') as next_action_type
            from leads
            where campaign_id = %s
              and status not in ('done', 'won', 'lost', 'blacklist')
              and status <> 'no_contact'
              and coalesce(scheduled_send_status, '') <> 'queued'
              and coalesce(payload ->> 'Next_Action_Type', '') not in ('none', '')
              and (next_action_date is null or next_action_date <= %s)
            order by priority asc, next_action_date asc nulls first, lead_id asc
            """,
            (campaign_id, today_iso),
        )
        actionable_rows = cur.fetchall()

    status_counts = {str(row.get("status") or "new"): int(row.get("count") or 0) for row in status_rows}
    actionable = [
        {
            "ID": row.get("lead_id") or "",
            "Unternehmen": row.get("company") or "",
            "Priority": str(row.get("priority") or 5),
            "Next_Action_Date": row.get("next_action_date").isoformat() if row.get("next_action_date") else "",
            "Next_Action_Type": row.get("next_action_type") or "",
        }
        for row in actionable_rows
    ]
    pending_drafts = [
        {
            "ID": row.get("lead_id") or "",
            "Unternehmen": row.get("company") or "",
        }
        for row in pending_rows
    ]
    return {
        "status_counts": status_counts,
        "pending_drafts": pending_drafts,
        "actionable": actionable,
    }


def postgres_save_leads(campaign_id: str, leads: list[dict[str, Any]]) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    normalized = sorted((_lead_payload_from_row(lead) for lead in leads if str(lead.get("ID") or "").strip()), key=_lead_sort_key)
    lead_ids = [lead["ID"] for lead in normalized]
    with postgres_connection() as conn, conn.cursor() as cur:
        for lead in normalized:
            _postgres_upsert_lead_row(cur, campaign_id, lead, Jsonb)
        if lead_ids:
            cur.execute(
                "delete from leads where campaign_id = %s and lead_id <> all(%s)",
                (campaign_id, lead_ids),
            )
            cur.execute(
                "delete from scheduled_sends where campaign_id = %s and lead_id <> all(%s)",
                (campaign_id, lead_ids),
            )
        else:
            cur.execute("delete from leads where campaign_id = %s", (campaign_id,))
            cur.execute("delete from scheduled_sends where campaign_id = %s", (campaign_id,))


def postgres_upsert_lead(campaign_id: str, lead: dict[str, Any]) -> None:
    ensure_postgres_ready()
    lead_id = str(lead.get("ID") or "").strip()
    if not lead_id:
        raise ValueError("Lead ID is required for postgres_upsert_lead().")
    _, _, Jsonb = _load_psycopg()
    normalized = _lead_payload_from_row(lead)
    with postgres_connection() as conn, conn.cursor() as cur:
        _postgres_upsert_lead_row(cur, campaign_id, normalized, Jsonb)


def postgres_upsert_leads(campaign_id: str, leads: list[dict[str, Any]]) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    normalized = [
        _lead_payload_from_row(lead)
        for lead in leads
        if str(lead.get("ID") or "").strip()
    ]
    if not normalized:
        return
    with postgres_connection() as conn, conn.cursor() as cur:
        for lead in normalized:
            _postgres_upsert_lead_row(cur, campaign_id, lead, Jsonb)


def _sync_scheduled_send_row(cur: Any, campaign_id: str, lead: dict[str, Any]) -> None:
    scheduled_at = _parse_schedule_timestamp(lead.get("Scheduled_Send_At", ""))
    channel = (lead.get("Scheduled_Send_Channel") or "").strip()
    status = (lead.get("Scheduled_Send_Status") or "").strip()
    if scheduled_at and channel == "email" and status:
        cur.execute(
            """
            insert into scheduled_sends (
                campaign_id, lead_id, channel, scheduled_at, status, last_error, attempts,
                approved_at, sent_at, smtp_message_id, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (campaign_id, lead_id, channel) do update set
                scheduled_at = excluded.scheduled_at,
                status = excluded.status,
                last_error = excluded.last_error,
                attempts = excluded.attempts,
                approved_at = excluded.approved_at,
                sent_at = excluded.sent_at,
                smtp_message_id = excluded.smtp_message_id,
                updated_at = now()
            """,
            (
                campaign_id,
                lead.get("ID", ""),
                channel,
                scheduled_at,
                status,
                lead.get("Scheduled_Send_Error", ""),
                _safe_int(lead.get("Scheduled_Send_Attempts"), 0),
                _parse_schedule_timestamp(lead.get("Approved_At", "")),
                _parse_schedule_timestamp(lead.get("Sent_At", "")),
                lead.get("SMTP_Message_ID", ""),
            ),
        )
    else:
        cur.execute(
            "delete from scheduled_sends where campaign_id = %s and lead_id = %s and channel = 'email'",
            (campaign_id, lead.get("ID", "")),
        )


def _contact_event_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.now()
    parsed = _parse_schedule_timestamp(text.replace(" ", "T")) if " " in text else _parse_schedule_timestamp(text)
    return parsed or datetime.now()


def _insert_contact_event_row(
    cur: Any,
    campaign_id: str,
    lead_id: str,
    *,
    occurred_at: Any,
    channel: str,
    outcome: str,
    notes: str = "",
) -> None:
    cur.execute(
        """
        insert into contact_events (campaign_id, lead_id, occurred_at, channel, outcome, notes)
        values (%s, %s, %s, %s, %s, %s)
        """,
        (campaign_id, lead_id, _contact_event_timestamp(occurred_at), channel, outcome, notes.strip()),
    )


def postgres_get_lead_by_id(lead_id: str, campaign_id: str = "") -> dict[str, Any] | None:
    ensure_postgres_ready()
    query = """
        select campaign_id, lead_id, payload, status, priority, next_action_date, scheduled_send_at,
               scheduled_send_channel, scheduled_send_status, scheduled_send_error,
               scheduled_send_attempts, approved_at, sent_at, smtp_message_id
        from leads
        where lead_id = %s
    """
    params: tuple[Any, ...] = (lead_id,)
    if campaign_id:
        query += " and campaign_id = %s"
        params = (lead_id, campaign_id)
    query += " order by campaign_id limit 1"
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    if not row:
        return None
    return _lead_row_to_payload(row, include_campaign_id=True)


def postgres_persist_outreach_lead(
    campaign_id: str,
    lead: dict[str, Any],
    *,
    contact_event: dict[str, Any] | None = None,
) -> None:
    ensure_postgres_ready()
    lead_id = str(lead.get("ID") or "").strip()
    if not lead_id:
        raise ValueError("Lead ID is required for postgres_persist_outreach_lead().")
    _, _, Jsonb = _load_psycopg()
    normalized = _lead_payload_from_row(lead)
    with postgres_connection() as conn, conn.cursor() as cur:
        _postgres_upsert_lead_row(cur, campaign_id, normalized, Jsonb)
        if contact_event:
            _insert_contact_event_row(
                cur,
                campaign_id,
                lead_id,
                occurred_at=contact_event.get("occurred_at") or datetime.now(),
                channel=str(contact_event.get("channel") or "").strip(),
                outcome=str(contact_event.get("outcome") or "").strip(),
                notes=str(contact_event.get("notes") or ""),
            )


def postgres_archive_stale_contacted_leads(campaign_id: str, cutoff_date_iso: str) -> int:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with archived as (
                update leads
                set status = 'no_contact',
                    next_action_date = null,
                    payload = jsonb_set(
                        jsonb_set(
                            jsonb_set(payload, '{Status}', to_jsonb('no_contact'::text), true),
                            '{Next_Action_Type}',
                            to_jsonb('none'::text),
                            true
                        ),
                        '{Next_Action_Date}',
                        to_jsonb(''::text),
                        true
                    ),
                    updated_at = now()
                where campaign_id = %s
                  and status = 'contacted'
                  and coalesce(payload ->> 'Contact_Count', '0') ~ '^[0-9]+$'
                  and (payload ->> 'Contact_Count')::integer >= 3
                  and coalesce(payload ->> 'Last_Contact_Date', '') ~ '^\\d{4}-\\d{2}-\\d{2}$'
                  and (payload ->> 'Last_Contact_Date')::date <= %s::date
                returning 1
            )
            select count(*) as count from archived
            """,
            (campaign_id, cutoff_date_iso),
        )
        row = cur.fetchone() or {}
    return int(row.get("count") or 0)


def postgres_record_contact_event(
    campaign_id: str,
    lead_id: str,
    *,
    occurred_at: str,
    channel: str,
    outcome: str,
    notes: str = "",
) -> None:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        _insert_contact_event_row(
            cur,
            campaign_id,
            lead_id,
            occurred_at=occurred_at,
            channel=channel,
            outcome=outcome,
            notes=notes,
        )


def postgres_list_due_scheduled_sends(limit: int = 100) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select campaign_id, lead_id, channel, scheduled_at, status, attempts, last_error
            from scheduled_sends
            where status = 'queued'
              and channel = 'email'
              and scheduled_at <= now()
            order by scheduled_at asc, campaign_id asc, lead_id asc
            limit %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def postgres_claim_due_scheduled_sends(limit: int = 100) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with due as (
                select campaign_id, lead_id
                from leads
                where scheduled_send_status = 'queued'
                  and scheduled_send_channel = 'email'
                  and scheduled_send_at <= now()
                order by scheduled_send_at asc, campaign_id asc, lead_id asc
                limit %s
                for update skip locked
            )
            update leads as l
            set scheduled_send_status = 'sending',
                payload = jsonb_set(l.payload, '{Scheduled_Send_Status}', to_jsonb('sending'::text), true),
                updated_at = now()
            from due
            where l.campaign_id = due.campaign_id
              and l.lead_id = due.lead_id
            returning l.campaign_id, l.lead_id, l.scheduled_send_at, l.scheduled_send_channel,
                      l.scheduled_send_attempts, l.scheduled_send_error
            """,
            (limit,),
        )
        rows = cur.fetchall()
        for row in rows:
            cur.execute(
                """
                update scheduled_sends
                set status = 'sending',
                    updated_at = now()
                where campaign_id = %s
                  and lead_id = %s
                  and channel = 'email'
                """,
                (row["campaign_id"], row["lead_id"]),
            )
    return [dict(row) for row in rows]


def bootstrap_postgres_from_files(force: bool = False) -> dict[str, int]:
    ensure_postgres_schema()
    if not force and postgres_campaign_count() > 0:
        return {"campaigns": 0, "leads": 0}
    if force:
        with postgres_connection() as conn, conn.cursor() as cur:
            cur.execute("delete from contact_events")
            cur.execute("delete from scheduled_sends")
            cur.execute("delete from leads")
            cur.execute("delete from campaigns")
            cur.execute("delete from app_meta where key = 'active_campaign_id'")

    registry = _read_json(REGISTRY_PATH, {"active_campaign_id": "", "campaigns": {}})
    config_paths = sorted(CAMPAIGNS_DIR.glob("*/config.json"))
    if not config_paths:
        return {"campaigns": 0, "leads": 0}

    imported_campaigns = 0
    imported_leads = 0
    for config_path in config_paths:
        try:
            raw_config = _read_json(config_path, {})
        except Exception:
            raw_config = {}
        if not raw_config:
            continue

        campaign_id = str(raw_config.get("id") or config_path.parent.name).strip()
        layout = _campaign_layout(campaign_id)
        hooks_path = ROOT_DIR / str(raw_config.get("hooks_library_path") or layout["hooks_library_path"].relative_to(ROOT_DIR))
        template_path = ROOT_DIR / str(raw_config.get("template_overrides_path") or layout["template_overrides_path"].relative_to(ROOT_DIR))
        raw_config["hooks_library_json"] = _read_json(hooks_path, {})
        raw_config["template_overrides_json"] = _read_json(template_path, {})
        raw_config.pop("portfolio_urls", None)
        raw_config.pop("portfolio_dir", None)
        raw_config.pop("flyer_path", None)
        raw_config.pop("hooks_library_path", None)
        raw_config.pop("template_overrides_path", None)
        config = postgres_save_campaign(raw_config)
        imported_campaigns += 1

        csv_path = ROOT_DIR / str(config.get("csv_path") or layout["csv_path"].relative_to(ROOT_DIR))
        leads: list[dict[str, Any]] = []
        if csv_path.exists():
            with csv_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter=";")
                for row in reader:
                    lead = {column: row.get(column, "") for column in ALL_COLUMNS}
                    leads.append(lead)
        if leads:
            from crm_store import ensure_lead_ids

            ensure_lead_ids(leads, campaign=config)
        postgres_save_leads(config["id"], leads)
        imported_leads += len(leads)

    active_id = str(registry.get("active_campaign_id") or "").strip()
    if not active_id and imported_campaigns:
        active_id = postgres_list_campaigns()[0]["id"]
    if active_id:
        postgres_set_active_campaign_id(active_id)

    global _POSTGRES_BOOTSTRAP_CHECKED
    _POSTGRES_BOOTSTRAP_CHECKED = True
    return {"campaigns": imported_campaigns, "leads": imported_leads}
