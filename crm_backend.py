"""
crm_backend.py - Backend selection and Postgres persistence helpers.
"""

from __future__ import annotations

import csv
import json
import os
import re
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
_POSTGRES_LOCAL = threading.local()

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


def _clear_thread_postgres_connection() -> None:
    conn = getattr(_POSTGRES_LOCAL, "connection", None)
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
    _POSTGRES_LOCAL.connection = None
    _POSTGRES_LOCAL.database_url = ""


def _thread_postgres_connection() -> Any:
    database_url = _database_url()
    conn = getattr(_POSTGRES_LOCAL, "connection", None)
    current_url = getattr(_POSTGRES_LOCAL, "database_url", "")
    if conn is not None and current_url == database_url and not getattr(conn, "closed", False):
        return conn

    _clear_thread_postgres_connection()
    psycopg, dict_row, _ = _load_psycopg()
    conn = psycopg.connect(database_url, row_factory=dict_row)
    _POSTGRES_LOCAL.connection = conn
    _POSTGRES_LOCAL.database_url = database_url
    return conn


@contextmanager
def postgres_connection() -> Iterator[Any]:
    conn = _thread_postgres_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        _clear_thread_postgres_connection()
        raise


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


def ensure_postgres_schema() -> None:
    global _POSTGRES_SCHEMA_READY
    if _POSTGRES_SCHEMA_READY:
        return

    with _POSTGRES_STATE_LOCK:
        if _POSTGRES_SCHEMA_READY:
            return
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
        row = cur.fetchone()
        if not row:
            return ""
        value = row.get("value_json")
        if isinstance(value, dict):
            return str(value.get("value") or "").strip()
        if isinstance(value, str):
            return value.strip()
        return str(value or "").strip()


def postgres_set_active_campaign_id(campaign_id: str) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into app_meta (key, value_json, updated_at)
            values ('active_campaign_id', %s, now())
            on conflict (key) do update
            set value_json = excluded.value_json,
                updated_at = now()
            """,
            (Jsonb({"value": campaign_id}),),
        )


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


def postgres_load_leads(campaign_id: str) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select lead_id, payload, status, priority, next_action_date, scheduled_send_at,
                   scheduled_send_channel, scheduled_send_status, scheduled_send_error,
                   scheduled_send_attempts, approved_at, sent_at, smtp_message_id
            from leads
            where campaign_id = %s
            order by lead_id
            """,
            (campaign_id,),
        )
        rows = cur.fetchall()
    leads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row.get("payload") or {})
        for column in ALL_COLUMNS:
            payload.setdefault(column, "")
        payload["ID"] = row.get("lead_id") or payload.get("ID", "")
        payload["Status"] = row.get("status") or payload.get("Status", "new")
        priority = row.get("priority")
        payload["Priority"] = str(priority if priority is not None else payload.get("Priority", "5"))
        payload["Next_Action_Date"] = row.get("next_action_date").isoformat() if row.get("next_action_date") else payload.get("Next_Action_Date", "")
        payload["Scheduled_Send_At"] = row.get("scheduled_send_at").isoformat() if row.get("scheduled_send_at") else payload.get("Scheduled_Send_At", "")
        payload["Scheduled_Send_Channel"] = row.get("scheduled_send_channel") or payload.get("Scheduled_Send_Channel", "")
        payload["Scheduled_Send_Status"] = row.get("scheduled_send_status") or payload.get("Scheduled_Send_Status", "")
        payload["Scheduled_Send_Error"] = row.get("scheduled_send_error") or payload.get("Scheduled_Send_Error", "")
        payload["Scheduled_Send_Attempts"] = str(row.get("scheduled_send_attempts") or payload.get("Scheduled_Send_Attempts") or "0")
        payload["Approved_At"] = row.get("approved_at").isoformat() if row.get("approved_at") else payload.get("Approved_At", "")
        payload["Sent_At"] = row.get("sent_at").isoformat() if row.get("sent_at") else payload.get("Sent_At", "")
        payload["SMTP_Message_ID"] = row.get("smtp_message_id") or payload.get("SMTP_Message_ID", "")
        leads.append(payload)
    leads.sort(key=_lead_sort_key)
    return leads


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


def postgres_save_leads(campaign_id: str, leads: list[dict[str, Any]]) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    normalized = sorted((_lead_payload_from_row(lead) for lead in leads if str(lead.get("ID") or "").strip()), key=_lead_sort_key)
    lead_ids = [lead["ID"] for lead in normalized]
    with postgres_connection() as conn, conn.cursor() as cur:
        for lead in normalized:
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
                (
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
                ),
            )
            _sync_scheduled_send_row(cur, campaign_id, lead)
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
    payload = dict(row.get("payload") or {})
    payload["ID"] = row.get("lead_id") or payload.get("ID", "")
    payload["Campaign_ID"] = row.get("campaign_id") or ""
    payload["Status"] = row.get("status") or payload.get("Status", "new")
    payload["Priority"] = str(row.get("priority") or payload.get("Priority") or "5")
    payload["Next_Action_Date"] = row.get("next_action_date").isoformat() if row.get("next_action_date") else payload.get("Next_Action_Date", "")
    payload["Scheduled_Send_At"] = row.get("scheduled_send_at").isoformat() if row.get("scheduled_send_at") else payload.get("Scheduled_Send_At", "")
    payload["Scheduled_Send_Channel"] = row.get("scheduled_send_channel") or payload.get("Scheduled_Send_Channel", "")
    payload["Scheduled_Send_Status"] = row.get("scheduled_send_status") or payload.get("Scheduled_Send_Status", "")
    payload["Scheduled_Send_Error"] = row.get("scheduled_send_error") or payload.get("Scheduled_Send_Error", "")
    payload["Scheduled_Send_Attempts"] = str(row.get("scheduled_send_attempts") or payload.get("Scheduled_Send_Attempts") or "0")
    payload["Approved_At"] = row.get("approved_at").isoformat() if row.get("approved_at") else payload.get("Approved_At", "")
    payload["Sent_At"] = row.get("sent_at").isoformat() if row.get("sent_at") else payload.get("Sent_At", "")
    payload["SMTP_Message_ID"] = row.get("smtp_message_id") or payload.get("SMTP_Message_ID", "")
    for column in ALL_COLUMNS:
        payload.setdefault(column, "")
    return payload


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
    timestamp = _parse_schedule_timestamp(occurred_at.replace(" ", "T")) if " " in occurred_at else _parse_schedule_timestamp(occurred_at)
    if timestamp is None:
        timestamp = datetime.now()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into contact_events (campaign_id, lead_id, occurred_at, channel, outcome, notes)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (campaign_id, lead_id, timestamp, channel, outcome, notes.strip()),
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
