"""
crm_backend.py - Backend selection and Postgres persistence helpers.
"""

from __future__ import annotations

import csv
import json
import os
import re
import atexit
import mimetypes
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv

from crm_fields import ALL_COLUMNS

load_dotenv()


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
POSTGRES_SCHEMA_VERSION = 4

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


def _lead_has_saved_drafts_sql(payload_sql: str = "payload") -> str:
    return (
        f"(coalesce({payload_sql} ->> 'Email_Draft', '') <> '' "
        f"or coalesce({payload_sql} ->> 'WhatsApp_Draft', '') <> '')"
    )


def _draft_stale_sql(payload_sql: str = "payload", draft_version_sql: str = "%s") -> str:
    has_drafts_sql = _lead_has_saved_drafts_sql(payload_sql)
    return (
        f"(coalesce({payload_sql} ->> 'Draft_Stale', '0') = '1' "
        f"or ({has_drafts_sql} "
        f"and coalesce({payload_sql} ->> 'Draft_Config_Version', '') <> '' "
        f"and coalesce({payload_sql} ->> 'Draft_Config_Version', '') <> {draft_version_sql}))"
    )


def _normalized_company_sql(payload_sql: str = "payload") -> str:
    return f"regexp_replace(lower(coalesce({payload_sql} ->> 'Unternehmen', '')), '[^a-z0-9]', '', 'g')"


def _campaign_draft_version(campaign_id: str) -> str:
    config = postgres_get_campaign(campaign_id)
    return str(config.get("draft_config_version") or config.get("config_version") or "1")


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
        "assets_dir": campaign_dir / "assets",
        "portfolio_dir": campaign_dir / "portfolio",
    }


def normalize_asset_path(path_value: str | Path) -> str:
    raw_path = str(path_value or "").strip()
    if not raw_path:
        raise ValueError("Asset path is required.")
    path = Path(raw_path)
    if path.is_absolute():
        try:
            path = path.relative_to(ROOT_DIR)
        except ValueError:
            path = Path(os.path.relpath(path, ROOT_DIR))
    normalized = str(path).replace("\\", "/")
    normalized = re.sub(r"^\./+", "", normalized)
    normalized = re.sub(r"/+", "/", normalized)
    return normalized.strip("/")


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
        "extra_queries": config.get("extra_queries") if isinstance(config.get("extra_queries"), list) else [],
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
            if current_version < 1:
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
                current_version = 1
                _upsert_app_meta_value(cur, "schema_version", current_version, Jsonb)
            if current_version < 2:
                cur.execute(
                    """
                    create table if not exists outbound_emails (
                        id bigserial primary key,
                        campaign_id text not null references campaigns(id) on delete cascade,
                        lead_id text not null,
                        smtp_message_id text not null unique,
                        recipient_email text not null default '',
                        subject text not null default '',
                        sent_at timestamptz not null,
                        source text not null default 'app',
                        status text not null default 'sent',
                        status_reason text not null default '',
                        last_event_type text not null default '',
                        last_event_at timestamptz,
                        last_sync_at timestamptz,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now()
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists mailbox_events (
                        id bigserial primary key,
                        campaign_id text references campaigns(id) on delete cascade,
                        lead_id text not null default '',
                        folder_name text not null default '',
                        mailbox_uid text not null default '',
                        event_at timestamptz not null,
                        event_type text not null default '',
                        from_address text not null default '',
                        subject text not null default '',
                        raw_message_id text not null default '',
                        related_smtp_message_id text not null default '',
                        reason text not null default '',
                        matched boolean not null default false,
                        metadata_json jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now(),
                        unique (folder_name, mailbox_uid)
                    )
                    """
                )
                cur.execute("create index if not exists idx_outbound_emails_campaign_sent_at on outbound_emails (campaign_id, sent_at desc)")
                cur.execute("create index if not exists idx_outbound_emails_campaign_lead on outbound_emails (campaign_id, lead_id, sent_at desc)")
                cur.execute("create index if not exists idx_outbound_emails_status on outbound_emails (status, sent_at desc)")
                cur.execute("create index if not exists idx_mailbox_events_campaign_event_at on mailbox_events (campaign_id, event_at desc)")
                cur.execute("create index if not exists idx_mailbox_events_related_message on mailbox_events (related_smtp_message_id, event_at desc)")
                cur.execute(
                    """
                    insert into outbound_emails (
                        campaign_id, lead_id, smtp_message_id, recipient_email, subject,
                        sent_at, source, status, created_at, updated_at
                    )
                    select campaign_id,
                           lead_id,
                           smtp_message_id,
                           coalesce(payload ->> 'Email', ''),
                           '',
                           sent_at,
                           'backfill',
                           'sent',
                           coalesce(sent_at, now()),
                           now()
                    from leads
                    where coalesce(smtp_message_id, '') <> ''
                      and sent_at is not null
                    on conflict (smtp_message_id) do nothing
                    """
                )
                current_version = 2
                _upsert_app_meta_value(cur, "schema_version", current_version, Jsonb)
            if current_version < 3:
                cur.execute(
                    "alter table campaigns add column if not exists extra_queries_json jsonb not null default '[]'::jsonb"
                )
                current_version = 3
                _upsert_app_meta_value(cur, "schema_version", current_version, Jsonb)
            if current_version < 4:
                cur.execute(
                    """
                    create table if not exists campaign_assets (
                        campaign_id text not null references campaigns(id) on delete cascade,
                        asset_path text not null,
                        content_type text not null default '',
                        data_bytes bytea not null,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now(),
                        primary key (campaign_id, asset_path)
                    )
                    """
                )
                cur.execute(
                    "create index if not exists idx_campaign_assets_campaign_path on campaign_assets (campaign_id, asset_path)"
                )
                current_version = 4
                _upsert_app_meta_value(cur, "schema_version", current_version, Jsonb)
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


def postgres_get_app_meta_value(key: str, default: Any = None) -> Any:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute("select value_json from app_meta where key = %s", (key,))
        row = cur.fetchone()
        if row is None:
            return default
        return _app_meta_value(row)


def postgres_set_app_meta_value(key: str, value: Any) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    with postgres_connection() as conn, conn.cursor() as cur:
        _upsert_app_meta_value(cur, key, value, Jsonb)


def _campaign_row_to_config(row: dict[str, Any]) -> dict[str, Any]:
    config = {key: row.get(key) for key in CAMPAIGN_COLUMNS}
    config.update(
        {
            "extra_queries": row.get("extra_queries_json") or [],
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
            insert into campaigns ({", ".join(CAMPAIGN_COLUMNS)}, extra_queries_json, hooks_library_json, template_overrides_json, updated_at)
            values ({", ".join(["%s"] * len(CAMPAIGN_COLUMNS))}, %s, %s, %s, now())
            on conflict (id) do update set
                {", ".join(f"{column} = excluded.{column}" for column in CAMPAIGN_COLUMNS if column != "id")},
                extra_queries_json = excluded.extra_queries_json,
                hooks_library_json = excluded.hooks_library_json,
                template_overrides_json = excluded.template_overrides_json,
                updated_at = now()
            """,
            (
                *values,
                Jsonb(normalized.get("extra_queries") or []),
                Jsonb(normalized.get("hooks_library_json") or {}),
                Jsonb(normalized.get("template_overrides_json") or {}),
            ),
        )
    return normalized


def postgres_upsert_campaign_asset(
    campaign_id: str,
    asset_path: str | Path,
    data: bytes,
    *,
    content_type: str = "",
) -> str:
    ensure_postgres_ready()
    normalized_path = normalize_asset_path(asset_path)
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise ValueError("Asset data is required.")
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into campaign_assets (campaign_id, asset_path, content_type, data_bytes, updated_at)
            values (%s, %s, %s, %s, now())
            on conflict (campaign_id, asset_path) do update set
                content_type = excluded.content_type,
                data_bytes = excluded.data_bytes,
                updated_at = now()
            """,
            (campaign_id, normalized_path, str(content_type or "").strip(), bytes(data)),
        )
    return normalized_path


def postgres_get_campaign_asset(campaign_id: str, asset_path: str | Path) -> dict[str, Any] | None:
    ensure_postgres_ready()
    normalized_path = normalize_asset_path(asset_path)
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select campaign_id, asset_path, content_type, data_bytes, updated_at
            from campaign_assets
            where campaign_id = %s and asset_path = %s
            limit 1
            """,
            (campaign_id, normalized_path),
        )
        row = cur.fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["data_bytes"] = bytes(payload.get("data_bytes") or b"")
    return payload


def postgres_list_campaign_assets(campaign_id: str, *, prefix: str = "") -> list[dict[str, Any]]:
    ensure_postgres_ready()
    normalized_prefix = normalize_asset_path(prefix) if prefix else ""
    params: list[Any] = [campaign_id]
    where_sql = "where campaign_id = %s"
    if normalized_prefix:
        where_sql += " and asset_path like %s"
        params.append(f"{normalized_prefix.rstrip('/')}/%")
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            select asset_path, content_type, octet_length(data_bytes) as size_bytes, updated_at
            from campaign_assets
            {where_sql}
            order by updated_at desc, asset_path asc
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


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


def _apply_effective_draft_stale(payload: dict[str, Any], draft_version: str) -> dict[str, Any]:
    has_drafts = any((payload.get(field) or "").strip() for field in ("Email_Draft", "WhatsApp_Draft"))
    stored_stale = (payload.get("Draft_Stale") or "").strip() == "1"
    row_draft_version = (payload.get("Draft_Config_Version") or "").strip()
    version_stale = has_drafts and row_draft_version and row_draft_version != draft_version
    payload["Draft_Stale"] = "1" if has_drafts and (stored_stale or version_stale) else "0"
    return payload


def _lead_row_to_payload(
    row: dict[str, Any],
    *,
    include_campaign_id: bool = False,
    draft_version: str = "",
) -> dict[str, Any]:
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
    if draft_version:
        _apply_effective_draft_stale(payload, draft_version)
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


def postgres_load_template_refresh_candidates(
    campaign_id: str,
    *,
    template_keys: list[str] | None = None,
    stale_only: bool = False,
) -> list[dict[str, Any]]:
    legacy_default_categories = {"kein_seo", "kein_kontakt", "veraltet", "not_ranked", "kein_design", "kein_ssl"}
    where_clauses = [
        "status = any(%s)",
        _lead_has_saved_drafts_sql(),
    ]
    params: list[Any] = [["new", "draft_ready"]]

    if stale_only:
        where_clauses.append("coalesce(payload ->> 'Draft_Stale', '0') = '1'")

    template_values = {value for value in (template_keys or []) if value}
    if "default" in template_values:
        template_values.update(legacy_default_categories)
    template_values = sorted(template_values)
    if template_values:
        where_clauses.append(
            """
            (
                coalesce(payload ->> 'Template_Used', '') = any(%s)
                or coalesce(payload ->> 'Template_Used', '') = ''
            )
            """
        )
        params.append(template_values)

    rows = _postgres_load_full_lead_rows(campaign_id, where_sql=" and ".join(where_clauses), params=tuple(params))
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
    draft_version = _campaign_draft_version(campaign_id)
    draft_stale_sql = _draft_stale_sql()
    company_key_sql = _normalized_company_sql("queue_leads.payload")
    blacklisted_company_sql = _normalized_company_sql("blacklisted.payload")
    query = """
        select lead_id,
               coalesce(queue_leads.payload ->> 'Unternehmen', '') as company,
               priority,
               coalesce(queue_leads.payload ->> 'Analyzed_At', '') as analyzed_at,
               coalesce(queue_leads.payload ->> 'Research_Stale', '0') as research_stale
        from leads queue_leads
        where queue_leads.campaign_id = %s
          and status = 'draft_ready'
          and not {draft_stale_sql}
          and not exists (
                select 1
                from leads blacklisted
                where blacklisted.campaign_id = queue_leads.campaign_id
                  and blacklisted.status = 'blacklist'
                  and {blacklisted_company_sql} = {company_key_sql}
          )
        order by priority asc, analyzed_at asc, lead_id asc
    """.format(
        draft_stale_sql=draft_stale_sql.replace("payload", "queue_leads.payload"),
        company_key_sql=company_key_sql,
        blacklisted_company_sql=blacklisted_company_sql,
    )
    params: list[Any] = [campaign_id, draft_version]
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
    draft_version = _campaign_draft_version(campaign_id)
    rows = _postgres_load_full_lead_rows(
        campaign_id,
        where_sql="""
            status = 'approved'
            and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
            and (
                coalesce(payload ->> 'Email_Draft', '') <> ''
                or coalesce(payload ->> 'WhatsApp_Draft', '') <> ''
            )
        """,
    )
    leads = [_lead_row_to_payload(row, draft_version=draft_version) for row in rows]
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
    draft_version = _campaign_draft_version(campaign_id)
    draft_stale_sql = _draft_stale_sql()
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
               case when {draft_stale_sql} then '1' else '0' end as draft_stale,
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
        cur.execute(query, tuple([draft_version, *params, page_size, offset]))
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
    draft_version = _campaign_draft_version(campaign_id)
    draft_stale_sql = _draft_stale_sql()
    company_key_sql = _normalized_company_sql()
    blacklisted_company_sql = _normalized_company_sql("blacklisted.payload")
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
    if not search_text and "blacklist" not in status_values:
        where_clauses.append(
            """
            not exists (
                select 1
                from leads blacklisted
                where blacklisted.campaign_id = leads.campaign_id
                  and blacklisted.status = 'blacklist'
                  and {blacklisted_company_sql} = {company_key_sql}
            )
            """.format(
                blacklisted_company_sql=blacklisted_company_sql,
                company_key_sql=company_key_sql,
            )
        )
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
        where_clauses.append(draft_stale_sql)
        params.append(draft_version)
    elif stale == "Research stale":
        where_clauses.append("coalesce(payload ->> 'Research_Stale', '0') = '1'")
    elif stale == "Fresh only":
        where_clauses.append(f"not {draft_stale_sql}")
        params.append(draft_version)
        where_clauses.append("coalesce(payload ->> 'Research_Stale', '0') <> '1'")

    where_sql = " and ".join(where_clauses)
    count_query = f"select count(*) as count from leads where {where_sql}"
    query = f"""
        select lead_id,
               coalesce(payload ->> 'Unternehmen', '') as company,
               status,
               priority,
               case when {draft_stale_sql} then '1' else '0' end as draft_stale,
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
        cur.execute(query, tuple([draft_version, *params, page_size, offset]))
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
    draft_version = _campaign_draft_version(campaign_id)
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
            )
        """,
    )
    return [_lead_row_to_payload(row, draft_version=draft_version) for row in rows]


def postgres_load_lead_metrics(campaign_id: str) -> dict[str, int]:
    ensure_postgres_ready()
    draft_version = _campaign_draft_version(campaign_id)
    draft_stale_sql = _draft_stale_sql()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select
                count(*) as total_leads,
                count(*) filter (where status = 'draft_ready') as draft_ready,
                count(*) filter (
                    where status = 'approved'
                      and coalesce(payload ->> 'Drafts_Approved', '0') = '1'
                      and not {draft_stale_sql}
                ) as approved_fresh,
                count(*) filter (where {draft_stale_sql}) as draft_stale,
                count(*) filter (where coalesce(payload ->> 'Research_Stale', '0') = '1') as research_stale
            from leads
            where campaign_id = %s
            """.format(draft_stale_sql=draft_stale_sql),
            (draft_version, draft_version, campaign_id),
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
    draft_version = _campaign_draft_version(campaign_id)
    draft_stale_sql = _draft_stale_sql()
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
                    or {draft_stale_sql}
                  )
              and status not in ('done', 'won', 'lost', 'blacklist')
            order by priority asc, lead_id asc
            """.format(draft_stale_sql=draft_stale_sql),
            (campaign_id, draft_version),
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


def _mail_event_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.now().astimezone()
    parsed = _parse_schedule_timestamp(text.replace(" ", "T")) if " " in text else _parse_schedule_timestamp(text)
    return parsed or datetime.now().astimezone()


def _mail_status_rank(status: str) -> int:
    normalized = (status or "").strip().lower()
    return {
        "sent": 0,
        "unknown": 1,
        "delivered": 2,
        "failed": 3,
        "replied": 4,
    }.get(normalized, 0)


def _mail_status_from_event_type(event_type: str) -> str:
    normalized = (event_type or "").strip().lower()
    if normalized in {"bounce_hard", "bounce_soft"}:
        return "failed"
    if normalized == "reply":
        return "replied"
    if normalized == "delivery_notice":
        return "delivered"
    return "unknown"


def _apply_outbound_status_update(
    cur: Any,
    *,
    smtp_message_id: str,
    event_type: str,
    event_at: Any,
    reason: str = "",
) -> None:
    mail_status = _mail_status_from_event_type(event_type)
    if not smtp_message_id or not mail_status:
        return

    event_timestamp = _mail_event_timestamp(event_at)
    cur.execute(
        """
        select status, last_event_at
        from outbound_emails
        where smtp_message_id = %s
        """,
        (smtp_message_id,),
    )
    row = cur.fetchone() or {}
    current_status = row.get("status") or "sent"
    current_event_at = row.get("last_event_at")
    should_update = _mail_status_rank(mail_status) > _mail_status_rank(current_status)
    if not should_update and _mail_status_rank(mail_status) == _mail_status_rank(current_status):
        should_update = current_event_at is None or event_timestamp >= current_event_at
    if not should_update:
        return

    cur.execute(
        """
        update outbound_emails
        set status = %s,
            status_reason = %s,
            last_event_type = %s,
            last_event_at = %s,
            updated_at = now()
        where smtp_message_id = %s
        """,
        (mail_status, reason.strip(), event_type.strip(), event_timestamp, smtp_message_id),
    )


def _insert_outbound_email_row(cur: Any, campaign_id: str, lead_id: str, outbound_email: dict[str, Any]) -> None:
    smtp_message_id = str(outbound_email.get("smtp_message_id") or "").strip()
    if not smtp_message_id:
        return

    cur.execute(
        """
        insert into outbound_emails (
            campaign_id, lead_id, smtp_message_id, recipient_email, subject,
            sent_at, source, status, status_reason, created_at, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, 'sent', '', now(), now())
        on conflict (smtp_message_id) do update
        set campaign_id = excluded.campaign_id,
            lead_id = excluded.lead_id,
            recipient_email = case
                when excluded.recipient_email <> '' then excluded.recipient_email
                else outbound_emails.recipient_email
            end,
            subject = case
                when excluded.subject <> '' then excluded.subject
                else outbound_emails.subject
            end,
            sent_at = excluded.sent_at,
            source = excluded.source,
            updated_at = now()
        """,
        (
            campaign_id,
            lead_id,
            smtp_message_id,
            str(outbound_email.get("recipient_email") or "").strip(),
            str(outbound_email.get("subject") or "").strip(),
            _mail_event_timestamp(outbound_email.get("sent_at") or datetime.now().astimezone()),
            str(outbound_email.get("source") or "app").strip() or "app",
        ),
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
    outbound_email: dict[str, Any] | None = None,
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
        if outbound_email:
            _insert_outbound_email_row(cur, campaign_id, lead_id, outbound_email)


def postgres_record_outbound_email(campaign_id: str, lead_id: str, outbound_email: dict[str, Any]) -> None:
    ensure_postgres_ready()
    lead_id = str(lead_id or "").strip()
    if not lead_id:
        raise ValueError("Lead ID is required for postgres_record_outbound_email().")
    with postgres_connection() as conn, conn.cursor() as cur:
        _insert_outbound_email_row(cur, campaign_id, lead_id, outbound_email)


def postgres_recent_outbound_emails(since_at: Any, *, campaign_id: str = "") -> list[dict[str, Any]]:
    ensure_postgres_ready()
    where_clauses = ["sent_at >= %s"]
    params: list[Any] = [_mail_event_timestamp(since_at)]
    if campaign_id:
        where_clauses.append("campaign_id = %s")
        params.append(campaign_id)
    query = f"""
        select campaign_id, lead_id, smtp_message_id, recipient_email, subject, sent_at,
               status, status_reason, last_event_type, last_event_at, last_sync_at
        from outbound_emails
        where {' and '.join(where_clauses)}
        order by sent_at desc, smtp_message_id desc
    """
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(query, tuple(params))
        return cur.fetchall()


def postgres_record_mailbox_event(event: dict[str, Any]) -> None:
    ensure_postgres_ready()
    _, _, Jsonb = _load_psycopg()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into mailbox_events (
                campaign_id, lead_id, folder_name, mailbox_uid, event_at, event_type,
                from_address, subject, raw_message_id, related_smtp_message_id, reason,
                matched, metadata_json, created_at, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (folder_name, mailbox_uid) do update
            set campaign_id = excluded.campaign_id,
                lead_id = excluded.lead_id,
                event_at = excluded.event_at,
                event_type = excluded.event_type,
                from_address = excluded.from_address,
                subject = excluded.subject,
                raw_message_id = excluded.raw_message_id,
                related_smtp_message_id = excluded.related_smtp_message_id,
                reason = excluded.reason,
                matched = excluded.matched,
                metadata_json = excluded.metadata_json,
                updated_at = now()
            """,
            (
                str(event.get("campaign_id") or "").strip() or None,
                str(event.get("lead_id") or "").strip(),
                str(event.get("folder_name") or "").strip(),
                str(event.get("mailbox_uid") or "").strip(),
                _mail_event_timestamp(event.get("event_at") or datetime.now().astimezone()),
                str(event.get("event_type") or "").strip(),
                str(event.get("from_address") or "").strip(),
                str(event.get("subject") or "").strip(),
                str(event.get("raw_message_id") or "").strip(),
                str(event.get("related_smtp_message_id") or "").strip(),
                str(event.get("reason") or "").strip(),
                bool(event.get("matched")),
                Jsonb(event.get("metadata") if isinstance(event.get("metadata"), dict) else {}),
            ),
        )
        if event.get("matched") and event.get("related_smtp_message_id"):
            _apply_outbound_status_update(
                cur,
                smtp_message_id=str(event.get("related_smtp_message_id") or "").strip(),
                event_type=str(event.get("event_type") or "").strip(),
                event_at=event.get("event_at") or datetime.now().astimezone(),
                reason=str(event.get("reason") or "").strip(),
            )


def postgres_mark_outbound_unknown(since_at: Any, *, campaign_id: str = "", synced_at: Any | None = None) -> int:
    ensure_postgres_ready()
    where_clauses = [
        "sent_at >= %s",
        "status in ('sent', 'unknown')",
    ]
    params: list[Any] = [
        _mail_event_timestamp(since_at),
        _mail_event_timestamp(synced_at or datetime.now().astimezone()),
    ]
    if campaign_id:
        where_clauses.append("campaign_id = %s")
        params.append(campaign_id)
    query = f"""
        update outbound_emails
        set status = 'unknown',
            last_sync_at = %s,
            updated_at = now()
        where {' and '.join(where_clauses)}
    """
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(query, tuple([params[1], params[0], *params[2:]]))
        return cur.rowcount or 0


def postgres_load_mail_summary(campaign_id: str, *, lookback_hours: int = 24) -> dict[str, Any]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select count(*) as total,
                   count(*) filter (where status = 'failed') as failed,
                   count(*) filter (where status = 'replied') as replied,
                   count(*) filter (where status = 'delivered') as delivered,
                   count(*) filter (where status = 'unknown') as unknown,
                   count(*) filter (where status = 'sent') as sent
            from outbound_emails
            where campaign_id = %s
              and sent_at >= now() - (%s * interval '1 hour')
            """,
            (campaign_id, max(1, int(lookback_hours or 24))),
        )
        row = cur.fetchone() or {}
    return {
        "total": int(row.get("total") or 0),
        "failed": int(row.get("failed") or 0),
        "replied": int(row.get("replied") or 0),
        "delivered": int(row.get("delivered") or 0),
        "unknown": int(row.get("unknown") or 0),
        "sent": int(row.get("sent") or 0),
        "last_sync_at": str(postgres_get_app_meta_value("mailbox_last_sync_at", "") or ""),
    }


def postgres_load_recent_mail_rows(
    campaign_id: str,
    *,
    lookback_hours: int = 24,
    limit: int = 200,
) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select outbound.smtp_message_id,
                   outbound.lead_id,
                   coalesce(leads.payload ->> 'Unternehmen', '') as company,
                   outbound.recipient_email,
                   outbound.subject,
                   outbound.sent_at,
                   outbound.status,
                   outbound.status_reason,
                   outbound.last_event_type,
                   outbound.last_event_at,
                   outbound.last_sync_at,
                   latest.from_address as latest_from_address,
                   latest.subject as latest_event_subject,
                   latest.reason as latest_event_reason
            from outbound_emails outbound
            left join leads
              on leads.campaign_id = outbound.campaign_id
             and leads.lead_id = outbound.lead_id
            left join lateral (
                select from_address, subject, reason
                from mailbox_events
                where related_smtp_message_id = outbound.smtp_message_id
                order by event_at desc, id desc
                limit 1
            ) latest on true
            where outbound.campaign_id = %s
              and outbound.sent_at >= now() - (%s * interval '1 hour')
            order by outbound.sent_at desc, outbound.smtp_message_id desc
            limit %s
            """,
            (campaign_id, max(1, int(lookback_hours or 24)), max(1, int(limit or 200))),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def postgres_load_mail_events_for_message(campaign_id: str, smtp_message_id: str) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select campaign_id, lead_id, folder_name, mailbox_uid, event_at, event_type,
                   from_address, subject, raw_message_id, related_smtp_message_id,
                   reason, matched, metadata_json
            from mailbox_events
            where related_smtp_message_id = %s
              and (%s = '' or campaign_id = %s)
            order by event_at desc, id desc
            """,
            (smtp_message_id.strip(), campaign_id, campaign_id),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def postgres_load_unmatched_mailbox_events(*, lookback_hours: int = 24, limit: int = 50) -> list[dict[str, Any]]:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select folder_name, mailbox_uid, event_at, event_type, from_address,
                   subject, raw_message_id, reason, metadata_json
            from mailbox_events
            where matched = false
              and event_at >= now() - (%s * interval '1 hour')
            order by event_at desc, id desc
            limit %s
            """,
            (max(1, int(lookback_hours or 24)), max(1, int(limit or 50))),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def postgres_get_lead_mail_status(campaign_id: str, lead_id: str) -> dict[str, Any] | None:
    ensure_postgres_ready()
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select smtp_message_id, recipient_email, subject, sent_at, status,
                   status_reason, last_event_type, last_event_at, last_sync_at
            from outbound_emails
            where campaign_id = %s
              and lead_id = %s
            order by sent_at desc, id desc
            limit 1
            """,
            (campaign_id, lead_id),
        )
        row = cur.fetchone()
    return dict(row) if row else None


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

        for asset_dir in (layout["assets_dir"], layout["portfolio_dir"]):
            if not asset_dir.exists():
                continue
            for asset_path in sorted(path for path in asset_dir.rglob("*") if path.is_file()):
                relative_path = normalize_asset_path(asset_path)
                guessed_type = mimetypes.guess_type(asset_path.name)[0] or ""
                postgres_upsert_campaign_asset(
                    config["id"],
                    relative_path,
                    asset_path.read_bytes(),
                    content_type=guessed_type,
                )

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


def postgres_restale_template_lead(campaign_id: str) -> bool:
    """Reset the -0000 template lead to a clean stale state for testing.

    Sets status=new, clears contact history, and pins Draft_Config_Version to '0'
    so the lead is always draft-stale regardless of campaign version.
    Returns True if the lead was found and updated, False if it didn't exist.
    """
    ensure_postgres_ready()
    campaign = postgres_get_campaign(campaign_id)
    id_prefix = str(campaign.get("id_prefix") or "LEAD").strip()
    lead_id = f"{id_prefix}-0000"
    reset_patch = {
        "Status": "new",
        "Draft_Stale": "1",
        "Research_Stale": "1",
        "Draft_Config_Version": "0",
        "Last_Contact_Date": "",
        "Next_Action_Date": "",
        "Next_Action_Type": "none",
        "Channel_Used": "",
        "Contact_Log": "",
        "Contact_Count": "0",
        "Sent_At": "",
        "SMTP_Message_ID": "",
        "Drafts_Approved": "0",
    }
    import json as _json

    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE leads
               SET status = 'new',
                   next_action_date = NULL,
                   payload = payload || %s::jsonb,
                   updated_at = now()
             WHERE lead_id = %s AND campaign_id = %s
            """,
            (_json.dumps(reset_patch), lead_id, campaign_id),
        )
        updated = cur.rowcount > 0
        conn.commit()
    return updated
