"""
app.py - Streamlit CRM frontend with saved multi-niche campaigns.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.parse
from copy import deepcopy
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

import crm_backend as backend
from campaign_service import (
    create_campaign,
    get_active_campaign,
    get_campaign,
    get_hooks_library_path,
    get_template_overrides_path,
    list_campaigns,
    mark_campaign_stage_run,
    resolve_csv_path,
    set_active_campaign,
    update_campaign,
)
from crm_mailer import format_phone_e164, send_email
from crm_scrape import scrape_campaign
from crm_store import (
    ALL_COLUMNS,
    TERMINAL_STATUSES,
    available_channels,
    get_bezirk,
    get_lead_by_id,
    load_leads,
    planned_channel,
    preferred_channel,
    save_lead,
    save_leads,
)
from crm_templates import (
    build_template_editor_change_scope,
    build_template_editor_snapshot,
    compose_email_draft,
    get_effective_hooks_library,
    get_effective_special_subject_option,
    get_effective_subject_templates,
    get_effective_templates,
    get_subject_options,
    get_template_override_payload,
    invalidate_campaign_copy_cache,
    mark_template_editor_pending_drafts_stale,
    parse_email_draft,
    refresh_targeted_pending_drafts,
)
from crm_schedule import clear_scheduled_send, queue_scheduled_email, scheduled_send_label
from crm_tracker import ARCHIVE_AFTER_DAYS, apply_contact_outcome, check_and_archive_stale, parse_contact_log


try:
    from crm_store import set_stored_draft_stale
except ImportError:
    def set_stored_draft_stale(lead: dict, is_stale: bool) -> None:
        flag = "1" if is_stale else "0"
        lead["Draft_Stale"] = flag
        lead["_Stored_Draft_Stale"] = flag


st.set_page_config(
    page_title="Campaign CRM",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded",
)


PRIORITY_COLOR = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🟢", 5: "⚫"}
CHANNEL_EMOJI = {"email": "📧", "phone": "📞", "whatsapp": "💬", "none": "⛔", "": "⛔"}
CHANNEL_LABELS = {"email": "📧 Email", "whatsapp": "💬 WhatsApp", "phone": "📞 Phone call", "none": "⛔ None"}
STATUS_EMOJI = {
    "new": "🆕", "draft_ready": "✍️", "approved": "✅",
    "contacted": "📤", "replied": "💬", "meeting_scheduled": "📅",
    "won": "🏆", "lost": "❌", "done": "🏁", "no_contact": "👻", "blacklist": "🚫",
}
ALL_LEADS_PAGE_SIZE_OPTIONS = [10, 25, 50]
ALL_LEADS_STATUS_OPTIONS = [
    "new",
    "draft_ready",
    "approved",
    "contacted",
    "replied",
    "meeting_scheduled",
    "done",
    "won",
    "lost",
    "no_contact",
    "blacklist",
]
ALL_LEADS_CHANNEL_OPTIONS = ["email", "whatsapp", "phone", "none"]
ALL_LEADS_PRIORITY_OPTIONS = ["1", "2", "3", "4", "5"]
REVIEW_QUEUE_PAGE_SIZE = 100
OUTREACH_PAGE_SIZE = 100
TEMPLATE_EDITOR_NOTICE_KEY = "template_editor_notice"
TEMPLATE_PLACEHOLDER_HELP = [
    ("hook", "The selected hook sentence(s) for this lead."),
    ("subject", "The auto-selected email subject from the shared subject library."),
    ("urgency", "An urgency line from GPT; usually blank in template-only mode."),
    ("salutation", "Formal greeting, for example 'Guten Tag Herr Muster,' or 'Sehr geehrte Damen und Herren,'."),
    ("contact", "Best direct contact label for the lead."),
    ("subject_intro", "Short direct prefix for subject lines, for example 'Herr Muster, '."),
    ("subject_name", "Short contact/company label for subject lines."),
    ("company", "The lead's company name."),
    ("price", "Lead-specific price if set, otherwise the campaign default price."),
    ("sender_name", "Sender name from Campaign Config."),
    ("sender_company", "Sender company from Campaign Config."),
    ("sender_company_signature", "Sender company line for signatures; blank when it would duplicate the sender name."),
    ("sender_company_phone", "Optional 'von <company>' suffix used in phone intros."),
    ("sender_website", "Sender website from Campaign Config."),
    ("sender_phone", "Sender phone from Campaign Config, including the line break used in signatures."),
    ("sender_email", "Sender email from Campaign Config, including the line break used in signatures."),
    ("rank_keyword", "Search phrase for this lead, for example 'Installateur 1140'."),
    ("rank_keyword_district", "District/location portion of the rank keyword, for example '1140' or 'Wien'."),
    ("competitors_line", "Short competitor list formatted for inline sentences."),
    ("competitors_short", "First competitor name only."),
    ("competitors_raw", "Raw competitor list joined by ' | '."),
    ("rating", "Google rating for the lead when available."),
    ("review_count", "Google review count for the lead when available."),
]


def _clear_cached_result(cache_func, *args) -> None:
    if args:
        cache_func.clear(*args)
        return
    cache_func.clear()


def invalidate_lead_cache(campaign_id: str) -> None:
    if campaign_id:
        _clear_cached_result(cached_leads, campaign_id)
        _clear_cached_result(cached_lead)
        _clear_cached_result(cached_campaign_metrics, campaign_id)
        _clear_cached_result(cached_dashboard_snapshot, campaign_id)
        _clear_cached_result(cached_review_queue)
        _clear_cached_result(cached_outreach_leads, campaign_id)
        _clear_cached_result(cached_outreach_summary)
        _clear_cached_result(cached_outreach_counts, campaign_id)
        _clear_cached_result(cached_all_leads_summary)
        _clear_cached_result(cached_recontact_leads, campaign_id)
        return
    _clear_cached_result(cached_leads)
    _clear_cached_result(cached_lead)
    _clear_cached_result(cached_campaign_metrics)
    _clear_cached_result(cached_dashboard_snapshot)
    _clear_cached_result(cached_review_queue)
    _clear_cached_result(cached_outreach_leads)
    _clear_cached_result(cached_outreach_summary)
    _clear_cached_result(cached_outreach_counts)
    _clear_cached_result(cached_all_leads_summary)
    _clear_cached_result(cached_recontact_leads)


def invalidate_campaign_cache(campaign_id: str = "") -> None:
    _clear_cached_result(cached_campaigns)
    _clear_cached_result(cached_active_campaign)
    if campaign_id:
        _clear_cached_result(cached_campaign, campaign_id)
        invalidate_lead_cache(campaign_id)
        return
    _clear_cached_result(cached_campaign)
    invalidate_lead_cache("")


def reload(*, campaign_id: str = "", campaign_changed: bool = False) -> None:
    if campaign_changed:
        invalidate_campaign_cache(campaign_id)
    elif campaign_id:
        invalidate_lead_cache(campaign_id)
    st.rerun()


def _set_template_editor_notice(level: str, message: str) -> None:
    st.session_state[TEMPLATE_EDITOR_NOTICE_KEY] = {"level": level, "message": message}


def _render_template_editor_notice() -> None:
    notice = st.session_state.pop(TEMPLATE_EDITOR_NOTICE_KEY, None)
    if not isinstance(notice, dict):
        return
    level = str(notice.get("level") or "info").strip()
    message = str(notice.get("message") or "").strip()
    if not message:
        return
    renderer = getattr(st, level, st.info)
    renderer(message)


def _render_template_placeholder_reference() -> None:
    with st.expander("Placeholder Reference", expanded=False):
        st.caption("These are the live placeholders available in subjects, hooks, and templates.")
        for placeholder, description in TEMPLATE_PLACEHOLDER_HELP:
            st.markdown(f"- `{{{{{placeholder}}}}}`: {description}")


def _humanize_template_editor_key(value: str) -> str:
    return (value or "").replace("_", " ").strip().title()


def _join_template_editor_labels(labels: list[str]) -> str:
    cleaned = [label for label in labels if label]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def _describe_template_editor_change(
    *,
    change_type: str,
    before_snapshot: dict[str, object],
    after_snapshot: dict[str, object],
    selected_template_key: str = "",
) -> str:
    if change_type == "subjects":
        return "subject settings"

    if change_type == "template":
        label = _humanize_template_editor_key(selected_template_key)
        return f"{label} template" if label else "template copy"

    before_hooks = before_snapshot.get("hooks") if isinstance(before_snapshot.get("hooks"), dict) else {}
    after_hooks = after_snapshot.get("hooks") if isinstance(after_snapshot.get("hooks"), dict) else {}
    changed_categories = sorted(
        _humanize_template_editor_key(key)
        for key in set(before_hooks) | set(after_hooks)
        if before_hooks.get(key, []) != after_hooks.get(key, [])
    )
    if not changed_categories:
        return "hook library"
    if len(changed_categories) <= 3:
        return f"{_join_template_editor_labels(changed_categories)} hooks"
    return (
        f"{_join_template_editor_labels(changed_categories[:2])}, and "
        f"{len(changed_categories) - 2} more hook categories"
    )


@st.cache_data(ttl=300)
def cached_campaigns() -> list[dict]:
    return list_campaigns()


@st.cache_data(ttl=300)
def cached_campaign(campaign_id: str) -> dict:
    return get_campaign(campaign_id)


@st.cache_data(ttl=300)
def cached_active_campaign() -> dict:
    return get_active_campaign()


@st.cache_data(ttl=300)
def cached_leads(campaign_id: str) -> list[dict]:
    campaign = cached_campaign(campaign_id)
    return load_leads(campaign=campaign)


@st.cache_data(ttl=300)
def cached_lead(campaign_id: str, lead_id: str) -> dict | None:
    campaign = cached_campaign(campaign_id)
    return get_lead_by_id(lead_id, campaign=campaign)


@st.cache_data(ttl=300)
def cached_campaign_metrics(campaign_id: str) -> dict[str, int]:
    if backend.is_postgres_backend():
        metric_loader = getattr(backend, "postgres_load_lead_metrics", None)
        if callable(metric_loader):
            return metric_loader(campaign_id)

    campaign = cached_campaign(campaign_id)
    leads = load_leads(campaign=campaign)
    counts = dict(_campaign_counts(leads))
    counts["total_leads"] = len(leads)
    return counts


@st.cache_data(ttl=300)
def cached_dashboard_snapshot(campaign_id: str) -> dict:
    if backend.is_postgres_backend():
        snapshot_loader = getattr(backend, "postgres_load_dashboard_snapshot", None)
        if callable(snapshot_loader):
            return snapshot_loader(campaign_id, date.today().isoformat())

    campaign = cached_campaign(campaign_id)
    leads = load_leads(campaign=campaign)
    today = date.today().isoformat()
    return {
        "status_counts": dict(_campaign_counts(leads)),
        "pending_drafts": [
            {"ID": lead.get("ID", ""), "Unternehmen": lead.get("Unternehmen", "")}
            for lead in leads
            if lead.get("Website_Category")
            and (not lead.get("Analyzed_At") or lead.get("Draft_Stale") == "1")
            and lead.get("Status") not in TERMINAL_STATUSES
        ],
        "actionable": [
            {
                "ID": lead.get("ID", ""),
                "Unternehmen": lead.get("Unternehmen", ""),
                "Priority": lead.get("Priority", "5"),
                "Next_Action_Date": lead.get("Next_Action_Date", ""),
                "Next_Action_Type": lead.get("Next_Action_Type", "none"),
            }
            for lead in leads
            if lead.get("Status") not in TERMINAL_STATUSES
            and lead.get("Status") != "no_contact"
            and (lead.get("Scheduled_Send_Status") or "").strip() != "queued"
            and lead.get("Next_Action_Type", "none") not in ("none", "")
            and (not lead.get("Next_Action_Date") or lead.get("Next_Action_Date") <= today)
        ],
    }


@st.cache_data(ttl=300)
def cached_review_queue(campaign_id: str, page: int, page_size: int) -> dict:
    if backend.is_postgres_backend():
        queue_loader = getattr(backend, "postgres_load_review_queue_summary", None)
        if callable(queue_loader):
            offset = max(0, (page - 1) * page_size)
            rows = queue_loader(campaign_id, limit=page_size + 1, offset=offset)
            return {
                "items": rows[:page_size],
                "has_more": len(rows) > page_size,
            }

    queue = [
        {
            "ID": lead.get("ID", ""),
            "Unternehmen": lead.get("Unternehmen", ""),
            "Priority": lead.get("Priority", "5"),
            "Analyzed_At": lead.get("Analyzed_At", ""),
            "Research_Stale": lead.get("Research_Stale", "0"),
        }
        for lead in cached_leads(campaign_id)
        if lead.get("Status") == "draft_ready" and lead.get("Draft_Stale") != "1"
    ]
    queue.sort(key=lambda lead: (int(lead.get("Priority") or 5), lead.get("Analyzed_At") or "", lead.get("ID") or ""))
    offset = max(0, (page - 1) * page_size)
    page_rows = queue[offset:offset + page_size + 1]
    return {
        "items": page_rows[:page_size],
        "has_more": len(page_rows) > page_size,
    }


@st.cache_data(ttl=300)
def cached_outreach_leads(campaign_id: str) -> list[dict]:
    if backend.is_postgres_backend():
        lead_loader = getattr(backend, "postgres_load_outreach_leads", None)
        if callable(lead_loader):
            return lead_loader(campaign_id)

    leads = cached_leads(campaign_id)
    approved = [
        lead for lead in leads
        if lead.get("Status") == "approved"
        and lead.get("Drafts_Approved") == "1"
        and any((lead.get(field) or "").strip() for field in ("Email_Draft", "WhatsApp_Draft", "Phone_Script"))
    ]
    approved.sort(key=lambda lead: (int(lead.get("Priority") or 5), lead.get("Unternehmen") or ""))
    return approved


def _outreach_summary_item(lead: dict) -> dict:
    return {
        "ID": lead.get("ID", ""),
        "Unternehmen": lead.get("Unternehmen", ""),
        "Status": lead.get("Status", "approved"),
        "Priority": lead.get("Priority", "5"),
        "Draft_Stale": lead.get("Draft_Stale", "0"),
        "Email": lead.get("Email", ""),
        "TelNr": lead.get("TelNr", ""),
        "Preferred_Channel": lead.get("Preferred_Channel", ""),
        "Next_Action_Type": lead.get("Next_Action_Type", ""),
        "Channel_Used": lead.get("Channel_Used", ""),
        "Has_Email_Draft": bool((lead.get("Email_Draft") or "").strip()),
        "Scheduled_Send_At": lead.get("Scheduled_Send_At", ""),
        "Scheduled_Send_Status": lead.get("Scheduled_Send_Status", ""),
        "Scheduled_Send_Error": lead.get("Scheduled_Send_Error", ""),
        "Sent_At": lead.get("Sent_At", ""),
    }


def _outreach_summary_matches(item: dict, search: str) -> bool:
    query = (search or "").strip().lower()
    if not query:
        return True
    return query in (item.get("ID") or "").lower() or query in (item.get("Unternehmen") or "").lower()


def _all_leads_summary_item(lead: dict) -> dict:
    return {
        "ID": lead.get("ID", ""),
        "Unternehmen": lead.get("Unternehmen", ""),
        "Status": lead.get("Status", "new"),
        "Priority": lead.get("Priority", "5"),
        "Draft_Stale": lead.get("Draft_Stale", "0"),
        "Research_Stale": lead.get("Research_Stale", "0"),
        "Email": lead.get("Email", ""),
        "TelNr": lead.get("TelNr", ""),
        "Kontaktname": lead.get("Kontaktname", ""),
        "Adresse": lead.get("Adresse", ""),
        "Planned_Channel": planned_channel(lead),
    }


def _all_leads_summary_matches(item: dict, search: str) -> bool:
    query = (search or "").strip().lower()
    if not query:
        return True
    return any(
        query in (item.get(field) or "").lower()
        for field in ("ID", "Unternehmen", "Adresse", "Kontaktname", "Email")
    )


def _all_leads_summary_filter(
    item: dict,
    statuses: tuple[str, ...],
    channels: tuple[str, ...],
    priorities: tuple[str, ...],
    stale: str,
) -> bool:
    if statuses and item.get("Status", "new") not in statuses:
        return False
    if channels and item.get("Planned_Channel", "none") not in channels:
        return False
    if priorities and item.get("Priority", "5") not in priorities:
        return False
    if stale == "Draft stale" and item.get("Draft_Stale") != "1":
        return False
    if stale == "Research stale" and item.get("Research_Stale") != "1":
        return False
    if stale == "Fresh only" and (item.get("Draft_Stale") == "1" or item.get("Research_Stale") == "1"):
        return False
    return True


def _all_leads_page_label(lead: dict) -> str:
    stale_tags: list[str] = []
    if lead.get("Draft_Stale") == "1":
        stale_tags.append("draft stale")
    if lead.get("Research_Stale") == "1":
        stale_tags.append("research stale")
    stale_suffix = f" | {', '.join(stale_tags)}" if stale_tags else ""
    status = lead.get("Status", "new")
    channel = lead.get("Planned_Channel", "none")
    priority = lead.get("Priority", "5")
    company = lead.get("Unternehmen", "")
    return (
        f"{lead.get('ID', '?')} | {company[:35]} | {STATUS_EMOJI.get(status, '')} {status}"
        f" | {CHANNEL_EMOJI.get(channel, '?')} {channel} | P{priority}{stale_suffix}"
    )


@st.cache_data(ttl=300)
def cached_all_leads_summary(
    campaign_id: str,
    search: str,
    statuses: tuple[str, ...],
    channels: tuple[str, ...],
    priorities: tuple[str, ...],
    stale: str,
    page: int,
    page_size: int,
) -> dict:
    if backend.is_postgres_backend():
        summary_loader = getattr(backend, "postgres_load_all_leads_summary", None)
        if callable(summary_loader):
            return summary_loader(
                campaign_id,
                search=search,
                statuses=list(statuses),
                channels=list(channels),
                priorities=list(priorities),
                stale=stale,
                page=page,
                page_size=page_size,
            )

    items = [_all_leads_summary_item(lead) for lead in cached_leads(campaign_id)]
    filtered = [
        item for item in items
        if _all_leads_summary_matches(item, search)
        and _all_leads_summary_filter(item, statuses, channels, priorities, stale)
    ]
    filtered.sort(key=lambda item: (int(item.get("Priority") or 5), item.get("Unternehmen") or "", item.get("ID") or ""))
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or ALL_LEADS_PAGE_SIZE_OPTIONS[1]))
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": filtered[start:end],
        "total_count": len(filtered),
        "page": page,
        "page_size": page_size,
    }


@st.cache_data(ttl=300)
def cached_outreach_summary(campaign_id: str, search: str, page: int, page_size: int) -> dict:
    if backend.is_postgres_backend():
        summary_loader = getattr(backend, "postgres_load_outreach_summary", None)
        if callable(summary_loader):
            return summary_loader(campaign_id, search=search, page=page, page_size=page_size)

    items = [_outreach_summary_item(lead) for lead in cached_outreach_leads(campaign_id)]
    filtered = [item for item in items if _outreach_summary_matches(item, search)]
    filtered.sort(key=lambda item: (int(item.get("Priority") or 5), item.get("Unternehmen") or "", item.get("ID") or ""))
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or OUTREACH_PAGE_SIZE))
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": filtered[start:end],
        "total_count": len(filtered),
        "page": page,
        "page_size": page_size,
    }


@st.cache_data(ttl=300)
def cached_outreach_counts(campaign_id: str) -> dict[str, int]:
    today = date.today().isoformat()
    if backend.is_postgres_backend():
        counts_loader = getattr(backend, "postgres_load_outreach_counts", None)
        if callable(counts_loader):
            return counts_loader(campaign_id, today)

    approved = cached_outreach_leads(campaign_id)
    all_leads = cached_leads(campaign_id)
    queued = [lead for lead in approved if (lead.get("Scheduled_Send_Status") or "").strip() == "queued"]
    return {
        "approved_total": len(approved),
        "queued_today": sum(1 for lead in queued if (lead.get("Scheduled_Send_At") or "")[:10] == today),
        "queued_later": sum(1 for lead in queued if (lead.get("Scheduled_Send_At") or "")[:10] != today),
        "send_errors": sum(1 for lead in approved if (lead.get("Scheduled_Send_Error") or "").strip()),
        "sent_today": sum(1 for lead in all_leads if (lead.get("Sent_At") or "")[:10] == today),
    }


@st.cache_data(ttl=300)
def cached_recontact_leads(campaign_id: str) -> list[dict]:
    if backend.is_postgres_backend():
        lead_loader = getattr(backend, "postgres_load_recontact_leads", None)
        if callable(lead_loader):
            return lead_loader(campaign_id)

    leads = cached_leads(campaign_id)
    return [
        lead for lead in leads
        if lead.get("Status") not in TERMINAL_STATUSES
        and _has_contact_history(lead)
        and any((lead.get(field) or "").strip() for field in ("Email_Draft", "WhatsApp_Draft", "Phone_Script"))
        and lead.get("Status") != "approved"
    ]


@st.cache_data(ttl=300)
def app_commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def _score_bar(score_str: str) -> str:
    try:
        score = int(score_str)
    except (TypeError, ValueError):
        return "-"
    filled = "█" * score
    empty = "░" * (10 - score)
    return f"{filled}{empty} {score}/10"


def _campaign_state_key(campaign: dict, name: str) -> str:
    return f"{name}_{campaign.get('id', 'default')}"


def _copy_state_default(value):
    return deepcopy(value)


def _next_review_selection(queue_ids: list[str], current_id: str) -> str:
    remaining = [lead_id for lead_id in queue_ids if lead_id != current_id]
    if not remaining:
        return ""
    try:
        current_index = queue_ids.index(current_id)
    except ValueError:
        return remaining[0]
    return remaining[current_index] if current_index < len(remaining) else remaining[-1]


def _channel_label(channel: str) -> str:
    return CHANNEL_LABELS.get(channel, channel)


def _apply_subject_suggestion(key_prefix: str) -> None:
    suggestion = (st.session_state.get(f"{key_prefix}_subject_pick") or "").strip()
    if suggestion:
        st.session_state[f"{key_prefix}_subject_text"] = suggestion


def _render_subject_editor(
    campaign: dict,
    lead: dict,
    *,
    key_prefix: str,
    current_subject: str,
    apply_suggestion_on_submit: bool = False,
) -> str:
    subject_options = get_subject_options(lead, current_subject=current_subject, campaign=campaign)
    text_key = f"{key_prefix}_subject_text"
    pick_key = f"{key_prefix}_subject_pick"
    initial_subject = current_subject or (subject_options[0] if subject_options else "")

    if text_key not in st.session_state:
        st.session_state[text_key] = initial_subject
    if pick_key not in st.session_state or st.session_state[pick_key] not in subject_options:
        st.session_state[pick_key] = current_subject if current_subject in subject_options else initial_subject

    subject = st.text_input(
        "Email subject",
        key=text_key,
        placeholder="Type any custom subject here",
        help="You can type a completely custom subject or pull in one of the quick suggestions below.",
    )
    selected_suggestion = ""
    if subject_options:
        if apply_suggestion_on_submit:
            selected_suggestion = st.selectbox(
                "Quick subject suggestions",
                options=subject_options,
                key=pick_key,
                help="Pick a suggestion and submit to use it, or edit the subject field directly.",
            )
        else:
            st.selectbox(
                "Quick subject suggestions",
                options=subject_options,
                key=pick_key,
                on_change=_apply_subject_suggestion,
                args=(key_prefix,),
                help="Choosing one copies it into the subject field above, which you can still edit afterwards.",
            )

    resolved_subject = subject.strip()
    if (
        apply_suggestion_on_submit
        and selected_suggestion
        and selected_suggestion != resolved_subject
        and resolved_subject == initial_subject.strip()
    ):
        resolved_subject = selected_suggestion.strip()
    return resolved_subject


def _extract_urls(*texts: str) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for text in texts:
        for match in re.findall(r"https?://[^\s<>\"]+", text or ""):
            url = match.rstrip(".,);:]")
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def _render_draft_links(*texts: str) -> None:
    urls = _extract_urls(*texts)
    if not urls:
        return
    st.caption("Links")
    for url in urls:
        st.markdown(f"- [{url}]({url})")


def _write_json_payload(path_value: str, payload: dict) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if payload:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif path.exists():
        path.unlink()


def _build_whatsapp_link(phone: str, draft: str) -> str | None:
    if not (phone or "").strip() or not (draft or "").strip():
        return None
    e164 = format_phone_e164(phone)
    return f"https://wa.me/{e164.lstrip('+')}?text={urllib.parse.quote(draft)}"


def _contact_log_entries(lead: dict) -> list[dict]:
    entries = parse_contact_log(lead.get("Contact_Log", ""))
    if entries:
        return entries

    fallback_at = (lead.get("Last_Contact_Date") or lead.get("Kontaktdatum") or "").strip()
    if not fallback_at:
        return []
    return [
        {
            "at": fallback_at,
            "channel": (lead.get("Channel_Used") or "").strip(),
            "outcome": (lead.get("Status") or "").strip(),
            "notes": "",
        }
    ]


def _first_contact_at(lead: dict) -> str:
    entries = _contact_log_entries(lead)
    timestamps = [entry.get("at", "").strip() for entry in entries if entry.get("at", "").strip()]
    if timestamps:
        return min(timestamps)
    return (lead.get("Kontaktdatum") or lead.get("Last_Contact_Date") or "").strip()


def _first_contact_sort_key(lead: dict) -> tuple[int, str]:
    first_contact = _first_contact_at(lead)
    if not first_contact:
        return (1, "9999-99-99")
    return (0, first_contact[:10])


def _days_since_first_contact(lead: dict) -> int | None:
    first_contact = _first_contact_at(lead)
    if not first_contact:
        return None
    try:
        first_date = date.fromisoformat(first_contact[:10])
    except ValueError:
        return None
    return (date.today() - first_date).days


def _has_contact_history(lead: dict) -> bool:
    if (lead.get("Kontaktdatum") or "").strip():
        return True
    if _contact_log_entries(lead):
        return True
    try:
        return int(lead.get("Contact_Count") or 0) > 0
    except (TypeError, ValueError):
        return False


def _render_contact_log(lead: dict) -> None:
    st.markdown("**Contact Log**")
    entries = _contact_log_entries(lead)
    if not entries:
        st.caption("No contact activity yet.")
        return
    for entry in reversed(entries[-10:]):
        at = entry.get("at") or "-"
        channel = entry.get("channel") or "-"
        outcome = entry.get("outcome") or "-"
        notes = entry.get("notes") or ""
        line = f"- `{at}` | `{channel}` | `{outcome}`"
        if notes:
            line += f" | {notes}"
        st.markdown(line)


def _persist_outreach_row(campaign: dict, row: dict, *, contact_event: dict | None = None) -> None:
    if backend.is_postgres_backend():
        persist = getattr(backend, "postgres_persist_outreach_lead", None)
        if callable(persist):
            persist(campaign["id"], row, contact_event=contact_event)
            return
    save_lead(row, campaign=campaign)


def _apply_draft_edits_to_row(
    row: dict,
    campaign: dict,
    *,
    subject: str,
    body: str,
    whatsapp: str,
    phone_script: str,
    selected_channel: str,
    schedule_choice: str = "",
) -> None:
    row["Email_Draft"] = compose_email_draft(subject, body)
    row["WhatsApp_Draft"] = whatsapp.strip()
    row["Phone_Script"] = phone_script.strip()
    valid_channels = available_channels(row)
    if selected_channel in valid_channels:
        row["Preferred_Channel"] = selected_channel
        if row.get("Status") not in TERMINAL_STATUSES:
            row["Next_Action_Type"] = selected_channel
    row["Drafts_Approved"] = "1"
    set_stored_draft_stale(row, False)
    row["Draft_Config_Version"] = str(campaign.get("draft_config_version") or campaign.get("config_version") or "1")
    if row.get("Status") in {"new", "draft_ready"}:
        row["Status"] = "approved"
    if schedule_choice in {"today", "tomorrow"} and "email" in available_channels(row):
        queue_scheduled_email(row, schedule_choice)
    elif schedule_choice == "clear":
        clear_scheduled_send(row)


def _save_draft_edits(
    campaign: dict,
    lead_id: str,
    *,
    subject: str,
    body: str,
    whatsapp: str,
    phone_script: str,
    selected_channel: str,
    schedule_choice: str = "",
) -> dict | None:
    row = get_lead_by_id(lead_id, campaign=campaign)
    if row is None:
        return None
    _apply_draft_edits_to_row(
        row,
        campaign,
        subject=subject,
        body=body,
        whatsapp=whatsapp,
        phone_script=phone_script,
        selected_channel=selected_channel,
        schedule_choice=schedule_choice,
    )
    _persist_outreach_row(campaign, row)
    return dict(row)


def _record_outreach_action(
    campaign: dict,
    lead_id: str,
    *,
    subject: str,
    body: str,
    whatsapp: str,
    phone_script: str,
    selected_channel: str,
    outcome: str,
    channel: str,
    notes: str = "",
    schedule_choice: str = "",
) -> bool:
    row = get_lead_by_id(lead_id, campaign=campaign)
    if row is None:
        return False
    _apply_draft_edits_to_row(
        row,
        campaign,
        subject=subject,
        body=body,
        whatsapp=whatsapp,
        phone_script=phone_script,
        selected_channel=selected_channel,
        schedule_choice=schedule_choice,
    )
    event = apply_contact_outcome(row, outcome, notes=notes, channel=channel)
    _persist_outreach_row(campaign, row, contact_event=event)
    return True


def _render_editable_draft_workspace(campaign: dict, lead: dict, *, key_prefix: str) -> None:
    lid = lead.get("ID", "?")
    channel_options = available_channels(lead)
    if not channel_options:
        st.warning("No usable outreach channel on this lead.")
        return

    current_subject, current_body = parse_email_draft(lead.get("Email_Draft", ""))
    planned = planned_channel(lead)
    selected_channel_key = f"{key_prefix}_planned_channel"
    subject_key = f"{key_prefix}_subject_text"
    email_body_key = f"{key_prefix}_email_body"
    whatsapp_key = f"{key_prefix}_wa"
    phone_key = f"{key_prefix}_phone"
    action_note_key = f"{key_prefix}_action_note"

    with st.form(f"{key_prefix}_workspace_form"):
        selected_channel = st.selectbox(
            "Planned channel",
            options=channel_options,
            index=channel_options.index(planned) if planned in channel_options else 0,
            format_func=_channel_label,
            key=selected_channel_key,
        )
        selected_subject = _render_subject_editor(
            campaign,
            lead,
            key_prefix=key_prefix,
            current_subject=current_subject,
            apply_suggestion_on_submit=True,
        )
        email_body = st.text_area("Email body", value=current_body, height=240, key=email_body_key)
        wa_draft = st.text_area("WhatsApp draft", value=lead.get("WhatsApp_Draft", ""), height=120, key=whatsapp_key)
        phone_script = st.text_area("Phone script", value=lead.get("Phone_Script", ""), height=220, key=phone_key)
        _render_draft_links(email_body, wa_draft, phone_script)
        action_note = st.text_input(
            "Action note",
            value="",
            key=action_note_key,
            placeholder="Optional note for the contact log",
        )
        st.caption(f"Queue status: {scheduled_send_label(lead)}")

        save_col, log_col = st.columns([1, 2])
        save_drafts = save_col.form_submit_button(
            "Save + Approve",
            key=f"{key_prefix}_save_drafts",
            type="primary",
            width="stretch",
        )
        log_col.caption("Saving here marks the draft approved/fresh without queueing an automatic send.")

        queue_today = False
        queue_tomorrow = False
        if "email" in channel_options:
            queue_col1, queue_col2 = st.columns(2)
            queue_today = queue_col1.form_submit_button(
                "Queue Send Today",
                key=f"{key_prefix}_queue_today",
                width="stretch",
            )
            queue_tomorrow = queue_col2.form_submit_button(
                "Queue Send Tomorrow",
                key=f"{key_prefix}_queue_tomorrow",
                width="stretch",
            )

        send_email_now = False
        mark_email_sent = False
        if "email" in channel_options:
            st.markdown("**Email**")
            email_col1, email_col2 = st.columns(2)
            send_email_now = email_col1.form_submit_button(
                "Send Email",
                key=f"{key_prefix}_send_email",
                width="stretch",
            )
            mark_email_sent = email_col2.form_submit_button(
                "Mark Email Sent",
                key=f"{key_prefix}_mark_email",
                width="stretch",
            )

        mark_whatsapp_sent = False
        if "whatsapp" in channel_options:
            st.markdown("**WhatsApp**")
            mark_whatsapp_sent = st.form_submit_button(
                "Mark WhatsApp Sent",
                key=f"{key_prefix}_mark_whatsapp",
                width="stretch",
            )

        called = voicemail = no_answer = False
        if "phone" in channel_options:
            st.markdown("**Phone**")
            phone_col1, phone_col2, phone_col3 = st.columns(3)
            called = phone_col1.form_submit_button(
                "Called",
                key=f"{key_prefix}_called",
                width="stretch",
            )
            voicemail = phone_col2.form_submit_button(
                "Voicemail",
                key=f"{key_prefix}_voicemail",
                width="stretch",
            )
            no_answer = phone_col3.form_submit_button(
                "No Answer",
                key=f"{key_prefix}_no_answer",
                width="stretch",
            )

        st.markdown("**Status**")
        status_cols = st.columns(2)
        mark_done = status_cols[0].form_submit_button(
            "Mark Done",
            key=f"{key_prefix}_done",
            width="stretch",
        )

    current_whatsapp = st.session_state.get(whatsapp_key, lead.get("WhatsApp_Draft", ""))
    wa_link = _build_whatsapp_link(lead.get("TelNr", ""), current_whatsapp)
    if "whatsapp" in channel_options:
        if wa_link:
            st.link_button("Open WhatsApp", wa_link)
        else:
            st.caption("WhatsApp link unavailable.")

    if save_drafts:
        _save_draft_edits(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            schedule_choice="clear",
        )
        reload(campaign_id=campaign["id"])
    elif queue_today:
        _save_draft_edits(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            schedule_choice="today",
        )
        reload(campaign_id=campaign["id"])
    elif queue_tomorrow:
        _save_draft_edits(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            schedule_choice="tomorrow",
        )
        reload(campaign_id=campaign["id"])
    elif send_email_now:
        row = get_lead_by_id(lid, campaign=campaign)
        if row is not None:
            _apply_draft_edits_to_row(
                row,
                campaign,
                subject=selected_subject,
                body=email_body,
                whatsapp=wa_draft,
                phone_script=phone_script,
                selected_channel=selected_channel,
                schedule_choice="clear",
            )
        with st.spinner("Sending email..."):
            ok = send_email(lid, notes=action_note, campaign=campaign, lead=row)
        if ok:
            reload(campaign_id=campaign["id"])
        elif row is not None:
            _persist_outreach_row(campaign, row)
    elif mark_email_sent:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="sent",
            channel="email",
            notes=action_note,
            schedule_choice="clear",
        ):
            reload(campaign_id=campaign["id"])
    elif mark_whatsapp_sent:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="sent",
            channel="whatsapp",
            notes=action_note,
            schedule_choice="clear",
        ):
            reload(campaign_id=campaign["id"])
    elif called:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="called",
            channel="phone",
            notes=action_note,
        ):
            reload(campaign_id=campaign["id"])
    elif voicemail:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="voicemail",
            channel="phone",
            notes=action_note,
        ):
            reload(campaign_id=campaign["id"])
    elif no_answer:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="no_answer",
            channel="phone",
            notes=action_note,
        ):
            reload(campaign_id=campaign["id"])
    elif mark_done:
        if _record_outreach_action(
            campaign,
            lid,
            subject=selected_subject,
            body=email_body,
            whatsapp=wa_draft,
            phone_script=phone_script,
            selected_channel=selected_channel,
            outcome="done",
            channel=selected_channel,
            notes=action_note or "Marked as done",
        ):
            reload(campaign_id=campaign["id"])


def _persist_channel_choice(campaign: dict, lead_id: str, channel: str) -> None:
    row = get_lead_by_id(lead_id, campaign=campaign)
    if row is None:
        return
    row["Preferred_Channel"] = channel
    if row.get("Status") in {"new", "draft_ready", "approved", "contacted"}:
        row["Next_Action_Type"] = channel
    save_lead(row, campaign=campaign)


def _is_bulk_email_ready(lead: dict) -> bool:
    return (
        planned_channel(lead) == "email"
        and (lead.get("Email") or "").strip() != ""
        and (lead.get("Email_Draft") or "").strip() != ""
        and lead.get("Draft_Stale") != "1"
        and (lead.get("Scheduled_Send_Status") or "").strip() != "queued"
    )


def _is_bulk_email_ready_summary(lead: dict) -> bool:
    return (
        planned_channel(lead) == "email"
        and (lead.get("Email") or "").strip() != ""
        and bool(lead.get("Has_Email_Draft"))
        and lead.get("Draft_Stale") != "1"
        and (lead.get("Scheduled_Send_Status") or "").strip() != "queued"
    )


def _init_outreach_state(campaign: dict) -> dict[str, str]:
    key_map = {
        "selection": _campaign_state_key(campaign, "outreach_bulk_selection"),
        "note": _campaign_state_key(campaign, "outreach_bulk_note"),
        "notice": _campaign_state_key(campaign, "outreach_bulk_notice"),
        "search": _campaign_state_key(campaign, "outreach_search"),
        "page": _campaign_state_key(campaign, "outreach_page"),
        "selected": _campaign_state_key(campaign, "outreach_selected"),
        "selected_widget": _campaign_state_key(campaign, "outreach_selected_widget"),
    }
    defaults = {
        "selection": [],
        "note": "",
        "notice": None,
        "search": "",
        "page": 1,
        "selected": "",
        "selected_widget": "",
    }
    for name, key in key_map.items():
        if key not in st.session_state:
            st.session_state[key] = _copy_state_default(defaults[name])
    return key_map


def _init_all_leads_state(campaign: dict) -> dict[str, str]:
    key_map = {
        "search": _campaign_state_key(campaign, "all_leads_search"),
        "status": _campaign_state_key(campaign, "all_leads_status"),
        "channel": _campaign_state_key(campaign, "all_leads_channel"),
        "priority": _campaign_state_key(campaign, "all_leads_priority"),
        "stale": _campaign_state_key(campaign, "all_leads_stale"),
        "page_size": _campaign_state_key(campaign, "all_leads_page_size"),
        "page": _campaign_state_key(campaign, "all_leads_page"),
        "selected": _campaign_state_key(campaign, "all_leads_selected"),
        "selected_widget": _campaign_state_key(campaign, "all_leads_selected_widget"),
    }
    defaults = {
        "search": "",
        "status": [],
        "channel": [],
        "priority": [],
        "stale": "All",
        "page_size": 25,
        "page": 1,
        "selected": "",
        "selected_widget": "",
    }
    for name, key in key_map.items():
        if key not in st.session_state:
            st.session_state[key] = _copy_state_default(defaults[name])
    return key_map


def _reset_all_leads_state(key_map: dict[str, str]) -> None:
    defaults = {
        "search": "",
        "status": [],
        "channel": [],
        "priority": [],
        "stale": "All",
        "page_size": 25,
        "page": 1,
        "selected": "",
        "selected_widget": "",
    }
    for name, key in key_map.items():
        st.session_state[key] = _copy_state_default(defaults[name])


def _campaign_counts(leads: list[dict]) -> dict[str, int]:
    counts = Counter(lead.get("Status", "new") for lead in leads)
    counts["draft_stale"] = sum(1 for lead in leads if lead.get("Draft_Stale") == "1")
    counts["research_stale"] = sum(1 for lead in leads if lead.get("Research_Stale") == "1")
    counts["approved_fresh"] = sum(
        1 for lead in leads
        if lead.get("Status") == "approved" and lead.get("Drafts_Approved") == "1" and lead.get("Draft_Stale") != "1"
    )
    return counts


def _generate_drafts(lead_ids: list[str], container) -> int:
    import datetime
    import time

    from openai import OpenAI

    from campaign_service import get_active_campaign, mark_campaign_stage_run
    from crm_analyze import analyze_website, build_no_website_analysis, select_channel_and_priority
    from crm_research import RATE_LIMIT_SEC, categorize_website, fetch_and_clean_html
    from crm_templates import (
        choose_hook,
        is_pending_template_refresh_target,
        pick_template_key,
        render_drafts,
        rerender_saved_draft,
    )
    from herold_scraper import HeroldFetcher

    campaign = get_active_campaign()
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        container.error("OPENAI_API_KEY not set in .env")
        return 0

    client = OpenAI(api_key=api_key)
    all_leads = load_leads(campaign=campaign)
    target_ids = set(lead_ids)
    targets = [lead for lead in all_leads if lead.get("ID") in target_ids]
    if not targets:
        return 0

    prog = container.progress(0.0)
    status_txt = container.empty()
    fetcher = HeroldFetcher(headless=True)
    done = 0

    try:
        for idx, lead in enumerate(targets):
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")[:35]
            was_stale = lead.get("Draft_Stale") == "1"
            status_txt.caption(f"Building drafts for {lid} - {company}")
            prog.progress(idx / len(targets))

            if was_stale and lead.get("Research_Stale") != "1" and is_pending_template_refresh_target(lead):
                status_txt.caption(f"Refreshing drafts for {lid} - {company}")
                rerender_saved_draft(lead, campaign=campaign)
                lead["Drafts_Approved"] = "0"
                save_lead(lead, campaign=campaign)
                done += 1
                continue

            website = lead.get("Website", "").strip()
            category = lead.get("Website_Category") or categorize_website(website)
            lead["Website_Category"] = category

            if category == "real":
                html = fetch_and_clean_html(website, fetcher)
                if html:
                    analysis = analyze_website(lead.get("Unternehmen", ""), website, html, client, campaign)
                else:
                    lead["Website_Category"] = "fetch_error"
                    analysis = build_no_website_analysis(campaign)
                time.sleep(RATE_LIMIT_SEC)
            else:
                analysis = build_no_website_analysis(campaign)

            lead["Website_Score"] = str(analysis.get("score", 0))
            lead["Pain_Points"] = " | ".join(analysis.get("pain_points", []))
            lead["Pain_Categories"] = " | ".join(analysis.get("pain_categories", []))

            primary, _, priority = select_channel_and_priority(lead, analysis)
            if (lead.get("Preferred_Channel") or "").strip() in available_channels(lead):
                lead["Preferred_Channel"] = lead["Preferred_Channel"].strip()
            else:
                lead["Preferred_Channel"] = primary
            lead["Priority"] = str(priority)
            if not lead.get("Next_Action_Date") and lead["Preferred_Channel"] != "none":
                lead["Next_Action_Date"] = date.today().isoformat()
                lead["Next_Action_Type"] = lead["Preferred_Channel"]
            elif lead.get("Status", "new") in {"new", "draft_ready", "approved"} and lead["Preferred_Channel"] != "none":
                lead["Next_Action_Type"] = lead["Preferred_Channel"]

            template_key = pick_template_key(analysis.get("pain_categories", []), lead)
            hook = choose_hook(template_key, lead, campaign=campaign)
            drafts = render_drafts(lead, hook, "", template_key=template_key, campaign=campaign)
            lead.update(drafts)
            lead["Draft_Config_Version"] = str(campaign.get("draft_config_version") or campaign.get("config_version") or "1")
            set_stored_draft_stale(lead, False)
            lead["Analyzed_At"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            lead["Drafts_Approved"] = "0"
            if lead.get("Status", "new") in {"new", "approved", "draft_ready"} or was_stale:
                lead["Status"] = "draft_ready"

            save_lead(lead, campaign=campaign)
            done += 1
            time.sleep(1)
    finally:
        fetcher.close()

    mark_campaign_stage_run(campaign["id"], "analyzed")
    prog.progress(1.0)
    status_txt.caption(f"Finished {done} draft(s)")
    return done


def _persist_campaign_copy_changes(
    campaign: dict,
    *,
    hooks_payload: dict | None = None,
    template_payload: dict | None = None,
) -> dict:
    updated_campaign = campaign
    if backend.is_postgres_backend():
        updates: dict[str, object] = {}
        if hooks_payload is not None:
            updates["hooks_library_json"] = hooks_payload
        if template_payload is not None:
            updates["template_overrides_json"] = template_payload
        if updates:
            updated_campaign = update_campaign(campaign["id"], updates, bump_version=False)
    else:
        if hooks_payload is not None:
            _write_json_payload(get_hooks_library_path(campaign), hooks_payload)
        if template_payload is not None:
            _write_json_payload(get_template_overrides_path(campaign), template_payload)
        updated_campaign = get_campaign(campaign["id"])
    invalidate_campaign_copy_cache(updated_campaign)
    return updated_campaign


def _build_template_editor_save_notice(
    *,
    change_label: str,
    changed: bool,
    stats: dict[str, int],
) -> tuple[str, str]:
    stale_marked = int(stats.get("stale_marked") or 0)
    stale_cleared = int(stats.get("stale_cleared") or 0)
    if not changed:
        return ("info", f"No changes detected in {change_label}. Pending drafts were left alone.")
    if stale_marked and stale_cleared:
        return (
            "success",
            f"Saved {change_label}. Marked {stale_marked} pending draft(s) stale and cleared {stale_cleared} pending stale draft(s).",
        )
    if stale_marked:
        return ("success", f"Saved {change_label}. Marked {stale_marked} pending draft(s) stale.")
    if stale_cleared:
        return (
            "success",
            f"Saved {change_label}. No pending drafts became stale; {stale_cleared} pending stale draft(s) are fresh again.",
        )
    return ("info", f"Saved {change_label}. No pending drafts were affected.")


def _apply_template_editor_change(
    campaign: dict,
    *,
    change_type: str,
    hooks_payload: dict | None = None,
    template_payload: dict | None = None,
    selected_template_key: str = "",
) -> None:
    snapshot_kwargs: dict[str, object] = {}
    if hooks_payload is not None:
        snapshot_kwargs["hooks_override"] = hooks_payload
    if template_payload is not None:
        snapshot_kwargs["template_override"] = template_payload

    before_snapshot = build_template_editor_snapshot(campaign=campaign)
    after_snapshot = build_template_editor_snapshot(campaign=campaign, **snapshot_kwargs)
    change_scope = build_template_editor_change_scope(
        before_snapshot,
        after_snapshot,
        change_type=change_type,
        selected_template_key=selected_template_key,
    )
    change_label = _describe_template_editor_change(
        change_type=change_type,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        selected_template_key=selected_template_key,
    )

    updated_campaign = _persist_campaign_copy_changes(
        campaign,
        hooks_payload=hooks_payload,
        template_payload=template_payload,
    )

    stats = {"stale_marked": 0, "stale_cleared": 0}
    if change_scope.get("changed"):
        template_keys = set(change_scope.get("template_keys") or [])
        stats = mark_template_editor_pending_drafts_stale(
            updated_campaign,
            template_keys=template_keys or None,
        )

    level, message = _build_template_editor_save_notice(
        change_label=change_label,
        changed=bool(change_scope.get("changed")),
        stats=stats,
    )
    _set_template_editor_notice(level, message)
    reload(campaign_id=campaign["id"], campaign_changed=True)


def _render_campaign_template_editor(campaign: dict) -> None:
    st.divider()
    st.subheader("Template Editor")
    _render_template_editor_notice()
    st.caption("Edit campaign-specific draft copy here. Saving changes only marks affected pending drafts as stale so you can regenerate the new wording without touching approved leads.")
    _render_template_placeholder_reference()

    subjects_tab, hooks_tab, templates_tab = st.tabs(["Subjects", "Hooks", "Templates"])

    with subjects_tab:
        effective_subjects = get_effective_subject_templates(campaign=campaign)
        effective_special_subject = get_effective_special_subject_option(campaign=campaign)
        with st.form(f"subject_editor_{campaign['id']}"):
            special_subject = st.text_input(
                "Pinned special subject",
                value=effective_special_subject,
                help="This stays at the top of the subject suggestion list. Leave it blank if you do not want a pinned option.",
            )
            subject_lines = st.text_area(
                "Subject suggestions (one per line)",
                value="\n".join(effective_subjects),
                height=260,
            )
            st.caption("These are the shared subject suggestions shown in the draft editor.")
            st.caption("Subject suggestions can also use the placeholders from the reference above.")
            c1, c2 = st.columns(2)
            save_subjects = c1.form_submit_button("Save Subject Settings", type="primary", width="stretch")
            reset_subjects = c2.form_submit_button("Reset Subject Settings", width="stretch")

        if save_subjects:
            payload = get_template_override_payload(campaign=campaign)
            payload["special_subject_option"] = special_subject.strip()
            payload["subject_templates"] = [line.strip() for line in subject_lines.splitlines() if line.strip()]
            _apply_template_editor_change(campaign, change_type="subjects", template_payload=payload)

        if reset_subjects:
            payload = get_template_override_payload(campaign=campaign)
            payload.pop("special_subject_option", None)
            payload.pop("subject_templates", None)
            _apply_template_editor_change(campaign, change_type="subjects", template_payload=payload)

    with hooks_tab:
        effective_hooks = get_effective_hooks_library(campaign=campaign)
        with st.form(f"hooks_editor_{campaign['id']}"):
            st.caption("One hook per line. Hooks can also use the same placeholders as templates.")
            st.caption("Use the placeholders from the reference above.")
            hook_inputs: dict[str, str] = {}
            for category, items in effective_hooks.items():
                with st.expander(category.replace("_", " ").title(), expanded=False):
                    hook_inputs[category] = st.text_area(
                        f"{category} hooks",
                        value="\n".join(items),
                        height=180,
                    )
            c1, c2 = st.columns(2)
            save_hooks = c1.form_submit_button("Save Hook Library", type="primary", width="stretch")
            reset_hooks = c2.form_submit_button("Reset All Hooks", width="stretch")

        if save_hooks:
            hooks_payload = {
                category: [line.strip() for line in text.splitlines() if line.strip()]
                for category, text in hook_inputs.items()
                if [line.strip() for line in text.splitlines() if line.strip()]
            }
            _apply_template_editor_change(campaign, change_type="hooks", hooks_payload=hooks_payload)

        if reset_hooks:
            _apply_template_editor_change(campaign, change_type="hooks", hooks_payload={})

    with templates_tab:
        effective_templates = get_effective_templates(campaign=campaign)
        template_keys = [key for key in effective_templates if key != "default"]
        selected_template_key = st.selectbox(
            "Template category",
            options=template_keys,
            format_func=lambda key: key.replace("_", " ").title(),
            key=f"template_editor_pick_{campaign['id']}",
        )
        template = effective_templates[selected_template_key]
        with st.form(f"template_editor_form_{campaign['id']}_{selected_template_key}"):
            st.caption("Edit the full template text used for future draft generation.")
            st.caption("Use the placeholders from the reference above.")
            email_template = st.text_area("Email template", value=template.get("email", ""), height=420)
            whatsapp_template = st.text_area("WhatsApp template", value=template.get("whatsapp", ""), height=160)
            phone_template = st.text_area("Phone script template", value=template.get("phone_script", ""), height=320)
            c1, c2 = st.columns(2)
            save_template = c1.form_submit_button("Save Template", type="primary", width="stretch")
            reset_template = c2.form_submit_button("Reset This Template", width="stretch")

        if save_template:
            payload = get_template_override_payload(campaign=campaign)
            templates_payload = payload.setdefault("templates", {})
            templates_payload[selected_template_key] = {
                "email": email_template,
                "whatsapp": whatsapp_template,
                "phone_script": phone_template,
            }
            _apply_template_editor_change(
                campaign,
                change_type="template",
                template_payload=payload,
                selected_template_key=selected_template_key,
            )

        if reset_template:
            payload = get_template_override_payload(campaign=campaign)
            templates_payload = payload.get("templates")
            if isinstance(templates_payload, dict):
                templates_payload.pop(selected_template_key, None)
                if not templates_payload:
                    payload.pop("templates", None)
            _apply_template_editor_change(
                campaign,
                change_type="template",
                template_payload=payload,
                selected_template_key=selected_template_key,
            )

    st.caption(
        "Need to apply the latest copy to pending stale drafts? This rebuilds only stale "
        "`new` / `draft_ready` drafts from Template Editor changes. Approved drafts stay untouched."
    )
    if st.button("Refresh Stale Pending Drafts", width="stretch", key=f"refresh_template_drafts_{campaign['id']}"):
        with st.spinner("Refreshing stale pending drafts from current templates..."):
            refreshed = refresh_targeted_pending_drafts(campaign)
        if refreshed:
            _set_template_editor_notice(
                "success",
                f"Refreshed {refreshed} stale pending draft(s).",
            )
        else:
            _set_template_editor_notice(
                "info",
                "No stale pending drafts were waiting for a template-editor refresh.",
            )
        reload(campaign_id=campaign["id"])


def _active_campaign_switch() -> dict:
    campaigns = cached_campaigns()
    campaign_ids = [campaign["id"] for campaign in campaigns]
    labels = {campaign["id"]: campaign.get("label", campaign["id"]) for campaign in campaigns}
    active = cached_active_campaign()

    st.sidebar.title("Campaign CRM")
    selected_id = st.sidebar.selectbox(
        "Active Campaign",
        options=campaign_ids,
        index=campaign_ids.index(active["id"]) if active["id"] in campaign_ids else 0,
        format_func=lambda cid: labels.get(cid, cid),
    )
    if selected_id != active["id"]:
        set_active_campaign(selected_id)
        reload(campaign_id=selected_id, campaign_changed=True)

    st.sidebar.caption(f"ID: `{selected_id}`")
    commit_sha = app_commit_sha()
    if commit_sha:
        st.sidebar.caption(f"App commit: `{commit_sha}`")
    return cached_campaign(selected_id)


def _render_dashboard(campaign: dict, dashboard_snapshot: dict, metrics: dict[str, int]) -> None:
    st.title("Dashboard")
    st.caption(f"Active campaign: {campaign.get('label', campaign['id'])}")

    total_leads = int(metrics.get("total_leads", 0))
    if not total_leads:
        st.warning("No leads found yet. Start on the Campaigns page and run Scrape.")
        return

    counts = dict(dashboard_snapshot.get("status_counts") or {})
    pending_drafts = list(dashboard_snapshot.get("pending_drafts") or [])
    actionable = list(dashboard_snapshot.get("actionable") or [])
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Leads", total_leads)
    col2.metric("Draft Ready", counts.get("draft_ready", 0))
    col3.metric("Approved", metrics.get("approved_fresh", 0))
    col4.metric("Drafts Stale", metrics.get("draft_stale", 0))
    col5.metric("Research Stale", metrics.get("research_stale", 0))

    if pending_drafts:
        st.divider()
        st.subheader(f"Generate Drafts ({len(pending_drafts)})")
        st.caption("Includes never-analyzed leads and drafts made stale by campaign changes.")
        if st.button(f"Generate All Pending ({len(pending_drafts)})", type="primary", key="gen_all_pending"):
            container = st.container()
            if _generate_drafts([lead["ID"] for lead in pending_drafts if lead.get("ID")], container):
                reload(campaign_id=campaign["id"])

    today = date.today().isoformat()

    st.divider()
    st.subheader(f"Today's Actions ({today})")
    if not actionable:
        st.success("Nothing due today.")
    else:
        for lead in actionable[:6]:
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")
            priority = lead.get("Priority", "5")
            channel = lead.get("Next_Action_Type", "none")
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                c1.markdown(f"**{lid}** - {company}")
                c2.markdown(f"{CHANNEL_EMOJI.get(channel, '?')} {channel}")
                c3.markdown(f"{PRIORITY_COLOR.get(int(priority) if str(priority).isdigit() else 5, '⚫')} P{priority}")
                c4.markdown(lead.get("Next_Action_Date") or today)

    st.divider()
    st.subheader("Pipeline")
    pipeline_order = ["new", "draft_ready", "approved", "contacted", "replied", "meeting_scheduled", "done", "won", "lost", "no_contact"]
    cols = st.columns(len(pipeline_order))
    for col, status in zip(cols, pipeline_order):
        icon = STATUS_EMOJI.get(status, "")
        col.metric(f"{icon} {status.replace('_', ' ').title()}", counts.get(status, 0))


def _render_review_queue(campaign: dict) -> None:
    st.title("Review Queue")
    page_key = _campaign_state_key(campaign, "review_queue_page")
    current_page = max(1, int(st.session_state.get(page_key, 1) or 1))
    queue_page = cached_review_queue(campaign["id"], current_page, REVIEW_QUEUE_PAGE_SIZE)
    queue = list(queue_page.get("items") or [])
    has_more = bool(queue_page.get("has_more"))

    if not queue and current_page > 1:
        st.session_state[page_key] = current_page - 1
        reload()
        return

    if not queue:
        st.success("No drafts waiting for review.")
        return

    stale_research = sum(1 for lead in queue if lead.get("Research_Stale") == "1")
    if stale_research:
        st.warning(f"{stale_research} lead(s) on this page still use stale research data.")

    skipped_key = _campaign_state_key(campaign, "review_queue_skipped")
    selected_key = _campaign_state_key(campaign, "review_queue_selected")
    selected_widget_key = _campaign_state_key(campaign, "review_queue_selected_widget")
    if skipped_key not in st.session_state:
        st.session_state[skipped_key] = []

    skipped_ids = set(st.session_state.get(skipped_key, []))
    visible_queue = [lead for lead in queue if lead.get("ID") not in skipped_ids]

    nav_prev, nav_info, nav_next = st.columns([1, 2, 1])
    if nav_prev.button("Prev Page", key=f"review_prev_{campaign['id']}", disabled=current_page <= 1):
        st.session_state[page_key] = current_page - 1
        reload()
    nav_info.info(f"Review page {current_page} | showing up to {REVIEW_QUEUE_PAGE_SIZE} draft(s)")
    if nav_next.button("Next Page", key=f"review_next_{campaign['id']}", disabled=not has_more):
        st.session_state[page_key] = current_page + 1
        reload()

    info_col, action_col = st.columns([4, 1])
    info_col.info(f"{len(queue)} draft(s) ready on this page")
    if skipped_ids and action_col.button("Restore Skipped", key=f"restore_skipped_{campaign['id']}"):
        st.session_state[skipped_key] = []
        st.session_state[selected_key] = queue[0].get("ID", "")
        st.session_state[selected_widget_key] = queue[0].get("ID", "")
        reload()

    if not visible_queue:
        st.warning("Everything in the review queue is skipped for this session.")
        return

    queue_ids = [lead.get("ID", "") for lead in visible_queue]
    if st.session_state.get(selected_key) not in queue_ids:
        st.session_state[selected_key] = queue_ids[0]
    if st.session_state.get(selected_widget_key) not in queue_ids:
        st.session_state[selected_widget_key] = st.session_state[selected_key]
    selected_id = st.selectbox(
        "Select lead",
        options=queue_ids,
        format_func=lambda lead_id: next(
            f"{lead.get('ID', '?')} - {lead.get('Unternehmen', '')[:45]}"
            for lead in visible_queue
            if lead.get("ID") == lead_id
        ),
        key=selected_widget_key,
    )
    if selected_id != st.session_state.get(selected_key):
        st.session_state[selected_key] = selected_id
    selected = cached_lead(campaign["id"], selected_id)
    if selected is None:
        st.error("The selected lead could not be loaded.")
        return
    lid = selected.get("ID", "?")
    company = selected.get("Unternehmen", "")
    website = selected.get("Website", "")
    plz, bezirk = get_bezirk(selected.get("Adresse", ""))

    st.markdown(f"### {lid} - {company}")
    meta1, meta2, meta3 = st.columns(3)
    meta1.markdown(f"**Location:** {plz or '-'} {bezirk}")
    meta2.markdown(f"**Website:** {selected.get('Website_Category') or '-'}")
    meta3.markdown(f"**Priority:** {selected.get('Priority') or '-'}")

    left, right = st.columns([2, 3])

    with left:
        st.subheader("Contact Info")
        st.text(f"Name:    {selected.get('Kontaktname') or '-'}")
        st.text(f"Email:   {selected.get('Email') or '-'}")
        st.text(f"Tel:     {selected.get('TelNr') or '-'}")
        st.text(f"Adresse: {selected.get('Adresse') or '-'}")
        st.divider()
        st.subheader("Research")
        st.markdown(f"**Website score:** {_score_bar(selected.get('Website_Score', ''))}")
        st.markdown(f"**Google rank keyword:** `{selected.get('Google_Rank_Keyword') or '-'}`")
        st.markdown(f"**Google rank:** {selected.get('Google_Rank_Position') or '-'}")
        st.markdown(f"**Google rating:** {selected.get('Google_Rating') or '-'}")
        if website and website not in ("X", ""):
            st.markdown(f"[Open website]({website})")
        if selected.get("Google_Maps_Link"):
            st.markdown(f"[Google Maps]({selected['Google_Maps_Link']})")
        if selected.get("Pain_Points"):
            st.markdown("**Pain points:**")
            for item in selected["Pain_Points"].split(" | "):
                if item.strip():
                    st.markdown(f"- {item.strip()}")

    with right:
        st.subheader("Drafts")
        channel_options = available_channels(selected)
        if not channel_options:
            st.error("No usable channel on this lead.")
            return

        current_subject, current_body = parse_email_draft(selected.get("Email_Draft", ""))
        review_key_prefix = f"review_{lid}"
        with st.form(f"{review_key_prefix}_form"):
            selected_channel = st.radio(
                "Default starting channel",
                options=channel_options,
                index=channel_options.index(preferred_channel(selected)),
                format_func=_channel_label,
                help="This is just the default first touch. You can change it later in Outreach or All Leads.",
                horizontal=True,
                key=f"{review_key_prefix}_channel",
            )
            selected_subject = _render_subject_editor(
                campaign,
                selected,
                key_prefix=review_key_prefix,
                current_subject=current_subject,
                apply_suggestion_on_submit=True,
            )
            email_body = st.text_area("Email body", value=current_body, height=260, key=f"{review_key_prefix}_email_body")
            wa_draft = st.text_area("WhatsApp draft", value=selected.get("WhatsApp_Draft", ""), height=120, key=f"{review_key_prefix}_wa")
            phone_script = st.text_area("Phone script", value=selected.get("Phone_Script", ""), height=240, key=f"{review_key_prefix}_phone")
            _render_draft_links(email_body, wa_draft, phone_script)
            st.caption(f"Queue status: {scheduled_send_label(selected)}")
            c1, c2, c3, c4, c5 = st.columns(5)
            approve = c1.form_submit_button("Approve", type="primary", width="stretch")
            queue_today = c2.form_submit_button("Send Today", width="stretch")
            queue_tomorrow = c3.form_submit_button("Send Tomorrow", width="stretch")
            skip = c4.form_submit_button("Skip", width="stretch")
            blacklist = c5.form_submit_button("Blacklist", width="stretch")

        if approve or queue_today or queue_tomorrow:
            lead = get_lead_by_id(lid, campaign=campaign)
            if lead is not None:
                lead["Email_Draft"] = compose_email_draft(selected_subject, email_body)
                lead["WhatsApp_Draft"] = wa_draft
                lead["Phone_Script"] = phone_script
                lead["Preferred_Channel"] = selected_channel
                lead["Next_Action_Type"] = selected_channel
                lead["Status"] = "approved"
                lead["Drafts_Approved"] = "1"
                set_stored_draft_stale(lead, False)
                if queue_today and "email" in available_channels(lead):
                    queue_scheduled_email(lead, "today")
                elif queue_tomorrow and "email" in available_channels(lead):
                    queue_scheduled_email(lead, "tomorrow")
                else:
                    clear_scheduled_send(lead)
                save_lead(lead, campaign=campaign)
            skipped_ids.discard(lid)
            st.session_state[skipped_key] = sorted(skipped_ids)
            st.session_state[selected_key] = _next_review_selection(queue_ids, lid)
            reload(campaign_id=campaign["id"])

        if skip:
            skipped_ids.add(lid)
            st.session_state[skipped_key] = sorted(skipped_ids)
            st.session_state[selected_key] = _next_review_selection(queue_ids, lid)
            reload()

        if blacklist:
            lead = get_lead_by_id(lid, campaign=campaign)
            if lead is not None:
                lead["Status"] = "blacklist"
                lead["Next_Action_Type"] = "none"
                clear_scheduled_send(lead)
                save_lead(lead, campaign=campaign)
            skipped_ids.discard(lid)
            st.session_state[skipped_key] = sorted(skipped_ids)
            st.session_state[selected_key] = _next_review_selection(queue_ids, lid)
            reload(campaign_id=campaign["id"])


def _outreach_page_label(lead: dict) -> str:
    company = lead.get("Unternehmen", "")
    channel = planned_channel(lead)
    priority = lead.get("Priority", "5")
    status = lead.get("Status", "approved")
    stale_suffix = " | stale draft" if lead.get("Draft_Stale") == "1" else ""
    queue_status = (lead.get("Scheduled_Send_Status") or "").strip()
    queue_suffix = f" | {queue_status}" if queue_status else ""
    return (
        f"{lead.get('ID', '?')} - {company[:40]} | {STATUS_EMOJI.get(status, '')} {status}"
        f" | {CHANNEL_EMOJI.get(channel, '?')} {channel} | "
        f"{PRIORITY_COLOR.get(int(priority) if str(priority).isdigit() else 5, '⚫')} P{priority}"
        f"{stale_suffix}{queue_suffix}"
    )


def _reset_outreach_search(state_keys: dict[str, str]) -> None:
    st.session_state[state_keys["search"]] = ""
    st.session_state[state_keys["page"]] = 1
    st.session_state[state_keys["selected"]] = ""
    st.session_state[state_keys["selected_widget"]] = ""


def _render_outreach(campaign: dict) -> None:
    st.title("Outreach")
    counts = cached_outreach_counts(campaign["id"])
    if counts.get("approved_total", 0) == 0:
        st.success("No approved leads ready in Outreach.")
        return

    st.caption("Outreach is for approved leads only. Use Re-contact for leads you already touched before.")
    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Queued Today", counts.get("queued_today", 0))
    q2.metric("Queued Later", counts.get("queued_later", 0))
    q3.metric("Send Errors", counts.get("send_errors", 0))
    q4.metric("Sent Today", counts.get("sent_today", 0))

    state_keys = _init_outreach_state(campaign)
    with st.form(key=f"outreach_filters_{campaign['id']}"):
        st.text_input(
            "Search approved leads",
            placeholder="Lead ID or company",
            key=state_keys["search"],
        )
        search_apply_col, search_reset_col = st.columns(2)
        apply_search = search_apply_col.form_submit_button("Apply Search", type="primary", width="stretch")
        reset_search = search_reset_col.form_submit_button(
            "Clear Search",
            width="stretch",
            on_click=_reset_outreach_search,
            args=(state_keys,),
        )
    if reset_search:
        reload()
    if apply_search:
        st.session_state[state_keys["page"]] = 1

    current_page = max(1, int(st.session_state.get(state_keys["page"], 1) or 1))
    search_text = (st.session_state.get(state_keys["search"]) or "").strip()
    summary_page = cached_outreach_summary(campaign["id"], search_text, current_page, OUTREACH_PAGE_SIZE)
    approved = list(summary_page.get("items") or [])
    total_count = int(summary_page.get("total_count") or 0)
    if not approved and current_page > 1:
        st.session_state[state_keys["page"]] = current_page - 1
        reload()
        return
    if not approved:
        st.info("No approved leads match the current search.")
        return

    total_pages = max(1, (total_count + OUTREACH_PAGE_SIZE - 1) // OUTREACH_PAGE_SIZE)
    stale_count = sum(1 for lead in approved if lead.get("Draft_Stale") == "1")
    if stale_count:
        st.warning(f"{stale_count} approved lead(s) on this page have stale drafts. You can still edit or send them here.")

    notice = st.session_state.get(state_keys["notice"])
    if notice:
        sent_ids = notice.get("sent_ids", [])
        failed_ids = notice.get("failed_ids", [])
        if sent_ids and not failed_ids:
            st.success(f"Bulk send complete: sent {len(sent_ids)} email(s).")
        elif sent_ids:
            st.warning(
                f"Bulk send partially completed: sent {len(sent_ids)} email(s), "
                f"failed {len(failed_ids)} ({', '.join(failed_ids)})."
            )
        else:
            st.error(f"Bulk send failed for {len(failed_ids)} email(s): {', '.join(failed_ids)}")
        st.session_state[state_keys["notice"]] = None

    bulk_ready = [lead for lead in approved if _is_bulk_email_ready_summary(lead)]
    bulk_labels = {
        lead["ID"]: (
            f"{lead['ID']} - {lead.get('Unternehmen', '')[:40]} | "
            f"{PRIORITY_COLOR.get(int(lead.get('Priority') or 5) if str(lead.get('Priority') or '').isdigit() else 5, '⚫')} "
            f"P{lead.get('Priority') or '5'}"
        )
        for lead in bulk_ready
        if lead.get("ID")
    }
    bulk_ids = list(bulk_labels)
    selected_ids = list(st.session_state.get(state_keys["selection"], []))
    page_widget_key = _campaign_state_key(campaign, f"outreach_page_selection_{current_page}")
    if page_widget_key not in st.session_state:
        st.session_state[page_widget_key] = [lead_id for lead_id in selected_ids if lead_id in bulk_labels]
    else:
        st.session_state[page_widget_key] = [
            lead_id for lead_id in st.session_state.get(page_widget_key, [])
            if lead_id in bulk_labels
        ]

    st.divider()
    st.subheader("Bulk Email")
    st.caption(
        f"{len(bulk_ready)} approved lead(s) on this page are ready for bulk email. "
        f"{len(selected_ids)} lead(s) are currently selected across pages."
    )
    st.caption("Bulk send respects the planned channel, so only email-ready leads appear here.")
    if not bulk_ids:
        st.info("No approved leads are currently ready for bulk email. Individual outreach actions below still work.")

    bulk_controls = st.columns(3)
    if bulk_controls[0].button(f"Select All ({len(bulk_ids)})", key=_campaign_state_key(campaign, "outreach_select_all")):
        st.session_state[state_keys["selection"]] = sorted({*selected_ids, *bulk_ids})
        st.session_state[page_widget_key] = list(bulk_ids)
        reload()
    if bulk_controls[1].button("Clear Page", key=_campaign_state_key(campaign, "outreach_clear_page")):
        st.session_state[state_keys["selection"]] = [lead_id for lead_id in selected_ids if lead_id not in bulk_ids]
        st.session_state[page_widget_key] = []
        reload()
    if bulk_controls[2].button("Clear All", key=_campaign_state_key(campaign, "outreach_clear_selection")):
        st.session_state[state_keys["selection"]] = []
        st.session_state[page_widget_key] = []
        reload()

    st.multiselect(
        "Select page leads to send",
        options=bulk_ids,
        format_func=lambda lead_id: bulk_labels.get(lead_id, lead_id),
        key=page_widget_key,
        placeholder="Choose email-ready leads on this page",
    )
    page_selected_ids = list(st.session_state.get(page_widget_key, []))
    merged_selection = [
        lead_id for lead_id in selected_ids
        if lead_id not in bulk_ids
    ] + page_selected_ids
    if merged_selection != st.session_state.get(state_keys["selection"], []):
        st.session_state[state_keys["selection"]] = merged_selection

    st.text_input(
        "Bulk action note",
        key=state_keys["note"],
        placeholder="Optional note added to every contact log entry",
    )

    send_selected = st.button(
        f"Send Selected ({len(st.session_state.get(state_keys['selection'], []))})",
        type="primary",
        disabled=not st.session_state.get(state_keys["selection"]),
        key=_campaign_state_key(campaign, "outreach_send_selected"),
    )
    if send_selected:
        selected_ids = list(st.session_state.get(state_keys["selection"], []))
        note = (st.session_state.get(state_keys["note"]) or "").strip()
        sent_ids: list[str] = []
        failed_ids: list[str] = []
        with st.spinner(f"Sending {len(selected_ids)} email(s)..."):
            for lead_id in selected_ids:
                if send_email(lead_id, notes=note, campaign=campaign):
                    sent_ids.append(lead_id)
                else:
                    failed_ids.append(lead_id)
        st.session_state[state_keys["notice"]] = {"sent_ids": sent_ids, "failed_ids": failed_ids}
        st.session_state[state_keys["selection"]] = failed_ids
        if not failed_ids:
            st.session_state[state_keys["note"]] = ""
        reload(campaign_id=campaign["id"])

    nav_prev, nav_info, nav_next = st.columns([1, 2, 1])
    if nav_prev.button("Prev Page", key=f"outreach_prev_{campaign['id']}", disabled=current_page <= 1):
        st.session_state[state_keys["page"]] = current_page - 1
        reload()
    nav_info.info(f"Outreach page {current_page} of {total_pages} | showing up to {OUTREACH_PAGE_SIZE} lead(s)")
    if nav_next.button("Next Page", key=f"outreach_next_{campaign['id']}", disabled=current_page >= total_pages):
        st.session_state[state_keys["page"]] = current_page + 1
        reload()

    if total_count:
        start = (current_page - 1) * OUTREACH_PAGE_SIZE + 1
        end = min(current_page * OUTREACH_PAGE_SIZE, total_count)
        st.caption(f"Showing {start}-{end} of {total_count} approved lead(s)")

    page_ids = [lead.get("ID", "") for lead in approved if lead.get("ID")]
    if st.session_state.get(state_keys["selected"]) not in page_ids:
        st.session_state[state_keys["selected"]] = page_ids[0]
    if st.session_state.get(state_keys["selected_widget"]) not in page_ids:
        st.session_state[state_keys["selected_widget"]] = st.session_state[state_keys["selected"]]

    summary_rows = [
        {
            "ID": lead.get("ID", ""),
            "Company": lead.get("Unternehmen", ""),
            "Priority": lead.get("Priority", "5"),
            "Channel": planned_channel(lead),
            "Queue": scheduled_send_label(lead),
            "Error": "yes" if (lead.get("Scheduled_Send_Error") or "").strip() else "",
            "Email Ready": "yes" if _is_bulk_email_ready_summary(lead) else "",
        }
        for lead in approved
    ]

    list_col, detail_col = st.columns([2, 3])
    with list_col:
        st.dataframe(summary_rows, width="stretch", hide_index=True)
        selected_id = st.selectbox(
            "Open lead",
            options=page_ids,
            format_func=lambda lead_id: next(
                _outreach_page_label(lead)
                for lead in approved
                if lead.get("ID") == lead_id
            ),
            key=state_keys["selected_widget"],
        )
        if selected_id != st.session_state.get(state_keys["selected"]):
            st.session_state[state_keys["selected"]] = selected_id

    with detail_col:
        selected = cached_lead(campaign["id"], st.session_state.get(state_keys["selected"], ""))
        if selected is None:
            st.error("The selected lead could not be loaded.")
            return
        if selected.get("Draft_Stale") == "1":
            st.warning("These drafts are stale for the current campaign config. You can still edit and send them here.")
        st.markdown(f"### {selected.get('ID', '?')} - {selected.get('Unternehmen', '')}")
        _render_editable_draft_workspace(campaign, selected, key_prefix=f"outreach_{selected.get('ID', '?')}")
        _render_contact_log(selected)


def _render_recontact(campaign: dict, leads: list[dict]) -> None:
    st.title("Re-contact")
    candidates = [
        lead for lead in leads
        if lead.get("Status") not in TERMINAL_STATUSES
        and _has_contact_history(lead)
        and any((lead.get(field) or "").strip() for field in ("Email_Draft", "WhatsApp_Draft", "Phone_Script"))
        and lead.get("Status") != "approved"
    ]
    candidates.sort(key=lambda lead: (_first_contact_sort_key(lead), int(lead.get("Priority") or 5), lead.get("Unternehmen") or ""))

    if not candidates:
        st.success("No previously contacted leads need re-contact right now.")
        return

    st.caption("Leads you already touched at least once, sorted by longest time since first contact.")

    for lead in candidates:
        lid = lead.get("ID", "?")
        company = lead.get("Unternehmen", "")
        status = lead.get("Status", "new")
        channel = planned_channel(lead)
        priority = lead.get("Priority", "5")
        days = _days_since_first_contact(lead)
        age_label = f"{days}d since first contact" if days is not None else "no first-contact date"
        label = (
            f"{lid} - {company[:38]} | {STATUS_EMOJI.get(status, '')} {status}"
            f" | {CHANNEL_EMOJI.get(channel, '?')} {channel} | "
            f"{PRIORITY_COLOR.get(int(priority) if str(priority).isdigit() else 5, '⚫')} P{priority} | {age_label}"
        )
        with st.expander(label):
            if lead.get("Draft_Stale") == "1":
                st.warning("These drafts are stale for the current campaign config. You can still edit and send them here.")
            st.caption(f"First contact: {_first_contact_at(lead) or '-'}")
            _render_editable_draft_workspace(campaign, lead, key_prefix=f"recontact_{lid}")
            _render_contact_log(lead)


def _render_all_leads(campaign: dict) -> None:
    st.title("All Leads")
    st.caption("Search applies on submit and results are paginated to keep the page responsive with large lead lists and browser extensions like Bitwarden.")

    state_keys = _init_all_leads_state(campaign)
    with st.form(key=f"all_leads_filters_{campaign['id']}"):
        st.text_input(
            "Search",
            placeholder="Company, address, contact, email...",
            key=state_keys["search"],
        )
        with st.expander("Filters", expanded=True):
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.multiselect("Status", ALL_LEADS_STATUS_OPTIONS, key=state_keys["status"])
            c2.multiselect("Channel", ALL_LEADS_CHANNEL_OPTIONS, key=state_keys["channel"])
            c3.multiselect("Priority", ALL_LEADS_PRIORITY_OPTIONS, key=state_keys["priority"])
            c4.selectbox("Freshness", ["All", "Draft stale", "Research stale", "Fresh only"], key=state_keys["stale"])
            page_size = c5.selectbox("Page size", ALL_LEADS_PAGE_SIZE_OPTIONS, key=state_keys["page_size"])

        f1, f2 = st.columns(2)
        apply_filters = f1.form_submit_button("Apply Filters", type="primary", width="stretch")
        reset_filters = f2.form_submit_button(
            "Reset Filters",
            width="stretch",
            on_click=_reset_all_leads_state,
            args=(state_keys,),
        )

    if reset_filters:
        reload()

    if apply_filters:
        st.session_state[state_keys["page"]] = 1

    current_page = max(1, int(st.session_state.get(state_keys["page"], 1) or 1))
    search = (st.session_state.get(state_keys["search"]) or "").strip()
    sel_status = tuple(st.session_state.get(state_keys["status"], []))
    sel_channel = tuple(st.session_state.get(state_keys["channel"], []))
    sel_priority = tuple(st.session_state.get(state_keys["priority"], []))
    sel_stale = st.session_state.get(state_keys["stale"], "All")
    summary_page = cached_all_leads_summary(
        campaign["id"],
        search,
        sel_status,
        sel_channel,
        sel_priority,
        sel_stale,
        current_page,
        page_size,
    )
    filtered = list(summary_page.get("items") or [])
    total_filtered = int(summary_page.get("total_count") or 0)
    if not filtered and current_page > 1:
        st.session_state[state_keys["page"]] = current_page - 1
        reload()
        return
    if total_filtered == 0:
        if search or sel_status or sel_channel or sel_priority or sel_stale != "All":
            st.info("No leads match the current filters.")
        else:
            st.warning("No leads in the active campaign yet.")
        return

    total_pages = max(1, (total_filtered + page_size - 1) // page_size)
    if current_page > total_pages:
        current_page = total_pages
        st.session_state[state_keys["page"]] = current_page
    if current_page < 1:
        current_page = 1
        st.session_state[state_keys["page"]] = current_page

    nav1, nav2, nav3 = st.columns([1, 2, 1])
    if nav1.button("Prev", key=f"all_leads_prev_{campaign['id']}", disabled=current_page <= 1):
        st.session_state[state_keys["page"]] = current_page - 1
        reload()
    nav2.caption(f"Page {current_page} of {total_pages}")
    if nav3.button("Next", key=f"all_leads_next_{campaign['id']}", disabled=current_page >= total_pages):
        st.session_state[state_keys["page"]] = current_page + 1
        reload()

    start = (current_page - 1) * page_size + 1
    end = min(current_page * page_size, total_filtered)
    st.caption(f"Showing {start}-{end} of {total_filtered} filtered leads")

    page_ids = [lead.get("ID", "") for lead in filtered if lead.get("ID")]
    if not page_ids:
        st.info("No leads match the current page.")
        return
    if st.session_state.get(state_keys["selected"]) not in page_ids:
        st.session_state[state_keys["selected"]] = page_ids[0]
    if st.session_state.get(state_keys["selected_widget"]) not in page_ids:
        st.session_state[state_keys["selected_widget"]] = st.session_state[state_keys["selected"]]

    summary_rows = [
        {
            "ID": lead.get("ID", ""),
            "Company": lead.get("Unternehmen", ""),
            "Status": lead.get("Status", "new"),
            "Priority": lead.get("Priority", "5"),
            "Channel": lead.get("Planned_Channel", "none"),
            "Stale": ", ".join(
                flag
                for flag, enabled in (
                    ("draft", lead.get("Draft_Stale") == "1"),
                    ("research", lead.get("Research_Stale") == "1"),
                )
                if enabled
            ),
        }
        for lead in filtered
    ]

    list_col, detail_col = st.columns([2, 3])
    with list_col:
        st.dataframe(summary_rows, width="stretch", hide_index=True)
        selected_id = st.selectbox(
            "Open lead",
            options=page_ids,
            format_func=lambda lead_id: next(
                _all_leads_page_label(lead)
                for lead in filtered
                if lead.get("ID") == lead_id
            ),
            key=state_keys["selected_widget"],
        )
        if selected_id != st.session_state.get(state_keys["selected"]):
            st.session_state[state_keys["selected"]] = selected_id

    with detail_col:
        lead = cached_lead(campaign["id"], st.session_state.get(state_keys["selected"], ""))
        if lead is None:
            st.error("The selected lead could not be loaded.")
            return

        lid = lead.get("ID", "?")
        st.markdown(f"### {lid} - {lead.get('Unternehmen', '')}")
        left, right = st.columns([2, 3])
        with left:
            st.markdown("**Contact Info**")
            st.text(f"Kontakt: {lead.get('Kontaktname') or '-'}")
            st.text(f"Email:   {lead.get('Email') or '-'}")
            st.text(f"Telefon: {lead.get('TelNr') or '-'}")
            st.text(f"Adresse: {lead.get('Adresse') or '-'}")
            website = lead.get("Website", "")
            if website and website not in ("X", ""):
                st.markdown(f"[Website]({website})")
            if lead.get("Google_Maps_Link"):
                st.markdown(f"[Maps]({lead['Google_Maps_Link']})")
            st.divider()
            st.markdown("**Research**")
            st.text(f"Website score: {lead.get('Website_Score') or '-'}")
            st.text(f"Google rank:   {lead.get('Google_Rank_Position') or '-'}")
            st.text(f"Template:      {lead.get('Template_Used') or '-'}")

            default_price = campaign.get("price_default") or "500"
            price_value = st.text_input("Price", value=lead.get("Price") or default_price, key=f"price_{lid}")
            lead_channel_options = available_channels(lead)
            if lead_channel_options:
                selected_channel = st.selectbox(
                    "Planned channel",
                    options=lead_channel_options,
                    index=lead_channel_options.index(planned_channel(lead)),
                    format_func=_channel_label,
                    key=f"channel_{lid}",
                )
            else:
                selected_channel = "none"
                st.caption("No usable outreach channel on this lead.")
            notes_value = st.text_area("Notes", value=lead.get("Notes", ""), key=f"notes_{lid}", height=100)
            if st.button("Save", key=f"save_{lid}"):
                row = get_lead_by_id(lid, campaign=campaign)
                if row is not None:
                    previous_channel = row.get("Preferred_Channel", "")
                    row["Price"] = price_value
                    row["Preferred_Channel"] = selected_channel
                    if selected_channel != "none" and row.get("Status") not in TERMINAL_STATUSES:
                        row["Next_Action_Type"] = selected_channel
                    if (
                        selected_channel != previous_channel
                        and (row.get("Scheduled_Send_Status") or "").strip() == "queued"
                    ):
                        clear_scheduled_send(row)
                    row["Notes"] = notes_value
                    save_lead(row, campaign=campaign)
                reload(campaign_id=campaign["id"])
            if st.button("Blacklist", key=f"blacklist_{lid}"):
                row = get_lead_by_id(lid, campaign=campaign)
                if row is not None:
                    row["Status"] = "blacklist"
                    row["Next_Action_Type"] = "none"
                    clear_scheduled_send(row)
                    save_lead(row, campaign=campaign)
                reload(campaign_id=campaign["id"])
            st.divider()
            _render_contact_log(lead)

        with right:
            if lead.get("Draft_Stale") == "1":
                st.warning("These drafts are stale for the current campaign config.")
            if lead.get("Research_Stale") == "1":
                st.warning("Research for this lead is stale for the current campaign config.")
            _render_editable_draft_workspace(campaign, lead, key_prefix=f"all_leads_{lid}")

            if (lead.get("Website_Category") or lead.get("Research_Stale") == "1") and st.button("Generate Drafts", key=f"regen_{lid}"):
                if _generate_drafts([lid], st.container()):
                    reload(campaign_id=campaign["id"])


def _render_campaigns_page(campaign: dict, metrics: dict[str, int]) -> None:
    st.title("Campaigns")
    st.caption("Create, switch, edit, and run pipeline stages for saved niche campaigns.")

    campaigns = cached_campaigns()
    summary_rows = [
        {
            "id": item["id"],
            "label": item.get("label", item["id"]),
            "keyword": item.get("keyword", ""),
            "location": item.get("location", ""),
            "config_version": item.get("config_version", 1),
            "last_scraped_at": item.get("last_scraped_at", ""),
            "last_analyzed_at": item.get("last_analyzed_at", ""),
        }
        for item in campaigns
    ]
    st.dataframe(summary_rows, width="stretch", hide_index=True)

    st.divider()
    create_left, create_right = st.columns([2, 3])
    with create_left:
        st.subheader("Create Campaign")
        with st.form("create_campaign"):
            keyword = st.text_input("Keyword", placeholder="Schluesseldienst")
            location = st.text_input("Location", placeholder="Wien")
            create_btn = st.form_submit_button("Create and Activate", type="primary")
        if create_btn and keyword.strip() and location.strip():
            create_campaign(keyword.strip(), location.strip(), activate=True)
            reload(campaign_changed=True)
    with create_right:
        st.subheader("Active Campaign")
        st.markdown(f"**{campaign.get('label', campaign['id'])}**")
        if backend.is_postgres_backend():
            st.text("Backend: postgres")
            st.text(f"Database campaign: {campaign.get('id', '')}")
        else:
            st.text(f"CSV: {resolve_csv_path(campaign)}")
        st.text(f"ID prefix: {campaign.get('id_prefix', '')}")
        st.text(f"Config version: {campaign.get('config_version', 1)}")

    st.divider()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Leads", metrics.get("total_leads", 0))
    c2.metric("Draft ready", metrics.get("draft_ready", 0))
    c3.metric("Approved", metrics.get("approved_fresh", 0))
    c4.metric("Draft stale", metrics.get("draft_stale", 0))
    c5.metric("Research stale", metrics.get("research_stale", 0))

    stage_cols = st.columns(5)
    if stage_cols[0].button("Scrape", type="primary", width="stretch"):
        with st.spinner("Scraping Herold..."):
            scrape_campaign(campaign)
            mark_campaign_stage_run(campaign["id"], "scraped")
        reload(campaign_id=campaign["id"], campaign_changed=True)
    if stage_cols[1].button("Migrate", width="stretch"):
        from crm_store import migrate

        with st.spinner("Assigning lead IDs and CRM fields..."):
            if migrate():
                mark_campaign_stage_run(campaign["id"], "migrated")
        reload(campaign_id=campaign["id"], campaign_changed=True)
    if stage_cols[2].button("Enrich", width="stretch"):
        from crm_enrich import main as enrich_main

        with st.spinner("Enriching contacts..."):
            enrich_main()
        reload(campaign_id=campaign["id"], campaign_changed=True)
    if stage_cols[3].button("Research", width="stretch"):
        from crm_research import main as research_main

        with st.spinner("Running research..."):
            research_main()
        reload(campaign_id=campaign["id"], campaign_changed=True)
    if stage_cols[4].button("Analyze", width="stretch"):
        from crm_analyze import main as analyze_main

        with st.spinner("Generating drafts..."):
            analyze_main()
        reload(campaign_id=campaign["id"], campaign_changed=True)

    st.caption(
        "Last runs: "
        f"scrape={campaign.get('last_scraped_at') or '-'} | "
        f"migrate={campaign.get('last_migrated_at') or '-'} | "
        f"enrich={campaign.get('last_enriched_at') or '-'} | "
        f"research={campaign.get('last_researched_at') or '-'} | "
        f"analyze={campaign.get('last_analyzed_at') or '-'}"
    )

    st.divider()
    st.subheader("Campaign Config")
    with st.form("campaign_config"):
        col1, col2 = st.columns(2)
        label = col1.text_input("Label", value=campaign.get("label", ""))
        rank_template = col2.text_input("Rank keyword template", value=campaign.get("rank_keyword_template", "{keyword} {plz}"))
        keyword = col1.text_input("Keyword", value=campaign.get("keyword", ""))
        location = col2.text_input("Location", value=campaign.get("location", ""))
        service_singular = col1.text_input("Service singular", value=campaign.get("service_singular", ""))
        service_plural = col2.text_input("Service plural", value=campaign.get("service_plural", ""))
        price_default = col1.text_input("Default price", value=str(campaign.get("price_default", "500")))
        price_monthly = col2.text_input("Monthly price", value=str(campaign.get("price_monthly", "25")))
        turnaround_days = col1.number_input("Turnaround days", min_value=1, value=int(campaign.get("turnaround_days", 14)))
        sender_name = col2.text_input("Sender name", value=campaign.get("sender_name", ""))
        sender_company = col1.text_input("Sender company", value=campaign.get("sender_company", ""))
        sender_website = col2.text_input("Sender website", value=campaign.get("sender_website", ""))
        sender_phone = col1.text_input("Sender phone", value=campaign.get("sender_phone", ""))
        sender_email = col2.text_input("Sender email", value=campaign.get("sender_email", ""))
        save_config = st.form_submit_button("Save Campaign Config", type="primary")

    if save_config:
        update_campaign(
            campaign["id"],
            {
                "label": label.strip(),
                "rank_keyword_template": rank_template.strip(),
                "keyword": keyword.strip(),
                "location": location.strip(),
                "service_singular": service_singular.strip(),
                "service_plural": service_plural.strip(),
                "price_default": price_default.strip(),
                "price_monthly": price_monthly.strip(),
                "turnaround_days": int(turnaround_days),
                "sender_name": sender_name.strip(),
                "sender_company": sender_company.strip(),
                "sender_website": sender_website.strip(),
                "sender_phone": sender_phone.strip(),
                "sender_email": sender_email.strip(),
            },
        )
        reload(campaign_id=campaign["id"], campaign_changed=True)

    _render_campaign_template_editor(campaign)


def _ensure_stale_contacts_archived(campaign: dict) -> None:
    archive_key = f"archived_stale_{campaign['id']}"
    if archive_key not in st.session_state:
        if backend.is_postgres_backend():
            archive_loader = getattr(backend, "postgres_archive_stale_contacted_leads", None)
            cutoff = (date.today() - timedelta(days=ARCHIVE_AFTER_DAYS)).isoformat()
            archived = archive_loader(campaign["id"], cutoff) if callable(archive_loader) else 0
            if archived:
                reload(campaign_id=campaign["id"])
        else:
            leads = cached_leads(campaign["id"])
            updated, archived = check_and_archive_stale([dict(lead) for lead in leads])
            if archived:
                save_leads(updated, campaign=campaign)
                reload(campaign_id=campaign["id"])
        st.session_state[archive_key] = True


campaign = _active_campaign_switch()
page = st.sidebar.radio(
    "Navigation",
    ["Campaigns", "Dashboard", "Review Queue", "Outreach", "Re-contact", "All Leads"],
    label_visibility="collapsed",
)
dashboard_snapshot: dict = {}

if page == "Campaigns":
    _render_campaigns_page(campaign, cached_campaign_metrics(campaign["id"]))
elif page == "Dashboard":
    dashboard_snapshot = cached_dashboard_snapshot(campaign["id"])
else:
    _ensure_stale_contacts_archived(campaign)

if page == "Dashboard":
    _render_dashboard(campaign, dashboard_snapshot, cached_campaign_metrics(campaign["id"]))
elif page == "Review Queue":
    _render_review_queue(campaign)
elif page == "Outreach":
    _render_outreach(campaign)
elif page == "Re-contact":
    _render_recontact(campaign, cached_recontact_leads(campaign["id"]))
else:
    _render_all_leads(campaign)
