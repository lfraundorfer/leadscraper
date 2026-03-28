"""
crm_store.py - Single source of truth for reading and writing the active CRM CSV.

All modules go through this layer so campaign switching works everywhere.
Writes are atomic (tmp file + rename) to prevent corruption on crash.
"""

from __future__ import annotations

import csv
import os
import re
import tempfile
from typing import Optional

import crm_backend as backend
from campaign_service import get_active_campaign, resolve_active_csv_path, resolve_csv_path
from crm_fields import ALL_COLUMNS, ORIGINAL_COLUMNS, TERMINAL_STATUSES, VALID_STATUSES


DELIMITER = ";"

WIEN_BEZIRK: dict[str, tuple[str, str]] = {
    "1010": ("1.", "Innere Stadt"),
    "1020": ("2.", "Leopoldstadt"),
    "1030": ("3.", "Landstraße"),
    "1040": ("4.", "Wieden"),
    "1050": ("5.", "Margareten"),
    "1060": ("6.", "Mariahilf"),
    "1070": ("7.", "Neubau"),
    "1080": ("8.", "Josefstadt"),
    "1090": ("9.", "Alsergrund"),
    "1100": ("10.", "Favoriten"),
    "1110": ("11.", "Simmering"),
    "1120": ("12.", "Meidling"),
    "1130": ("13.", "Hietzing"),
    "1140": ("14.", "Penzing"),
    "1150": ("15.", "Rudolfsheim-Fünfhaus"),
    "1160": ("16.", "Ottakring"),
    "1170": ("17.", "Hernals"),
    "1180": ("18.", "Währing"),
    "1190": ("19.", "Döbling"),
    "1200": ("20.", "Brigittenau"),
    "1210": ("21.", "Floridsdorf"),
    "1220": ("22.", "Donaustadt"),
    "1230": ("23.", "Liesing"),
}


def get_bezirk(adresse: str) -> tuple[str, str]:
    """Extract PLZ and Bezirk name from address string. Returns (plz, bezirk_name)."""
    m = re.search(r"\b(1[012]\d{2})\b", adresse)
    if m:
        plz = m.group(1)
        if plz in WIEN_BEZIRK:
            return plz, WIEN_BEZIRK[plz][1]
        return plz, ""
    return "", ""


def is_mobile(tel: str) -> bool:
    """Returns True if the phone number is an Austrian mobile (starts with +436 or 06)."""
    normalized = tel.replace(" ", "").replace("-", "").replace("/", "")
    return normalized.startswith("+436") or normalized.startswith("06")


def available_channels(lead: dict) -> list[str]:
    """Return usable channels in default-priority order."""
    channels: list[str] = []
    email = (lead.get("Email") or "").strip()
    tel = (lead.get("TelNr") or "").strip()

    if email:
        channels.append("email")
    if tel and is_mobile(tel):
        channels.append("whatsapp")
    if tel:
        channels.append("phone")
    return channels


def default_preferred_channel(lead: dict) -> str:
    """Default to email first, then WhatsApp, then phone."""
    channels = available_channels(lead)
    return channels[0] if channels else "none"


def _contact_count(lead: dict) -> int:
    try:
        return int(lead.get("Contact_Count") or 0)
    except (TypeError, ValueError):
        return 0


def preferred_channel(lead: dict) -> str:
    """
    Resolve the user-preferred starting channel.

    Falls back to legacy fields for untouched pre-contact leads so existing data
    keeps working after the new column is introduced.
    """
    channels = available_channels(lead)
    if not channels:
        return "none"

    saved = (lead.get("Preferred_Channel") or "").strip()
    if saved in channels:
        return saved

    if _contact_count(lead) == 0:
        for field in ("Next_Action_Type", "Channel_Used"):
            legacy = (lead.get(field) or "").strip()
            if legacy in channels:
                return legacy

    return channels[0]


def planned_channel(lead: dict) -> str:
    """
    Resolve the channel the UI should show as the current plan.

    Prefer the scheduled next action, then the saved preference, then the most
    recent used channel, then the default preference.
    """
    channels = available_channels(lead)
    if not channels:
        return "none"

    current = (lead.get("Next_Action_Type") or "").strip()
    if current in channels:
        return current

    preferred = preferred_channel(lead)
    if preferred in channels:
        return preferred

    used = (lead.get("Channel_Used") or "").strip()
    if used in channels:
        return used

    return channels[0]


def _resolve_csv_path(csv_path: str = "", campaign: Optional[dict] = None) -> str:
    if csv_path:
        return csv_path
    if campaign:
        return resolve_csv_path(campaign)
    return resolve_active_csv_path()


def progress_save_interval(default: int = 10) -> int:
    raw = (os.getenv("CRM_SAVE_EVERY") or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _data_exists(row: dict, fields: list[str]) -> bool:
    return any((row.get(field) or "").strip() for field in fields)


def _draft_version(campaign: Optional[dict] = None) -> str:
    active = campaign or get_active_campaign()
    return str(active.get("draft_config_version") or active.get("config_version") or "1")


def _research_version(campaign: Optional[dict] = None) -> str:
    active = campaign or get_active_campaign()
    return str(active.get("research_config_version") or active.get("config_version") or "1")


def _apply_staleness_defaults(row: dict, draft_version: str, research_version: str) -> dict:
    if not row.get("Status"):
        row["Status"] = "new"
    if not row.get("Contact_Count"):
        row["Contact_Count"] = "0"
    if not row.get("Drafts_Approved"):
        row["Drafts_Approved"] = "0"
    row["Preferred_Channel"] = preferred_channel(row)

    has_research = _data_exists(
        row,
        [
            "Website_Category",
            "Website_Score",
            "Google_Rating",
            "Google_Rank_Keyword",
            "Research_Config_Version",
        ],
    )
    has_drafts = _data_exists(
        row,
        [
            "Email_Draft",
            "WhatsApp_Draft",
            "Phone_Script",
            "Draft_Config_Version",
        ],
    )

    if has_research and not row.get("Research_Config_Version"):
        row["Research_Config_Version"] = research_version
    if has_drafts and not row.get("Draft_Config_Version"):
        row["Draft_Config_Version"] = draft_version

    row_research_version = (row.get("Research_Config_Version") or "").strip()
    row_draft_version = (row.get("Draft_Config_Version") or "").strip()

    row["Research_Stale"] = "1" if has_research and row_research_version and row_research_version != research_version else "0"
    row["Draft_Stale"] = "1" if has_drafts and row_draft_version and row_draft_version != draft_version else "0"
    return row


def ensure_lead_ids(leads: list[dict], campaign: Optional[dict] = None) -> int:
    """
    Assign IDs only to leads that are still missing one.
    Keeps existing IDs stable so repeated scrapes do not renumber the whole campaign.
    Returns the number of IDs assigned.
    """
    active_campaign = campaign or get_active_campaign()
    prefix = (active_campaign.get("id_prefix") or "LEAD").upper()
    draft_version = _draft_version(active_campaign)
    research_version = _research_version(active_campaign)

    next_number = 0
    for lead in leads:
        lead_id = str(lead.get("ID") or "").strip()
        if not lead_id.startswith(f"{prefix}-"):
            continue
        suffix = lead_id.rsplit("-", 1)[-1]
        try:
            next_number = max(next_number, int(suffix))
        except ValueError:
            continue

    assigned = 0
    for lead in leads:
        if not str(lead.get("ID") or "").strip():
            next_number += 1
            lead["ID"] = f"{prefix}-{next_number:04d}"
            assigned += 1
        if not lead.get("Status"):
            lead["Status"] = "new"
        if not lead.get("Contact_Count"):
            lead["Contact_Count"] = "0"
        if not lead.get("Drafts_Approved"):
            lead["Drafts_Approved"] = "0"
        lead["Preferred_Channel"] = preferred_channel(lead)
        if not lead.get("Draft_Config_Version") and _data_exists(lead, ["Email_Draft", "WhatsApp_Draft", "Phone_Script"]):
            lead["Draft_Config_Version"] = draft_version
        if not lead.get("Research_Config_Version") and _data_exists(lead, ["Website_Category", "Google_Rank_Keyword", "Google_Rating"]):
            lead["Research_Config_Version"] = research_version

    return assigned


def load_leads(csv_path: str = "", campaign: Optional[dict] = None) -> list[dict]:
    """Load all rows from the active CRM CSV. Missing columns default to ''."""
    if backend.is_postgres_backend() and not csv_path:
        active_campaign = campaign or get_active_campaign()
        leads = backend.postgres_load_leads(active_campaign["id"])
        draft_version = _draft_version(active_campaign)
        research_version = _research_version(active_campaign)
        return [_apply_staleness_defaults(dict(lead), draft_version, research_version) for lead in leads]

    path = _resolve_csv_path(csv_path=csv_path, campaign=campaign)
    if not os.path.exists(path):
        return []

    draft_version = _draft_version(campaign)
    research_version = _research_version(campaign)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        leads = []
        for row in reader:
            for col in ALL_COLUMNS:
                if col not in row:
                    row[col] = ""
            leads.append(_apply_staleness_defaults(dict(row), draft_version, research_version))
    return leads


def save_leads(leads: list[dict], csv_path: str = "", campaign: Optional[dict] = None) -> None:
    """Atomically write all leads back to the active CSV (tmp + rename)."""
    if backend.is_postgres_backend() and not csv_path:
        active_campaign = campaign or get_active_campaign()
        draft_version = _draft_version(active_campaign)
        research_version = _research_version(active_campaign)
        normalized = []
        for lead in leads:
            row = {col: lead.get(col, "") for col in ALL_COLUMNS}
            normalized.append(_apply_staleness_defaults(row, draft_version, research_version))
        backend.postgres_save_leads(active_campaign["id"], normalized)
        return

    path = _resolve_csv_path(csv_path=csv_path, campaign=campaign)
    dir_ = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(dir_, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8",
        dir=dir_, delete=False, suffix=".tmp"
    ) as tmp:
        writer = csv.DictWriter(
            tmp, fieldnames=ALL_COLUMNS,
            delimiter=DELIMITER, extrasaction="ignore"
        )
        writer.writeheader()
        draft_version = _draft_version(campaign)
        research_version = _research_version(campaign)
        for lead in leads:
            row = {col: lead.get(col, "") for col in ALL_COLUMNS}
            row = _apply_staleness_defaults(row, draft_version, research_version)
            writer.writerow(row)
        tmp_path = tmp.name

    os.replace(tmp_path, path)


def save_lead(lead: dict, csv_path: str = "", campaign: Optional[dict] = None) -> None:
    """Persist a single lead row without rewriting the whole campaign in Postgres mode."""
    lead_id = str(lead.get("ID") or "").strip()
    if not lead_id:
        raise ValueError("Lead ID is required for save_lead().")

    if backend.is_postgres_backend() and not csv_path:
        active_campaign = campaign or get_active_campaign()
        row = {col: lead.get(col, "") for col in ALL_COLUMNS}
        normalized = _apply_staleness_defaults(row, _draft_version(active_campaign), _research_version(active_campaign))
        backend.postgres_upsert_lead(active_campaign["id"], normalized)
        return

    leads = load_leads(csv_path=csv_path, campaign=campaign)
    replaced = False
    for index, existing in enumerate(leads):
        if str(existing.get("ID") or "").strip() != lead_id:
            continue
        leads[index] = dict(lead)
        replaced = True
        break
    if not replaced:
        leads.append(dict(lead))
    save_leads(leads, csv_path=csv_path, campaign=campaign)


def save_leads_batch(leads: list[dict], csv_path: str = "", campaign: Optional[dict] = None) -> None:
    """Persist only the provided leads, leaving all other rows untouched."""
    if backend.is_postgres_backend() and not csv_path:
        active_campaign = campaign or get_active_campaign()
        draft_version = _draft_version(active_campaign)
        research_version = _research_version(active_campaign)
        normalized = []
        for lead in leads:
            lead_id = str(lead.get("ID") or "").strip()
            if not lead_id:
                continue
            row = {col: lead.get(col, "") for col in ALL_COLUMNS}
            normalized.append(_apply_staleness_defaults(row, draft_version, research_version))
        backend.postgres_upsert_leads(active_campaign["id"], normalized)
        return

    existing = load_leads(csv_path=csv_path, campaign=campaign)
    by_id = {str(lead.get("ID") or "").strip(): dict(lead) for lead in leads if str(lead.get("ID") or "").strip()}
    if not by_id:
        return
    replaced_ids = set()
    for index, row in enumerate(existing):
        lead_id = str(row.get("ID") or "").strip()
        if lead_id not in by_id:
            continue
        existing[index] = by_id[lead_id]
        replaced_ids.add(lead_id)
    for lead_id, row in by_id.items():
        if lead_id not in replaced_ids:
            existing.append(row)
    save_leads(existing, csv_path=csv_path, campaign=campaign)


def get_lead_by_id(lead_id: str, campaign: Optional[dict] = None) -> Optional[dict]:
    """Find a single lead by its ID field in the active campaign."""
    if backend.is_postgres_backend():
        active_campaign = campaign or get_active_campaign()
        lead = backend.postgres_get_lead_by_id(lead_id.strip(), campaign_id=active_campaign.get("id", ""))
        if lead is not None:
            return _apply_staleness_defaults(lead, _draft_version(active_campaign), _research_version(active_campaign))
        return None
    for lead in load_leads(campaign=campaign):
        if lead.get("ID", "").strip() == lead_id.strip():
            return lead
    return None


def update_lead(lead_id: str, updates: dict, campaign: Optional[dict] = None) -> bool:
    """Load all leads, update one row by ID, save atomically. Returns True if found."""
    if backend.is_postgres_backend():
        active_campaign = campaign or get_active_campaign()
        lead = get_lead_by_id(lead_id, campaign=active_campaign)
        if lead is None:
            return False
        lead.update(updates)
        save_lead(lead, campaign=active_campaign)
        return True

    leads = load_leads(campaign=campaign)
    found = False
    for lead in leads:
        if lead.get("ID", "").strip() == lead_id.strip():
            lead.update(updates)
            found = True
            break
    if found:
        save_leads(leads, campaign=campaign)
    return found


def migrate() -> int:
    """
    One-time: add new columns to the active campaign CSV and assign IDs.
    Returns number of rows migrated.
    """
    campaign = get_active_campaign()
    path = resolve_csv_path(campaign)
    leads = load_leads(campaign=campaign)
    if not leads:
        print(f"No leads found in '{path}'. Nothing to migrate.")
        return 0

    assigned = ensure_lead_ids(leads, campaign=campaign)
    if assigned == 0:
        print(f"Already migrated: all {len(leads)} leads already have IDs.")
        return 0

    save_leads(leads, campaign=campaign)
    print(f"Migrated {assigned} lead(s) -> '{path}'")
    return assigned
