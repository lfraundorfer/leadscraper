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

from campaign_service import get_active_campaign, resolve_active_csv_path, resolve_csv_path


DELIMITER = ";"

# Original columns (must stay in this order)
ORIGINAL_COLUMNS = [
    "Unternehmen", "Website", "TelNr", "Email", "Kontaktname",
    "Kontaktdatum", "Source", "Notes", "Adresse",
    "Google_Maps_Link", "FirmenABC_Link",
]

# New CRM columns added by `migrate`
NEW_COLUMNS = [
    "ID",
    "Status",
    "Channel_Used",
    "Preferred_Channel",
    "Priority",
    # Research results
    "Website_Category",
    "Website_Score",
    "Pain_Points",
    "Pain_Categories",
    "Google_Rating",
    "Google_Review_Count",
    "Google_Review_Snippets",
    "Google_Rank_Keyword",
    "Google_Rank_Position",
    "Google_Map_Pack",
    "Google_Competitors",
    "Research_Config_Version",
    "Research_Stale",
    # Generated drafts
    "Email_Draft",
    "WhatsApp_Draft",
    "Phone_Script",
    "Drafts_Approved",
    "Template_Used",
    "Draft_Config_Version",
    "Draft_Stale",
    # Pricing
    "Price",
    # CRM tracking
    "Last_Contact_Date",
    "Next_Action_Date",
    "Next_Action_Type",
    "Contact_Count",
    "Contact_Log",
    "Enriched_At",
    "Analyzed_At",
]

ALL_COLUMNS = ORIGINAL_COLUMNS + NEW_COLUMNS

VALID_STATUSES = {
    "new", "draft_ready", "approved", "contacted", "replied",
    "meeting_scheduled", "won", "lost", "done", "no_contact", "blacklist",
}

TERMINAL_STATUSES = {"won", "lost", "done", "blacklist"}

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


def load_leads(csv_path: str = "", campaign: Optional[dict] = None) -> list[dict]:
    """Load all rows from the active CRM CSV. Missing columns default to ''."""
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


def get_lead_by_id(lead_id: str) -> Optional[dict]:
    """Find a single lead by its ID field in the active campaign."""
    for lead in load_leads():
        if lead.get("ID", "").strip() == lead_id.strip():
            return lead
    return None


def update_lead(lead_id: str, updates: dict) -> bool:
    """Load all leads, update one row by ID, save atomically. Returns True if found."""
    leads = load_leads()
    found = False
    for lead in leads:
        if lead.get("ID", "").strip() == lead_id.strip():
            lead.update(updates)
            found = True
            break
    if found:
        save_leads(leads)
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

    prefix = (campaign.get("id_prefix") or "LEAD").upper()
    already_done = sum(1 for lead in leads if lead.get("ID", "").startswith(f"{prefix}-"))
    if already_done == len(leads):
        print(f"Already migrated: all {len(leads)} leads have {prefix}- IDs.")
        return 0

    draft_version = _draft_version(campaign)
    research_version = _research_version(campaign)
    for i, lead in enumerate(leads, start=1):
        lead["ID"] = f"{prefix}-{i:04d}"
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

    save_leads(leads, campaign=campaign)
    print(f"Migrated {len(leads)} leads -> '{path}'")
    print(f"IDs: {prefix}-0001 ... {prefix}-{len(leads):04d}")
    return len(leads)
