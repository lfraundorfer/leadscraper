"""
crm_store.py – Single source of truth for reading/writing the CRM CSV.

All other modules import from here — they never touch the CSV directly.
Writes are atomic (tmp file + rename) to prevent corruption on crash.
"""

from __future__ import annotations

import csv
import os
import re
import tempfile
from datetime import date
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CRM_CSV_PATH = os.getenv("CRM_CSV_PATH", "new_leads.csv")
DELIMITER = ";"

# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

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
    "Priority",
    # Research results
    "Website_Category",
    "Website_Score",
    "Pain_Points",
    "Pain_Categories",
    "Mobile_Screenshot",
    "Google_Rating",
    "Google_Review_Count",
    "Google_Review_Snippets",
    "Google_Rank_Keyword",
    "Google_Rank_Position",
    "Google_Map_Pack",
    "Google_Competitors",
    # Generated drafts
    "Email_Draft",
    "WhatsApp_Draft",
    "Phone_Script",
    "Drafts_Approved",
    "Template_Used",
    # Pricing
    "Price",
    # CRM tracking
    "Last_Contact_Date",
    "Next_Action_Date",
    "Next_Action_Type",
    "Contact_Count",
    "Enriched_At",
    "Analyzed_At",
]

ALL_COLUMNS = ORIGINAL_COLUMNS + NEW_COLUMNS

# Valid status values
VALID_STATUSES = {
    "new", "draft_ready", "approved", "contacted", "replied",
    "meeting_scheduled", "won", "lost", "no_contact", "blacklist",
}

TERMINAL_STATUSES = {"won", "lost", "blacklist"}

# ---------------------------------------------------------------------------
# Wien Bezirk map (PLZ → (number, name))
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def load_leads() -> list[dict]:
    """Load all rows from the CRM CSV. Missing new columns default to ''."""
    if not os.path.exists(CRM_CSV_PATH):
        return []
    with open(CRM_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        leads = []
        for row in reader:
            # Fill any missing new columns with empty string
            for col in ALL_COLUMNS:
                if col not in row:
                    row[col] = ""
            leads.append(dict(row))
    return leads


def save_leads(leads: list[dict]) -> None:
    """Atomically write all leads back to the CSV (tmp + rename)."""
    dir_ = os.path.dirname(os.path.abspath(CRM_CSV_PATH)) or "."
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8",
        dir=dir_, delete=False, suffix=".tmp"
    ) as tmp:
        writer = csv.DictWriter(
            tmp, fieldnames=ALL_COLUMNS,
            delimiter=DELIMITER, extrasaction="ignore"
        )
        writer.writeheader()
        for lead in leads:
            # Ensure all columns exist
            row = {col: lead.get(col, "") for col in ALL_COLUMNS}
            writer.writerow(row)
        tmp_path = tmp.name

    os.replace(tmp_path, CRM_CSV_PATH)


def get_lead_by_id(lead_id: str) -> Optional[dict]:
    """Find a single lead by its ID field."""
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


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def migrate() -> int:
    """
    One-time: add new columns to an existing CSV and assign IDs.
    Returns number of rows migrated.
    """
    leads = load_leads()
    if not leads:
        print(f"No leads found in '{CRM_CSV_PATH}'. Nothing to migrate.")
        return 0

    # Check if already migrated (all leads have INSTWIEN- IDs)
    already_done = sum(1 for l in leads if l.get("ID", "").startswith("INSTWIEN-"))
    if already_done == len(leads):
        print(f"Already migrated: all {len(leads)} leads have INSTWIEN- IDs.")
        return 0

    # Assign INSTWIEN-0001 … INSTWIEN-NNNN to every lead in order
    for i, lead in enumerate(leads, start=1):
        lead["ID"] = f"INSTWIEN-{i:04d}"
        if not lead.get("Status"):
            lead["Status"] = "new"
        if not lead.get("Contact_Count"):
            lead["Contact_Count"] = "0"
        if not lead.get("Drafts_Approved"):
            lead["Drafts_Approved"] = "0"

    save_leads(leads)
    print(f"Migrated {len(leads)} leads → '{CRM_CSV_PATH}'")
    print(f"IDs: INSTWIEN-0001 … INSTWIEN-{len(leads):04d}")
    return len(leads)
