"""
crm_fields.py - Shared CRM lead schema constants.
"""

from __future__ import annotations

import re

# Original columns (must stay in this order)
ORIGINAL_COLUMNS = [
    "Unternehmen",
    "Website",
    "TelNr",
    "Email",
    "Kontaktname",
    "Kontaktdatum",
    "Source",
    "Notes",
    "Adresse",
    "Google_Maps_Link",
    "FirmenABC_Link",
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
    "Approved_At",
    "Scheduled_Send_At",
    "Scheduled_Send_Channel",
    "Scheduled_Send_Status",
    "Scheduled_Send_Error",
    "Scheduled_Send_Attempts",
    "Sent_At",
    "SMTP_Message_ID",
    "Enriched_At",
    "Analyzed_At",
]

ALL_COLUMNS = ORIGINAL_COLUMNS + NEW_COLUMNS

VALID_STATUSES = {
    "new",
    "draft_ready",
    "approved",
    "contacted",
    "replied",
    "meeting_scheduled",
    "won",
    "lost",
    "done",
    "no_contact",
    "blacklist",
}

PRE_CONTACT_STATUSES = {"new", "draft_ready", "approved"}
TERMINAL_STATUSES = {"won", "lost", "done", "blacklist"}


def normalize_company_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def is_pre_contact_status(status: str) -> bool:
    normalized = (status or "").strip() or "new"
    return normalized in PRE_CONTACT_STATUSES
