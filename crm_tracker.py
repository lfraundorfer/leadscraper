"""
crm_tracker.py – State machine for logging contact attempts and scheduling follow-ups.
"""

from __future__ import annotations

import crm_backend as backend
import json
from datetime import date, datetime, timedelta

from crm_schedule import clear_scheduled_send
from crm_store import default_preferred_channel, load_leads, save_leads, update_lead, TERMINAL_STATUSES

# Follow-up delays in days after each contact attempt
FOLLOWUP_DAYS = [3, 4, 7]  # Day 0→3, Day 3→7, Day 7→14 (then auto-archive)
ARCHIVE_AFTER_DAYS = 14    # Days after last contact before auto-archiving as no_contact

# Outcome → new Status mapping
OUTCOME_STATUS = {
    "sent":       "contacted",
    "called":     "contacted",
    "voicemail":  "contacted",
    "no_answer":  "contacted",
    "replied":    "replied",
    "meeting":    "meeting_scheduled",
    "won":        "won",
    "done":       "done",
    "lost":       "lost",
    "blacklist":  "blacklist",
}

# Outcome → inferred channel
OUTCOME_CHANNEL = {
    "sent":      "email",
    "called":    "phone",
    "voicemail": "phone",
    "no_answer": "phone",
}


def parse_contact_log(raw: str) -> list[dict]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    entries: list[dict] = []
    for item in data:
        if isinstance(item, dict):
            entries.append(
                {
                    "at": str(item.get("at") or "").strip(),
                    "channel": str(item.get("channel") or "").strip(),
                    "outcome": str(item.get("outcome") or "").strip(),
                    "notes": str(item.get("notes") or "").strip(),
                }
            )
    return entries


def format_contact_log(entries: list[dict]) -> str:
    return json.dumps(entries, ensure_ascii=False)


def append_contact_log(lead: dict, *, at: str, channel: str, outcome: str, notes: str = "") -> None:
    entries = parse_contact_log(lead.get("Contact_Log", ""))
    entries.append(
        {
            "at": at,
            "channel": channel,
            "outcome": outcome,
            "notes": notes.strip(),
        }
    )
    lead["Contact_Log"] = format_contact_log(entries)


def log_contact(lead_id: str, outcome: str, notes: str = "", channel: str = "", campaign: dict | None = None) -> None:
    """
    Log a contact attempt and update the lead state accordingly.

    Outcomes:
      sent / called / voicemail / no_answer  → Status = "contacted" + schedule follow-up
      replied       → Status = "replied"
      meeting       → Status = "meeting_scheduled"
      done / won / lost / blacklist → terminal states
    """
    leads = load_leads(campaign=campaign)
    lead = next((l for l in leads if l.get("ID", "").strip() == lead_id.strip()), None)
    if lead is None:
        print(f"Lead {lead_id} not found.")
        return

    now = datetime.now()
    today_str = now.date().isoformat()
    timestamp = now.strftime("%Y-%m-%d %H:%M")
    new_status = OUTCOME_STATUS.get(outcome, "contacted")

    # Infer channel if not provided
    used_channel = channel or OUTCOME_CHANNEL.get(outcome, lead.get("Channel_Used", ""))

    # Increment contact count for outreach attempts
    if outcome in ("sent", "called", "voicemail", "no_answer"):
        count = int(lead.get("Contact_Count") or 0) + 1
        lead["Contact_Count"] = str(count)

        # Set Kontaktdatum on first contact
        if not lead.get("Kontaktdatum"):
            lead["Kontaktdatum"] = today_str

        # Schedule next follow-up
        next_date, next_type = _calculate_next_action(lead, count, current_channel=used_channel)
        lead["Next_Action_Date"] = next_date
        lead["Next_Action_Type"] = next_type
    else:
        # Terminal or replied — no more automatic scheduling
        lead["Next_Action_Type"] = "none"
        lead["Next_Action_Date"] = ""

    lead["Status"] = new_status
    lead["Last_Contact_Date"] = today_str
    lead["Channel_Used"] = used_channel
    clear_scheduled_send(lead)
    append_contact_log(lead, at=timestamp, channel=used_channel, outcome=outcome, notes=notes)

    # Append notes
    if notes:
        existing = lead.get("Notes", "")
        sep = " | " if existing else ""
        lead["Notes"] = f"{existing}{sep}[{today_str}] {notes}"

    save_leads(leads, campaign=campaign)
    active_campaign = campaign
    if active_campaign is None:
        try:
            from campaign_service import get_active_campaign
            active_campaign = get_active_campaign()
        except Exception:
            active_campaign = None
    if backend.is_postgres_backend() and active_campaign is not None:
        backend.postgres_record_contact_event(
            active_campaign.get("id", ""),
            lead_id,
            occurred_at=timestamp,
            channel=used_channel,
            outcome=outcome,
            notes=notes,
        )
    print(f"Logged: {lead_id} → {new_status} | next: {lead.get('Next_Action_Type', '-')} on {lead.get('Next_Action_Date', '-')}")


def _calculate_next_action(lead: dict, contact_count: int, current_channel: str = "") -> tuple[str, str]:
    """Return (next_action_date_str, next_action_type) based on contact count."""
    today = date.today()

    if contact_count >= len(FOLLOWUP_DAYS) + 1:
        # All attempts exhausted
        return "", "none"

    delay_idx = min(contact_count - 1, len(FOLLOWUP_DAYS) - 1)
    delay = FOLLOWUP_DAYS[delay_idx]
    next_date = (today + timedelta(days=delay)).isoformat()

    # Determine next channel (secondary/tertiary)
    primary = (
        current_channel
        or lead.get("Next_Action_Type")
        or lead.get("Preferred_Channel")
        or lead.get("Channel_Used")
        or default_preferred_channel(lead)
    )
    tel = lead.get("TelNr", "")
    email = lead.get("Email", "")

    from crm_store import is_mobile
    has_mobile = bool(tel) and is_mobile(tel)
    has_email = bool(email)

    if contact_count == 1:
        # Second touch: use secondary channel
        if primary == "email":
            next_type = "whatsapp" if has_mobile else "phone" if tel else "none"
        elif primary == "whatsapp":
            next_type = "phone" if tel else "none"
        else:  # phone
            next_type = "email" if has_email else "whatsapp" if has_mobile else "none"
    else:
        # Final touch: always phone if available
        next_type = "phone" if tel else "none"

    return next_date, next_type


def check_and_archive_stale(leads: list[dict]) -> tuple[list[dict], int]:
    """
    Auto-archive leads that have been contacted 3+ times with no reply
    and Last_Contact_Date is more than ARCHIVE_AFTER_DAYS days ago.
    Returns (modified_leads, archived_count).
    """
    today = date.today()
    archived = 0

    for lead in leads:
        if lead.get("Status") != "contacted":
            continue
        count = int(lead.get("Contact_Count") or 0)
        if count < 3:
            continue
        last = lead.get("Last_Contact_Date", "")
        if not last:
            continue
        try:
            last_date = date.fromisoformat(last)
        except ValueError:
            continue
        if (today - last_date).days >= ARCHIVE_AFTER_DAYS:
            lead["Status"] = "no_contact"
            lead["Next_Action_Type"] = "none"
            lead["Next_Action_Date"] = ""
            archived += 1

    return leads, archived
