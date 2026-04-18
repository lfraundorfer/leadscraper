"""
crm_schedule.py - Shared scheduling helpers for queued email sending.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


VIENNA_TZ = ZoneInfo("Europe/Vienna")
BUSINESS_DAY_START = time(hour=9, minute=0)
BUSINESS_DAY_END = time(hour=17, minute=0)


def vienna_now() -> datetime:
    return datetime.now(VIENNA_TZ)


def is_business_day(value: date) -> bool:
    return value.weekday() < 5


def next_business_day(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_business_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def combine_vienna(value: date, at: time) -> datetime:
    return datetime.combine(value, at, tzinfo=VIENNA_TZ)


def scheduled_send_datetime(choice: str, now: datetime | None = None) -> datetime | None:
    current = now or vienna_now()
    today = current.date()

    if choice == "today":
        # Send immediately — no weekend or business-hours check.
        return current

    if choice == "tomorrow":
        if is_business_day(today):
            target = next_business_day(today)
        else:
            target = today
            while not is_business_day(target):
                target += timedelta(days=1)
        return combine_vienna(target, BUSINESS_DAY_START)

    return None


def clear_scheduled_send(lead: dict) -> None:
    lead["Scheduled_Send_At"] = ""
    lead["Scheduled_Send_Channel"] = ""
    lead["Scheduled_Send_Status"] = ""
    lead["Scheduled_Send_Error"] = ""
    lead["Scheduled_Send_Attempts"] = "0"
    lead["SMTP_Message_ID"] = lead.get("SMTP_Message_ID", "")


def queue_scheduled_email(lead: dict, choice: str, now: datetime | None = None) -> datetime | None:
    scheduled_for = scheduled_send_datetime(choice, now=now)
    if scheduled_for is None:
        return None
    lead["Approved_At"] = (now or vienna_now()).isoformat()
    # Once an email is queued, it should stop appearing as a manual next action.
    lead["Next_Action_Type"] = "none"
    lead["Next_Action_Date"] = ""
    lead["Scheduled_Send_At"] = scheduled_for.isoformat()
    lead["Scheduled_Send_Channel"] = "email"
    lead["Scheduled_Send_Status"] = "queued"
    lead["Scheduled_Send_Error"] = ""
    lead["Scheduled_Send_Attempts"] = "0"
    return scheduled_for


def scheduled_send_label(lead: dict) -> str:
    raw = (lead.get("Scheduled_Send_At") or "").strip()
    status = (lead.get("Scheduled_Send_Status") or "").strip()
    if not raw:
        return "Not scheduled"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return raw
    return f"{status or 'queued'} for {dt.astimezone(VIENNA_TZ).strftime('%Y-%m-%d %H:%M %Z')}"
