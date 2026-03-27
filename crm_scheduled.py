"""
crm_scheduled.py - Process queued email sends.
"""

from __future__ import annotations

from datetime import datetime

import crm_backend as backend
from campaign_service import get_campaign, list_campaigns
from crm_mailer import send_email_result
from crm_schedule import vienna_now
from crm_store import load_leads, save_leads


def _mark_send_result(campaign: dict, lead_id: str, *, ok: bool, message_id: str = "", error: str = "") -> None:
    leads = load_leads(campaign=campaign)
    changed = False
    for lead in leads:
        if (lead.get("ID") or "").strip() != lead_id.strip():
            continue
        if ok:
            lead["Scheduled_Send_Status"] = "sent"
            lead["Scheduled_Send_Error"] = ""
            lead["Sent_At"] = datetime.now().astimezone().isoformat()
            if message_id:
                lead["SMTP_Message_ID"] = message_id
        else:
            lead["Scheduled_Send_Status"] = "queued"
            lead["Scheduled_Send_Error"] = error.strip()
            lead["Scheduled_Send_Attempts"] = str(int(lead.get("Scheduled_Send_Attempts") or 0) + 1)
        changed = True
        break
    if changed:
        save_leads(leads, campaign=campaign)


def _queued_due_leads(limit: int = 100) -> list[tuple[dict, dict]]:
    now = vienna_now()
    due: list[tuple[dict, dict]] = []
    for campaign in list_campaigns():
        leads = load_leads(campaign=campaign)
        for lead in leads:
            if (lead.get("Scheduled_Send_Status") or "").strip() != "queued":
                continue
            if (lead.get("Scheduled_Send_Channel") or "").strip() != "email":
                continue
            raw = (lead.get("Scheduled_Send_At") or "").strip()
            if not raw:
                continue
            try:
                scheduled_for = datetime.fromisoformat(raw)
            except ValueError:
                continue
            if scheduled_for <= now:
                due.append((campaign, lead))
    due.sort(key=lambda item: ((item[1].get("Scheduled_Send_At") or ""), item[0].get("id", ""), item[1].get("ID", "")))
    return due[:limit]


def main(limit: int = 100, dry_run: bool = False) -> int:
    if backend.is_postgres_backend():
        items = []
        source_rows = (
            backend.postgres_list_due_scheduled_sends(limit=limit)
            if dry_run
            else backend.postgres_claim_due_scheduled_sends(limit=limit)
        )
        for row in source_rows:
            try:
                campaign = get_campaign(row["campaign_id"])
            except Exception:
                continue
            lead_id = str(row.get("lead_id") or "").strip()
            if not lead_id:
                continue
            items.append((campaign, {"ID": lead_id}))
    else:
        items = _queued_due_leads(limit=limit)

    if not items:
        print("No queued email sends are due right now.")
        return 0

    print(f"Processing {len(items)} queued email(s)...")
    sent = 0
    failed = 0
    for campaign, lead_ref in items:
        lead_id = str(lead_ref.get("ID") or lead_ref.get("lead_id") or "").strip()
        if not lead_id:
            continue
        print(f"  {campaign.get('id', '?')} :: {lead_id}")
        result = send_email_result(lead_id, dry_run=dry_run, notes="Scheduled send", campaign=campaign)
        if result.get("ok"):
            sent += 1
            if dry_run:
                print("    DRY RUN")
        else:
            failed += 1
            if dry_run:
                print(f"    WOULD FAIL: {result.get('error') or 'unknown error'}")
            else:
                _mark_send_result(campaign, lead_id, ok=False, error=str(result.get("error") or "send_failed"))

    print(f"Done. Sent: {sent} | Failed: {failed} | Dry run: {'yes' if dry_run else 'no'}")
    return sent
