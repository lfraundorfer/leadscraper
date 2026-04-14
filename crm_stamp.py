"""
crm_stamp.py - Apply one identical email draft to leads in the active campaign.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from campaign_service import get_active_campaign
from crm_fields import TERMINAL_STATUSES, is_pre_contact_status
from crm_schedule import clear_scheduled_send
from crm_store import load_leads, save_leads_batch, set_stored_draft_stale
from crm_templates import (
    build_slots,
    compose_email_draft,
    fill_template,
    get_effective_shared_templates,
    get_effective_special_subject_option,
    get_effective_subject_templates,
    parse_email_draft,
)


def build_email_draft(
    *,
    draft: str = "",
    draft_file: str = "",
    subject: str = "",
    body: str = "",
    body_file: str = "",
) -> str:
    draft_text = ""
    if draft_file:
        draft_text = Path(draft_file).read_text(encoding="utf-8")
    elif draft:
        draft_text = draft

    if draft_text:
        parsed_subject, parsed_body = parse_email_draft(draft_text)
        if not parsed_subject:
            raise ValueError("Stamped email draft needs a subject line. Use 'Betreff: ...' in --draft/--draft-file.")
        return compose_email_draft(parsed_subject, parsed_body)

    body_text = Path(body_file).read_text(encoding="utf-8") if body_file else body
    subject_text = subject.strip()
    if not subject_text:
        raise ValueError("Use --subject together with --body or --body-file, or provide --draft/--draft-file.")
    return compose_email_draft(subject_text, body_text)


def build_campaign_template_email_draft(
    *,
    lead: dict,
    campaign: dict | None = None,
    subject: str = "",
    hook: str = "",
) -> str:
    active_campaign = campaign or get_active_campaign()
    email_template = (get_effective_shared_templates(campaign=active_campaign).get("email") or "").strip()
    if not email_template:
        raise ValueError(f"Campaign {active_campaign['id']} has no shared email template.")

    resolved_subject_template = subject.strip()
    if not resolved_subject_template:
        resolved_subject_template = get_effective_special_subject_option(campaign=active_campaign).strip()
    if not resolved_subject_template:
        subject_templates = get_effective_subject_templates(campaign=active_campaign)
        resolved_subject_template = subject_templates[0].strip() if subject_templates else ""

    slots = build_slots(lead, hook.strip(), "", campaign=active_campaign)
    slots["hook"] = hook.strip()
    rendered_subject = fill_template(resolved_subject_template, slots).strip() if resolved_subject_template else ""
    slots["subject"] = rendered_subject
    rendered = fill_template(email_template, slots)
    parsed_subject, parsed_body = parse_email_draft(rendered)
    if parsed_subject:
        return compose_email_draft(parsed_subject, parsed_body)
    if rendered_subject:
        return compose_email_draft(rendered_subject, rendered)
    raise ValueError("Campaign shared email template did not render a subject. Set a campaign subject or pass --subject.")


def _stamp_matching_leads(
    *,
    render_email_draft: Callable[[dict, dict], str],
    single_id: str = "",
) -> dict[str, int | str]:
    campaign = get_active_campaign()
    leads = load_leads(campaign=campaign)
    target_id = single_id.strip()

    matched = 0
    updated: list[dict] = []
    skipped_terminal = 0
    skipped_non_pre_contact = 0
    skipped_missing_email = 0

    for lead in leads:
        lead_id = (lead.get("ID") or "").strip()
        if target_id and lead_id != target_id:
            continue
        matched += 1

        status = (lead.get("Status") or "new").strip() or "new"
        if status in TERMINAL_STATUSES:
            skipped_terminal += 1
            continue
        if not is_pre_contact_status(status):
            skipped_non_pre_contact += 1
            continue
        if not (lead.get("Email") or "").strip():
            skipped_missing_email += 1
            continue

        lead["Email_Draft"] = render_email_draft(lead, campaign)
        lead["Drafts_Approved"] = "1"
        lead["Preferred_Channel"] = "email"
        lead["Next_Action_Type"] = "email"
        lead["Draft_Config_Version"] = str(campaign.get("draft_config_version") or campaign.get("config_version") or "1")
        set_stored_draft_stale(lead, False)
        clear_scheduled_send(lead)
        if status in {"new", "draft_ready"}:
            lead["Status"] = "approved"
        updated.append(dict(lead))

    if target_id and matched == 0:
        raise ValueError(f"Lead {target_id} not found in campaign {campaign['id']}.")

    if updated:
        save_leads_batch(updated, campaign=campaign)

    return {
        "campaign_id": str(campaign.get("id") or ""),
        "matched": matched,
        "updated": len(updated),
        "skipped_terminal": skipped_terminal,
        "skipped_non_pre_contact": skipped_non_pre_contact,
        "skipped_missing_email": skipped_missing_email,
    }


def stamp_email_drafts(
    *,
    email_draft: str,
    single_id: str = "",
) -> dict[str, int | str]:
    return _stamp_matching_leads(
        render_email_draft=lambda _lead, _campaign: email_draft,
        single_id=single_id,
    )


def stamp_campaign_template_drafts(
    *,
    subject: str = "",
    hook: str = "",
    single_id: str = "",
) -> dict[str, int | str]:
    return _stamp_matching_leads(
        render_email_draft=lambda lead, campaign: build_campaign_template_email_draft(
            lead=lead,
            campaign=campaign,
            subject=subject,
            hook=hook,
        ),
        single_id=single_id,
    )


def main(
    *,
    draft: str = "",
    draft_file: str = "",
    campaign_template: bool = False,
    subject: str = "",
    body: str = "",
    body_file: str = "",
    hook: str = "",
    single_id: str = "",
) -> dict[str, int | str]:
    if campaign_template:
        if any(value.strip() for value in (draft, draft_file, body, body_file)):
            raise ValueError("Use either --campaign-template or --draft/--body inputs, not both.")
        result = stamp_campaign_template_drafts(
            subject=subject,
            hook=hook,
            single_id=single_id,
        )
    else:
        email_draft = build_email_draft(
            draft=draft,
            draft_file=draft_file,
            subject=subject,
            body=body,
            body_file=body_file,
        )
        result = stamp_email_drafts(email_draft=email_draft, single_id=single_id)
    scope = f" in {result['campaign_id']}" if result.get("campaign_id") else ""
    print(
        f"Stamped {result['updated']} lead(s){scope}. "
        f"Skipped terminal={result['skipped_terminal']} "
        f"non_pre_contact={result['skipped_non_pre_contact']} "
        f"missing_email={result['skipped_missing_email']}"
    )
    return result
