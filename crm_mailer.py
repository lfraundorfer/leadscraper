"""
crm_mailer.py – Send generated email drafts via SMTP + WhatsApp link helper.
"""

from __future__ import annotations

import os
import re
import smtplib
import urllib.parse
import html
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid

from campaign_service import get_active_campaign
from crm_store import get_lead_by_id, TERMINAL_STATUSES
from crm_tracker import log_contact


def format_phone_e164(phone: str) -> str:
    """Normalize an Austrian phone number to E.164 (+43...) for wa.me links."""
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("43"):
        return "+" + digits
    if digits.startswith("0"):
        return "+43" + digits[1:]
    if digits.startswith("6") or digits.startswith("7"):
        # Looks like a mobile number without country/trunk prefix
        return "+43" + digits
    return "+" + digits


def get_whatsapp_link(lead_id: str) -> str | None:
    """
    Build a wa.me deep-link for the lead's WhatsApp draft.
    Returns the URL string, or None if phone/draft is missing.
    Opens WhatsApp (web or app) with the message pre-filled.
    """
    lead = get_lead_by_id(lead_id)
    if lead is None:
        return None
    phone = lead.get("TelNr", "").strip()
    draft = lead.get("WhatsApp_Draft", "").strip()
    if not phone or not draft:
        return None
    e164 = format_phone_e164(phone)
    encoded = urllib.parse.quote(draft)
    return f"https://wa.me/{e164.lstrip('+')}?text={encoded}"


def _parse_draft(draft: str) -> tuple[str, str]:
    """
    Parse Email_Draft into (subject, body).
    Expected format:
        Betreff: <subject line>

        <body text>
    """
    lines = draft.strip().splitlines()
    subject = ""
    body_lines = []
    body_started = False

    for line in lines:
        if not body_started and line.lower().startswith("betreff:"):
            subject = line[8:].strip()
        elif not body_started and line.strip() == "" and subject:
            body_started = True
        elif body_started:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()
    return subject, body


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def send_email(lead_id: str, dry_run: bool = False, notes: str = "") -> bool:
    """
    Build and send the generated email for a lead.
    On success, logs the contact attempt.
    Returns True on success.
    """
    lead = get_lead_by_id(lead_id)
    if lead is None:
        print(f"Lead {lead_id} not found.")
        return False

    # Validations
    if lead.get("Status") in TERMINAL_STATUSES:
        print(f"Lead {lead_id} is in terminal state '{lead['Status']}'. Not sending.")
        return False

    if not lead.get("Drafts_Approved", "0") == "1":
        print(f"Lead {lead_id} drafts not approved yet. Review in the app first.")
        return False
    if lead.get("Draft_Stale") == "1":
        print(f"Lead {lead_id} draft is stale for the current campaign config. Re-run analyze first.")
        return False

    to_addr = lead.get("Email", "").strip()
    if not to_addr:
        print(f"Lead {lead_id} has no email address.")
        return False

    draft = lead.get("Email_Draft", "").strip()
    if not draft:
        print(f"Lead {lead_id} has no email draft. Run `python crm.py analyze` first.")
        return False

    subject, body = _parse_draft(draft)
    if not subject:
        print(f"Could not parse subject from Email_Draft for {lead_id}.")
        return False

    campaign = get_active_campaign()
    sender_name = (campaign.get("sender_name") or os.getenv("SENDER_NAME", "Linus")).strip()
    sender_email = (campaign.get("sender_email") or os.getenv("SENDER_EMAIL", "")).strip()
    if not sender_email:
        print("SENDER_EMAIL not set in .env")
        return False
    from_header = formataddr((sender_name, sender_email))
    sender_domain = sender_email.split("@", 1)[1] if "@" in sender_email else ""
    include_html = _env_enabled("EMAIL_INCLUDE_HTML", default=True)

    # Build HTML: escape, bold **text**, preserve line breaks
    def _to_html(text: str) -> str:
        escaped = html.escape(text)
        bolded = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)

        def _linkify(match: re.Match[str]) -> str:
            raw_url = match.group(0)
            url = raw_url.rstrip(".,);:]")
            suffix = raw_url[len(url):]
            return f"<a href=\"{url}\">{url}</a>{suffix}"

        linked = re.sub(r"https?://[^\s<]+", _linkify, bolded)
        return "<br>\n".join(linked.splitlines())

    html_lines = _to_html(body)

    def _apply_headers(message) -> None:
        message["Subject"] = subject
        message["From"] = from_header
        message["To"] = to_addr
        message["Reply-To"] = sender_email
        message["Date"] = formatdate(localtime=True)
        message["Message-ID"] = make_msgid(domain=sender_domain) if sender_domain else make_msgid()

    if include_html:
        msg = MIMEMultipart("alternative")
        _apply_headers(msg)
        msg.attach(MIMEText(body, "plain", "utf-8"))
        html_body = f"<html><body><p>{html_lines}</p></body></html>"
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    else:
        msg = MIMEText(body, "plain", "utf-8")
        _apply_headers(msg)

    if dry_run:
        print("=" * 60)
        print(f"DRY RUN – would send to: {to_addr}")
        print(f"From: {from_header}")
        print(f"Subject: {subject}")
        print("-" * 60)
        print(body)
        print("=" * 60)
        return True

    # Send via SMTP
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", sender_email)
    smtp_pass = os.getenv("SMTP_PASS", "")

    if not smtp_pass:
        print("SMTP_PASS not set in .env")
        return False

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        print(f"Email sent to {to_addr} ({lead.get('Unternehmen', '')})")
        log_contact(lead_id, "sent", notes=notes, channel="email")
        return True
    except Exception as e:
        print(f"Failed to send email for {lead_id}: {e}")
        return False
