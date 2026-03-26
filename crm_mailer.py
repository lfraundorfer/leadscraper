"""
crm_mailer.py – Send generated email drafts via SMTP + WhatsApp link helper.
"""

from __future__ import annotations

import os
import re
import smtplib
import urllib.parse
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from crm_store import get_lead_by_id, TERMINAL_STATUSES
from crm_tracker import log_contact

# Optional flyer image — set FLYER_IMAGE=flyer.png (or full path) in .env
_FLYER_PATH = os.getenv("FLYER_IMAGE", "flyer.png")


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


def send_email(lead_id: str, dry_run: bool = False) -> bool:
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

    sender_name = os.getenv("SENDER_NAME", "Linus")
    sender_email = os.getenv("SENDER_EMAIL", "")
    if not sender_email:
        print("SENDER_EMAIL not set in .env")
        return False

    # Build HTML: escape, bold **text**, preserve line breaks
    def _to_html(text: str) -> str:
        escaped = text.replace("&", "&amp;").replace("<", "&lt;")
        bolded = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        return "<br>\n".join(bolded.splitlines())

    html_lines = _to_html(body)

    flyer_path = _FLYER_PATH
    has_flyer = bool(flyer_path and os.path.isfile(flyer_path))

    if has_flyer:
        # multipart/related so the inline image CID resolves
        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_addr

        alt = MIMEMultipart("alternative")
        msg.attach(alt)
        alt.attach(MIMEText(body, "plain", "utf-8"))
        html = (
            f"<html><body>"
            f"<p>{html_lines}</p>"
            f"<br><img src='cid:flyer' style='max-width:600px;display:block;'>"
            f"</body></html>"
        )
        alt.attach(MIMEText(html, "html", "utf-8"))

        with open(flyer_path, "rb") as fh:
            img = MIMEImage(fh.read())
        img.add_header("Content-ID", "<flyer>")
        img.add_header("Content-Disposition", "inline", filename=os.path.basename(flyer_path))
        msg.attach(img)
    else:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_addr
        msg.attach(MIMEText(body, "plain", "utf-8"))
        html = f"<html><body><p>{html_lines}</p></body></html>"
        msg.attach(MIMEText(html, "html", "utf-8"))

    if dry_run:
        print("=" * 60)
        print(f"DRY RUN – would send to: {to_addr}")
        print(f"From: {sender_name} <{sender_email}>")
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
        log_contact(lead_id, "sent", channel="email")
        return True
    except Exception as e:
        print(f"Failed to send email for {lead_id}: {e}")
        return False
