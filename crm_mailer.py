"""
crm_mailer.py – Send generated email drafts via SMTP + WhatsApp link helper.
"""

from __future__ import annotations

import mimetypes
import os
import re
import smtplib
import urllib.parse
import html
from datetime import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid

import crm_backend as backend
from campaign_service import get_active_campaign
from crm_store import TERMINAL_STATUSES, get_lead_by_id, save_lead
from crm_tracker import apply_contact_outcome


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


def get_whatsapp_link(lead_id: str, campaign: dict | None = None) -> str | None:
    """
    Build a wa.me deep-link for the lead's WhatsApp draft.
    Returns the URL string, or None if phone/draft is missing.
    Opens WhatsApp (web or app) with the message pre-filled.
    """
    lead = get_lead_by_id(lead_id, campaign=campaign)
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
    text = (draft or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return "", ""

    lines = text.splitlines()
    if lines and lines[0].lower().startswith("betreff:"):
        subject = lines[0][8:].strip()
        body_lines = lines[1:]
        if body_lines and body_lines[0] == "":
            body_lines = body_lines[1:]
        return subject, "\n".join(body_lines)

    return "", text


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


_IMG_RE = re.compile(r"\{\{img:([^|}]+)\|([^}]*)\}\}")
_URL_RE = re.compile(r"https?://[^\s<]+")


def _normalize_inline_image_path(path_value: str, base_dir: str) -> str:
    raw_path = (path_value or "").strip()
    if not raw_path:
        return ""
    if os.path.isabs(raw_path):
        try:
            raw_path = os.path.relpath(raw_path, base_dir)
        except ValueError:
            pass
    return backend.normalize_asset_path(raw_path)


def _load_inline_image(path: str, base_dir: str, campaign: dict | None = None) -> tuple[bytes, str] | None:
    normalized_path = _normalize_inline_image_path(path, base_dir)
    if not normalized_path:
        return None
    local_path = path if os.path.isabs(path) else os.path.join(base_dir, normalized_path)
    try:
        with open(local_path, "rb") as handle:
            data = handle.read()
        return data, str(mimetypes.guess_type(local_path)[0] or "")
    except OSError:
        pass

    if backend.is_postgres_backend():
        active_campaign = campaign or get_active_campaign()
        asset = backend.postgres_get_campaign_asset(active_campaign["id"], normalized_path)
        if asset and asset.get("data_bytes"):
            return bytes(asset["data_bytes"]), str(asset.get("content_type") or "")
    return None


def _extract_inline_images(
    html_text: str, base_dir: str, campaign: dict | None = None
) -> tuple[str, list[tuple[str, str, bytes, str]]]:
    """Replace {{img:path|alt}} in HTML-converted body with <img src="cid:..."> tags.
    Returns (modified_html, [(cid, path, raw_bytes, content_type), ...]).
    Missing files fall back to [alt text].
    """
    images: list[tuple[str, str, bytes, str]] = []

    def _replace(m: re.Match) -> str:
        path = m.group(1).strip()
        alt = html.escape(m.group(2).strip())
        resolved = _load_inline_image(path, base_dir, campaign=campaign)
        if resolved is None:
            return f"[{m.group(2).strip()}]"
        data, content_type = resolved
        cid = make_msgid()
        images.append((cid, path, data, content_type))
        return f'<img src="cid:{cid[1:-1]}" alt="{alt}" style="max-width:100%">'

    return _IMG_RE.sub(_replace, html_text), images


def _strip_img_placeholders(text: str) -> str:
    """Replace {{img:path|alt}} with [alt] for the plain-text version."""
    return _IMG_RE.sub(lambda m: f"[{m.group(2).strip()}]", text)


def _render_html_body(text: str) -> str:
    """Convert plain-text draft content into HTML without collapsing layout."""
    escaped = html.escape(text)
    bolded = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)

    def _linkify(match: re.Match[str]) -> str:
        raw_url = match.group(0)
        url = raw_url.rstrip(".,);:]")
        suffix = raw_url[len(url):]
        return f"<a href=\"{url}\">{url}</a>{suffix}"

    linked = _URL_RE.sub(_linkify, bolded)
    # Split on blank lines for paragraphs; convert remaining single newlines to <br>.
    # white-space: pre-wrap is not supported by Gmail/Outlook so we use explicit tags.
    paragraphs = re.split(r"\n{2,}", linked)
    parts = [p.replace("\n", "<br>\n") for p in paragraphs]
    return "".join(f"<p>{p}</p>" for p in parts if p.strip())


def send_email_result(
    lead_id: str,
    dry_run: bool = False,
    notes: str = "",
    campaign: dict | None = None,
    lead: dict | None = None,
) -> dict[str, str | bool]:
    """
    Build and send the generated email for a lead.
    On success, logs the contact attempt.
    Returns a dict with `ok`, `message_id`, and `error`.
    """
    lead = dict(lead) if lead is not None else get_lead_by_id(lead_id, campaign=campaign)
    if lead is None:
        print(f"Lead {lead_id} not found.")
        return {"ok": False, "message_id": "", "error": "lead_not_found"}

    # Validations
    if lead.get("Status") in TERMINAL_STATUSES:
        print(f"Lead {lead_id} is in terminal state '{lead['Status']}'. Not sending.")
        return {"ok": False, "message_id": "", "error": "terminal_status"}

    if not lead.get("Drafts_Approved", "0") == "1":
        print(f"Lead {lead_id} drafts not approved yet. Review in the app first.")
        return {"ok": False, "message_id": "", "error": "draft_not_approved"}
    if lead.get("Draft_Stale") == "1":
        print(f"Lead {lead_id} draft is stale for the current campaign config. Re-run analyze first.")
        return {"ok": False, "message_id": "", "error": "draft_stale"}

    to_addr = lead.get("Email", "").strip()
    if not to_addr:
        print(f"Lead {lead_id} has no email address.")
        return {"ok": False, "message_id": "", "error": "missing_email"}

    draft = lead.get("Email_Draft", "").strip()
    if not draft:
        print(f"Lead {lead_id} has no email draft. Run `python crm.py analyze` first.")
        return {"ok": False, "message_id": "", "error": "missing_draft"}

    subject, body = _parse_draft(draft)
    if not subject:
        print(f"Could not parse subject from Email_Draft for {lead_id}.")
        return {"ok": False, "message_id": "", "error": "missing_subject"}

    active_campaign = campaign or get_active_campaign()
    sender_name = (active_campaign.get("sender_name") or os.getenv("SENDER_NAME", "Linus")).strip()
    sender_email = (active_campaign.get("sender_email") or os.getenv("SENDER_EMAIL", "")).strip()
    if not sender_email:
        print("SENDER_EMAIL not set in .env")
        return {"ok": False, "message_id": "", "error": "missing_sender_email"}
    from_header = formataddr((sender_name, sender_email))
    sender_domain = sender_email.split("@", 1)[1] if "@" in sender_email else ""
    include_html = _env_enabled("EMAIL_INCLUDE_HTML", default=True)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    plain_body = _strip_img_placeholders(body)
    html_fragment, inline_images = _extract_inline_images(_render_html_body(body), base_dir, campaign=active_campaign)

    def _apply_headers(message) -> None:
        message["Subject"] = subject
        message["From"] = from_header
        message["To"] = to_addr
        message["Reply-To"] = sender_email
        message["Date"] = formatdate(localtime=True)
        message["Message-ID"] = make_msgid(domain=sender_domain) if sender_domain else make_msgid()

    if include_html:
        html_body = f"<html><body>{html_fragment}</body></html>"
        if inline_images:
            msg = MIMEMultipart("related")
            _apply_headers(msg)
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(plain_body, "plain", "utf-8"))
            alt.attach(MIMEText(html_body, "html", "utf-8"))
            msg.attach(alt)
            for cid, path, data, content_type in inline_images:
                subtype = "png"
                if str(content_type or "").startswith("image/"):
                    subtype = str(content_type).split("/", 1)[1] or "png"
                else:
                    guessed_type = mimetypes.guess_type(path)[0] or ""
                    if guessed_type.startswith("image/"):
                        subtype = guessed_type.split("/", 1)[1] or "png"
                img_part = MIMEImage(data, _subtype=subtype)
                img_part["Content-ID"] = cid
                img_part.add_header(
                    "Content-Disposition", "inline",
                    filename=os.path.basename(path),
                )
                msg.attach(img_part)
        else:
            msg = MIMEMultipart("alternative")
            _apply_headers(msg)
            msg.attach(MIMEText(plain_body, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))
    else:
        msg = MIMEText(plain_body, "plain", "utf-8")
        _apply_headers(msg)

    if dry_run:
        print("=" * 60)
        print(f"DRY RUN – would send to: {to_addr}")
        print(f"From: {from_header}")
        print(f"Subject: {subject}")
        print("-" * 60)
        print(body)
        print("=" * 60)
        return {"ok": True, "message_id": str(msg.get("Message-ID") or ""), "error": ""}

    # Send via SMTP
    smtp_host = os.getenv("SMTP_HOST", "smtp.hostinger.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", sender_email)
    smtp_pass = os.getenv("SMTP_PASS", "")

    if not smtp_pass:
        print("SMTP_PASS not set in .env")
        return {"ok": False, "message_id": "", "error": "missing_smtp_pass"}

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        print(f"Email sent to {to_addr} ({lead.get('Unternehmen', '')})")
        sent_at = datetime.now().astimezone()
        event = apply_contact_outcome(lead, "sent", notes=notes, channel="email", now=sent_at)
        message_id = str(msg.get("Message-ID") or "")
        lead["Sent_At"] = sent_at.isoformat()
        lead["SMTP_Message_ID"] = message_id
        lead["Scheduled_Send_Error"] = ""
        if (lead.get("Scheduled_Send_Channel") or "").strip() == "email":
            lead["Scheduled_Send_Status"] = "sent"
        if backend.is_postgres_backend():
            backend.postgres_persist_outreach_lead(
                active_campaign["id"],
                lead,
                contact_event=event,
                outbound_email={
                    "smtp_message_id": message_id,
                    "recipient_email": to_addr,
                    "subject": subject,
                    "sent_at": sent_at,
                    "source": "app",
                },
            )
        else:
            save_lead(lead, campaign=active_campaign)
        return {"ok": True, "message_id": message_id, "error": ""}
    except Exception as e:
        print(f"Failed to send email for {lead_id}: {e}")
        return {"ok": False, "message_id": "", "error": str(e)}


def send_email(
    lead_id: str,
    dry_run: bool = False,
    notes: str = "",
    campaign: dict | None = None,
    lead: dict | None = None,
) -> bool:
    return bool(send_email_result(lead_id, dry_run=dry_run, notes=notes, campaign=campaign, lead=lead).get("ok"))
