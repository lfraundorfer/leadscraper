"""
crm_mail_sync.py - IMAP mailbox sync for bounce/reply visibility.
"""

from __future__ import annotations

import imaplib
import os
import re
from datetime import datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.header import decode_header, make_header
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from html import unescape
from typing import Any

import crm_backend as backend


DEFAULT_LOOKBACK_HOURS = 24
_DAEMON_TERMS = (
    "mailer-daemon",
    "mail delivery subsystem",
    "postmaster",
    "delivery status notification",
    "mail delivery failed",
    "returned mail",
    "failure notice",
    "delivery failure",
    "undeliver",
)


def _env_required(name: str) -> str:
    value = str(os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for mailbox sync.")
    return value


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).strip()
    except Exception:
        return value.strip()


def _normalize_message_id(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1]
    return text.strip().lower()


def _message_id_variants(value: str | None) -> list[str]:
    normalized = _normalize_message_id(value)
    if not normalized:
        return []
    return [normalized, f"<{normalized}>"]


def _extract_addresses(value: str | None) -> list[str]:
    addresses: list[str] = []
    for _, email in getaddresses([value or ""]):
        candidate = email.strip().lower()
        if candidate and candidate not in addresses:
            addresses.append(candidate)
    return addresses


def _strip_html(text: str) -> str:
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", text or "")
    cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _message_text(message: Message) -> str:
    parts: list[str] = []
    if message.is_multipart():
        for part in message.walk():
            content_type = part.get_content_type()
            disposition = (part.get("Content-Disposition") or "").lower()
            if disposition.startswith("attachment"):
                continue
            try:
                payload_bytes = part.get_payload(decode=True)
            except Exception:
                payload_bytes = None
            if not payload_bytes:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload_bytes.decode(charset, errors="replace")
            except LookupError:
                decoded = payload_bytes.decode("utf-8", errors="replace")
            if content_type == "text/plain":
                parts.append(decoded)
            elif content_type == "text/html" and not parts:
                parts.append(_strip_html(decoded))
        return "\n".join(part.strip() for part in parts if part.strip()).strip()
    try:
        payload_bytes = message.get_payload(decode=True)
    except Exception:
        payload_bytes = None
    if not payload_bytes:
        return ""
    charset = message.get_content_charset() or "utf-8"
    try:
        decoded = payload_bytes.decode(charset, errors="replace")
    except LookupError:
        decoded = payload_bytes.decode("utf-8", errors="replace")
    if message.get_content_type() == "text/html":
        return _strip_html(decoded)
    return decoded.strip()


def _delivery_status_fields(message: Message) -> dict[str, str]:
    fields: dict[str, str] = {}
    if not message.is_multipart():
        return fields
    for part in message.walk():
        if part.get_content_type() != "message/delivery-status":
            continue
        payload = part.get_payload()
        blocks = payload if isinstance(payload, list) else [payload]
        for block in blocks:
            if not isinstance(block, Message):
                continue
            for key, value in block.items():
                lowered = key.lower()
                if lowered not in fields and value:
                    fields[lowered] = _decode_header_value(value)
    return fields


def _embedded_original_message(message: Message) -> Message | None:
    if not message.is_multipart():
        return None
    for part in message.walk():
        if part.get_content_type() != "message/rfc822":
            continue
        payload = part.get_payload()
        if isinstance(payload, list) and payload:
            candidate = payload[0]
            if isinstance(candidate, Message):
                return candidate
    return None


def _extract_candidate_message_ids(message: Message, body_text: str, status_fields: dict[str, str]) -> list[str]:
    candidates: list[str] = []
    for header_name in ("In-Reply-To", "References", "Original-Message-ID"):
        header_value = _decode_header_value(message.get(header_name))
        for match in re.findall(r"<([^>]+)>", header_value):
            normalized = _normalize_message_id(match)
            if normalized and normalized not in candidates:
                candidates.append(normalized)

    embedded = _embedded_original_message(message)
    if embedded is not None:
        normalized = _normalize_message_id(_decode_header_value(embedded.get("Message-ID")))
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    for key in ("original-message-id",):
        normalized = _normalize_message_id(status_fields.get(key, ""))
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    for match in re.findall(r"<([^>]+@[^>]+)>", body_text or ""):
        normalized = _normalize_message_id(match)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _extract_candidate_recipients(message: Message, body_text: str, status_fields: dict[str, str]) -> list[str]:
    recipients: list[str] = []
    for key in ("x-failed-recipients", "final-recipient", "original-recipient"):
        raw_value = status_fields.get(key, "")
        raw_value = raw_value.split(";", 1)[-1] if ";" in raw_value else raw_value
        for address in _extract_addresses(raw_value):
            if address not in recipients:
                recipients.append(address)

    embedded = _embedded_original_message(message)
    if embedded is not None:
        for address in _extract_addresses(embedded.get("To")):
            if address not in recipients:
                recipients.append(address)

    for match in re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}", body_text or "", re.IGNORECASE):
        address = match.lower()
        if address not in recipients:
            recipients.append(address)
    return recipients


def parse_mailbox_message(raw_bytes: bytes) -> dict[str, Any]:
    message = message_from_bytes(raw_bytes, policy=policy.default)
    subject = _decode_header_value(message.get("Subject"))
    from_addresses = _extract_addresses(message.get("From"))
    from_address = from_addresses[0] if from_addresses else ""
    to_addresses = _extract_addresses(message.get("To"))
    body_text = _message_text(message)
    status_fields = _delivery_status_fields(message)
    candidate_message_ids = _extract_candidate_message_ids(message, body_text, status_fields)
    candidate_recipients = _extract_candidate_recipients(message, body_text, status_fields)
    raw_message_id = _decode_header_value(message.get("Message-ID"))
    try:
        event_at = parsedate_to_datetime(message.get("Date", ""))
    except Exception:
        event_at = None
    if event_at is None:
        event_at = datetime.now(timezone.utc)
    elif event_at.tzinfo is None:
        event_at = event_at.replace(tzinfo=timezone.utc)

    action = (status_fields.get("action") or "").strip().lower()
    status_code = (status_fields.get("status") or "").strip()
    diagnostic = (status_fields.get("diagnostic-code") or status_fields.get("diagnosticcode") or "").strip()
    subject_lower = subject.lower()
    from_lower = from_address.lower()
    is_daemon = any(term in from_lower or term in subject_lower for term in _DAEMON_TERMS)

    if action == "failed" or status_code.startswith("5"):
        event_type = "bounce_hard"
    elif action in {"delayed", "delay"} or status_code.startswith("4") or ("delay" in subject_lower and is_daemon):
        event_type = "bounce_soft"
    elif action in {"delivered", "relayed", "expanded"} or ("delivered" in subject_lower and is_daemon and "not delivered" not in subject_lower):
        event_type = "delivery_notice"
    elif candidate_message_ids and not is_daemon:
        event_type = "reply"
    elif is_daemon:
        event_type = "unknown_notice"
    else:
        event_type = "unknown_notice"

    reason_parts = [part for part in (action, status_code, diagnostic) if part]
    if event_type == "reply":
        reason = f"Reply received from {from_address or 'unknown sender'}"
    elif reason_parts:
        reason = " | ".join(reason_parts)
    else:
        reason = subject or body_text[:160]

    return {
        "event_at": event_at,
        "event_type": event_type,
        "from_address": from_address,
        "to_addresses": to_addresses,
        "subject": subject,
        "raw_message_id": raw_message_id,
        "candidate_message_ids": candidate_message_ids,
        "candidate_recipients": candidate_recipients,
        "reason": reason.strip(),
        "is_daemon": is_daemon,
        "metadata": {
            "action": action,
            "status_code": status_code,
            "diagnostic": diagnostic,
            "candidate_message_ids": candidate_message_ids,
            "candidate_recipients": candidate_recipients,
        },
    }


def is_relevant_mailbox_message(parsed_message: dict[str, Any]) -> bool:
    event_type = str(parsed_message.get("event_type") or "").strip()
    if event_type == "reply":
        return bool(parsed_message.get("candidate_message_ids"))
    if parsed_message.get("is_daemon"):
        return bool(parsed_message.get("candidate_message_ids") or parsed_message.get("candidate_recipients"))
    return False


def match_mailbox_event(
    parsed_message: dict[str, Any],
    *,
    outbound_by_message_id: dict[str, dict[str, Any]],
    outbound_by_recipient: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    for candidate_id in parsed_message.get("candidate_message_ids", []):
        match = outbound_by_message_id.get(_normalize_message_id(candidate_id))
        if match:
            return {"matched": True, "row": match, "matched_on": "message_id"}

    event_at = parsed_message.get("event_at")
    recipient_matches: list[dict[str, Any]] = []
    seen_message_ids: set[str] = set()
    for recipient in parsed_message.get("candidate_recipients", []):
        for row in outbound_by_recipient.get(recipient.lower(), []):
            smtp_message_id = str(row.get("smtp_message_id") or "")
            if not smtp_message_id or smtp_message_id in seen_message_ids:
                continue
            sent_at = row.get("sent_at")
            if isinstance(event_at, datetime) and isinstance(sent_at, datetime) and sent_at > event_at:
                continue
            recipient_matches.append(row)
            seen_message_ids.add(smtp_message_id)
    if len(recipient_matches) == 1:
        return {"matched": True, "row": recipient_matches[0], "matched_on": "recipient"}
    return {"matched": False, "row": None, "matched_on": ""}


def _recent_outbound_indexes(since_at: datetime) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    rows = backend.postgres_recent_outbound_emails(since_at)
    outbound_by_message_id: dict[str, dict[str, Any]] = {}
    outbound_by_recipient: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        normalized_id = _normalize_message_id(str(row.get("smtp_message_id") or ""))
        if normalized_id:
            outbound_by_message_id[normalized_id] = row
        recipient = str(row.get("recipient_email") or "").strip().lower()
        if recipient:
            outbound_by_recipient.setdefault(recipient, []).append(row)
    for recipients in outbound_by_recipient.values():
        recipients.sort(key=lambda row: row.get("sent_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return outbound_by_message_id, outbound_by_recipient


def _imap_message_bytes(client: imaplib.IMAP4_SSL, uid: str) -> bytes:
    status, data = client.uid("fetch", uid, "(BODY.PEEK[])")
    if status != "OK":
        raise RuntimeError(f"IMAP fetch failed for UID {uid}.")
    for item in data or []:
        if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], (bytes, bytearray)):
            return bytes(item[1])
    raise RuntimeError(f"IMAP returned no body for UID {uid}.")


def _imap_search_uids(client: imaplib.IMAP4_SSL, folder_name: str, since_at: datetime) -> list[str]:
    status, _ = client.select(folder_name, readonly=True)
    if status != "OK":
        raise RuntimeError(f"Could not select IMAP folder {folder_name!r}.")
    since_token = since_at.strftime("%d-%b-%Y")
    status, data = client.uid("search", None, f'SINCE {since_token}')
    if status != "OK":
        raise RuntimeError(f"IMAP search failed for folder {folder_name!r}.")
    raw = data[0].decode("utf-8", errors="ignore") if data and data[0] else ""
    return [uid for uid in raw.split() if uid]


def _configured_folders() -> tuple[str, str]:
    inbox_folder = str(os.getenv("IMAP_INBOX_FOLDER") or "INBOX").strip() or "INBOX"
    sent_folder = str(os.getenv("IMAP_SENT_FOLDER") or "").strip()
    return inbox_folder, sent_folder


def sync_mailbox(*, lookback_hours: int = DEFAULT_LOOKBACK_HOURS) -> dict[str, Any]:
    if not backend.is_postgres_backend():
        raise RuntimeError("Mailbox sync requires CRM_BACKEND=postgres.")

    imap_host = _env_required("IMAP_HOST")
    imap_port = _env_int("IMAP_PORT", 993)
    imap_user = _env_required("IMAP_USER")
    imap_pass = _env_required("IMAP_PASS")
    lookback_hours = max(1, int(lookback_hours or DEFAULT_LOOKBACK_HOURS))
    synced_at = datetime.now(timezone.utc)
    since_at = synced_at - timedelta(hours=lookback_hours)
    inbox_folder, sent_folder = _configured_folders()
    outbound_by_message_id, outbound_by_recipient = _recent_outbound_indexes(since_at)

    summary = {
        "synced_at": synced_at.isoformat(),
        "lookback_hours": lookback_hours,
        "inbox_seen": 0,
        "inbox_relevant": 0,
        "matched": 0,
        "unmatched": 0,
        "sent_backfilled": 0,
        "unknown_marked": 0,
    }

    with imaplib.IMAP4_SSL(imap_host, imap_port) as client:
        client.login(imap_user, imap_pass)

        sent_uids: list[str] = []
        if sent_folder and sent_folder != inbox_folder:
            sent_uids = _imap_search_uids(client, sent_folder, since_at)
        for uid in sent_uids:
            raw_bytes = _imap_message_bytes(client, uid)
            parsed = parse_mailbox_message(raw_bytes)
            normalized_message_id = _normalize_message_id(parsed.get("raw_message_id"))
            if not normalized_message_id:
                continue
            row = outbound_by_message_id.get(normalized_message_id)
            if not row:
                continue
            backend.postgres_record_outbound_email(
                str(row.get("campaign_id") or ""),
                str(row.get("lead_id") or ""),
                {
                    "smtp_message_id": str(row.get("smtp_message_id") or ""),
                    "recipient_email": (
                        parsed.get("to_addresses", [str(row.get("recipient_email") or "")])[0]
                        if parsed.get("to_addresses", [str(row.get("recipient_email") or "")])
                        else str(row.get("recipient_email") or "")
                    ),
                    "subject": parsed.get("subject") or str(row.get("subject") or ""),
                    "sent_at": parsed.get("event_at") or row.get("sent_at"),
                    "source": "mailbox_sent",
                },
            )
            summary["sent_backfilled"] += 1

        inbox_uids = _imap_search_uids(client, inbox_folder, since_at)
        for uid in inbox_uids:
            summary["inbox_seen"] += 1
            raw_bytes = _imap_message_bytes(client, uid)
            parsed = parse_mailbox_message(raw_bytes)
            if not is_relevant_mailbox_message(parsed):
                continue
            summary["inbox_relevant"] += 1
            match = match_mailbox_event(
                parsed,
                outbound_by_message_id=outbound_by_message_id,
                outbound_by_recipient=outbound_by_recipient,
            )
            row = match.get("row") or {}
            backend.postgres_record_mailbox_event(
                {
                    "campaign_id": row.get("campaign_id") if match.get("matched") else "",
                    "lead_id": row.get("lead_id") if match.get("matched") else "",
                    "folder_name": inbox_folder,
                    "mailbox_uid": uid,
                    "event_at": parsed.get("event_at"),
                    "event_type": parsed.get("event_type"),
                    "from_address": parsed.get("from_address"),
                    "subject": parsed.get("subject"),
                    "raw_message_id": parsed.get("raw_message_id"),
                    "related_smtp_message_id": row.get("smtp_message_id") if match.get("matched") else "",
                    "reason": parsed.get("reason"),
                    "matched": bool(match.get("matched")),
                    "metadata": {
                        **(parsed.get("metadata") if isinstance(parsed.get("metadata"), dict) else {}),
                        "matched_on": match.get("matched_on") or "",
                    },
                }
            )
            if match.get("matched"):
                summary["matched"] += 1
            else:
                summary["unmatched"] += 1

    summary["unknown_marked"] = backend.postgres_mark_outbound_unknown(since_at, synced_at=synced_at)
    backend.postgres_set_app_meta_value("mailbox_last_sync_at", synced_at.isoformat())
    backend.postgres_set_app_meta_value("mailbox_last_sync_summary", summary)
    return summary
