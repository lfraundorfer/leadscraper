from __future__ import annotations

import unittest
from datetime import datetime, timezone
from email.message import EmailMessage

from crm_mail_sync import match_mailbox_event, parse_mailbox_message


class MailSyncParsingTests(unittest.TestCase):
    def test_parse_hard_bounce_delivery_status(self) -> None:
        raw_message = b"""From: Mail Delivery Subsystem <mailer-daemon@example.com>
Subject: Delivery Status Notification (Failure)
Date: Fri, 03 Apr 2026 10:00:00 +0000
Content-Type: multipart/report; report-type=delivery-status; boundary="BOUNDARY"

--BOUNDARY
Content-Type: text/plain; charset="utf-8"

This is the mail system at host example.com.

--BOUNDARY
Content-Type: message/delivery-status

Action: failed
Status: 5.1.1
Diagnostic-Code: smtp; 550 5.1.1 user unknown
Final-Recipient: rfc822; bad@example.com
Original-Message-ID: <orig@example.com>

--BOUNDARY--
"""
        parsed = parse_mailbox_message(raw_message)
        self.assertEqual(parsed["event_type"], "bounce_hard")
        self.assertIn("orig@example.com", parsed["candidate_message_ids"])
        self.assertIn("bad@example.com", parsed["candidate_recipients"])
        self.assertIn("5.1.1", parsed["reason"])

    def test_parse_reply_uses_in_reply_to(self) -> None:
        message = EmailMessage()
        message["From"] = "Customer <customer@example.com>"
        message["To"] = "Linus <you@example.com>"
        message["Subject"] = "Re: Quick question"
        message["Date"] = "Fri, 03 Apr 2026 11:00:00 +0000"
        message["In-Reply-To"] = "<orig@example.com>"
        message.set_content("Sounds good, call me tomorrow.")

        parsed = parse_mailbox_message(message.as_bytes())
        self.assertEqual(parsed["event_type"], "reply")
        self.assertEqual(parsed["from_address"], "customer@example.com")
        self.assertIn("orig@example.com", parsed["candidate_message_ids"])

    def test_parse_delivery_notice(self) -> None:
        raw_message = b"""From: Mail Delivery System <mailer-daemon@example.com>
Subject: Delivery Status Notification (Success)
Date: Fri, 03 Apr 2026 12:00:00 +0000
Content-Type: multipart/report; report-type=delivery-status; boundary="BOUNDARY"

--BOUNDARY
Content-Type: message/delivery-status

Action: delivered
Status: 2.0.0
Final-Recipient: rfc822; good@example.com
Original-Message-ID: <delivered@example.com>

--BOUNDARY--
"""
        parsed = parse_mailbox_message(raw_message)
        self.assertEqual(parsed["event_type"], "delivery_notice")
        self.assertIn("good@example.com", parsed["candidate_recipients"])


class MailSyncMatchingTests(unittest.TestCase):
    def test_message_id_match_beats_recipient_fallback(self) -> None:
        sent_at = datetime(2026, 4, 3, 9, 0, tzinfo=timezone.utc)
        outbound_row = {
            "campaign_id": "camp",
            "lead_id": "LEAD-1",
            "smtp_message_id": "<orig@example.com>",
            "recipient_email": "bad@example.com",
            "sent_at": sent_at,
        }
        parsed = {
            "event_at": datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc),
            "candidate_message_ids": ["orig@example.com"],
            "candidate_recipients": ["bad@example.com"],
        }
        matched = match_mailbox_event(
            parsed,
            outbound_by_message_id={"orig@example.com": outbound_row},
            outbound_by_recipient={"bad@example.com": [outbound_row]},
        )
        self.assertTrue(matched["matched"])
        self.assertEqual(matched["matched_on"], "message_id")

    def test_ambiguous_recipient_fallback_stays_unmatched(self) -> None:
        parsed = {
            "event_at": datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc),
            "candidate_message_ids": [],
            "candidate_recipients": ["shared@example.com"],
        }
        row_a = {
            "campaign_id": "camp",
            "lead_id": "LEAD-1",
            "smtp_message_id": "<a@example.com>",
            "recipient_email": "shared@example.com",
            "sent_at": datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc),
        }
        row_b = {
            "campaign_id": "camp",
            "lead_id": "LEAD-2",
            "smtp_message_id": "<b@example.com>",
            "recipient_email": "shared@example.com",
            "sent_at": datetime(2026, 4, 3, 7, 0, tzinfo=timezone.utc),
        }
        matched = match_mailbox_event(
            parsed,
            outbound_by_message_id={},
            outbound_by_recipient={"shared@example.com": [row_a, row_b]},
        )
        self.assertFalse(matched["matched"])


if __name__ == "__main__":
    unittest.main()
