from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import crm_stamp


class StampEmailDraftTests(unittest.TestCase):
    def test_build_email_draft_from_subject_and_body_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            body_path = Path(tmpdir) / "body.txt"
            body_path.write_text("Hallo Wien,\n\nwir haben eine Idee.", encoding="utf-8")

            draft = crm_stamp.build_email_draft(
                subject="Kurze Frage",
                body_file=str(body_path),
            )

        self.assertEqual(draft, "Betreff: Kurze Frage\n\nHallo Wien,\n\nwir haben eine Idee.")

    def test_build_email_draft_rejects_missing_subject_in_full_draft(self) -> None:
        with self.assertRaisesRegex(ValueError, "needs a subject line"):
            crm_stamp.build_email_draft(draft="Nur Text ohne Betreff")

    @patch("crm_stamp.build_slots")
    @patch("crm_stamp.get_effective_subject_templates")
    @patch("crm_stamp.get_effective_special_subject_option")
    @patch("crm_stamp.get_effective_shared_templates")
    def test_build_campaign_template_email_draft_uses_campaign_template_stack(
        self,
        mock_get_effective_shared_templates,
        mock_get_effective_special_subject_option,
        mock_get_effective_subject_templates,
        mock_build_slots,
    ) -> None:
        mock_get_effective_shared_templates.return_value = {
            "email": "Betreff: {{subject}}\n\n{{salutation}}\n\n{{hook}}\n\n{{sender_name}}",
        }
        mock_get_effective_special_subject_option.return_value = "Fixpreis fuer {{company}}"
        mock_get_effective_subject_templates.return_value = []
        mock_build_slots.return_value = {
            "company": "Agentur Alpha",
            "salutation": "Guten Tag,",
            "sender_name": "Linus",
        }

        draft = crm_stamp.build_campaign_template_email_draft(
            lead={"ID": "WERB-0001"},
            campaign={"id": "werbeagentur_wien"},
            hook="Ich habe eine Idee fuer Ihre Sichtbarkeit.",
        )

        self.assertEqual(
            draft,
            "Betreff: Fixpreis fuer Agentur Alpha\n\nGuten Tag,\n\nIch habe eine Idee fuer Ihre Sichtbarkeit.\n\nLinus",
        )


class StampEmailCommandTests(unittest.TestCase):
    @patch("crm_stamp.save_leads_batch")
    @patch("crm_stamp.load_leads")
    @patch("crm_stamp.get_active_campaign")
    def test_stamp_email_drafts_updates_only_eligible_email_leads(
        self,
        mock_get_active_campaign,
        mock_load_leads,
        mock_save_leads_batch,
    ) -> None:
        mock_get_active_campaign.return_value = {"id": "werbeagentur_wien", "config_version": "7"}
        mock_load_leads.return_value = [
            {
                "ID": "WERB-0001",
                "Status": "new",
                "Email": "hello@example.com",
                "Scheduled_Send_Status": "queued",
                "Scheduled_Send_At": "2026-04-14T09:00:00+02:00",
                "Scheduled_Send_Channel": "email",
            },
            {
                "ID": "WERB-0002",
                "Status": "draft_ready",
                "Email": "draft@example.com",
            },
            {
                "ID": "WERB-0003",
                "Status": "approved",
                "Email": "approved@example.com",
            },
            {
                "ID": "WERB-0004",
                "Status": "contacted",
                "Email": "contacted@example.com",
            },
            {
                "ID": "WERB-0005",
                "Status": "won",
                "Email": "won@example.com",
            },
            {
                "ID": "WERB-0006",
                "Status": "new",
                "Email": "",
            },
        ]

        result = crm_stamp.stamp_email_drafts(
            email_draft="Betreff: Einheitlich\n\nHallo zusammen",
        )

        self.assertEqual(result["updated"], 3)
        self.assertEqual(result["skipped_non_pre_contact"], 1)
        self.assertEqual(result["skipped_terminal"], 1)
        self.assertEqual(result["skipped_missing_email"], 1)
        mock_save_leads_batch.assert_called_once()
        saved_rows = mock_save_leads_batch.call_args.args[0]

        self.assertEqual([row["ID"] for row in saved_rows], ["WERB-0001", "WERB-0002", "WERB-0003"])
        self.assertEqual(saved_rows[0]["Status"], "approved")
        self.assertEqual(saved_rows[1]["Status"], "approved")
        self.assertEqual(saved_rows[2]["Status"], "approved")
        self.assertEqual(saved_rows[0]["Drafts_Approved"], "1")
        self.assertEqual(saved_rows[0]["Preferred_Channel"], "email")
        self.assertEqual(saved_rows[0]["Next_Action_Type"], "email")
        self.assertEqual(saved_rows[0]["Draft_Stale"], "0")
        self.assertEqual(saved_rows[0]["Draft_Config_Version"], "7")
        self.assertEqual(saved_rows[0]["Scheduled_Send_Status"], "")
        self.assertEqual(saved_rows[0]["Scheduled_Send_At"], "")

    @patch("crm_stamp.load_leads")
    @patch("crm_stamp.get_active_campaign")
    def test_stamp_email_drafts_errors_for_unknown_single_id(
        self,
        mock_get_active_campaign,
        mock_load_leads,
    ) -> None:
        mock_get_active_campaign.return_value = {"id": "werbeagentur_wien"}
        mock_load_leads.return_value = [{"ID": "WERB-0001", "Status": "new", "Email": "a@example.com"}]

        with self.assertRaisesRegex(ValueError, "Lead WERB-9999 not found"):
            crm_stamp.stamp_email_drafts(
                email_draft="Betreff: Einheitlich\n\nHallo",
                single_id="WERB-9999",
            )

    @patch("crm_stamp.build_campaign_template_email_draft")
    @patch("crm_stamp.save_leads_batch")
    @patch("crm_stamp.load_leads")
    @patch("crm_stamp.get_active_campaign")
    def test_stamp_campaign_template_drafts_renders_from_active_campaign_template(
        self,
        mock_get_active_campaign,
        mock_load_leads,
        mock_save_leads_batch,
        mock_build_campaign_template_email_draft,
    ) -> None:
        mock_get_active_campaign.return_value = {"id": "werbeagentur_wien", "config_version": "9"}
        mock_load_leads.return_value = [
            {"ID": "WERB-0001", "Status": "new", "Email": "a@example.com"},
            {"ID": "WERB-0002", "Status": "contacted", "Email": "b@example.com"},
        ]
        mock_build_campaign_template_email_draft.return_value = "Betreff: Kampagne\n\nHallo"

        result = crm_stamp.stamp_campaign_template_drafts(
            subject="",
            hook="Gemeinsamer Hook",
        )

        self.assertEqual(result["updated"], 1)
        mock_build_campaign_template_email_draft.assert_called_once()
        self.assertEqual(mock_build_campaign_template_email_draft.call_args.kwargs["hook"], "Gemeinsamer Hook")
        saved_rows = mock_save_leads_batch.call_args.args[0]
        self.assertEqual(saved_rows[0]["Email_Draft"], "Betreff: Kampagne\n\nHallo")


if __name__ == "__main__":
    unittest.main()
