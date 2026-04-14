from __future__ import annotations

import csv
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import crm
import herold_scraper
from crm_scrape import scrape_campaign
from crm_store import load_leads, save_leads
from herold_scraper import CSV_FIELDS, Lead


def _append_temp_rows(path: str, leads: list[Lead]) -> None:
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
        for lead in leads:
            writer.writerow(
                {
                    "Unternehmen": lead.unternehmen,
                    "Website": lead.website,
                    "TelNr": lead.tel_nr,
                    "Email": lead.email,
                    "Kontaktname": lead.kontaktname,
                    "Kontaktdatum": lead.kontaktdatum,
                    "Source": lead.source,
                    "Notes": lead.notes,
                    "Adresse": lead.adresse,
                    "Google_Maps_Link": lead.google_maps_link,
                    "FirmenABC_Link": lead.firmenABC_link,
                }
            )


def _make_lead(name: str, *, email: str = "", tel: str = "") -> Lead:
    return Lead(
        unternehmen=name,
        website=f"https://{name.lower().replace(' ', '')}.example.com",
        tel_nr=tel,
        email=email,
        adresse="1010 Wien",
        source="https://www.herold.at/example",
        google_maps_link="https://maps.example.com",
    )


class ScrapeCheckpointingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.env_patch = patch.dict(os.environ, {"CRM_BACKEND": "csv"}, clear=False)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.campaign = {
            "id": "werbeagentur_wien",
            "keyword": "Werbeagenturen",
            "location": "Wien",
            "csv_path": os.path.join(self.tempdir.name, "leads.csv"),
            "id_prefix": "WERBWIEN",
            "config_version": 1,
            "draft_config_version": 1,
            "research_config_version": 1,
            "extra_queries": [],
        }

    def _load_names(self) -> list[str]:
        return [lead["Unternehmen"] for lead in load_leads(campaign=self.campaign)]

    def test_interrupt_preserves_completed_page_checkpoint(self) -> None:
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")

        def fake_scrape_to_csv(*, output: str, category: str, location: str, on_page_checkpoint=None, **_: object) -> dict:
            _append_temp_rows(output, [alpha])
            if on_page_checkpoint is not None:
                on_page_checkpoint([alpha], page_num=1, total_pages=2, category=category, location=location, total_new=1)
            raise KeyboardInterrupt()

        with patch("crm_scrape.scrape_to_csv", side_effect=fake_scrape_to_csv):
            with self.assertRaises(KeyboardInterrupt):
                scrape_campaign(self.campaign, no_search=True)

        leads = load_leads(campaign=self.campaign)
        self.assertEqual([lead["Unternehmen"] for lead in leads], ["Alpha GmbH"])
        self.assertEqual(leads[0]["ID"], "WERBWIEN-0001")

    def test_rerun_does_not_duplicate_checkpointed_leads_and_keeps_ids_stable(self) -> None:
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")
        beta = _make_lead("Beta GmbH", tel="06641234567")

        def interrupted_scrape(*, output: str, category: str, location: str, on_page_checkpoint=None, **_: object) -> dict:
            _append_temp_rows(output, [alpha])
            if on_page_checkpoint is not None:
                on_page_checkpoint([alpha], page_num=1, total_pages=2, category=category, location=location, total_new=1)
            raise KeyboardInterrupt()

        def resumed_scrape(*, output: str, category: str, location: str, on_page_checkpoint=None, **_: object) -> dict:
            _append_temp_rows(output, [alpha])
            if on_page_checkpoint is not None:
                on_page_checkpoint([alpha], page_num=1, total_pages=2, category=category, location=location, total_new=1)
            _append_temp_rows(output, [beta])
            if on_page_checkpoint is not None:
                on_page_checkpoint([beta], page_num=2, total_pages=2, category=category, location=location, total_new=2)
            return {"output": output, "new_entries": 2, "total_pages": 2}

        with patch("crm_scrape.scrape_to_csv", side_effect=interrupted_scrape):
            with self.assertRaises(KeyboardInterrupt):
                scrape_campaign(self.campaign, no_search=True)

        with patch("crm_scrape.scrape_to_csv", side_effect=resumed_scrape):
            result = scrape_campaign(self.campaign, no_search=True)

        leads = {lead["Unternehmen"]: lead for lead in load_leads(campaign=self.campaign)}
        self.assertEqual(set(leads), {"Alpha GmbH", "Beta GmbH"})
        self.assertEqual(leads["Alpha GmbH"]["ID"], "WERBWIEN-0001")
        self.assertEqual(leads["Beta GmbH"]["ID"], "WERBWIEN-0002")
        self.assertEqual(result["new_entries"], 1)
        self.assertEqual(result["assigned_ids"], 1)

    def test_extra_queries_share_dedupe_state(self) -> None:
        self.campaign["extra_queries"] = [{"keyword": "Werbeunternehmen", "location": "Wien"}]
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")
        beta = _make_lead("Beta GmbH", email="beta@example.com")

        def fake_scrape_to_csv(*, output: str, category: str, location: str, on_page_checkpoint=None, **_: object) -> dict:
            if category == "Werbeagenturen":
                page_leads = [alpha]
            else:
                page_leads = [alpha, beta]
            _append_temp_rows(output, page_leads)
            if on_page_checkpoint is not None:
                on_page_checkpoint(page_leads, page_num=1, total_pages=1, category=category, location=location, total_new=len(page_leads))
            return {"output": output, "new_entries": len(page_leads), "total_pages": 1}

        with patch("crm_scrape.scrape_to_csv", side_effect=fake_scrape_to_csv):
            result = scrape_campaign(self.campaign, no_search=True)

        leads = load_leads(campaign=self.campaign)
        self.assertEqual(sorted(lead["Unternehmen"] for lead in leads), ["Alpha GmbH", "Beta GmbH"])
        self.assertEqual(result["new_entries"], 2)
        self.assertEqual(result["assigned_ids"], 2)

    def test_checkpoint_saves_do_not_clobber_existing_fields(self) -> None:
        existing = {
            "ID": "WERBWIEN-0007",
            "Unternehmen": "Existing GmbH",
            "Status": "approved",
            "Notes": "keep me",
            "Email_Draft": "already drafted",
            "Scheduled_Send_Status": "queued",
        }
        save_leads([existing], campaign=self.campaign)
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")

        def fake_scrape_to_csv(*, output: str, category: str, location: str, on_page_checkpoint=None, **_: object) -> dict:
            _append_temp_rows(output, [alpha])
            if on_page_checkpoint is not None:
                on_page_checkpoint([alpha], page_num=1, total_pages=1, category=category, location=location, total_new=1)
            return {"output": output, "new_entries": 1, "total_pages": 1}

        with patch("crm_scrape.scrape_to_csv", side_effect=fake_scrape_to_csv):
            scrape_campaign(self.campaign, no_search=True)

        leads = {lead["Unternehmen"]: lead for lead in load_leads(campaign=self.campaign)}
        self.assertEqual(leads["Existing GmbH"]["Status"], "approved")
        self.assertEqual(leads["Existing GmbH"]["Notes"], "keep me")
        self.assertEqual(leads["Existing GmbH"]["Email_Draft"], "already drafted")
        self.assertEqual(leads["Existing GmbH"]["Scheduled_Send_Status"], "queued")

    def test_reconciliation_persists_rows_missed_by_callback(self) -> None:
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")

        def fake_scrape_to_csv(*, output: str, **_: object) -> dict:
            _append_temp_rows(output, [alpha])
            return {"output": output, "new_entries": 1, "total_pages": 1}

        with patch("crm_scrape.scrape_to_csv", side_effect=fake_scrape_to_csv):
            result = scrape_campaign(self.campaign, no_search=True)

        leads = load_leads(campaign=self.campaign)
        self.assertEqual([lead["Unternehmen"] for lead in leads], ["Alpha GmbH"])
        self.assertEqual(leads[0]["ID"], "WERBWIEN-0001")
        self.assertEqual(result["new_entries"], 1)
        self.assertEqual(result["assigned_ids"], 1)


class CmdScrapeStageTests(unittest.TestCase):
    def _args(self) -> SimpleNamespace:
        return SimpleNamespace(
            keyword="",
            location="",
            pages="",
            page_pause=4.0,
            search_pause=2.5,
            no_search=True,
            visible=False,
            dump_html="",
            verbose=False,
        )

    def test_cmd_scrape_marks_campaign_stage_on_success(self) -> None:
        with patch("campaign_service.get_active_campaign", return_value={"id": "camp"}), \
             patch("campaign_service.mark_campaign_stage_run") as mark_stage, \
             patch("crm_scrape.scrape_campaign", return_value={"new_entries": 1, "output": "x", "total_pages": 1}):
            crm.cmd_scrape(self._args())

        mark_stage.assert_called_once_with("camp", "scraped")

    def test_cmd_scrape_does_not_mark_campaign_stage_on_interrupt(self) -> None:
        with patch("campaign_service.get_active_campaign", return_value={"id": "camp"}), \
             patch("campaign_service.mark_campaign_stage_run") as mark_stage, \
             patch("crm_scrape.scrape_campaign", side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt):
                crm.cmd_scrape(self._args())

        mark_stage.assert_not_called()


class HeroldScraperCallbackTests(unittest.TestCase):
    def test_scrape_to_csv_calls_page_checkpoint_after_page_write(self) -> None:
        alpha = _make_lead("Alpha GmbH", email="alpha@example.com")
        callback = Mock()

        class FakeFetcher:
            def __init__(self, headless: bool = True, wait_ms: int = 8000) -> None:
                self.headless = headless
                self.wait_ms = wait_ms

            def get(self, url: str, dump_dir: str = "") -> str:
                return "<html></html>"

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tempdir:
            output = os.path.join(tempdir, "probe.csv")
            with patch.object(herold_scraper, "HAS_PLAYWRIGHT", True), \
                 patch.object(herold_scraper, "HeroldFetcher", FakeFetcher), \
                 patch.object(herold_scraper, "detect_total_pages", return_value=1), \
                 patch.object(herold_scraper, "parse_herold_page", return_value=[alpha]):
                result = herold_scraper.scrape_to_csv(
                    "Werbeagenturen",
                    "Wien",
                    output=output,
                    no_search=True,
                    on_page_checkpoint=callback,
                )

        self.assertEqual(result["new_entries"], 1)
        callback.assert_called_once()
        page_leads = callback.call_args.args[0]
        self.assertEqual(len(page_leads), 1)
        self.assertEqual(page_leads[0].unternehmen, "Alpha GmbH")
        self.assertEqual(callback.call_args.kwargs["page_num"], 1)
        self.assertEqual(callback.call_args.kwargs["total_pages"], 1)
        self.assertEqual(callback.call_args.kwargs["category"], "Werbeagenturen")
        self.assertEqual(callback.call_args.kwargs["location"], "Wien")
        self.assertEqual(callback.call_args.kwargs["total_new"], 1)


if __name__ == "__main__":
    unittest.main()
