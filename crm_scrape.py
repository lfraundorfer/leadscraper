"""
crm_scrape.py - Backend-aware lead scraping for the active campaign.
"""

from __future__ import annotations

import csv
import os
import re
import tempfile

import crm_backend as backend
from campaign_service import resolve_csv_path
from crm_fields import ALL_COLUMNS, ORIGINAL_COLUMNS
from crm_store import ensure_lead_ids, load_leads, save_leads
from herold_scraper import CSV_FIELDS, scrape_to_csv


def _normalize_company(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def scrape_campaign(
    campaign: dict,
    *,
    pages: str = "",
    page_pause: float = 4.0,
    search_pause: float = 2.5,
    no_search: bool = False,
    visible: bool = False,
    dump_html: str = "",
    verbose: bool = False,
) -> dict:
    """
    Scrape Herold into the active backend.

    CSV backend keeps the existing file-based behavior.
    Postgres backend uses a temporary CSV for scraper dedupe, then merges only
    truly new leads back into the database while preserving existing CRM state.
    """
    if not backend.is_postgres_backend():
        output = resolve_csv_path(campaign)
        return scrape_to_csv(
            category=campaign["keyword"],
            location=campaign["location"],
            output=output,
            pages=pages,
            page_pause=page_pause,
            search_pause=search_pause,
            no_search=no_search,
            visible=visible,
            dump_html=dump_html,
            verbose=verbose,
        )

    existing_leads = load_leads(campaign=campaign)
    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False, suffix=".csv") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
            writer.writeheader()
            for lead in existing_leads:
                writer.writerow({column: lead.get(column, "") for column in ORIGINAL_COLUMNS})
            temp_path = handle.name

        scrape_result = scrape_to_csv(
            category=campaign["keyword"],
            location=campaign["location"],
            output=temp_path,
            pages=pages,
            page_pause=page_pause,
            search_pause=search_pause,
            no_search=no_search,
            visible=visible,
            dump_html=dump_html,
            verbose=verbose,
        )

        with open(temp_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter=";")
            scraped_rows = [{column: row.get(column, "") for column in ORIGINAL_COLUMNS} for row in reader]
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

    merged = [dict(lead) for lead in existing_leads]
    seen_keys = {_normalize_company(lead.get("Unternehmen", "")) for lead in existing_leads if lead.get("Unternehmen", "").strip()}
    new_entries = 0

    for row in scraped_rows:
        company_key = _normalize_company(row.get("Unternehmen", ""))
        if not company_key or company_key in seen_keys:
            continue
        lead = {column: "" for column in ALL_COLUMNS}
        for column in ORIGINAL_COLUMNS:
            lead[column] = row.get(column, "")
        merged.append(lead)
        seen_keys.add(company_key)
        new_entries += 1

    assigned = ensure_lead_ids(merged, campaign=campaign)
    save_leads(merged, campaign=campaign)

    return {
        "output": f"postgres://campaigns/{campaign.get('id', '')}/leads",
        "new_entries": new_entries,
        "assigned_ids": assigned,
        "total_pages": scrape_result.get("total_pages", 0),
    }
