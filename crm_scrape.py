"""
crm_scrape.py - Backend-aware lead scraping for the active campaign.
"""

from __future__ import annotations

import csv
import os
import tempfile

import crm_backend as backend
from campaign_service import resolve_csv_path
from crm_fields import ALL_COLUMNS, ORIGINAL_COLUMNS, normalize_company_key
from crm_store import ensure_lead_ids, load_leads, save_leads
from herold_scraper import CSV_FIELDS, scrape_to_csv


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

    Scraping always runs against a temporary CSV snapshot seeded with the
    current campaign rows. That keeps the scraper's resume/dedupe behavior while
    merging only truly new companies back into the CRM without clobbering
    existing statuses, notes, or drafts.
    """
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
    seen_keys = {
        normalize_company_key(lead.get("Unternehmen", ""))
        for lead in existing_leads
        if lead.get("Unternehmen", "").strip()
    }
    new_entries = 0

    for row in scraped_rows:
        company_key = normalize_company_key(row.get("Unternehmen", ""))
        if not company_key or company_key in seen_keys:
            continue
        lead = {column: "" for column in ALL_COLUMNS}
        for column in ORIGINAL_COLUMNS:
            lead[column] = row.get(column, "")
        merged.append(lead)
        seen_keys.add(company_key)
        new_entries += 1

    for extra_query in campaign.get("extra_queries") or []:
        eq_kw = (extra_query.get("keyword") or "").strip() or campaign["keyword"]
        eq_loc = (extra_query.get("location") or "").strip() or campaign["location"]
        if not eq_kw or not eq_loc:
            continue
        extra_temp = ""
        try:
            with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False, suffix=".csv") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
                writer.writeheader()
                for lead in merged:
                    writer.writerow({column: lead.get(column, "") for column in ORIGINAL_COLUMNS})
                extra_temp = handle.name

            scrape_to_csv(
                category=eq_kw,
                location=eq_loc,
                output=extra_temp,
                pages=pages,
                page_pause=page_pause,
                search_pause=search_pause,
                no_search=no_search,
                visible=visible,
                dump_html=dump_html,
                verbose=verbose,
            )

            with open(extra_temp, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter=";")
                for row in reader:
                    company_key = normalize_company_key(row.get("Unternehmen", ""))
                    if not company_key or company_key in seen_keys:
                        continue
                    lead = {column: "" for column in ALL_COLUMNS}
                    for column in ORIGINAL_COLUMNS:
                        lead[column] = row.get(column, "")
                    merged.append(lead)
                    seen_keys.add(company_key)
                    new_entries += 1
        finally:
            if extra_temp and os.path.exists(extra_temp):
                os.remove(extra_temp)

    assigned = ensure_lead_ids(merged, campaign=campaign)
    save_leads(merged, campaign=campaign)

    return {
        "output": (
            f"postgres://campaigns/{campaign.get('id', '')}/leads"
            if backend.is_postgres_backend()
            else resolve_csv_path(campaign)
        ),
        "new_entries": new_entries,
        "assigned_ids": assigned,
        "total_pages": scrape_result.get("total_pages", 0),
    }
