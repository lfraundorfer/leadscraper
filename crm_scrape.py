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
from crm_store import assign_missing_lead_ids, current_max_lead_number, load_leads, save_leads_batch
from herold_scraper import CSV_FIELDS, Lead, scrape_to_csv
from tqdm import tqdm


def _lead_to_original_row(lead: Lead) -> dict[str, str]:
    return {
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


def _row_to_campaign_lead(row: dict[str, str]) -> dict[str, str]:
    lead = {column: "" for column in ALL_COLUMNS}
    for column in ORIGINAL_COLUMNS:
        lead[column] = row.get(column, "")
    return lead


def _write_temp_seed(path: str, leads: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow({column: lead.get(column, "") for column in ORIGINAL_COLUMNS})


def _read_original_rows(path: str) -> list[dict[str, str]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        return [{column: row.get(column, "") for column in ORIGINAL_COLUMNS} for row in reader]


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
    existing_leads = [dict(lead) for lead in load_leads(campaign=campaign)]
    merged = [dict(lead) for lead in existing_leads]
    seen_keys = {
        normalize_company_key(lead.get("Unternehmen", ""))
        for lead in existing_leads
        if lead.get("Unternehmen", "").strip()
    }
    new_entries = 0
    next_number = current_max_lead_number(existing_leads, campaign=campaign)
    assigned_ids = 0

    def checkpoint_rows(
        rows: list[dict[str, str]],
        *,
        category_label: str,
        location_label: str,
        page_num: int | None = None,
        total_pages_label: int | None = None,
    ) -> None:
        nonlocal next_number, new_entries, assigned_ids

        new_batch: list[dict[str, str]] = []
        new_keys: list[str] = []
        for row in rows:
            company_key = normalize_company_key(row.get("Unternehmen", ""))
            if not company_key or company_key in seen_keys:
                continue
            new_batch.append(_row_to_campaign_lead(row))
            new_keys.append(company_key)

        if not new_batch:
            return

        next_number, assigned = assign_missing_lead_ids(new_batch, campaign=campaign, next_number=next_number)
        save_leads_batch(new_batch, campaign=campaign)

        assigned_ids += assigned
        new_entries += len(new_batch)
        for company_key, lead in zip(new_keys, new_batch):
            seen_keys.add(company_key)
            merged.append(dict(lead))

        if page_num is not None and total_pages_label is not None:
            tqdm.write(
                f"Checkpointed {len(new_batch)} new leads from {category_label}/{location_label} "
                f"page {page_num}/{total_pages_label}"
            )

    def run_query(category_label: str, location_label: str) -> dict:
        temp_path = ""
        try:
            with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False, suffix=".csv") as handle:
                temp_path = handle.name
            _write_temp_seed(temp_path, merged)

            def on_page_checkpoint(
                page_leads: list[Lead],
                *,
                page_num: int,
                total_pages: int,
                category: str,
                location: str,
                total_new: int,
            ) -> None:
                _ = total_new
                checkpoint_rows(
                    [_lead_to_original_row(lead) for lead in page_leads],
                    category_label=category,
                    location_label=location,
                    page_num=page_num,
                    total_pages_label=total_pages,
                )

            try:
                scrape_result = scrape_to_csv(
                    category=category_label,
                    location=location_label,
                    output=temp_path,
                    pages=pages,
                    page_pause=page_pause,
                    search_pause=search_pause,
                    no_search=no_search,
                    visible=visible,
                    dump_html=dump_html,
                    verbose=verbose,
                    on_page_checkpoint=on_page_checkpoint,
                )
            except BaseException:
                checkpoint_rows(
                    _read_original_rows(temp_path),
                    category_label=category_label,
                    location_label=location_label,
                )
                raise

            checkpoint_rows(
                _read_original_rows(temp_path),
                category_label=category_label,
                location_label=location_label,
            )
            return scrape_result
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    scrape_result = run_query(campaign["keyword"], campaign["location"])

    for extra_query in campaign.get("extra_queries") or []:
        eq_kw = (extra_query.get("keyword") or "").strip() or campaign["keyword"]
        eq_loc = (extra_query.get("location") or "").strip() or campaign["location"]
        if not eq_kw or not eq_loc:
            continue
        run_query(eq_kw, eq_loc)

    return {
        "output": (
            f"postgres://campaigns/{campaign.get('id', '')}/leads"
            if backend.is_postgres_backend()
            else resolve_csv_path(campaign)
        ),
        "new_entries": new_entries,
        "assigned_ids": assigned_ids,
        "total_pages": scrape_result.get("total_pages", 0),
    }
