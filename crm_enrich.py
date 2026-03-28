"""
crm_enrich.py – Fill Kontaktname from FirmenABC for leads that are missing it.

Reuses HeroldFetcher and fetch_firmenabc_contacts from herold_scraper.py.
"""

from __future__ import annotations

import time
from datetime import datetime

from campaign_service import get_active_campaign, mark_campaign_stage_run
from herold_scraper import HeroldFetcher, fetch_firmenabc_contacts
from crm_store import load_leads, progress_save_interval, save_leads_batch


RATE_LIMIT_SEC = 2.0


def enrich_lead(lead: dict, fetcher: HeroldFetcher) -> str:
    """
    Fetch the FirmenABC page for this lead and return the contact name.
    Returns '' if not found or link is missing.
    """
    url = lead.get("FirmenABC_Link", "").strip()
    if not url or url == "X":
        return ""
    if "firmenabc.at" not in url:
        return ""
    # Skip bare domain (no company path)
    if url.rstrip("/") in ("https://firmenabc.at", "http://firmenabc.at", "https://www.firmenabc.at"):
        return ""

    return fetch_firmenabc_contacts(url, fetcher)


def main(force: bool = False, single_id: str = "") -> None:
    """
    CLI entry point for `python crm.py enrich`.
    Enriches all leads missing Kontaktname (or a single lead if --id given).
    Periodically checkpoints progress to survive interruption without rewriting
    the full dataset after every single lead.
    """
    campaign = get_active_campaign()
    leads = load_leads(campaign=campaign)
    if not leads:
        print("No leads found. Run `python crm.py migrate` first.")
        return

    # Filter to leads that need enrichment
    if single_id:
        targets = [l for l in leads if l.get("ID", "").strip() == single_id]
        if not targets:
            print(f"Lead {single_id} not found.")
            return
    else:
        targets = [
            l for l in leads
            if (force or not l.get("Enriched_At"))
            and l.get("FirmenABC_Link", "").strip()
            and l.get("FirmenABC_Link", "").strip() != "X"
        ]

    print(f"Enriching {len(targets)} lead(s)…")

    fetcher = HeroldFetcher(headless=True)
    enriched = 0
    skipped = 0
    processed = 0
    dirty = False
    dirty_batch: list[dict] = []
    save_every = progress_save_interval()

    try:
        for lead in targets:
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")

            if not force and lead.get("Kontaktname", "").strip():
                print(f"  {lid} {company[:40]} – already has name, skipping")
                skipped += 1
                continue

            name = enrich_lead(lead, fetcher)
            now = datetime.now().strftime("%Y-%m-%d %H:%M")

            if name:
                lead["Kontaktname"] = name.split("\n")[0]  # take first name if multiple
                lead["Enriched_At"] = now
                enriched += 1
                print(f"  {lid} {company[:40]} → {lead['Kontaktname']}")
            else:
                lead["Enriched_At"] = now  # mark as attempted so we don't retry
                print(f"  {lid} {company[:40]} → (no name found)")

            processed += 1
            dirty = True
            dirty_batch.append(dict(lead))
            if processed % save_every == 0:
                save_leads_batch(dirty_batch, campaign=campaign)
                dirty_batch = []
                dirty = False
            time.sleep(RATE_LIMIT_SEC)

    finally:
        fetcher.close()
        if dirty:
            save_leads_batch(dirty_batch, campaign=campaign)

    mark_campaign_stage_run(campaign["id"], "enriched")
    print(f"\nDone. Enriched: {enriched} | Skipped (already had name): {skipped}")
