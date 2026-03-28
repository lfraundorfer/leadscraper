"""
crm_research.py - Deep research per lead for the active campaign:
  1. Website HTML fetch + cleaning
  2. Google Reviews (rating, count, snippets) via Places API
  3. Google local search rank + top competitors for the campaign keyword
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from campaign_service import format_rank_keyword, get_active_campaign, mark_campaign_stage_run
from herold_scraper import HeroldFetcher, DIRECTORY_DOMAINS
from crm_store import get_bezirk, load_leads, progress_save_interval, save_leads_batch

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RATE_LIMIT_SEC = 3.0
MAX_HTML_CHARS = 12_000

# Additional directory domains specific to Austrian business listings
EXTRA_DIRECTORY_DOMAINS = {
    "baufinder.at", "evi.gv.at", "ksv.at", "meinestelle.de",
    "unternehmen24", "jobwald", "wohnnet.at", "bauwohnwelt.at",
    "bezirkstipp.at", "oeffnungszeitenbuch", "firmeninfo.at",
    "handschlagqualitaet.at", "installer.at", "meister.at",
    "branchenbuch", "stadtbranchenbuch",
}

ALL_DIRECTORY_DOMAINS = DIRECTORY_DOMAINS | EXTRA_DIRECTORY_DOMAINS


# ---------------------------------------------------------------------------
# Website categorisation
# ---------------------------------------------------------------------------

def categorize_website(url: str) -> str:
    """Returns 'real', 'directory', or 'none'."""
    if not url or url.strip() in ("", "X", "x"):
        return "none"
    url_lower = url.lower()
    for domain in ALL_DIRECTORY_DOMAINS:
        if domain in url_lower:
            return "directory"
    return "real"


def fetch_and_clean_html(url: str, fetcher: HeroldFetcher) -> str:
    """
    Fetch URL with Playwright. Strip scripts/styles/svg.
    Return first MAX_HTML_CHARS chars of cleaned text. Returns '' on failure.
    """
    try:
        html = fetcher.get(url)
    except Exception as e:
        return ""

    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "svg", "noscript", "head"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s{2,}", " ", text)
    return text[:MAX_HTML_CHARS]


# ---------------------------------------------------------------------------
# Google Reviews via Places API (New)
# ---------------------------------------------------------------------------

def get_places_data(company_name: str, address: str, api_key: str) -> dict:
    """
    Use Google Places API (New) to get rating, review count, and review snippets.
    Returns dict: rating, review_count, snippets (list of "N★: text" strings).
    Negative reviews (1-2 stars) are prefixed with [NEG] so GPT can use them as hooks.
    """
    result = {"rating": "", "review_count": "", "snippets": []}
    if not api_key:
        return result

    query = f"{company_name} {address}".strip()
    if not query:
        return result

    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.rating,places.userRatingCount,places.reviews",
    }
    body = {"textQuery": query, "languageCode": "de"}

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return result

    places = data.get("places", [])
    if not places:
        return result

    place = places[0]
    rating = place.get("rating")
    count = place.get("userRatingCount")
    if rating is not None:
        result["rating"] = str(round(rating, 1))
    if count is not None:
        result["review_count"] = str(count)

    snippets = []
    for review in place.get("reviews", []):
        text = (review.get("text") or {}).get("text", "").strip()
        stars = review.get("rating", 5)
        if text and len(text) > 15:
            prefix = "[NEG] " if stars <= 2 else ""
            snippets.append(f"{prefix}{stars}★: {text[:500]}")

    result["snippets"] = snippets
    return result


# ---------------------------------------------------------------------------
# Google local search rank + competitors
# ---------------------------------------------------------------------------

def check_google_rank(company_name: str, plz: str, campaign: dict, fetcher: HeroldFetcher) -> dict:
    """
    Search Google for the active campaign rank keyword and find:
    - The rank/position of this company (1-10 or "not_found")
    - Whether it appears in the local map pack
    - Top 3 organic competitor names

    Returns dict: rank_keyword, rank_position, map_pack (yes/no), competitors (list)
    """
    result = {
        "rank_keyword": "",
        "rank_position": "",
        "map_pack": "",
        "competitors": [],
    }
    keyword = format_rank_keyword(campaign, plz=plz)
    if not keyword:
        return result

    result["rank_keyword"] = keyword
    search_url = f"https://www.google.com/search?q={keyword.replace(' ', '+')}&hl=de&gl=at&num=10"

    try:
        html = fetcher.get(search_url)
    except Exception:
        result["rank_position"] = "error"
        return result

    soup = BeautifulSoup(html, "lxml")

    # Normalize company name for matching
    norm_company = _normalize_name(company_name)
    stop_words = _campaign_stop_words(campaign)

    # --- Map pack (local 3-pack) ---
    # Google local pack results have specific containers
    map_pack_found = False
    local_pack = soup.find_all(class_=re.compile(r"rllt__details|VkpGBb|cXedhc"))
    local_pack_names = []
    for item in local_pack:
        name_el = item.find(class_=re.compile(r"rllt__details|dbg0pd|OSrXXb"))
        if not name_el:
            name_el = item
        name_text = name_el.get_text(separator=" ", strip=True)
        if name_text:
            local_pack_names.append(name_text[:60])
            if _name_matches(norm_company, _normalize_name(name_text), stop_words=stop_words):
                map_pack_found = True

    result["map_pack"] = "yes" if map_pack_found else "no"

    # --- Organic results ---
    organic_names = []
    position = 0

    # Google organic results: typically in <div class="g"> or similar containers
    result_containers = soup.find_all("div", class_=re.compile(r"^(g|tF2Cxc|Gx5Zad)$"))
    if not result_containers:
        # Broader fallback
        result_containers = soup.find_all("div", attrs={"data-hveid": True})

    found_position = "not_found"
    for container in result_containers:
        # Each container should have a title/heading
        heading = container.find(["h3", "h2"])
        if not heading:
            continue
        name_text = heading.get_text(strip=True)
        if not name_text:
            continue
        position += 1
        organic_names.append(name_text[:60])
        if _name_matches(norm_company, _normalize_name(name_text), stop_words=stop_words):
            found_position = str(position)
        if position >= 10:
            break

    result["rank_position"] = found_position

    # Top 3 competitors = first 3 organic results that are NOT our company
    competitors = [
        n for n in organic_names
        if not _name_matches(norm_company, _normalize_name(n), stop_words=stop_words)
    ][:3]
    result["competitors"] = competitors

    return result


def _normalize_name(name: str) -> str:
    """Lowercase, remove GmbH/KG/etc. and punctuation for fuzzy matching."""
    name = name.lower()
    for suffix in ["gmbh", "kg", "e.u.", "e.k.", "ges.m.b.h.", "og", "oeg", "nfg"]:
        name = name.replace(suffix, "")
    name = re.sub(r"[^a-z0-9äöü\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _campaign_stop_words(campaign: dict) -> set[str]:
    words: set[str] = {"und", "der", "die", "in", "wien"}
    for value in [campaign.get("keyword", ""), campaign.get("location", "")]:
        normalized = _normalize_name(str(value))
        words.update(w for w in normalized.split() if len(w) > 1)
    return words


def _name_matches(a: str, b: str, stop_words: set[str] | None = None) -> bool:
    """Check if two normalized company names overlap significantly."""
    words_a = set(a.split())
    words_b = set(b.split())
    stop = stop_words or {"und", "der", "die", "wien"}
    words_a -= stop
    words_b -= stop
    if not words_a or not words_b:
        return False
    overlap = words_a & words_b
    # Match if ≥50% of the shorter set overlaps
    shorter = min(len(words_a), len(words_b))
    return len(overlap) / shorter >= 0.5


# ---------------------------------------------------------------------------
# Main research function
# ---------------------------------------------------------------------------

def research_lead(lead: dict, fetcher: HeroldFetcher, campaign: dict) -> dict:
    """
    Run all research steps for a single lead.
    Returns dict of updates to apply to the lead.
    """
    updates = {}

    # 1. Website categorization
    website = lead.get("Website", "").strip()
    category = categorize_website(website)
    updates["Website_Category"] = category

    # 2. Website HTML
    if category == "real":
        html_text = fetch_and_clean_html(website, fetcher)
        updates["_website_html"] = html_text  # in-memory only, not persisted
        if not html_text:
            updates["Website_Category"] = "fetch_error"
        time.sleep(RATE_LIMIT_SEC)

    # 3. Google Reviews via Places API
    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
    if api_key:
        reviews = get_places_data(
            lead.get("Unternehmen", ""),
            lead.get("Adresse", ""),
            api_key,
        )
        updates["Google_Rating"] = reviews["rating"]
        updates["Google_Review_Count"] = reviews["review_count"]
        updates["Google_Review_Snippets"] = " | ".join(reviews["snippets"])
        time.sleep(1.0)  # Places API is fast, short pause is enough

    # 4. Google rank + competitors
    adresse = lead.get("Adresse", "")
    plz, bezirk = get_bezirk(adresse)
    if plz:
        rank_info = check_google_rank(lead.get("Unternehmen", ""), plz, campaign, fetcher)
        updates["Google_Rank_Keyword"] = rank_info["rank_keyword"]
        updates["Google_Rank_Position"] = rank_info["rank_position"]
        updates["Google_Map_Pack"] = rank_info["map_pack"]
        updates["Google_Competitors"] = " | ".join(rank_info["competitors"])
        time.sleep(RATE_LIMIT_SEC)

    updates["Research_Config_Version"] = str(campaign.get("research_config_version") or campaign.get("config_version") or "1")
    updates["Research_Stale"] = "0"

    return updates


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _id_number(lead_id: str) -> int:
    """Extract numeric suffix from LEAD-0581 -> 581. Returns 0 if unparseable."""
    parts = lead_id.rsplit("-", 1)
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


def main(force: bool = False, single_id: str = "", from_id: str = "") -> None:
    """
    CLI entry point for `python crm.py research`.
    Runs all research steps for each lead and saves results.
    """
    if not os.getenv("GOOGLE_PLACES_API_KEY"):
        print("WARNING: GOOGLE_PLACES_API_KEY not set — Google Reviews will be skipped.")
        print("         Add it to your .env file to enable review data.")

    campaign = get_active_campaign()
    leads = load_leads(campaign=campaign)
    if not leads:
        print("No leads found. Run `python crm.py migrate` first.")
        return

    if single_id:
        targets = [l for l in leads if l.get("ID", "").strip() == single_id]
        if not targets:
            print(f"Lead {single_id} not found.")
            return
    else:
        targets = [
            l for l in leads
            if force or not l.get("Website_Category") or l.get("Research_Stale") == "1"
        ]
        if from_id:
            from_num = _id_number(from_id)
            targets = [l for l in targets if _id_number(l.get("ID", "")) >= from_num]
            print(f"Resuming from {from_id} (#{from_num})")

    print(f"Researching {len(targets)} lead(s)…")

    fetcher = HeroldFetcher(headless=True)
    done = 0
    dirty = False
    dirty_batch: list[dict] = []
    save_every = progress_save_interval()

    try:
        for lead in targets:
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")
            print(f"  {lid} {company[:40]}…", end=" ", flush=True)

            updates = research_lead(lead, fetcher, campaign)

            # Apply updates (skip _website_html — it's in-memory only)
            for k, v in updates.items():
                if not k.startswith("_"):
                    lead[k] = v

            done += 1
            dirty = True
            dirty_batch.append(dict(lead))
            if done % save_every == 0:
                save_leads_batch(dirty_batch, campaign=campaign)
                dirty_batch = []
                dirty = False

            score_info = f"cat={lead.get('Website_Category', '?')} | ★{lead.get('Google_Rating', '?')} | rank={lead.get('Google_Rank_Position', '?')}"
            print(score_info)

    finally:
        fetcher.close()
        if dirty:
            save_leads_batch(dirty_batch, campaign=campaign)

    mark_campaign_stage_run(campaign["id"], "researched")
    print(f"\nDone. Researched {done} lead(s).")
