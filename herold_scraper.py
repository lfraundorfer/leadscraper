#!/usr/bin/env python3
"""
herold_scraper.py – Crawls herold.at for any category + location.

Extracts: company name, phone, email, website, address, contact name (from firmenabc).
If no website on herold.at → DuckDuckGo search. No result → marks X.
Appends only NEW entries (deduplicates by normalized company name).
Also adds Google Maps link and FirmenABC link per entry.

Install:
    pip install playwright beautifulsoup4 lxml ddgs tqdm requests
    playwright install chromium

Usage:
    python herold_scraper.py Installateur Wien
    python herold_scraper.py Elektriker Graz --output elektrik_graz.csv
    python herold_scraper.py Installateur Wien --no-search
    python herold_scraper.py Installateur Wien --pages 1-5
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Set
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from ddgs import DDGS
    HAS_DDG = True
except ImportError:
    try:
        from duckduckgo_search import DDGS
        HAS_DDG = True
    except ImportError:
        HAS_DDG = False


class _TqdmHandler(logging.Handler):
    """Routes log records through tqdm.write so they don't break the progress bar."""
    def emit(self, record):
        tqdm.write(self.format(record))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _slugify(s: str) -> str:
    s = s.lower()
    for a, b in [("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss")]:
        s = s.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def build_page_url(category: str, location: str, page: int) -> str:
    cat, loc = _slugify(category), _slugify(location)
    base = f"https://www.herold.at/gelbe-seiten/{loc}/{cat}/"
    return base if page <= 1 else f"{base}seite/{page}/"


def detect_total_pages(html: str) -> int:
    """Read 'Seite 1/28' from <title> → 28. Falls back to max pagination link."""
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text() if soup.title else ""
    m = re.search(r"Seite \d+/(\d+)", title)
    if m:
        return int(m.group(1))
    nums = [
        int(re.search(r"/seite/(\d+)/", a["href"]).group(1))
        for a in soup.find_all("a", href=re.compile(r"/seite/\d+/"))
        if re.search(r"/seite/(\d+)/", a.get("href", ""))
    ]
    return max(nums) if nums else 1

CSV_FIELDS = [
    "Unternehmen", "Website", "TelNr", "Email", "Kontaktname",
    "Kontaktdatum", "Source", "Notes", "Adresse",
    "Google_Maps_Link", "FirmenABC_Link",
]

# Sites that are directories, NOT company websites
DIRECTORY_DOMAINS = {
    "herold.at", "firmenabc.at", "wko.at", "gelbe-seiten",
    "cylex.at", "yelp.", "google.com", "wikipedia.",
    "facebook.com", "xing.com", "linkedin.com", "meinestadt.",
    "stadtbranchenbuch", "handschlagqualitaet.at", "bauwohnwelt.at",
    "wo-in-wien.at", "susi.at", "infoisinfo.", "compnet.at",
    "oeffnungszeitenbuch.", "firmeninfo.at", "firmeneintrag.",
    "creditreform.", "tupalo.at", "kompass.com", "gutgemacht.at",
    "gecheckt.at", "bezirkstipp.at", "wohnnet.at", "vienna.net",
    "cumaps.net", "bauwohnwelt.at",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Lead:
    unternehmen: str = ""
    website: str = ""
    tel_nr: str = ""
    email: str = ""
    kontaktname: str = ""
    kontaktdatum: str = ""
    source: str = ""
    notes: str = ""
    adresse: str = ""
    google_maps_link: str = ""
    firmenABC_link: str = ""


# ---------------------------------------------------------------------------
# Playwright page fetcher
# ---------------------------------------------------------------------------

class HeroldFetcher:
    """Renders herold.at pages with a real Chromium browser."""

    def __init__(self, headless: bool = True, wait_ms: int = 8000):
        if not HAS_PLAYWRIGHT:
            raise RuntimeError(
                "playwright not installed.\n"
                "Run: pip install playwright && playwright install chromium"
            )
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=headless)
        self._ctx = self._browser.new_context(user_agent=USER_AGENT)
        self._page = self._ctx.new_page()
        self._wait_ms = wait_ms

    def get(self, url: str, dump_dir: str = "") -> str:
        logging.info("Fetching: %s", url)
        try:
            self._page.goto(url, wait_until="networkidle", timeout=45_000)
        except PWTimeout:
            logging.warning("networkidle timed out for %s – using what we have", url)

        # Scroll to trigger lazy-loaded listings
        self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        self._page.wait_for_timeout(1500)
        self._page.evaluate("window.scrollTo(0, 0)")
        self._page.wait_for_timeout(500)

        html = self._page.content()

        if dump_dir:
            _dump_html(html, url, dump_dir)

        return html

    def close(self):
        try:
            self._browser.close()
            self._pw.stop()
        except Exception:
            pass


def _dump_html(html: str, url: str, dump_dir: str) -> None:
    os.makedirs(dump_dir, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]", "_", url.lower())[-60:]
    path = os.path.join(dump_dir, f"dump_{slug}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    logging.debug("Dumped HTML to %s", path)


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_herold_page(html: str, page_url: str) -> List[Lead]:
    soup = BeautifulSoup(html, "lxml")
    leads: List[Lead] = []

    # herold.at renders <article itemtype="…/LocalBusiness"> for each listing
    cards = soup.select('article[itemtype*="LocalBusiness"]')
    if not cards:
        # Broader fallback
        cards = soup.select("article[itemprop='item']") or soup.select("article")

    logging.debug("Found %d article cards on %s", len(cards), page_url)

    for card in cards:
        lead = _extract_card(card, page_url)
        if lead.unternehmen:
            leads.append(lead)

    if not leads:
        # Last resort: JSON-LD
        leads = _parse_json_ld(soup, page_url)

    return leads


def _extract_card(card, page_url: str) -> Lead:
    lead = Lead(source=page_url)

    # --- Company name: herold uses <meta itemprop="name"> ---
    name_meta = card.find("meta", attrs={"itemprop": "name"})
    if name_meta:
        lead.unternehmen = _clean(name_meta.get("content", ""))
    if not lead.unternehmen:
        # Fallback: first heading
        for tag in ("h2", "h3", "h1"):
            el = card.find(tag)
            if el:
                lead.unternehmen = _clean(el.get_text(" ", strip=True))
                break
    if not lead.unternehmen:
        return lead

    # --- Phone ---
    tel_a = card.find("a", href=re.compile(r"^tel:", re.I))
    if tel_a:
        raw = tel_a.get("href", "").replace("tel:", "").strip()
        lead.tel_nr = _clean(tel_a.get_text(strip=True) or raw)

    # --- Email ---
    mail_a = card.find("a", href=re.compile(r"^mailto:", re.I))
    if mail_a:
        lead.email = mail_a.get("href", "").replace("mailto:", "").strip()

    # --- Website (exclude directory links) ---
    for a in card.find_all("a", href=re.compile(r"^https?://", re.I)):
        href = a.get("href", "")
        if not _is_directory(href):
            lead.website = href
            break

    # --- Address: look for itemprop address block, or postal-code pattern ---
    addr_el = card.find(attrs={"itemprop": "address"})
    if addr_el:
        street = addr_el.find(attrs={"itemprop": "streetAddress"})
        postal = addr_el.find(attrs={"itemprop": "postalCode"})
        city   = addr_el.find(attrs={"itemprop": "addressLocality"})
        parts  = [el.get("content") or el.get_text(strip=True) for el in [street, postal, city] if el]
        lead.adresse = _clean(" ".join(p for p in parts if p))
    if not lead.adresse:
        text = card.get_text(" ", strip=True)
        m = re.search(r"\b1\d{3}\s+Wien\b[^,;<]{0,60}", text)
        if m:
            lead.adresse = _clean(m.group(0))

    # --- Herold detail link as source ---
    for a in card.find_all("a", href=True):
        href = a.get("href", "")
        if "/gelbe-seiten/" in href and href != page_url:
            lead.source = href if href.startswith("http") else "https://www.herold.at" + href
            break

    return lead


def _parse_json_ld(soup: BeautifulSoup, page_url: str) -> List[Lead]:
    leads: List[Lead] = []
    for script in soup.find_all("script", type=re.compile(r"ld\+json", re.I)):
        raw = script.string or script.get_text("", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        entries = data if isinstance(data, list) else [data]
        for item in entries:
            if not isinstance(item, dict):
                continue
            t = str(item.get("@type", ""))
            if not any(x in t for x in ("Organization", "LocalBusiness")):
                continue
            lead = Lead(source=page_url)
            lead.unternehmen = _clean(item.get("name", ""))
            lead.tel_nr = _clean(item.get("telephone", ""))
            url = _clean(item.get("url", ""))
            if url and not _is_directory(url):
                lead.website = url
            addr = item.get("address", {})
            if isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("postalCode", ""),
                    addr.get("addressLocality", ""),
                ]
                lead.adresse = _clean(" ".join(p for p in parts if p))
            if lead.unternehmen:
                leads.append(lead)

    return leads


# ---------------------------------------------------------------------------
# FirmenABC contact name extraction
# ---------------------------------------------------------------------------

_CONTACT_ROLES = {"geschäftsführer", "inhaber", "inhaberin", "prokurist", "prokurismus"}


def fetch_firmenabc_contacts(url: str, fetcher: "HeroldFetcher") -> str:
    """Fetch a firmenabc.at company page and return contact names (GF/Inhaber).
    Returns newline-joined 'Herr/Frau Name' strings, or '' if none found."""
    if not url or "firmenabc.at" not in url:
        return ""
    try:
        html = fetcher.get(url)
    except Exception as e:
        logging.debug("Could not fetch firmenabc page %s: %s", url, e)
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Company data lives in the #crefo section.
    # Structure: #crefo > div.hidden > div.grid (the key-value grid) > ...
    grid = soup.find(id="crefo")
    if not grid:
        return ""

    # The actual key-value grid is the child div with grid-cols-7
    inner = grid.find("div", class_=re.compile(r"grid-cols-7"))
    if not inner:
        return ""

    children = [c for c in inner.children if hasattr(c, "name") and c.name]
    names: list[str] = []
    seen: set[str] = set()

    for i, child in enumerate(children):
        role_h4 = child.find("h4", class_=re.compile(r"font-bold"))
        if not role_h4:
            continue
        role = role_h4.get_text(strip=True).lower()
        if not any(r in role for r in _CONTACT_ROLES):
            continue

        # The very next sibling div holds the person entries
        if i + 1 >= len(children):
            continue
        person_div = children[i + 1]

        # Linked persons (GmbH): <a href="/person/..."><span class="break-words">Herr ...</span></a>
        for a in person_div.find_all("a", href=re.compile(r"/person/")):
            span = a.find("span", class_=re.compile(r"break-words"))
            name = _clean(span.get_text(strip=True) if span else a.get("title", ""))
            if name and name not in seen:
                names.append(name)
                seen.add(name)

        # Unlinked persons (e.U.): plain <span class="block break-words ...">Herr ...</span>
        if not any(names):
            for span in person_div.find_all("span", class_=re.compile(r"block")):
                name = _clean(span.get_text(strip=True))
                if name and name not in seen and re.search(r"\b(Herr|Frau)\b", name):
                    names.append(name)
                    seen.add(name)

    return "\n".join(names)


# ---------------------------------------------------------------------------
# Web search helpers
# ---------------------------------------------------------------------------

def _is_directory(url: str) -> bool:
    return any(d in url.lower() for d in DIRECTORY_DOMAINS)


def find_website(company: str, pause: float) -> str:
    """DuckDuckGo search for company's own website. Returns URL or ''."""
    if not HAS_DDG:
        logging.warning("duckduckgo-search not installed – skipping website search")
        return ""

    query = f'"{company}" Wien Installateur'
    attempt = 0
    while attempt < 3:
        try:
            time.sleep(pause * (2 ** attempt))
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=8))
            for r in results:
                url = r.get("href", "")
                if url and not _is_directory(url):
                    return url
            return ""
        except Exception as e:
            logging.debug("DDG search attempt %d failed for '%s': %s", attempt + 1, company, e)
            attempt += 1

    return ""


def find_firmenabc(company: str, pause: float) -> str:
    """Find firmenabc.at listing URL for this company."""
    if not HAS_DDG:
        return ""
    query = f'site:firmenabc.at {company}'
    try:
        time.sleep(pause)
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=5))
        for r in results:
            url = r.get("href", "")
            if "firmenabc.at" in url and "/firmen/" not in url:
                return url
    except Exception as e:
        logging.debug("FirmenABC search failed for '%s': %s", company, e)
    return ""


def google_maps_link(company: str, address: str = "") -> str:
    q = f"{company} {address}".strip() if address else f"{company} Wien"
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(q)}"


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def load_existing_keys(csv_path: str) -> Set[str]:
    keys: Set[str] = set()
    if not os.path.exists(csv_path):
        return keys
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                name = row.get("Unternehmen", "").strip()
                if name:
                    keys.add(_normalize_key(name))
    except Exception as e:
        logging.warning("Could not read existing CSV '%s': %s", csv_path, e)
    return keys


def write_lead(lead: Lead, csv_path: str, seen: Set[str]) -> bool:
    """Write a single lead immediately. Returns True if written, False if duplicate/skipped."""
    key = _normalize_key(lead.unternehmen)
    if not key or key in seen:
        return False
    is_new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
        if is_new_file:
            writer.writeheader()
        writer.writerow({
            "Unternehmen":      lead.unternehmen,
            "Website":          lead.website or "X",
            "TelNr":            lead.tel_nr,
            "Email":            lead.email,
            "Kontaktname":      lead.kontaktname,
            "Kontaktdatum":     lead.kontaktdatum,
            "Source":           lead.source,
            "Notes":            lead.notes,
            "Adresse":          lead.adresse,
            "Google_Maps_Link": lead.google_maps_link,
            "FirmenABC_Link":   lead.firmenABC_link,
        })
    seen.add(key)
    return True


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _parse_page_range(s: str):
    parts = s.split("-")
    if len(parts) == 2:
        return int(parts[0]), int(parts[1])
    return int(parts[0]), int(parts[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scrape herold.at for any category + location → CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python herold_scraper.py Installateur Wien\n"
               "  python herold_scraper.py Elektriker Graz --output elektrik.csv\n"
               "  python herold_scraper.py Installateur Wien --pages 1-5 --no-search",
    )
    parser.add_argument("category",       help="Business category, e.g. Installateur, Elektriker")
    parser.add_argument("location",       help="Location, e.g. Wien, Graz")
    parser.add_argument("--output",       default="",     help="Output CSV (default: {category}_{location}.csv)")
    parser.add_argument("--pages",        default="",     help="Limit page range, e.g. 1-5 (default: all pages)")
    parser.add_argument("--page-pause",   type=float, default=4.0,  help="Seconds between pages (default: 4)")
    parser.add_argument("--search-pause", type=float, default=2.5,  help="Base pause between searches (default: 2.5)")
    parser.add_argument("--no-search",    action="store_true",       help="Skip website/firmenabc web searches")
    parser.add_argument("--visible",      action="store_true",       help="Show browser window")
    parser.add_argument("--dump-html",    default="",                help="Dir to dump raw HTML (debug)")
    parser.add_argument("--verbose",      action="store_true",       help="Verbose logging")
    args = parser.parse_args()

    # Logging: route everything through tqdm so it doesn't break the progress bar.
    # Only show warnings+ normally; verbose enables debug.
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    handler = _TqdmHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logging.basicConfig(level=log_level, handlers=[handler], force=True)

    if not HAS_PLAYWRIGHT:
        tqdm.write("ERROR: playwright not installed.  pip install playwright && playwright install chromium")
        return

    # Auto-generate output filename if not given
    output = args.output or f"{_slugify(args.category)}_{_slugify(args.location)}.csv"

    seen = load_existing_keys(output)
    if seen:
        tqdm.write(f"Resuming – {len(seen)} existing entries in '{output}' will be skipped.")

    fetcher = HeroldFetcher(headless=not args.visible)
    total_new = 0

    try:
        # ── Fetch page 1 to detect total page count ──────────────────────────
        first_url = build_page_url(args.category, args.location, 1)
        tqdm.write(f"Detecting pages for: {args.category} in {args.location} …")
        first_html = fetcher.get(first_url, dump_dir=args.dump_html)
        total_pages = detect_total_pages(first_html)

        if args.pages:
            page_start, page_end = _parse_page_range(args.pages)
            page_end = min(page_end, total_pages)
        else:
            page_start, page_end = 1, total_pages

        tqdm.write(f"Found {total_pages} pages total → scraping {page_start}–{page_end} → '{output}'")

        pages = list(range(page_start, page_end + 1))

        with tqdm(total=0, unit="entry", ncols=90,
                  bar_format="p{desc}: {percentage:3.0f}% |{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}") as pbar:
            for page_num in pages:
                if page_num == 1 and first_html:
                    html = first_html
                    first_html = ""
                else:
                    html = ""
                    url = build_page_url(args.category, args.location, page_num)
                    for attempt in range(3):
                        try:
                            html = fetcher.get(url, dump_dir=args.dump_html)
                            break
                        except Exception as exc:
                            wait = args.page_pause * (2 ** attempt)
                            tqdm.write(f"WARN: page {page_num} attempt {attempt+1} failed ({exc}) – retrying in {wait:.0f}s")
                            time.sleep(wait)

                if not html:
                    tqdm.write(f"ERROR: skipping page {page_num} – could not fetch")
                    continue

                url = build_page_url(args.category, args.location, page_num)
                leads = parse_herold_page(html, url)
                if not leads:
                    tqdm.write(f"WARN: page {page_num} returned 0 listings")

                new_leads = [l for l in leads if l.unternehmen and _normalize_key(l.unternehmen) not in seen]
                pbar.reset(total=len(new_leads))
                pbar.set_description(f"{page_num}/{page_end}")

                for lead in new_leads:
                    pbar.set_postfix_str(lead.unternehmen[:35], refresh=True)

                    if not args.no_search:
                        if not lead.website:
                            lead.website = find_website(lead.unternehmen, args.search_pause)
                        if not lead.firmenABC_link:
                            lead.firmenABC_link = find_firmenabc(lead.unternehmen, args.search_pause)

                    if lead.firmenABC_link and not lead.kontaktname:
                        lead.kontaktname = fetch_firmenabc_contacts(lead.firmenABC_link, fetcher)
                        time.sleep(args.search_pause)

                    lead.google_maps_link = google_maps_link(lead.unternehmen, lead.adresse)

                    if write_lead(lead, output, seen):
                        total_new += 1
                    pbar.update(1)

                if page_num < page_end:
                    time.sleep(args.page_pause)

    finally:
        fetcher.close()

    tqdm.write(f"Done. {total_new} new entries written to '{output}'.")


if __name__ == "__main__":
    main()
