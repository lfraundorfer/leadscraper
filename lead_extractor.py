#!/usr/bin/env python3
"""Austrian local business lead extractor.

Usage example:
    python lead_extractor.py --category Elektriker --location Wien --max-results 200 --output leads.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional
from urllib.parse import quote, quote_plus

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import phonenumbers
except Exception:  # pragma: no cover - optional runtime dependency
    phonenumbers = None

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
except Exception:  # pragma: no cover - optional runtime dependency
    webdriver = None
    ChromeOptions = None
    FirefoxOptions = None


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


@dataclass
class Lead:
    company_name: str = ""
    inhaber: str = ""
    phone: str = ""
    website: str = ""
    employees: str = ""
    source: str = ""


class SeleniumRenderer:
    def __init__(self, browser: str = "chromium", wait_seconds: float = 8.0):
        self.browser = browser
        self.wait_seconds = wait_seconds
        self.driver = None
        self._start()

    def _start(self) -> None:
        if webdriver is None:
            logging.warning("Selenium is not installed. Falling back to HTTP mode.")
            return
        try:
            if self.browser == "firefox":
                options = FirefoxOptions()
                options.add_argument("-headless")
                self.driver = webdriver.Firefox(options=options)
            else:
                options = ChromeOptions()
                options.add_argument("--headless=new")
                options.add_argument("--disable-gpu")
                options.add_argument("--no-sandbox")
                options.add_argument("--window-size=1600,2600")
                self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(45)
        except Exception as exc:
            logging.warning("Could not start Selenium browser (%s). Falling back to HTTP mode.", exc)
            self.driver = None

    def get_page_source(self, url: str) -> str:
        if not self.driver:
            return ""
        try:
            self.driver.get(url)
            end = time.time() + self.wait_seconds
            while time.time() < end:
                ready_state = self.driver.execute_script("return document.readyState")
                if ready_state == "complete":
                    break
                time.sleep(0.2)

            # Trigger lazy-loaded listings.
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.0)
            return self.driver.page_source or ""
        except Exception as exc:
            logging.debug("Selenium failed for %s: %s", url, exc)
            return ""

    def close(self) -> None:
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass


class BaseScraper:
    source_name = ""

    def __init__(
        self,
        session: requests.Session,
        pause_seconds: float = 0.2,
        debug_dump_dir: str = "",
        renderer: Optional[SeleniumRenderer] = None,
    ):
        self.session = session
        self.pause_seconds = pause_seconds
        self.debug_dump_dir = debug_dump_dir
        self.renderer = renderer

    def fetch(self, category: str, location: str, max_results: int) -> List[Lead]:
        raise NotImplementedError

    def _get(self, url: str, headers: Optional[dict] = None) -> Optional[requests.Response]:
        try:
            resp = self.session.get(url, headers=headers, timeout=15)
            if resp.status_code >= 400:
                logging.debug("%s returned status %s", url, resp.status_code)
                return None
            time.sleep(self.pause_seconds)
            return resp
        except requests.RequestException as exc:
            logging.debug("Request failed for %s: %s", url, exc)
            return None

    def _get_html(self, url: str, headers: Optional[dict] = None) -> str:
        if self.renderer:
            html = self.renderer.get_page_source(url)
            if html:
                time.sleep(self.pause_seconds)
                return html
            return ""
        resp = self._get(url, headers=headers)
        return resp.text if resp else ""

    def _dump_html(self, html: str, reason: str) -> None:
        if not self.debug_dump_dir:
            return
        try:
            os.makedirs(self.debug_dump_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_source = re.sub(r"[^a-zA-Z0-9_-]", "_", self.source_name.lower())
            filename = f"{ts}_{safe_source}_{reason}.html"
            out_path = os.path.join(self.debug_dump_dir, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html)
            logging.info("Saved debug dump for %s: %s", self.source_name, out_path)
        except Exception as exc:
            logging.debug("Failed to write debug dump for %s: %s", self.source_name, exc)


class FirmenABCScraper(BaseScraper):
    source_name = "FirmenABC"

    def fetch(self, category: str, location: str, max_results: int) -> List[Lead]:
        query = f"{category} {location}"
        url = f"https://www.firmenabc.at/suche/?q={quote_plus(query)}"
        html = self._get_html(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        leads: List[Lead] = parse_json_ld_leads(soup, self.source_name, max_results)

        cards = soup.select("article, .search-result, .result-item, .company-card")
        if not cards:
            cards = soup.find_all(["li", "div"], class_=re.compile("result|company|entry", re.I))

        for card in cards:
            lead = parse_company_block(card, self.source_name)
            if lead.company_name:
                leads.append(lead)
            if len(leads) >= max_results:
                break
        if not leads:
            self._dump_html(html, "no_results")
        return leads


class HeroldScraper(BaseScraper):
    source_name = "Herold"

    def fetch(self, category: str, location: str, max_results: int) -> List[Lead]:
        # Warmup request helps pages that vary output by session/cookies.
        self._get_html("https://www.herold.at/")

        category_slug = slugify_path_segment(category)
        location_slug = slugify_path_segment(location)
        category_q = quote_plus(category)
        location_q = quote_plus(location)

        candidate_urls = [
            f"https://www.herold.at/gelbe-seiten/{location_slug}/?q={category_q}",
            f"https://www.herold.at/gelbe-seiten/{category_slug}/?where={location_q}",
            f"https://www.herold.at/gelbe-seiten/{category_slug}/{location_slug}/",
        ]

        browser_headers = {
            "Referer": "https://www.herold.at/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
        }

        all_leads: List[Lead] = []
        dumped = False
        for i, url in enumerate(candidate_urls):
            html = self._get_html(url, headers=browser_headers)
            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")
            leads: List[Lead] = parse_json_ld_leads(soup, self.source_name, max_results)

            cards = soup.select(
                "article, .company-card, .result-list-item, .result-item, [data-testid*='result']"
            )
            if not cards:
                cards = soup.find_all(["li", "div"], class_=re.compile("result|company|entry|listing", re.I))

            for card in cards:
                lead = parse_company_block(card, self.source_name)
                if lead.company_name:
                    leads.append(lead)
                if len(leads) >= max_results:
                    break

            if not leads and not dumped:
                self._dump_html(html, f"no_results_try_{i+1}")
                dumped = True

            all_leads.extend(leads)

        return dedupe_leads(all_leads)[:max_results]


class WKOScraper(BaseScraper):
    source_name = "WKO"

    def fetch(self, category: str, location: str, max_results: int) -> List[Lead]:
        query = f"{category} {location}"
        url = f"https://firmen.wko.at/SearchSimple.aspx?q={quote_plus(query)}"
        html = self._get_html(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        leads: List[Lead] = parse_json_ld_leads(soup, self.source_name, max_results)

        cards = soup.select("article, .search-result, .company, .result")
        if not cards:
            cards = soup.find_all(["li", "div"], class_=re.compile("result|company|entry", re.I))

        for card in cards:
            lead = parse_company_block(card, self.source_name)
            if lead.company_name:
                leads.append(lead)
            if len(leads) >= max_results:
                break
        if not leads:
            self._dump_html(html, "no_results")
        return leads


class GISAScraper(BaseScraper):
    source_name = "GISA"

    def fetch(self, category: str, location: str, max_results: int) -> List[Lead]:
        # Public entry page; exact search endpoints change regularly, so we keep this scraper tolerant.
        query = f"{category} {location}"
        url = f"https://www.gisa.gv.at/abfrage?suchbegriff={quote_plus(query)}"
        html = self._get_html(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        leads: List[Lead] = parse_json_ld_leads(soup, self.source_name, max_results)

        cards = soup.select("article, .result, .search-result, .entry")
        if not cards:
            cards = soup.find_all(["li", "div"], class_=re.compile("result|company|entry", re.I))

        for card in cards:
            lead = parse_company_block(card, self.source_name)
            if lead.company_name:
                leads.append(lead)
            if len(leads) >= max_results:
                break
        if not leads:
            self._dump_html(html, "no_results")
        return leads


def clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def clean_company_name(name: str) -> str:
    name = clean(name)
    if not name:
        return ""

    name = re.sub(r"^(?:Verifiziert|Nicht verifiziert)\s+", "", name, flags=re.I)
    name = re.sub(r"\b\d(?:[.,]\d)?\s*\(\d+\)", "", name)
    name = re.split(
        r"\b(?:Mehr Details|Kontakt:|Termin buchen|Jetzt geöffnet|Jetzt geö|Mehr Detail)\b",
        name,
        maxsplit=1,
        flags=re.I,
    )[0]
    name = clean(name.strip(" -|,;"))
    return name


def is_noise_company_name(name: str) -> bool:
    value = clean(name).lower()
    if not value:
        return True

    blocked_fragments = [
        "top branchen",
        "branchen a-z",
        "telefonbuch",
        "kostenlosen firmeneintrag",
        "online buchung",
        "kategorien",
        "für unternehmer",
        "firma suchen",
        "mein firmeneintrag",
        "personen suchen",
        "häufige fragen",
        "datenquellen",
        "kundenservice",
        "gratis-check",
        "angebot sichern",
        "verwandte branchen",
        "bundesland",
        "kooperationsbörse",
        "nachfolgebörse",
    ]
    return any(fragment in value for fragment in blocked_fragments)


def is_plausible_lead(lead: Lead) -> bool:
    lead.company_name = clean_company_name(lead.company_name)
    lead.inhaber = clean(lead.inhaber)
    lead.phone = normalize_phone(lead.phone)
    lead.website = normalize_url(lead.website)
    lead.employees = clean(lead.employees)

    if not lead.company_name or is_noise_company_name(lead.company_name):
        return False
    if not (lead.phone or lead.website):
        return False

    if lead.website:
        blocked_domains = ("herold.at", "firmenabc.at", "wko.at", "gisa.gv.at")
        if any(domain in lead.website.lower() for domain in blocked_domains):
            return False
    return True


def filter_leads(leads: Iterable[Lead]) -> List[Lead]:
    result: List[Lead] = []
    for lead in leads:
        if is_plausible_lead(lead):
            result.append(lead)
    return result


def slugify_path_segment(value: str) -> str:
    text = clean(value).lower()
    text = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return quote(text, safe="-")


def normalize_phone(phone: str, default_region: str = "AT") -> str:
    phone = clean(phone)
    if not phone:
        return ""

    if phonenumbers:
        try:
            parsed = phonenumbers.parse(phone, default_region)
            if phonenumbers.is_possible_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            pass

    # Fallback for Austrian numbers.
    digits = re.sub(r"[^\d+]", "", phone)
    if digits.startswith("00"):
        return "+" + digits[2:]
    if digits.startswith("+"):
        return digits
    if digits.startswith("0"):
        return "+43" + digits[1:]
    return digits


def normalize_url(url: str) -> str:
    url = clean(url)
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", url):
        return "https://" + url
    return url


def parse_json_ld_leads(soup: BeautifulSoup, source_name: str, max_results: int) -> List[Lead]:
    leads: List[Lead] = []
    scripts = soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)})
    for script in scripts:
        if len(leads) >= max_results:
            break

        raw = script.string or script.get_text("", strip=True)
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        stack = [data]
        while stack and len(leads) < max_results:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue

            for value in item.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)

            schema_type = item.get("@type")
            types: List[str] = []
            if isinstance(schema_type, str):
                types = [schema_type]
            elif isinstance(schema_type, list):
                types = [t for t in schema_type if isinstance(t, str)]

            if not any(t.lower() in {"organization", "localbusiness"} for t in types):
                continue

            company_name = clean_company_name(item.get("name", ""))
            if not company_name:
                continue

            phone = normalize_phone(clean(item.get("telephone", "")))
            website = normalize_url(clean(item.get("url", "")))

            inhaber = ""
            founder = item.get("founder")
            if isinstance(founder, dict):
                inhaber = clean(founder.get("name", ""))
            elif isinstance(founder, str):
                inhaber = clean(founder)

            employees = ""
            employee_value = item.get("numberOfEmployees")
            if isinstance(employee_value, dict):
                employees = clean(str(employee_value.get("value", "")))
            elif employee_value is not None:
                employees = clean(str(employee_value))

            leads.append(
                Lead(
                    company_name=company_name,
                    inhaber=inhaber,
                    phone=phone,
                    website=website,
                    employees=employees,
                    source=source_name,
                )
            )

    return leads


def parse_company_block(node, source_name: str) -> Lead:
    text = clean(node.get_text(" ", strip=True))

    name = ""
    name_node = node.find(["h1", "h2", "h3", "h4", "a", "strong"], string=True)
    if name_node:
        name = clean_company_name(name_node.get_text(" ", strip=True))

    if not name:
        # Fall back to first sentence-like token, avoid random small labels.
        candidate = text.split("  ")[0] if "  " in text else text
        name = clean_company_name(candidate[:120])

    phone = ""
    tel_link = node.find("a", href=re.compile(r"^tel:", re.I))
    if tel_link:
        phone = clean(tel_link.get("href", "").replace("tel:", ""))
    else:
        phone_match = re.search(r"(?:\+43|0043|0)[\d\s\-/()]{6,}", text)
        if phone_match:
            phone = clean(phone_match.group(0))

    website = ""
    web_link = node.find("a", href=re.compile(r"https?://", re.I))
    if web_link:
        href = web_link.get("href", "")
        if "google" not in href and "herold.at" not in href and "firmenabc.at" not in href and "wko.at" not in href:
            website = href

    inhaber = ""
    owner_match = re.search(
        r"(?:Inhaber|Inhaberin|Gesch[aä]ftsf[üu]hrer(?:in)?|Owner)\s*:?\s*([A-ZÄÖÜ][^,;|]{2,80})",
        text,
        re.I,
    )
    if owner_match:
        inhaber = clean(owner_match.group(1))

    employees = ""
    employees_match = re.search(
        r"(?:Mitarbeiter(?:zahl)?|Employees?)\s*:?\s*(\d{1,5})",
        text,
        re.I,
    )
    if employees_match:
        employees = clean(employees_match.group(1))

    return Lead(
        company_name=name,
        inhaber=inhaber,
        phone=normalize_phone(phone),
        website=normalize_url(website),
        employees=employees,
        source=source_name,
    )


def dedupe_leads(leads: Iterable[Lead]) -> List[Lead]:
    chosen = {}

    for lead in leads:
        name_key = re.sub(r"[^a-z0-9]", "", lead.company_name.lower())
        web_key = re.sub(r"^https?://", "", (lead.website or "").lower()).strip("/")
        phone_key = re.sub(r"\D", "", lead.phone)

        dedupe_key = "|".join([name_key, web_key, phone_key])
        if not name_key:
            continue

        existing = chosen.get(dedupe_key)
        if not existing:
            chosen[dedupe_key] = lead
            continue

        # Keep the richer record.
        existing_score = sum(1 for v in [existing.inhaber, existing.phone, existing.website, existing.employees] if v)
        new_score = sum(1 for v in [lead.inhaber, lead.phone, lead.website, lead.employees] if v)
        if new_score > existing_score:
            chosen[dedupe_key] = lead

    return list(chosen.values())


def relevance_score(lead: Lead, category: str, location: str) -> int:
    score = 0
    hay = f"{lead.company_name} {lead.source}".lower()
    if category.lower() in hay:
        score += 2
    if location.lower() in hay:
        score += 1
    if lead.phone:
        score += 1
    if lead.website:
        score += 1
    return score


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)

    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "de-AT,de;q=0.9,en;q=0.8"})
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def export_csv(leads: Iterable[Lead], output_file: str) -> None:
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Company Name", "Inhaber", "Phone", "Website", "Employees"])
        for lead in leads:
            writer.writerow([
                lead.company_name,
                lead.inhaber,
                lead.phone,
                lead.website,
                lead.employees,
            ])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Austrian local business leads and export CSV.")
    parser.add_argument("--category", required=True, help="Business category, e.g. Elektriker")
    parser.add_argument("--location", required=True, help="Location, e.g. Wien or postal code")
    parser.add_argument("--max-results", type=int, default=100, help="Maximum number of rows in final CSV")
    parser.add_argument("--output", default="leads.csv", help="Output CSV path")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs")
    parser.add_argument(
        "--debug-dump-dir",
        default="",
        help="Optional directory to save source HTML when a source returns 0 results",
    )
    parser.add_argument("--selenium", action="store_true", help="Use Selenium browser rendering for AJAX pages")
    parser.add_argument(
        "--selenium-browser",
        choices=["chromium", "firefox"],
        default="chromium",
        help="Browser engine for Selenium mode",
    )
    parser.add_argument(
        "--selenium-wait-seconds",
        type=float,
        default=8.0,
        help="Max wait after page load in Selenium mode",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    session = build_session()
    renderer = None
    if args.selenium:
        renderer = SeleniumRenderer(
            browser=args.selenium_browser,
            wait_seconds=args.selenium_wait_seconds,
        )

    try:
        scrapers: List[BaseScraper] = [
            FirmenABCScraper(session, debug_dump_dir=args.debug_dump_dir, renderer=renderer),
            GISAScraper(session, debug_dump_dir=args.debug_dump_dir, renderer=renderer),
            HeroldScraper(session, debug_dump_dir=args.debug_dump_dir, renderer=renderer),
            WKOScraper(session, debug_dump_dir=args.debug_dump_dir, renderer=renderer),
        ]

        all_leads: List[Lead] = []
        per_source_limit = max(20, args.max_results)

        for scraper in scrapers:
            logging.info("Fetching leads from %s ...", scraper.source_name)
            leads = scraper.fetch(args.category, args.location, per_source_limit)
            logging.info("%s: %d raw matches", scraper.source_name, len(leads))
            all_leads.extend(leads)

        filtered = filter_leads(all_leads)
        logging.info("After quality filters: %d leads", len(filtered))
        deduped = dedupe_leads(filtered)
        deduped.sort(key=lambda x: relevance_score(x, args.category, args.location), reverse=True)
        final_rows = deduped[: args.max_results]

        export_csv(final_rows, args.output)
        logging.info("Wrote %d leads to %s", len(final_rows), args.output)
    finally:
        if renderer:
            renderer.close()


if __name__ == "__main__":
    main()
