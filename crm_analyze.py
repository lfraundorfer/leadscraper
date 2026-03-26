"""
crm_analyze.py – OpenAI API: website analysis + personalized message generation.

For each lead:
  1. Fetches website HTML (if real website)
  2. Calls GPT-4o to score website and identify pain points
  3. Calls GPT-4o to generate email, WhatsApp, and phone script
  4. Assigns channel + priority
  5. Updates CSV with all results
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, timedelta

from openai import OpenAI

from herold_scraper import HeroldFetcher
from crm_store import load_leads, save_leads, get_bezirk, is_mobile, WIEN_BEZIRK
from crm_research import (
    categorize_website, fetch_and_clean_html,
    ALL_DIRECTORY_DOMAINS, RATE_LIMIT_SEC
)
from crm_templates import render_drafts, pick_template_key, choose_hook

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODEL = "gpt-4o-mini"
SENDER_NAME = os.getenv("SENDER_NAME", "Linus")
SENDER_PHONE = os.getenv("SENDER_PHONE", "")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")

NO_WEBSITE_ANALYSIS = {
    "score": 0,
    "score_reason": "Kein eigener Webauftritt vorhanden",
    "pain_points": [
        "Kein eigener Webauftritt – der Betrieb ist online unsichtbar",
        "Keine Möglichkeit für Kunden, online Kontakt aufzunehmen",
        "Konkurrenten mit Website gewinnen diese Kunden automatisch",
    ],
    "pain_categories": ["no_website"],
    "strengths": [],
    "best_hook": "Ihre Konkurrenten haben alle Websites – Kunden, die Sie suchen, finden nur diese",
    "urgency_angle": "Jeder Tag ohne Website ist ein verlorener Auftrag",
}

# ---------------------------------------------------------------------------
# Channel selection
# ---------------------------------------------------------------------------

def select_channel_and_priority(lead: dict, analysis: dict) -> tuple[str, str, int]:
    """
    Returns (primary_channel, next_action_type, priority 1-5).
    """
    email = lead.get("Email", "").strip()
    tel = lead.get("TelNr", "").strip()
    has_mobile = bool(tel) and is_mobile(tel)
    has_email = bool(email)
    has_phone = bool(tel)

    score = analysis.get("score", 5)
    website_cat = lead.get("Website_Category", "")

    # Determine primary channel
    if not has_email and not has_phone:
        return "none", "none", 5

    if website_cat in ("none", "") or score == 0:
        # No website → phone first (strongest pitch)
        primary = "phone" if has_phone else ("whatsapp" if has_mobile else "email" if has_email else "none")
    else:
        primary = "email" if has_email else ("whatsapp" if has_mobile else "phone" if has_phone else "none")

    # Calculate priority
    base = 3
    if score == 0 or website_cat == "none":
        base = 1
    elif score <= 3:
        base = 1
    elif score <= 5:
        base = 2
    elif score <= 7:
        base = 3
    else:
        base = 4

    review_count_str = lead.get("Google_Review_Count", "")
    try:
        review_count = int(review_count_str)
    except (ValueError, TypeError):
        review_count = 99

    adjustments = 0.0
    if lead.get("Kontaktname", "").strip():
        adjustments -= 0.5
    if review_count < 5:
        adjustments -= 0.5
    if lead.get("Google_Rank_Position", "") == "not_found":
        adjustments -= 0.5
    if not has_email and has_phone:
        adjustments += 1.0

    priority = max(1, min(5, round(base + adjustments)))

    return primary, primary, priority


# ---------------------------------------------------------------------------
# Claude: Website analysis
# ---------------------------------------------------------------------------

WEBSITE_ANALYSIS_PROMPT = """\
Du bist ein Webdesign-Experte für österreichische KMU (Handwerksbetriebe).
Analysiere die folgende Website eines Wiener Installateurbetriebs.
Antworte AUSSCHLIESSLICH in diesem JSON-Format – kein Markdown, kein Text davor/danach:

{{
  "score": <Integer 1-10>,
  "score_reason": "<Ein präziser Satz warum dieser Score>",
  "pain_points": ["<Konkreter Mangel auf Deutsch, z.B. 'Keine Mobiloptimierung'>", ...],
  "pain_categories": ["<Kategorie-Keys aus der Liste unten>"],
  "strengths": ["<Stärke falls vorhanden, sonst leere Liste>"],
  "best_hook": "<Der überzeugendste Einzelpunkt für das Verkaufsgespräch – maximal 1 Satz>",
  "urgency_angle": "<Warum sollte der Betrieb JETZT handeln? – 1 Satz>"
}}

BEWERTUNGSKRITERIEN & KATEGORIE-KEYS:
1. "kein_ssl"         – URL beginnt mit http:// statt https://
2. "kein_mobil"       – Kein <meta name="viewport">, nicht responsive, bricht auf kleinen Screens
3. "kein_kontakt"     – Kein tel:-Link, kein wa.me-Link, kein Kontaktformular sichtbar
4. "veraltet"         – Copyright-Jahr > 5 Jahre alt, veraltetes Design, alte Inhalte
5. "platzhalter"      – "Coming soon", "demnächst", "under construction", Wartungsseite
6. "kein_seo"         – Fehlende <meta description>, kein H1, generischer Seitenname, keine Keywords
7. "keine_bewertungen"– Kein eingebettetes Review-Widget (Google, ProvenExpert etc.)
8. "kein_design"      – Tabellenbasiertes Layout, extrem altes Design, kaum Inhalt

Mehrere Kategorien möglich. Maximal die 3 wichtigsten als pain_categories.

SCORE-SKALA:
1–3: Sehr schlecht (kein SSL, kein Mobile, kaum Inhalt, veraltet)
4–5: Mangelhaft (grundlegende Funktionen fehlen)
6–7: Durchschnittlich (funktionsfähig aber verbesserungswürdig)
8–9: Gut (modern, mobil, konversionsorientiert)
10:  Ausgezeichnet

Sei kritisch und realistisch. Die meisten Handwerkerbetriebe liegen bei 3–6.

UNTERNEHMEN: {company}
URL: {url}
WEBSITE-INHALT (bereinigter Text):
---
{html}
---"""


def analyze_website(company: str, url: str, html: str, client: OpenAI) -> dict:
    """Call GPT-4o to score the website. Returns analysis dict."""
    prompt = WEBSITE_ANALYSIS_PROMPT.format(company=company, url=url, html=html[:12_000])
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            max_tokens=600,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        return {**NO_WEBSITE_ANALYSIS, "score_reason": f"Analyse-Fehler: {e}"}


# ---------------------------------------------------------------------------
# GPT: Hook-only generation (token-saving mode)
# ---------------------------------------------------------------------------

# Human-readable description for each pain category key
CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "no_website":        "Kein eigener Webauftritt – online komplett unsichtbar",
    "platzhalter":       "Platzhalter-/Under-Construction-Seite – Kunden sehen nichts",
    "kein_mobil":        "Nicht mobiloptimiert – 65%+ aller Suchen kommen vom Smartphone",
    "kein_kontakt":      "Kein Kontaktweg online (kein Formular, kein Click-to-Call, kein WhatsApp)",
    "bad_reviews":       "Schlechte oder sehr wenige Google-Bewertungen – kostet Vertrauen",
    "not_ranked":        "Nicht auf Google Seite 1 – Konkurrenten nehmen diese Kunden",
    "kein_seo":          "Keine SEO-Optimierung – Google ignoriert diese Seite",
    "veraltet":          "Veraltete Website (Design/Inhalt) – wirkt unprofessionell",
    "kein_ssl":          "Kein HTTPS – Browser zeigt 'Nicht sicher'-Warnung",
    "kein_design":       "Sehr veraltetes Design – schreckt potenzielle Kunden ab",
    "keine_bewertungen": "Kein Bewertungs-Widget – Vertrauen fehlt sichtbar",
}

HOOK_PROMPT = """\
Du bist ein erfahrener Verkaufstexter in Wien.
Schreibe einen personalisierten, knackigen Hook für einen Installateurbetrieb.
Antworte AUSSCHLIESSLICH als JSON – kein Markdown, kein Text davor/danach:

{{
  "hook": "<2-3 Sätze: nenne die 1-2 stärksten gefundenen Probleme UND verbinde sie mit dem passenden Kernargument>",
  "urgency": "<1 Satz: warum jetzt handeln>"
}}

KERNARGUMENTE (in dieser Priorität – wähle das stärkste das passt):
1. MEHR KUNDEN – Der Betrieb verliert täglich Kunden die online suchen und zur Konkurrenz gehen
2. BESSERE BEWERTUNGEN – Schlechte oder wenige Bewertungen kosten Vertrauen und Aufträge
3. KONKURRENZ SCHLAGEN – Mitbewerber ranken höher auf Google und nehmen diese Kunden weg

GEFUNDENE PROBLEME (alle, wichtigste zuerst):
{pain_categories_explained}

ECHTE DATEN:
- Betrieb: {company}
- Website-Score: {score}/10
- Google-Bewertungen: {rating}★ ({review_count} Rezensionen)
- Google-Rank für "{rank_keyword}": {rank_position}
- Sichtbare Konkurrenten: {competitors}

REGELN:
- Konkret und spezifisch – keine generischen Phrasen
- Echte Zahlen/Beobachtungen einbauen wo möglich
- Spezifischer Konkurrent namentlich erwähnen wenn vorhanden
- Maximal 3 Sätze für den Hook, Sie-Form
- IMMER direkte Anrede: "Sie verlieren", "Ihr Betrieb", "Ihre Website" – niemals dritte Person ("Firma X verliert")"""


def generate_hook(lead: dict, analysis: dict, client: OpenAI) -> tuple[str, str]:
    """
    Generate a 2-3 sentence hook + urgency line via GPT.
    Uses all pain categories, framed around the 3 core outcomes.
    Returns (hook, urgency).
    """
    adresse = lead.get("Adresse", "")
    plz, _ = get_bezirk(adresse)
    rank_kw = lead.get("Google_Rank_Keyword") or f"Installateur {plz or 'Wien'}"

    # Build explained pain categories list (all of them, in priority order)
    pain_categories = analysis.get("pain_categories", [])
    if not pain_categories:
        # Fall back to pain_points text
        pain_points = analysis.get("pain_points", [])
        pain_categories_explained = "\n".join(f"- {p}" for p in pain_points) or "- Allgemeiner Verbesserungsbedarf"
    else:
        pain_categories_explained = "\n".join(
            f"- {CATEGORY_DESCRIPTIONS.get(c, c)}" for c in pain_categories
        )

    prompt = HOOK_PROMPT.format(
        company=lead.get("Unternehmen", ""),
        score=analysis.get("score", "?"),
        pain_categories_explained=pain_categories_explained,
        rating=lead.get("Google_Rating", "keine Angabe") or "keine Angabe",
        review_count=lead.get("Google_Review_Count", "0") or "0",
        rank_position=lead.get("Google_Rank_Position", "nicht gefunden") or "nicht gefunden",
        rank_keyword=rank_kw,
        competitors=lead.get("Google_Competitors", "keine erfasst") or "keine erfasst",
    )

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                max_tokens=200,
                temperature=0.7,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            data = json.loads(raw)
            return data.get("hook", ""), data.get("urgency", "")
        except Exception:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    return "", "Jetzt ist der richtige Zeitpunkt."


# ---------------------------------------------------------------------------
# Claude: Message generation (full, legacy mode)
# ---------------------------------------------------------------------------

MESSAGES_PROMPT = """\
Du bist ein freiberuflicher Webdesigner aus Wien (Name: {sender_name}) und erstellst personalisierte Akquise-Materialien für einen konkreten Installateurbetrieb.

UNTERNEHMEN: {company}
KONTAKT: {contact}
ADRESSE: {address}
PLZ/BEZIRK: {plz} – {bezirk}

WEBSITE: {website}
WEBSITE-SCORE: {score}/10
HAUPTPROBLEME: {pain_points}
BEST HOOK: {best_hook}
DRINGLICHKEIT: {urgency}

GOOGLE-BEWERTUNGEN: {rating} Sterne | {review_count} Bewertungen
BEWERTUNGS-SNIPPETS: {snippets}

GOOGLE-SUCHE "{rank_keyword}":
  Position: {rank_position}
  Im Local 3-Pack (Karte): {map_pack}
  Sichtbare Konkurrenten: {competitors}

Generiere drei Outreach-Materialien auf Deutsch.
Antworte AUSSCHLIESSLICH in diesem JSON-Format (kein Markdown, kein Text davor/danach):

{{
  "email": {{
    "subject": "<Betreff – konkret, max 60 Zeichen, nicht generisch>",
    "body": "<E-Mail-Body 150-200 Wörter>"
  }},
  "whatsapp": "<WhatsApp-Nachricht 60-80 Wörter>",
  "phone_script": "<Gesprächsskript – strukturiert wie unten beschrieben>"
}}

--- EMAIL (150-200 Wörter, formell, Sie-Form) ---
• Keine generische Begrüßung – direkt mit einem konkreten Befund einsteigen
• Konkurrenten NAMENTLICH erwähnen wenn der Betrieb nicht gefunden wird:
  "Während [Konkurrent] bei der Suche nach '{rank_keyword}' ganz oben steht, erscheinen Sie nicht."
• 2–3 konkrete Schwächen aus den Daten nennen (Website, Reviews, Ranking)
• Wirtschaftlicher Schaden: Was verlieren sie dadurch konkret? (Aufträge, Vertrauen)
• Soft CTA: "Darf ich Ihnen kurz zeigen...?" oder "Kurzgespräch nächste Woche?"
• Signatur: {sender_name} | Webdesign Wien{sender_phone_line}{sender_email_line}

--- WHATSAPP (60-80 Wörter, direkter Einstieg, respektvoll) ---
• Keine formale Anrede ("Sehr geehrte...") – stattdessen: Name oder "Guten Tag"
• Kein Link in der ersten Nachricht
• Endet mit einer einfachen Ja/Nein-Frage oder einem Terminangebot

--- PHONE_SCRIPT ---
OPENING (10-15 Sek): [Einstieg + Vorstellung + Erlaubnis]
HOOK (20-30 Sek): [Das KONKRETE Problem das ihnen Kunden kostet]
PUNKTE:
1. [Problem was passiert gerade?]
2. [Was biete ich an?]
3. [Was ändert sich konkret?]
EINWÄNDE:
"Kein Interesse" → [Antwort max 2 Sätze]
"Haben schon eine Website" → [Antwort max 2 Sätze]
"Zu teuer" → [Antwort max 2 Sätze]
"Keine Zeit" → [Antwort max 2 Sätze]
CTA: [Terminvorschlag mit 2 konkreten Optionen]"""


def generate_messages(lead: dict, analysis: dict, client: OpenAI) -> dict:
    """Call Claude to generate email, WhatsApp, and phone script. Returns dict."""
    contact = lead.get("Kontaktname", "").strip() or "Sehr geehrte Damen und Herren"
    adresse = lead.get("Adresse", "")
    plz, bezirk = get_bezirk(adresse)

    pain_points_raw = analysis.get("pain_points", [])
    pain_points_str = " | ".join(pain_points_raw) if pain_points_raw else "Keine spezifischen Mängel identifiziert"

    competitors = lead.get("Google_Competitors", "")
    rank_pos = lead.get("Google_Rank_Position", "")
    rank_kw = lead.get("Google_Rank_Keyword", f"Installateur {plz}")

    phone_line = f"\n  Tel: {SENDER_PHONE}" if SENDER_PHONE else ""
    email_line = f"\n  {SENDER_EMAIL}" if SENDER_EMAIL else ""

    prompt = MESSAGES_PROMPT.format(
        sender_name=SENDER_NAME,
        company=lead.get("Unternehmen", ""),
        contact=contact,
        address=adresse,
        plz=plz or "Wien",
        bezirk=bezirk or "Wien",
        website=lead.get("Website", "keine Website"),
        score=analysis.get("score", "?"),
        pain_points=pain_points_str,
        best_hook=analysis.get("best_hook", ""),
        urgency=analysis.get("urgency_angle", ""),
        rating=lead.get("Google_Rating", "?") or "keine Angabe",
        review_count=lead.get("Google_Review_Count", "?") or "0",
        snippets=lead.get("Google_Review_Snippets", "") or "keine",
        rank_keyword=rank_kw,
        rank_position=rank_pos or "nicht gefunden",
        map_pack=lead.get("Google_Map_Pack", "?") or "nein",
        competitors=competitors or "keine erfasst",
        sender_phone_line=phone_line,
        sender_email_line=email_line,
    )

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                max_tokens=2000,
                temperature=0.7,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            data = json.loads(raw)
            return {
                "Email_Draft": f"Betreff: {data['email']['subject']}\n\n{data['email']['body']}",
                "WhatsApp_Draft": data.get("whatsapp", ""),
                "Phone_Script": data.get("phone_script", ""),
            }
        except Exception as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                return {
                    "Email_Draft": f"[Generierungsfehler: {e}]",
                    "WhatsApp_Draft": "",
                    "Phone_Script": "",
                }
    return {"Email_Draft": "", "WhatsApp_Draft": "", "Phone_Script": ""}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(force: bool = False, single_id: str = "", no_review: bool = False, limit: int = 0, gpt_hooks: bool = False) -> None:
    """
    CLI entry for `python crm.py analyze`.
    Fetches website HTML, analyzes with Claude, generates messages.
    """
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in environment / .env file")
        return

    client = OpenAI(api_key=api_key)
    leads = load_leads()

    if not leads:
        print("No leads found. Run `python crm.py migrate` first.")
        return

    if single_id:
        targets = [l for l in leads if l.get("ID", "").strip() == single_id]
        if not targets:
            print(f"Lead {single_id} not found.")
            return
    else:
        targets = [l for l in leads if force or not l.get("Analyzed_At")]
        if limit:
            targets = targets[:limit]

    print(f"Analyzing {len(targets)} lead(s) with Claude…")
    fetcher = HeroldFetcher(headless=True)
    done = 0

    try:
        for lead in targets:
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")
            website = lead.get("Website", "").strip()
            print(f"\n  {lid} {company[:45]}")

            # 1. Determine website category
            category = lead.get("Website_Category") or categorize_website(website)
            lead["Website_Category"] = category

            # 2. Website analysis
            if category == "real":
                print(f"       fetching {website[:50]}…", end=" ", flush=True)
                html = fetch_and_clean_html(website, fetcher)
                if html:
                    analysis = analyze_website(company, website, html, client)
                    print(f"score={analysis.get('score', '?')}/10")
                else:
                    lead["Website_Category"] = "fetch_error"
                    analysis = NO_WEBSITE_ANALYSIS
                    print("fetch error")
                time.sleep(RATE_LIMIT_SEC)
            else:
                analysis = NO_WEBSITE_ANALYSIS
                print(f"       website={category}, using default analysis")

            # Store analysis results
            lead["Website_Score"] = str(analysis.get("score", 0))
            pain_points = analysis.get("pain_points", [])
            lead["Pain_Points"] = " | ".join(pain_points)
            pain_categories = analysis.get("pain_categories", [])
            lead["Pain_Categories"] = " | ".join(pain_categories)

            # 3. Channel selection & priority
            primary, next_type, priority = select_channel_and_priority(lead, analysis)
            lead["Channel_Used"] = primary
            lead["Priority"] = str(priority)

            if not lead.get("Next_Action_Date") and primary != "none":
                lead["Next_Action_Date"] = date.today().isoformat()
                lead["Next_Action_Type"] = next_type

            # 4. Generate messages (local templates + local hooks by default)
            print(f"       building draft…", end=" ", flush=True)
            template_key = pick_template_key(pain_categories, lead)
            if gpt_hooks:
                hook, urgency = generate_hook(lead, analysis, client)
            else:
                hook = choose_hook(template_key, lead)
                urgency = ""
            messages = render_drafts(lead, hook, urgency, template_key=template_key)
            lead.update(messages)
            print(f"done (template: {messages.get('Template_Used', '?')})")

            # 5. Update status
            lead["Analyzed_At"] = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M")
            if no_review:
                lead["Status"] = "approved"
                lead["Drafts_Approved"] = "1"
            else:
                if lead.get("Status", "new") == "new":
                    lead["Status"] = "draft_ready"

            save_leads(leads)
            done += 1

            # Brief pause between leads to be polite to Claude API
            time.sleep(1)

    finally:
        fetcher.close()

    print(f"\nDone. Analyzed {done} lead(s). Status set to {'approved' if no_review else 'draft_ready'}.")
    if not no_review:
        print("Run `streamlit run app.py` to review and approve drafts.")
