"""
crm_templates.py – Pre-written outreach templates per pain point category.

Drafts use local templates and a local hook library by default.
GPT hooks remain optional, but the normal path is template-only.

Slot syntax: {{slot_name}}  (double braces — replaced by fill_template())
ALL slots in templates must use {{key}} — no single-brace {key} format.

Available slots:
  {{hook}}             – GPT-generated 2-3 sentence specific hook
  {{urgency}}          – GPT-generated 1 sentence urgency
  {{company}}          – Company name (nur wenn ein direkter Konkurrenzvergleich nötig ist)
  {{salutation}}       – "Guten Tag Herr Mustermann," or "Sehr geehrte Damen und Herren,"
  {{contact}}          – Direct contact label for phone/email use
  {{subject_intro}}    – "Herr Mustermann, " or "" as a direct subject prefix
  {{price}}            – Price from CSV, else active campaign default
  {{sender_name}}      – Sender name from active campaign config
  {{sender_company}}   – Sender company from active campaign config
  {{sender_company_signature}} – Sender company line for signatures (deduplicated)
  {{sender_company_phone}} – Optional " von ..." suffix for phone scripts
  {{sender_website}}   – Sender website from active campaign config
  {{sender_phone}}     – Sender phone from active campaign config
  {{sender_email}}     – Sender email from active campaign config
  {{competitors_line}} – " Mustermann Haustechnik, WienInstall GmbH" or " Ihre Mitbewerber"
  {{competitors_short}}– First competitor name
  {{rank_keyword}}     – "Installateur 1140"
  {{rank_keyword_district}} – "1140" or "Wien"
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re

import crm_backend as backend

_LEGACY_HOOKS_LIBRARY_PATH = Path(__file__).parent / "hooks_library.json"
_LEGACY_TEMPLATE_OVERRIDES_PATH = Path(__file__).parent / "template_overrides.json"
_hooks_override_cache: dict[str, dict[str, list[str]]] = {}
_template_override_cache: dict[str, dict] = {}

_PROTECTED_REFRESH_STATUSES = {
    "approved",
    "contacted",
    "replied",
    "meeting_scheduled",
    "won",
    "lost",
    "no_contact",
    "blacklist",
}


def _resolve_campaign(campaign: dict | None = None) -> dict | None:
    if campaign is not None:
        return campaign
    try:
        from campaign_service import get_active_campaign
        return get_active_campaign()
    except Exception:
        return None


def _resolve_hooks_library_path(campaign: dict | None = None) -> Path:
    active_campaign = _resolve_campaign(campaign)
    if active_campaign is not None:
        try:
            from campaign_service import get_hooks_library_path
            return Path(get_hooks_library_path(active_campaign))
        except Exception:
            pass
    return _LEGACY_HOOKS_LIBRARY_PATH


def _resolve_template_overrides_path(campaign: dict | None = None) -> Path:
    active_campaign = _resolve_campaign(campaign)
    if active_campaign is not None:
        try:
            from campaign_service import get_template_overrides_path
            return Path(get_template_overrides_path(active_campaign))
        except Exception:
            pass
    return _LEGACY_TEMPLATE_OVERRIDES_PATH


def _load_hooks_override(campaign: dict | None = None) -> dict[str, list[str]]:
    active_campaign = _resolve_campaign(campaign)
    if backend.is_postgres_backend() and active_campaign is not None:
        cache_key = f"campaign:{active_campaign.get('id', '')}:hooks"
        if cache_key not in _hooks_override_cache:
            payload = active_campaign.get("hooks_library_json")
            _hooks_override_cache[cache_key] = payload if isinstance(payload, dict) else {}
        return _hooks_override_cache[cache_key]

    path = _resolve_hooks_library_path(campaign)
    cache_key = str(path)
    if cache_key not in _hooks_override_cache:
        if path.exists():
            try:
                _hooks_override_cache[cache_key] = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                _hooks_override_cache[cache_key] = {}
        else:
            _hooks_override_cache[cache_key] = {}
    return _hooks_override_cache[cache_key]


def _load_template_overrides(campaign: dict | None = None) -> dict:
    active_campaign = _resolve_campaign(campaign)
    if backend.is_postgres_backend() and active_campaign is not None:
        cache_key = f"campaign:{active_campaign.get('id', '')}:template_overrides"
        if cache_key not in _template_override_cache:
            payload = active_campaign.get("template_overrides_json")
            _template_override_cache[cache_key] = payload if isinstance(payload, dict) else {}
        return _template_override_cache[cache_key]

    path = _resolve_template_overrides_path(campaign)
    cache_key = str(path)
    if cache_key not in _template_override_cache:
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                _template_override_cache[cache_key] = payload if isinstance(payload, dict) else {}
            except Exception:
                _template_override_cache[cache_key] = {}
        else:
            _template_override_cache[cache_key] = {}
    return _template_override_cache[cache_key]


def invalidate_campaign_copy_cache(campaign: dict | None = None) -> None:
    active_campaign = _resolve_campaign(campaign)
    if backend.is_postgres_backend() and active_campaign is not None:
        _hooks_override_cache.pop(f"campaign:{active_campaign.get('id', '')}:hooks", None)
        _template_override_cache.pop(f"campaign:{active_campaign.get('id', '')}:template_overrides", None)
        return
    _hooks_override_cache.pop(str(_resolve_hooks_library_path(campaign)), None)
    _template_override_cache.pop(str(_resolve_template_overrides_path(campaign)), None)


def _clean_string_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def get_effective_hooks_library(campaign: dict | None = None) -> dict[str, list[str]]:
    hooks = {key: list(values) for key, values in HOOKS.items()}
    override = _load_hooks_override(campaign=campaign)
    for key, values in override.items():
        cleaned = _clean_string_list(values)
        if cleaned:
            hooks[key] = cleaned
    return hooks


def get_template_override_payload(campaign: dict | None = None) -> dict:
    return deepcopy(_load_template_overrides(campaign=campaign))


def get_effective_subject_templates(campaign: dict | None = None) -> list[str]:
    override = _load_template_overrides(campaign=campaign)
    if "subject_templates" in override:
        return _clean_string_list(override.get("subject_templates"))
    return list(SUBJECT_TEMPLATES)


def get_effective_special_subject_option(campaign: dict | None = None) -> str:
    override = _load_template_overrides(campaign=campaign)
    if "special_subject_option" in override:
        value = override.get("special_subject_option")
        return str(value).strip() if value is not None else ""
    return SPECIAL_SUBJECT_OPTION


def _campaign_value(campaign: dict | None, key: str, env_key: str = "", default: str = "") -> str:
    active_campaign = _resolve_campaign(campaign)
    if active_campaign is not None:
        value = active_campaign.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    if env_key:
        env_value = os.getenv(env_key, "").strip()
        if env_value:
            return env_value
    return default

# ---------------------------------------------------------------------------
# Hook building blocks (Bausteine) – pre-written, direct "Sie" address
# No GPT call needed. get_hook() picks one deterministically by lead ID.
# ---------------------------------------------------------------------------

HOOKS: dict[str, list[str]] = {
    "no_website": [
        "Ihr Betrieb ist online unsichtbar – wer heute 'Installateur Wien' googelt, findet Sie einfach nicht.",
        "Ohne eigene Website verlieren Sie täglich Kunden an Mitbewerber die online präsent sind.",
        "Wer online nach einem Installateur in Wien sucht, findet Sie nicht – diese Kunden sind verloren bevor sie überhaupt anrufen könnten.",
        "Sie verlieren täglich potenzielle Aufträge, weil Ihr Betrieb im Internet schlicht nicht existiert.",
    ],
    "platzhalter": [
        "Ihre Website zeigt gerade nur eine Platzhalterseite – Kunden die Sie finden, sehen nichts und gehen sofort weiter.",
        "Wer heute Ihre Website besucht, sieht eine Baustelle statt Ihres Betriebs. Diese Kunden verpassen Sie täglich.",
        "Solange Ihre Website im 'Aufbau' ist, verlieren Sie jeden Tag Interessenten die klicken, nichts sehen und weitergehen.",
    ],
    "kein_mobil": [
        "Über 65% der Kunden suchen am Handy nach einem Installateur – wenn Ihre Website dabei nicht richtig lädt, rufen sie stattdessen den Nächsten an.",
        "Ihre Website funktioniert auf dem Smartphone nicht richtig. Sie verlieren damit täglich Kunden die mobil suchen.",
        "Wer Ihren Betrieb am Handy findet und eine kaputte Darstellung sieht, ist innerhalb von Sekunden weg – zur Konkurrenz.",
        "Fast zwei Drittel aller Suchanfragen nach Installateuren kommen heute vom Handy. Wenn Ihre Seite dabei nicht funktioniert, verlieren Sie diese Kunden.",
    ],
    "kein_kontakt": [
        "Wer Ihre Website besucht, findet keinen einfachen Weg Sie zu kontaktieren – kein Klick-zum-Anruf, kein Formular. Diese Kunden rufen jemand anderen an.",
        "Sie verlieren Anfragen, weil Besucher Ihrer Website die Telefonnummer abtippen müssen statt einfach draufzuklicken.",
        "Ohne direkten Kontaktweg auf der Website gehen Ihnen täglich Interessenten verloren – der Aufwand des Abtippens reicht, um Kunden zu verlieren.",
    ],
    "veraltet": [
        "Ihre Website wirkt veraltet – potenzielle Kunden schließen den Tab und suchen weiter, bevor sie überhaupt gelesen haben was Sie anbieten.",
        "Ein veraltetes Design kostet Sie täglich Vertrauen. Kunden entscheiden in Sekunden: weiter oder nicht.",
        "Wenn die Website 10 Jahre alt wirkt, fragen sich Kunden: ist dieser Betrieb noch aktiv? Viele wählen dann einen Mitbewerber mit modernerem Auftritt.",
        "Sie verlieren Aufträge nicht wegen Ihrer Arbeit – sondern wegen des ersten Eindrucks den Ihre veraltete Website macht.",
    ],
    "kein_seo": [
        "Wer 'Installateur Wien' googelt, findet Sie nicht auf der ersten Seite. Diese Kunden gehen täglich an Ihre Mitbewerber verloren.",
        "Sie verlieren täglich Aufträge an Betriebe die bei Google oben stehen – nicht weil sie besser sind, sondern weil ihre Website besser optimiert ist.",
        "In der Google-Suche nach Installateuren in Wien tauchen Sie nicht auf Seite 1 auf. Die Kunden die dort suchen, finden Sie nicht.",
        "Kunden die heute aktiv nach einem Installateur in Wien suchen, landen bei der Konkurrenz – weil Sie in der Google-Suche nicht sichtbar sind.",
    ],
    "bad_reviews": [
        "Bevor ein Kunde anruft, schaut er Ihre Google-Bewertungen an. Wenige oder schlechte Rezensionen – und er wählt stattdessen die Konkurrenz.",
        "Sie verlieren Kunden an Betriebe mit besseren Google-Bewertungen, noch bevor Sie die Chance haben anzubieten.",
        "Schlechte Google-Bewertungen kosten Sie Vertrauen – und Vertrauen ist das was Kunden dazu bringt anzurufen statt weiterzusuchen.",
    ],
    "keine_bewertungen": [
        "Mit nur wenigen Google-Bewertungen verlieren Sie täglich Kunden an Mitbewerber die mehr Rezensionen haben.",
        "Kunden vergleichen Bewertungen bevor sie anrufen – mit wenigen Rezensionen stehen Sie schlechter da als die Konkurrenz.",
        "Ein Betrieb mit 50+ Google-Bewertungen gewinnt fast immer gegen einen mit kaum Bewertungen – egal wie gut die Arbeit ist.",
    ],
    "not_ranked": [
        "Bei 'Installateur Wien' tauchen Sie in der Google-Suche nicht auf – Ihre Mitbewerber kassieren diese Kunden täglich.",
        "Wer heute nach Ihren Leistungen sucht, findet Ihre Konkurrenz. Sie verlieren diese Aufträge ohne es zu merken.",
        "In der Google-Suche für Installateure in Wien sind Sie nicht auf Seite 1 sichtbar. Die Kunden die dort suchen, sehen nur Ihre Mitbewerber.",
    ],
    "kein_design": [
        "Das Design Ihrer Website schreckt potenzielle Kunden ab – viele springen sofort ab und suchen weiter.",
        "Ihre Website wirkt nicht professionell genug. Sie verlieren damit Kunden die sich für einen anderen Betrieb entscheiden.",
        "Kunden urteilen über Ihren Betrieb nach dem ersten Eindruck Ihrer Website – und der kostet Sie täglich Aufträge.",
    ],
    "kein_ssl": [
        "Ihr Browser zeigt 'Nicht sicher' für Ihre Website – viele Kunden verlassen die Seite sofort wenn sie diese Warnung sehen.",
        "Ohne HTTPS verlieren Sie Kunden die Ihre Website besuchen und den Sicherheitshinweis sehen.",
    ],
}

# ---------------------------------------------------------------------------
# Subject line building blocks – 20 universal variants for all emails
# Uses {{subject_intro}} for direct owner/founder addressing when available
# ---------------------------------------------------------------------------

SUBJECT_TEMPLATES: list[str] = [
    "{{subject_intro}}kurze Frage zu Ihrer Website",
    "{{subject_intro}}kurzer Blick auf {{rank_keyword}}",
    "{{subject_intro}}Idee für Ihren Webauftritt",
    "{{subject_intro}}Ihre Website auf dem Handy",
    "{{subject_intro}}Ihr Google-Auftritt in Wien",
    "{{subject_intro}}mehr Klarheit für Ihre Website",
    "{{subject_intro}}eine kleine Website-Idee",
    "{{subject_intro}}Ihr Auftritt in der lokalen Suche",
    "{{subject_intro}}ein kurzer Website-Check",
    "{{subject_intro}}mehr Anfragen über die Website",
    "Kurze Frage zu Ihrem Webauftritt",
    "Ein Vorschlag für Ihre Website",
    "Idee für {{rank_keyword}}",
    "Ihr erster Eindruck online",
    "Ihre Website in der lokalen Suche",
    "Website-Beispiel für Ihren Betrieb",
    "Kurze Rückfrage zu Ihrer Website",
    "Ein modernerer Auftritt online",
    "Ihr Webauftritt für {{rank_keyword}}",
    "2 Ideen für Ihre Website",
]

SPECIAL_SUBJECT_OPTION = "Ihre neue Website für nur 500 € Fixpreis"


def get_hook(category: str, lead_id: str = "", campaign: dict | None = None) -> str:
    """Pick a hook deterministically by lead ID (same lead always gets same hook).
    Checks the active campaign hooks_library.json first, falls back to built-in HOOKS.
    """
    override = _load_hooks_override(campaign=campaign)
    options = override.get(category) or HOOKS.get(category) or HOOKS.get("kein_seo", [""])
    idx = int("".join(filter(str.isdigit, lead_id)) or "0") % len(options)
    return options[idx]


def _stable_index(seed: str, length: int) -> int:
    if length <= 0:
        return 0
    digest = hashlib.sha1((seed or "0").encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % length


def get_subject_options(
    lead: dict,
    hook: str = "",
    urgency: str = "",
    current_subject: str = "",
    campaign: dict | None = None,
) -> list[str]:
    """Render the 20 shared subject lines for a lead and optionally preserve a custom current subject."""
    slots = build_slots(lead, hook, urgency, campaign=campaign)
    subject_templates = get_effective_subject_templates(campaign=campaign)
    special_subject_option = get_effective_special_subject_option(campaign=campaign)
    seen: set[str] = set()
    options: list[str] = []

    if current_subject:
        current = current_subject.strip()
        if current:
            seen.add(current)
            options.append(current)

    if special_subject_option and special_subject_option not in seen:
        seen.add(special_subject_option)
        options.append(special_subject_option)

    for template in subject_templates:
        rendered = fill_template(template, slots).strip()
        if rendered and rendered not in seen:
            seen.add(rendered)
            options.append(rendered)

    return options


def get_subject(lead: dict, hook: str = "", urgency: str = "", campaign: dict | None = None) -> str:
    """Pick one of the shared subject lines deterministically by lead ID."""
    options = get_subject_options(lead, hook=hook, urgency=urgency, campaign=campaign)
    idx = _stable_index(lead.get("ID", ""), len(options))
    return options[idx] if options else "Kurze Frage zu Ihrer Sichtbarkeit"


_GENERIC_HOOK_PATTERNS = {
    "sie verlieren täglich": 6,
    "65%": 8,
    "google-bewertungen": 5,
    "erste eindruck": 4,
    "am handy": 3,
    "bei google": 3,
    "erste seite": 3,
    "zur konkurrenz": 3,
    "nicht sichtbar": 3,
    "google schickt": 3,
}


def _normalize_for_repetition(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9äöüß%+ ]+", " ", (text or "").lower())).strip()


def _hook_repetition_score(candidate: str, template_key: str, lead: dict, campaign: dict | None = None) -> int:
    """
    Lower score is better.
    Penalizes:
    - repeated numbers/stats like 65%
    - repeated trigger phrases also present in the template body
    - repeated opening trigrams
    - hooks that start with very generic patterns
    """
    slots = build_slots(lead, hook="", urgency="", campaign=campaign)
    slots["subject"] = get_subject(lead, campaign=campaign)
    candidate_filled = fill_template(candidate, slots)

    templates = get_effective_templates(campaign=campaign)
    tmpl = templates.get(template_key, templates["default"])
    context = " ".join(
        [
            slots["subject"],
            fill_template(tmpl["email"], {**slots, "hook": ""}),
            fill_template(tmpl["whatsapp"], {**slots, "hook": ""}),
            fill_template(tmpl["phone_script"], {**slots, "hook": ""}),
        ]
    )

    cand_norm = _normalize_for_repetition(candidate_filled)
    ctx_norm = _normalize_for_repetition(context)
    if not cand_norm:
        return 999

    score = 0

    for phrase, weight in _GENERIC_HOOK_PATTERNS.items():
        if phrase in cand_norm:
            score += weight
            if phrase in ctx_norm:
                score += weight * 2

    cand_numbers = set(re.findall(r"\b\d+\+?%?\b", cand_norm))
    ctx_numbers = set(re.findall(r"\b\d+\+?%?\b", ctx_norm))
    score += 8 * len(cand_numbers & ctx_numbers)

    cand_words = cand_norm.split()
    ctx_words = ctx_norm.split()
    cand_trigrams = {" ".join(cand_words[i:i + 3]) for i in range(max(0, len(cand_words) - 2))}
    ctx_trigrams = {" ".join(ctx_words[i:i + 3]) for i in range(max(0, len(ctx_words) - 2))}
    score += 4 * len({g for g in cand_trigrams & ctx_trigrams if len(g) > 10})

    if len(cand_words) >= 3:
        opening = " ".join(cand_words[:3])
        if opening in ctx_norm:
            score += 10

    return score


def choose_hook(template_key: str, lead: dict, campaign: dict | None = None) -> str:
    """
    Pick the least repetitive local hook for the chosen template.
    Still deterministic: ties are broken by lead ID.
    """
    override = _load_hooks_override(campaign=campaign)
    options = override.get(template_key) or HOOKS.get(template_key) or HOOKS.get("kein_seo", [""])
    if not options:
        return ""

    ranked = sorted(
        enumerate(options),
        key=lambda item: (
            _hook_repetition_score(item[1], template_key, lead, campaign=campaign),
            abs(item[0] - _stable_index(lead.get("ID", ""), len(options))),
            item[0],
        ),
    )
    return ranked[0][1]


_GENERATE_HOOKS_PROMPT = """\
Du bist ein erfahrener Verkaufstexter in Wien. Ich brauche knackige Cold-Email-Hooks für Installateurbetriebe.

REGELN:
- Immer direkte "Sie"-Anrede: "Sie verlieren", "Ihr Betrieb", "Ihre Website" – niemals dritte Person
- 1-2 Sätze pro Hook, maximal 40 Wörter
- Konkret, FOMO-getrieben, kein Marketing-Blabla
- Auf Deutsch, österreichischer Ton
- Die Hooks pro Kategorie müssen unterschiedlich anfangen und dürfen nicht wie derselbe Satz in Varianten wirken
- Vermeide wiederkehrende Standardphrasen wie "Sie verlieren täglich Kunden" oder dieselbe Statistik in mehreren Hooks

Für jede Kategorie: 8 verschiedene Hooks. Variiere den Einstieg (Frage, Aussage, Zahl, Vergleich).

KATEGORIEN:
1. no_website – Betrieb hat gar keine eigene Website, ist online unsichtbar
2. platzhalter – Website ist eine "Coming soon" / Under-construction-Seite, zeigt nichts
3. kein_mobil – Website nicht mobiloptimiert, 65%+ der Suchanfragen kommen vom Smartphone
4. kein_kontakt – Kein Click-to-Call, kein Kontaktformular, kein WhatsApp-Button
5. veraltet – Website >5 Jahre alt, veraltetes Design, wirkt nicht mehr professionell
6. kein_seo – Nicht auf Google Seite 1, keine lokalen Keywords, schlechtes Ranking
7. bad_reviews – Schlechte Google-Bewertungen (1–3 Sterne), kostet Vertrauen
8. keine_bewertungen – Kaum Google-Bewertungen (<10), Konkurrenz hat viel mehr
9. not_ranked – Bei lokaler Google-Suche ("Installateur Wien") gar nicht gefunden
10. kein_design – Sehr veraltetes / amateurhaftes Design, schreckt Kunden ab

Antworte AUSSCHLIESSLICH als JSON (kein Markdown, kein Text):
{
  "no_website": ["hook1", "hook2", ...],
  "platzhalter": [...],
  ...
}"""


def generate_hooks_library(force: bool = False, campaign: dict | None = None) -> None:
    """Call GPT once to generate quality hooks per category → saves to the active campaign hook library."""
    active_campaign = _resolve_campaign(campaign)
    hooks_path = _resolve_hooks_library_path(campaign=campaign)
    if backend.is_postgres_backend() and active_campaign is not None:
        existing = active_campaign.get("hooks_library_json") or {}
        if existing and not force:
            print(f"Hook library already exists for {active_campaign.get('id', '')}. Use --force to regenerate.")
            return
    elif hooks_path.exists() and not force:
        print(f"{hooks_path.name} already exists at {hooks_path}. Use --force to regenerate.")
        return

    import re
    from openai import OpenAI
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set.")
        return

    client = OpenAI(api_key=api_key)
    print("Generating hook library via GPT (1 API call)…")

    resp = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=3000,
        temperature=0.8,
        messages=[{"role": "user", "content": _GENERATE_HOOKS_PROMPT}],
    )
    raw = resp.choices[0].message.content.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    library = json.loads(raw)

    if backend.is_postgres_backend() and active_campaign is not None:
        from campaign_service import update_campaign
        update_campaign(active_campaign["id"], {"hooks_library_json": library})
        invalidate_campaign_copy_cache(active_campaign)
    else:
        hooks_path.parent.mkdir(parents=True, exist_ok=True)
        hooks_path.write_text(
            json.dumps(library, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _hooks_override_cache.pop(str(hooks_path), None)

    total = sum(len(v) for v in library.values())
    if backend.is_postgres_backend() and active_campaign is not None:
        print(f"Done. {total} hooks across {len(library)} categories saved in the database for {active_campaign['id']}.")
    else:
        print(f"Done. {total} hooks across {len(library)} categories saved to {hooks_path}")
        print("Edit the campaign hooks_library.json freely — it overrides the built-in defaults.")


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

TEMPLATES: dict[str, dict[str, str]] = {

    # -----------------------------------------------------------------------
    "no_website": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Wenn jemand heute "Installateur{{rank_keyword_district}}" googelt, findet er{{competitors_line}} – aber nicht Ihr Unternehmen. Diese Kunden gehen zur Konkurrenz, ohne dass Sie auch nur die Chance bekommen anzurufen.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – wir entwickeln für Installateurbetriebe professionelle Websites zum Festpreis von €{{price}} einmalig und setzen sie innerhalb von 2 Wochen um. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – wäre das grundsätzlich interessant?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, mein Name ist {{sender_name}}{{sender_company_phone}}. Ich weiß, Sie arbeiten gerade – darf ich Ihnen ganz kurz eine Frage stellen? Es dauert auch nur 30 Sekunden."

HOOK: "{{hook}} – Und das bedeutet: Kunden die online nach einem Installateur in Wien suchen, finden Sie nicht und rufen jemand anderen an."

FRAGE: "Mal eine kurze Frage: Wie kommen die meisten Ihrer Kunden zu Ihnen – eher über Google oder über Empfehlungen?"

ANTWORT EMPFEHLUNGEN: "Das höre ich oft. Aber viele Betriebe verlieren jeden Tag potenzielle Neukunden, gerade weil sie online nicht zu finden sind. Und genau das Problem lösen wir."

LÖSUNG: "Als auf Handwerksbetriebe in Wien spezialisierte Digitalagentur entwickeln wir professionelle Websites für Installateurbetriebe: mobil optimiert, mit Click-to-Call und lokal besser sichtbar bei Google. Das realisieren wir zum Festpreis von €{{price}} einmalig."

EINWÄNDE:
"Zu teuer" → "€{{price}} einmalig – das ist weniger als ein einziger verlorener Auftrag. Und wir liefern in 2 Wochen."
"Kein Interesse" → "Ich möchte Ihnen auch gar nichts am Telefon verkaufen. Ich würde Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken – Sie schauen sich das in Ruhe an, und wenn's nichts für Sie ist, ignorieren Sie's einfach."
"Haben genug Kunden" → "Perfekt. Dann wäre eine Website gut um das auch in Zukunft zu sichern – Empfehlungen können schwanken."
"Keine Zeit" → "Das läuft komplett auf unserer Seite – wir brauchen von Ihnen nur ein kurzes Gespräch fürs Briefing."

CTA: "Okay, dann verbleiben wir so: Ich schicke Ihnen 2-3 konkrete Ideen per E-Mail oder WhatsApp. Sie schauen sich das kurz an – auf welche Adresse darf ich das schicken?" """,
    },

    # -----------------------------------------------------------------------
    "kein_mobil": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Über 65% aller Google-Suchanfragen nach Installateuren kommen vom Smartphone. Eine Website die auf dem Handy nicht richtig funktioniert – zu kleine Schrift, Buttons nicht klickbar – verliert diese Besucher in Sekunden. Google bestraft das zusätzlich mit schlechterem Ranking.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – 65% der Kunden suchen am Handy. Wir entwickeln mobiloptimierte Websites zum Festpreis von €{{price}} einmalig und setzen sie innerhalb von 2 Wochen um. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Darf ich Ihnen kurz eine Frage stellen, dauert 30 Sekunden?"

HOOK: "{{hook}} – Und das ist ein Problem, weil über 65% der Kunden heute am Handy nach einem Installateur suchen. Wenn die Website auf dem Handy nicht funktioniert, gehen die Leute einfach weiter."

FRAGE: "Wie kommen bei Ihnen die meisten Kunden rein – eher über Google oder über Empfehlungen?"

ANTWORT EMPFEHLUNGEN: "Das kenne ich. Aber gerade über das Handy verlieren viele Betriebe täglich Neukunden – die suchen schnell, finden die Website, es funktioniert nicht richtig, und sie rufen den Nächsten an."

LÖSUNG: "Wir entwickeln mobiloptimierte Websites für Installateurbetriebe – mit Click-to-Call, WhatsApp-Option und klarer Nutzerführung. Das realisieren wir zum Festpreis von €{{price}} einmalig."

EINWÄNDE:
"Wir haben schon eine Website" → "Ja, ich habe sie gesehen – das Problem ist die Mobildarstellung. Die lässt sich beheben."
"Zu teuer" → "€{{price}} einmalig – der erste neue Kunde über die Website zahlt das locker."
"Kein Interesse" → "Ich möchte Ihnen nichts am Telefon verkaufen. Darf ich Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken? Sie schauen's in Ruhe an."
"Keine Zeit" → "Das erledigen wir komplett – wir brauchen nur ein kurzes Briefing von Ihnen."

CTA: "Dann schicke ich Ihnen 2-3 konkrete Ideen. Auf welche E-Mail-Adresse oder Handynummer darf ich das senden?" """,
    },

    # -----------------------------------------------------------------------
    "kein_kontakt": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Wenn jemand Ihre Website besucht und keinen einfachen Weg findet, Sie zu kontaktieren – kein Formular, kein Click-to-Call, kein WhatsApp-Button – verlässt er die Seite und ruft den Nächsten an. Der Aufwand des Abtippens einer Nummer reicht aus, um Kunden zu verlieren.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – Kunden, die Ihre Website besuchen, finden keinen einfachen Kontaktweg. Wir lösen das mit einer professionellen Website mit klaren Kontaktwegen zum Festpreis von €{{price}} einmalig. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Kurze Frage – darf ich?"

HOOK: "{{hook}} – Das bedeutet: Wer Ihre Website findet, kann Sie nicht direkt kontaktieren. Kein Klick-zum-Anruf, kein Formular – viele gehen dann einfach weiter."

FRAGE: "Kommen aktuell viele Anfragen über Ihre Website rein?"

LÖSUNG: "Wir entwickeln Websites mit klaren Kontaktwegen: Click-to-Call, Kontaktformular und auf Wunsch WhatsApp-Integration. So erreichen Interessenten Ihren Betrieb direkt – gerade bei dringenden Anfragen."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Haben schon eine Website" → "Sehe ich – das Problem ist der fehlende Kontaktweg. Das ist schnell nachgerüstet."
"Zu teuer" → "€{{price}} einmalig – das zahlt sich beim ersten Auftrag aus."
"Kein Interesse" → "Darf ich Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken? Schauen Sie's in Ruhe an."
"Keine Zeit" → "Das läuft komplett bei uns – Sie brauchen nur einmal kurz Inputs geben."

CTA: "Auf welche Adresse darf ich Ihnen die Ideen schicken?" """,
    },

    # -----------------------------------------------------------------------
    "kein_seo": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Wer in Wien nach einem Installateur sucht, googelt "{{rank_keyword}}". Ganz oben erscheinen{{competitors_line}} – Sie tauchen auf der ersten Seite der Google-Suche nicht auf. Das sind täglich Kunden die aktiv nach Ihren Leistungen suchen und stattdessen zur Konkurrenz gehen.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – bei "{{rank_keyword}}" findet man Sie nicht. Wir entwickeln SEO-optimierte Websites zum Festpreis von €{{price}} einmalig. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse an einem kurzen Gespräch?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Darf ich kurz?"

HOOK: "{{hook}} – Ich habe '{{rank_keyword}}' gegoogelt – da erscheinen{{competitors_line}}, aber Sie nicht. Das sind täglich Kunden die aktiv suchen und jemand anderen finden."

FRAGE: "Kommen aktuell Anfragen über Google bei Ihnen an?"

LÖSUNG: "Wir entwickeln SEO-optimierte Websites speziell für Installateurbetriebe in Wien: mit lokaler Keyword-Ausrichtung, technischer Optimierung und sauberer Struktur, damit Sie bei '{{rank_keyword}}' besser gefunden werden."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Haben schon eine Website" → "Sehe ich – das Problem liegt an der fehlenden SEO-Optimierung. Die Website ist da, aber Google zeigt sie nicht."
"Zu teuer" → "€{{price}} einmalig – der erste Auftrag über Google zahlt das."
"Kein Interesse" → "Dann schicke ich Ihnen einfach 2-3 konkrete Ideen per E-Mail. Schauen Sie sich das in Ruhe an."
"Keine Zeit" → "Das läuft komplett bei uns. Wir brauchen nur ein kurzes Briefing."

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },

    # -----------------------------------------------------------------------
    "veraltet": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

In einer Branche die auf Vertrauen und Qualität aufbaut, entscheidet der erste Eindruck. Eine veraltete Website sendet die falsche Botschaft: Kunden fragen sich, ob der Betrieb noch aktiv ist – und wählen lieber einen Mitbewerber mit modernerem Auftritt. Das kostet täglich Aufträge.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – ein veralteter Webauftritt kostet täglich Kunden. Wir modernisieren Ihre Website zum Festpreis von €{{price}} einmalig und setzen das innerhalb von 2 Wochen um. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Darf ich kurz?"

HOOK: "{{hook}} – Eine veraltete Website kann potenzielle Kunden abschrecken bevor sie überhaupt anrufen. Sie sehen das Design und denken: ist der Betrieb noch aktiv?"

FRAGE: "Kommen bei Ihnen aktuell genug Anfragen über das Internet?"

LÖSUNG: "Wir modernisieren Ihren Auftritt vollständig: mit zeitgemäßem Design, mobiler Optimierung und besserer lokaler Auffindbarkeit bei Google. So spiegelt Ihre Website die Qualität Ihres Betriebs wider."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Läuft so auch" → "Momentan vielleicht. Die Frage ist wie viele Kunden abspringen bevor sie anrufen."
"Zu teuer" → "€{{price}} einmalig. Das zahlt sich beim ersten zusätzlichen Auftrag aus."
"Kein Interesse" → "Ich schicke Ihnen einfach 2-3 konkrete Ideen per E-Mail – schauen Sie's in Ruhe an."
"Keine Zeit" → "Das erledigen wir – von Ihnen brauchen wir nur ein kurzes Briefing."

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },

    # -----------------------------------------------------------------------
    "platzhalter": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Wer heute Ihre Website besucht, sieht eine Platzhalterseite – keine Leistungen, kein Kontakt, keine Information. Diese Besucher sind verloren bevor sie überhaupt die Chance hatten, Sie anzurufen.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – Ihre Website zeigt gerade nur eine Platzhalterseite. Wir ersetzen sie durch eine vollständige Website zum Festpreis von €{{price}} einmalig. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Kurze Frage – darf ich?"

HOOK: "{{hook}} – Ich habe Ihre Website aufgerufen und gesehen, dass sie noch im Aufbau ist. Das bedeutet: Kunden die Sie online finden, sehen nichts und gehen weiter."

LÖSUNG: "Wir bringen das rasch in Ordnung: innerhalb von 2 Wochen mit einer vollständigen, professionellen Website – mit allem, was ein Installateurbetrieb online braucht."

PREIS: "Festpreis: €{{price}} einmalig. Die vollständige Website steht innerhalb von 2 Wochen online."

EINWÄNDE:
"Bauen das selbst" → "Gut zu hören. Darf ich fragen wann das fertig sein soll? Wir könnten das schneller liefern."
"Zu teuer" → "€{{price}} einmalig – schnell umgesetzt, sofort online."
"Kein Interesse" → "Darf ich Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken?"
"Keine Zeit" → "Das erledigen wir komplett – wir brauchen nur kurze Inputs von Ihnen."

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },

    # -----------------------------------------------------------------------
    "bad_reviews": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Bevor ein neuer Kunde anruft, schaut er sich die Google-Bewertungen an. Wenige oder schlechte Bewertungen – und er wählt stattdessen einen Mitbewerber mit 50+ positiven Rezensionen. Eine gute Website schafft Vertrauen bevor Kunden überhaupt auf die Sternchen schauen. Das kostet täglich Aufträge, ohne dass Sie es merken.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – wenige oder schlechte Google-Bewertungen kosten Aufträge. Eine professionelle Website stärkt Vertrauen und lässt sich zum Festpreis von €{{price}} einmalig umsetzen. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Darf ich kurz?"

HOOK: "{{hook}} – Kunden schauen sich vor dem Anruf die Google-Bewertungen an. Wenige oder schlechte Bewertungen reichen aus, damit sie den Nächsten anrufen."

FRAGE: "Merken Sie, dass Kunden die Bewertungen ansprechen?"

LÖSUNG: "Eine professionelle Website baut Vertrauen auf, bevor Kunden überhaupt auf die Sterne schauen: mit klarer Leistungsübersicht, Referenzen und persönlicher Vorstellung."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Bewertungen stimmen nicht" → "Das glaube ich Ihnen. Eine gute Website zeigt was wirklich dahintersteckt."
"Zu teuer" → "€{{price}} einmalig – weniger als ein verlorener Auftrag."
"Kein Interesse" → "Darf ich Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken?"

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },

    # -----------------------------------------------------------------------
    "not_ranked": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Wenn jemand heute "{{rank_keyword}}" googelt, erscheinen{{competitors_line}} ganz oben. Sie tauchen auf der ersten Seite der Google-Suche nicht auf – und die erste Seite ist alles was zählt. Diese Kunden suchen aktiv und gehen zur Konkurrenz.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – bei "{{rank_keyword}}" findet man Sie nicht, aber{{competitors_line}}. Wir lösen das mit einer SEO-optimierten Website zum Festpreis von €{{price}} einmalig. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Darf ich kurz?"

HOOK: "{{hook}} – Ich habe '{{rank_keyword}}' gegoogelt – da erscheinen{{competitors_line}}, aber Sie nicht. Täglich verlieren Sie so Kunden die aktiv suchen."

FRAGE: "Kommen aktuell Anfragen über Google bei Ihnen an?"

LÖSUNG: "Wir entwickeln SEO-optimierte Websites für Ihren Bezirk: mit lokaler Keyword-Ausrichtung, technischer Optimierung und sauberer Struktur, damit Sie bei '{{rank_keyword}}' besser gefunden werden."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Haben schon eine Website" → "Sehe ich – das Problem liegt an der SEO. Die Website ist da, Google zeigt sie nur nicht."
"Zu teuer" → "€{{price}} – der erste Auftrag über Google zahlt das."
"Kein Interesse" → "Ich schicke Ihnen einfach 2-3 konkrete Ideen per E-Mail."
"Keine Zeit" → "Läuft komplett bei uns – kurzes Briefing reicht."

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },

    # -----------------------------------------------------------------------
    "keine_bewertungen": {
        "email": """\
Betreff: {{subject}}

{{salutation}}

{{hook}}

**Festpreis: €{{price}} einmalig.** Wir entwickeln Websites für Handwerksbetriebe in Wien: modern, mobiloptimiert, mit klaren Kontaktwegen und einer sauberen Basis für lokale Sichtbarkeit bei Google.

Kunden die einen Installateur suchen, vergleichen Bewertungen. Ein Betrieb mit 50+ positiven Rezensionen gewinnt fast immer gegen einen mit kaum Bewertungen – egal wie gut die Arbeit ist. Eine professionelle Website schafft Vertrauen bevor Kunden auf die Sternchen schauen. Das ist die Realität der Google-Suche.

Wenn das grundsätzlich relevant ist, antworte ich gern mit 2-3 konkreten Ideen für Ihren Betrieb.

Mit freundlichen Grüßen,
{{sender_name}}
{{sender_company_signature}}
{{sender_website}}{{sender_phone}}""",

        "whatsapp": """\
{{salutation}} {{hook}} – mit wenigen Google-Bewertungen verlieren Betriebe täglich Kunden an Mitbewerber. Eine professionelle Website stärkt Vertrauen und lässt sich zum Festpreis von €{{price}} einmalig umsetzen. Wenn Sie möchten, schicke ich Ihnen dazu kurz 2-3 Ideen – Interesse?""",

        "phone_script": """\
OPENING: "Spreche ich mit {{contact}}? – Guten Tag, {{sender_name}}{{sender_company_phone}}. Kurze Frage – 30 Sekunden?"

HOOK: "{{hook}} – Kunden vergleichen Bewertungen bevor sie anrufen. Wenige Rezensionen reichen, damit sie den Nächsten wählen."

FRAGE: "Kommen aktuell Anfragen über Google bei Ihnen an?"

LÖSUNG: "Eine professionelle Website kann das ausgleichen: mit Referenzprojekten, klaren Leistungen und persönlicher Vorstellung – so entsteht Vertrauen, bevor jemand nur die Bewertungen vergleicht."

PREIS: "Festpreis: €{{price}} einmalig. Umsetzung innerhalb von 2 Wochen."

EINWÄNDE:
"Haben genug Kunden" → "Gut. Aber Empfehlungen schwanken – mit Google kommen Neukunden konstant."
"Zu teuer" → "€{{price}} einmalig – das zahlt sich beim ersten Neukunden aus."
"Kein Interesse" → "Darf ich Ihnen einfach 2-3 konkrete Ideen per E-Mail schicken?"

CTA: "Auf welche Adresse schicke ich Ihnen die Ideen?" """,
    },
}

DEFAULT_TEMPLATE_KEY = "kein_seo"

# Fallback
TEMPLATES["default"] = TEMPLATES[DEFAULT_TEMPLATE_KEY]


def get_effective_templates(campaign: dict | None = None) -> dict[str, dict[str, str]]:
    templates = {key: dict(value) for key, value in TEMPLATES.items() if key != "default"}
    override = _load_template_overrides(campaign=campaign)
    override_templates = override.get("templates")
    if isinstance(override_templates, dict):
        for template_key, channels in override_templates.items():
            if template_key not in templates or not isinstance(channels, dict):
                continue
            for channel in ("email", "whatsapp", "phone_script"):
                value = channels.get(channel)
                if isinstance(value, str):
                    templates[template_key][channel] = value
    templates["default"] = dict(templates.get(DEFAULT_TEMPLATE_KEY, {}))
    return templates


# ---------------------------------------------------------------------------
# Template selection
# ---------------------------------------------------------------------------

# Priority: mehr Kunden → bessere Bewertungen → Konkurrenz schlagen
CATEGORY_PRIORITY = [
    "no_website",
    "platzhalter",
    "kein_mobil",
    "kein_kontakt",
    "veraltet",
    "kein_design",
    "bad_reviews",
    "keine_bewertungen",
    "not_ranked",
    "kein_seo",
    "kein_ssl",
]


def pick_template_key(pain_categories: list[str], lead: dict) -> str:
    """
    Pick the best template based on pain categories + real data.

    Priority:
    1. Mehr Kunden – visibility/usability issues
    2. Bessere Bewertungen – backed by real Places API data
    3. Konkurrenz schlagen – confirmed not ranking on Google
    """
    # Real data checks
    snippets = lead.get("Google_Review_Snippets", "") or ""
    rating_str = lead.get("Google_Rating", "") or ""
    try:
        rating = float(rating_str)
    except ValueError:
        rating = 5.0
    has_neg_reviews = "[NEG]" in snippets or (rating_str and rating < 3.5)

    review_count_str = lead.get("Google_Review_Count", "") or ""
    try:
        review_count = int(review_count_str)
    except ValueError:
        review_count = 999
    has_few_reviews = review_count < 10  # data from Places API, not website widget

    not_ranking = (
        lead.get("Google_Rank_Position") == "not_found"
        and bool((lead.get("Google_Competitors") or "").strip())
    )

    # Walk priority list
    for key in CATEGORY_PRIORITY:
        if key not in pain_categories:
            continue
        # Data-gated keys: only use if backed by real evidence
        if key == "bad_reviews" and not has_neg_reviews:
            continue
        if key == "keine_bewertungen" and not has_few_reviews:
            continue
        if key == "not_ranked" and not not_ranking:
            continue
        # kein_mobil: only if score <= 5 (don't fire on borderline sites)
        if key == "kein_mobil":
            score_str = lead.get("Website_Score", "") or ""
            try:
                score = int(score_str)
            except ValueError:
                score = 10
            if score > 5:
                continue
        return key

    # Real data overrides even if GPT didn't detect the category
    if has_neg_reviews:
        return "bad_reviews"
    if has_few_reviews and rating_str:  # only if we actually have Places API data
        return "keine_bewertungen"
    if not_ranking:
        return "not_ranked"

    return "default"


# ---------------------------------------------------------------------------
# Template filling
# ---------------------------------------------------------------------------

def fill_template(template_str: str, slots: dict) -> str:
    """Replace {{slot}} markers in template_str with values from slots dict."""
    result = template_str
    for key, value in slots.items():
        result = result.replace("{{" + key + "}}", str(value))
    return result


def parse_email_draft(draft: str) -> tuple[str, str]:
    """Parse 'Betreff: ...' email draft into (subject, body)."""
    text = (draft or "").strip()
    if not text:
        return "", ""

    lines = text.splitlines()
    if lines and lines[0].lower().startswith("betreff:"):
        subject = lines[0][8:].strip()
        body = "\n".join(lines[1:]).strip()
        return subject, body

    return "", text


def compose_email_draft(subject: str, body: str) -> str:
    """Compose the stored email draft format."""
    subject_clean = (subject or "").strip()
    body_clean = (body or "").strip()
    if subject_clean:
        return f"Betreff: {subject_clean}\n\n{body_clean}".strip()
    return body_clean


_HONORIFICS = {"herr", "frau"}
_NAME_TITLES = {
    "ing", "ing.", "dipl", "dipl.", "dipl.-ing", "dipl.-ing.", "di", "dr", "dr.",
    "mag", "mag.", "prof", "prof.", "fh", "(fh)", "mba", "msc", "bsc",
}


def _clean_name_token(token: str) -> str:
    return token.strip().strip(",;:")


def _is_name_title(token: str) -> bool:
    base = _clean_name_token(token).lower().strip("().")
    return base in {t.strip("().") for t in _NAME_TITLES}


def _format_contact_for_direct_use(contact_name: str) -> tuple[str, str]:
    """
    Returns (subject/contact label, salutation).
    Examples:
      "Herr Bodziany Mateusz" -> ("Herr Bodziany", "Guten Tag Herr Bodziany,")
      "Frau Ing. Hallwirth Elisabeth" -> ("Frau Hallwirth", "Guten Tag Frau Hallwirth,")
      "Christian Trilsam" -> ("Christian", "Guten Tag Christian,")
    """
    raw = (contact_name or "").strip()
    if not raw:
        return "", "Sehr geehrte Damen und Herren,"

    tokens = [_clean_name_token(t) for t in raw.split() if _clean_name_token(t)]
    if not tokens:
        return "", "Sehr geehrte Damen und Herren,"

    for i, token in enumerate(tokens):
        if token.lower() in _HONORIFICS:
            j = i + 1
            while j < len(tokens) and _is_name_title(tokens[j]):
                j += 1
            if j < len(tokens):
                label = f"{token} {tokens[j]}"
            else:
                label = token
            return label, f"Guten Tag {label},"

    for token in tokens:
        if not _is_name_title(token):
            return token, f"Guten Tag {token},"

    fallback = tokens[0]
    return fallback, f"Guten Tag {fallback},"


def build_slots(lead: dict, hook: str, urgency: str, campaign: dict | None = None) -> dict:
    """Build the full slots dict for a lead."""
    from campaign_service import format_rank_keyword
    from crm_store import get_bezirk

    active_campaign = _resolve_campaign(campaign)
    company = lead.get("Unternehmen", "")
    contact_name = (lead.get("Kontaktname") or "").strip()
    contact_direct, salutation = _format_contact_for_direct_use(contact_name)
    subject_intro = f"{contact_direct}, " if contact_direct else ""

    adresse = lead.get("Adresse", "")
    plz, bezirk = get_bezirk(adresse)

    competitors_raw = lead.get("Google_Competitors", "") or ""
    competitors_list = [c.strip() for c in competitors_raw.split(" | ") if c.strip()]
    if competitors_list:
        competitors_line = " " + ", ".join(competitors_list[:2])
        competitors_short = competitors_list[0]
    else:
        competitors_line = " Ihre Mitbewerber"
        competitors_short = "Ihre Mitbewerber"

    rank_keyword = lead.get("Google_Rank_Keyword") or (
        format_rank_keyword(active_campaign, plz=plz) if active_campaign is not None else f"Installateur {plz or 'Wien'}"
    )
    rank_keyword_district = plz or "Wien"

    price = lead.get("Price") or _campaign_value(active_campaign, "price_default", "PRICE_DEFAULT", "500")

    sender_name = _campaign_value(active_campaign, "sender_name", "SENDER_NAME", "Linus Fraundorfer")
    sender_company = _campaign_value(active_campaign, "sender_company", "SENDER_COMPANY", "Digitalagentur Megaphonia")
    sender_website = _campaign_value(active_campaign, "sender_website", "SENDER_WEBSITE", "https://www.megaphonia.com")
    sender_phone_raw = _campaign_value(active_campaign, "sender_phone", "SENDER_PHONE", "0677 617 517 70")
    sender_email_raw = _campaign_value(active_campaign, "sender_email", "SENDER_EMAIL", "")
    sender_name_norm = re.sub(r"\s+", " ", sender_name).strip().lower()
    sender_company_norm = re.sub(r"\s+", " ", sender_company).strip().lower()
    sender_company_signature = sender_company
    if sender_company_norm and sender_company_norm in sender_name_norm:
        sender_company_signature = ""
    sender_company_phone = f" von {sender_company}" if sender_company else ""
    sender_phone = f"\n{sender_phone_raw}" if sender_phone_raw else ""
    sender_email = f"\n{sender_email_raw}" if sender_email_raw else ""

    return {
        "hook": hook,
        "urgency": urgency,
        "company": company,
        "salutation": salutation,
        "contact": contact_direct or contact_name or company or "Ihnen",
        "subject_name": contact_direct if contact_direct else "Ihr Unternehmen",
        "subject_intro": subject_intro,
        "price": price,
        "sender_name": sender_name,
        "sender_company": sender_company,
        "sender_company_signature": sender_company_signature,
        "sender_company_phone": sender_company_phone,
        "sender_website": sender_website,
        "sender_phone": sender_phone,
        "sender_email": sender_email,
        "competitors_raw": competitors_raw,
        "competitors_line": competitors_line,
        "competitors_short": competitors_short,
        "rank_keyword": rank_keyword,
        "rank_keyword_district": rank_keyword_district,
        "rating": lead.get("Google_Rating", ""),
        "review_count": lead.get("Google_Review_Count", ""),
    }


def render_drafts(
    lead: dict,
    hook: str,
    urgency: str,
    template_key: str | None = None,
    campaign: dict | None = None,
) -> dict:
    """
    Fill the appropriate template with lead data + selected hook.
    Returns {"Email_Draft": str, "WhatsApp_Draft": str, "Phone_Script": str, "Template_Used": str}
    """
    pain_categories_raw = lead.get("Pain_Categories", "") or ""
    pain_categories = [c.strip() for c in pain_categories_raw.split(" | ") if c.strip()]

    key = template_key or pick_template_key(pain_categories, lead)
    templates = get_effective_templates(campaign=campaign)
    tmpl = templates.get(key, templates["default"])
    if not (hook or "").strip():
        hook = choose_hook(key, lead, campaign=campaign)
    slots = build_slots(lead, hook, urgency, campaign=campaign)
    if slots["hook"]:
        slots["hook"] = fill_template(slots["hook"], slots)

    # Inject one shared subject line from the universal subject library
    slots["subject"] = get_subject(lead, hook=hook, urgency=urgency, campaign=campaign)

    email_filled = fill_template(tmpl["email"], slots)
    subject, body = parse_email_draft(email_filled)
    email_draft = compose_email_draft(subject, body) if subject else email_filled

    return {
        "Email_Draft": email_draft,
        "WhatsApp_Draft": fill_template(tmpl["whatsapp"], slots),
        "Phone_Script": fill_template(tmpl["phone_script"], slots),
        "Template_Used": key,
    }


def has_saved_drafts(lead: dict) -> bool:
    return any((lead.get(field) or "").strip() for field in ("Email_Draft", "WhatsApp_Draft", "Phone_Script"))


def is_pending_template_refresh_target(lead: dict) -> bool:
    if not has_saved_drafts(lead):
        return False
    if (lead.get("Drafts_Approved") or "0") == "1":
        return False
    status = (lead.get("Status") or "new").strip()
    return status not in _PROTECTED_REFRESH_STATUSES


def rerender_saved_draft(lead: dict, campaign: dict | None = None) -> dict:
    pain_categories_raw = lead.get("Pain_Categories", "") or ""
    pain_categories = [c.strip() for c in pain_categories_raw.split(" | ") if c.strip()]
    template_key = (lead.get("Template_Used") or "").strip()
    effective_templates = get_effective_templates(campaign=campaign)
    if template_key not in effective_templates:
        template_key = pick_template_key(pain_categories, lead)

    hook = choose_hook(template_key, lead, campaign=campaign)
    drafts = render_drafts(lead, hook, "", template_key=template_key, campaign=campaign)
    lead.update(drafts)

    active_campaign = _resolve_campaign(campaign) or {}
    lead["Draft_Config_Version"] = str(active_campaign.get("draft_config_version") or active_campaign.get("config_version") or "1")
    lead["Draft_Stale"] = "0"
    if (lead.get("Status") or "new").strip() == "new":
        lead["Status"] = "draft_ready"
    return drafts


def refresh_saved_drafts(
    leads: list[dict],
    campaign: dict | None = None,
    lead_ids: set[str] | None = None,
    pending_only: bool = True,
) -> int:
    count = 0
    for lead in leads:
        lead_id = (lead.get("ID") or "").strip()
        if lead_ids is not None and lead_id not in lead_ids:
            continue
        if pending_only and not is_pending_template_refresh_target(lead):
            continue
        if not has_saved_drafts(lead):
            continue
        rerender_saved_draft(lead, campaign=campaign)
        count += 1
    return count
