"""
campaign_service.py - Saved campaign registry and active campaign resolution.

Campaigns are stored under campaigns/<campaign_id>/ with a config.json and
campaign-local hooks/templates. The legacy installateur setup is bootstrapped
into the registry so existing data keeps working.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import crm_backend as backend

ROOT_DIR = Path(__file__).resolve().parent
CAMPAIGNS_DIR = ROOT_DIR / "campaigns"
REGISTRY_PATH = CAMPAIGNS_DIR / "registry.json"
LEGACY_CAMPAIGN_ID = "installateur_wien"
LAST_RUN_FIELDS = {
    "last_scraped_at",
    "last_migrated_at",
    "last_enriched_at",
    "last_researched_at",
    "last_analyzed_at",
}
RESEARCH_RELEVANT_FIELDS = {"keyword", "location", "rank_keyword_template"}


def slugify(value: str) -> str:
    text = (value or "").strip().lower()
    for old, new in [("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")]:
        text = text.replace(old, new)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def build_campaign_id(keyword: str, location: str) -> str:
    keyword_slug = slugify(keyword) or "campaign"
    location_slug = slugify(location) or "default"
    return f"{keyword_slug}_{location_slug}"


def build_id_prefix(keyword: str, location: str) -> str:
    keyword_part = re.sub(r"[^A-Z0-9]", "", slugify(keyword).upper())[:4] or "LEAD"
    location_part = re.sub(r"[^A-Z0-9]", "", slugify(location).upper())[:4] or "LOC"
    return f"{keyword_part}{location_part}"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return deepcopy(default)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _default_offer_summary(keyword: str, location: str) -> str:
    return (
        f"Wir entwickeln moderne, mobiloptimierte Websites fuer {keyword}-Betriebe "
        f"in {location}, mit klaren Kontaktwegen und besserer lokaler Sichtbarkeit."
    )


def _default_example_intro(keyword: str) -> str:
    return f"Wenn das relevant ist, schicke ich Ihnen gern 2-3 konkrete Ideen fuer einen {keyword}-Betrieb:"


def _campaign_layout(campaign_id: str) -> dict[str, Path]:
    campaign_dir = CAMPAIGNS_DIR / campaign_id
    return {
        "campaign_dir": campaign_dir,
        "csv_path": campaign_dir / "leads.csv",
        "hooks_library_path": campaign_dir / "hooks_library.json",
        "template_overrides_path": campaign_dir / "template_overrides.json",
        "archive_dir": campaign_dir / "archive",
    }


def _default_campaign_config(keyword: str, location: str, campaign_id: str | None = None) -> dict[str, Any]:
    cid = campaign_id or build_campaign_id(keyword, location)
    layout = _campaign_layout(cid)
    return {
        "id": cid,
        "label": f"{keyword} {location}".strip(),
        "keyword": keyword.strip(),
        "location": location.strip(),
        "csv_path": str(layout["csv_path"].relative_to(ROOT_DIR)),
        "id_prefix": build_id_prefix(keyword, location),
        "rank_keyword_template": "{keyword} {plz}",
        "price_default": os.getenv("PRICE_DEFAULT", "500"),
        "price_monthly": os.getenv("PRICE_MONTHLY", "25"),
        "turnaround_days": 14,
        "hooks_library_path": str(layout["hooks_library_path"].relative_to(ROOT_DIR)),
        "template_overrides_path": str(layout["template_overrides_path"].relative_to(ROOT_DIR)),
        "sender_name": os.getenv("SENDER_NAME", "Linus"),
        "sender_company": os.getenv("SENDER_COMPANY", "Digitalagentur"),
        "sender_website": os.getenv("SENDER_WEBSITE", ""),
        "sender_phone": os.getenv("SENDER_PHONE", ""),
        "sender_email": os.getenv("SENDER_EMAIL", ""),
        "offer_summary": _default_offer_summary(keyword, location),
        "example_intro": _default_example_intro(keyword),
        "service_singular": keyword.strip(),
        "service_plural": f"{keyword.strip()}-Betriebe",
        "extra_queries": [],
        "config_version": 1,
        "draft_config_version": 1,
        "research_config_version": 1,
        "last_scraped_at": "",
        "last_migrated_at": "",
        "last_enriched_at": "",
        "last_researched_at": "",
        "last_analyzed_at": "",
    }


def _legacy_campaign_config() -> dict[str, Any]:
    layout = _campaign_layout(LEGACY_CAMPAIGN_ID)
    return {
        "id": LEGACY_CAMPAIGN_ID,
        "label": "Installateur Wien",
        "keyword": "Installateur",
        "location": "Wien",
        "csv_path": str(layout["csv_path"].relative_to(ROOT_DIR)),
        "id_prefix": "INSTWIEN",
        "rank_keyword_template": "{keyword} {plz}",
        "price_default": os.getenv("PRICE_DEFAULT", "500"),
        "price_monthly": os.getenv("PRICE_MONTHLY", "25"),
        "turnaround_days": 14,
        "hooks_library_path": str(layout["hooks_library_path"].relative_to(ROOT_DIR)),
        "template_overrides_path": str(layout["template_overrides_path"].relative_to(ROOT_DIR)),
        "sender_name": os.getenv("SENDER_NAME", "Linus"),
        "sender_company": os.getenv("SENDER_COMPANY", "Digitalagentur Megaphonia"),
        "sender_website": os.getenv("SENDER_WEBSITE", "https://www.megaphonia.com"),
        "sender_phone": os.getenv("SENDER_PHONE", ""),
        "sender_email": os.getenv("SENDER_EMAIL", ""),
        "offer_summary": _default_offer_summary("Installateur", "Wien"),
        "example_intro": "Wenn das relevant ist, schicke ich Ihnen gern 2-3 konkrete Ideen dazu:",
        "service_singular": "Installateur",
        "service_plural": "Installateurbetriebe",
        "extra_queries": [],
        "config_version": 1,
        "draft_config_version": 1,
        "research_config_version": 1,
        "last_scraped_at": "",
        "last_migrated_at": "",
        "last_enriched_at": "",
        "last_researched_at": "",
        "last_analyzed_at": "",
    }


def _move_file_if_needed(source: Path, destination: Path) -> bool:
    if not source.exists() or source == destination or destination.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))
    return True


def _merge_directory_if_needed(source: Path, destination: Path) -> bool:
    if not source.exists() or not source.is_dir() or source == destination:
        return False
    destination.mkdir(parents=True, exist_ok=True)
    moved = False
    for item in sorted(source.iterdir(), key=lambda path: path.name):
        target = destination / item.name
        if item.is_dir():
            moved = _merge_directory_if_needed(item, target) or moved
            continue
        if target.exists():
            continue
        shutil.move(str(item), str(target))
        moved = True
    try:
        source.rmdir()
    except OSError:
        pass
    return moved


def _normalize_legacy_installateur_campaign() -> None:
    config_path = _campaign_config_path(LEGACY_CAMPAIGN_ID)
    config = _read_json(config_path, {})
    if not config:
        return

    layout = _campaign_layout(LEGACY_CAMPAIGN_ID)
    layout["archive_dir"].mkdir(parents=True, exist_ok=True)

    _move_file_if_needed(ROOT_DIR / "new_leads.csv", layout["csv_path"])
    _move_file_if_needed(ROOT_DIR / "hooks_library.json", layout["hooks_library_path"])
    _move_file_if_needed(ROOT_DIR / "template_overrides.json", layout["template_overrides_path"])
    _move_file_if_needed(ROOT_DIR / "installateur_wien.csv", layout["archive_dir"] / "installateur_wien.csv")

    for backup in sorted(ROOT_DIR.glob("new_leads.csv.bak_*")):
        _move_file_if_needed(backup, layout["archive_dir"] / backup.name)

    desired_paths = {
        "csv_path": str(layout["csv_path"].relative_to(ROOT_DIR)),
        "hooks_library_path": str(layout["hooks_library_path"].relative_to(ROOT_DIR)),
        "template_overrides_path": str(layout["template_overrides_path"].relative_to(ROOT_DIR)),
    }
    changed = False
    for key, value in desired_paths.items():
        if config.get(key) != value:
            config[key] = value
            changed = True
    if "portfolio_urls" in config:
        config.pop("portfolio_urls", None)
        changed = True
    if "portfolio_dir" in config:
        config.pop("portfolio_dir", None)
        changed = True
    if "flyer_path" in config:
        config.pop("flyer_path", None)
        changed = True
    if changed:
        _write_json(config_path, config)


def ensure_campaign_system() -> None:
    if backend.is_postgres_backend():
        backend.ensure_postgres_ready()
        return
    CAMPAIGNS_DIR.mkdir(parents=True, exist_ok=True)
    registry = _read_json(REGISTRY_PATH, {"active_campaign_id": "", "campaigns": {}})
    changed = False

    if LEGACY_CAMPAIGN_ID not in registry.get("campaigns", {}):
        legacy_config_path = CAMPAIGNS_DIR / LEGACY_CAMPAIGN_ID / "config.json"
        _write_json(legacy_config_path, _legacy_campaign_config())
        registry.setdefault("campaigns", {})[LEGACY_CAMPAIGN_ID] = {
            "label": "Installateur Wien",
            "config_path": str(legacy_config_path.relative_to(ROOT_DIR)),
        }
        changed = True

    if not registry.get("active_campaign_id"):
        registry["active_campaign_id"] = LEGACY_CAMPAIGN_ID
        changed = True

    _normalize_legacy_installateur_campaign()

    if changed:
        _write_json(REGISTRY_PATH, registry)


def load_registry() -> dict[str, Any]:
    if backend.is_postgres_backend():
        return backend.postgres_load_registry()
    ensure_campaign_system()
    registry = _read_json(REGISTRY_PATH, {"active_campaign_id": LEGACY_CAMPAIGN_ID, "campaigns": {}})
    registry.setdefault("campaigns", {})
    if not registry.get("active_campaign_id"):
        registry["active_campaign_id"] = LEGACY_CAMPAIGN_ID
    return registry


def save_registry(registry: dict[str, Any]) -> None:
    if backend.is_postgres_backend():
        active_id = str(registry.get("active_campaign_id") or "").strip()
        if active_id:
            backend.postgres_set_active_campaign_id(active_id)
        return
    _write_json(REGISTRY_PATH, registry)


def resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT_DIR / path


def _campaign_layout_for_config(campaign: dict[str, Any]) -> dict[str, Path]:
    campaign_id = campaign.get("id") or build_campaign_id(campaign.get("keyword", ""), campaign.get("location", ""))
    return _campaign_layout(campaign_id)


def resolve_csv_path(campaign: dict[str, Any]) -> str:
    default_path = _campaign_layout_for_config(campaign)["csv_path"]
    return str(resolve_path(campaign.get("csv_path") or default_path))


def resolve_active_csv_path() -> str:
    return resolve_csv_path(get_active_campaign())


def resolve_campaign_file(campaign: dict[str, Any], key: str) -> str:
    return str(resolve_path(campaign.get(key, "")))


def get_campaign(campaign_id: str) -> dict[str, Any]:
    if backend.is_postgres_backend():
        return backend.postgres_get_campaign(campaign_id)
    registry = load_registry()
    meta = registry.get("campaigns", {}).get(campaign_id)
    if not meta:
        raise KeyError(f"Unknown campaign: {campaign_id}")
    config_path = resolve_path(meta["config_path"])
    config = _read_json(config_path, {})
    if not config:
        raise FileNotFoundError(f"Campaign config missing: {config_path}")
    config.setdefault("extra_queries", [])
    config.setdefault("config_version", 1)
    config.setdefault("draft_config_version", config.get("config_version", 1))
    config.setdefault("research_config_version", config.get("config_version", 1))
    return config


def list_campaigns() -> list[dict[str, Any]]:
    if backend.is_postgres_backend():
        return backend.postgres_list_campaigns()
    registry = load_registry()
    campaigns: list[dict[str, Any]] = []
    for cid in sorted(registry.get("campaigns", {})):
        try:
            config = get_campaign(cid)
        except Exception:
            continue
        campaigns.append(config)
    return campaigns


def get_active_campaign() -> dict[str, Any]:
    if backend.is_postgres_backend():
        return backend.postgres_get_active_campaign()
    registry = load_registry()
    active_id = registry.get("active_campaign_id") or LEGACY_CAMPAIGN_ID
    return get_campaign(active_id)


def set_active_campaign(campaign_id: str) -> dict[str, Any]:
    if backend.is_postgres_backend():
        backend.postgres_set_active_campaign_id(campaign_id)
        return backend.postgres_get_campaign(campaign_id)
    registry = load_registry()
    if campaign_id not in registry.get("campaigns", {}):
        raise KeyError(f"Unknown campaign: {campaign_id}")
    registry["active_campaign_id"] = campaign_id
    save_registry(registry)
    return get_campaign(campaign_id)


def _campaign_config_path(campaign_id: str) -> Path:
    return CAMPAIGNS_DIR / campaign_id / "config.json"


def save_campaign_config(config: dict[str, Any]) -> dict[str, Any]:
    if backend.is_postgres_backend():
        return backend.postgres_save_campaign(config)
    config_path = _campaign_config_path(config["id"])
    _write_json(config_path, config)
    registry = load_registry()
    registry.setdefault("campaigns", {})[config["id"]] = {
        "label": config.get("label", config["id"]),
        "config_path": str(config_path.relative_to(ROOT_DIR)),
    }
    save_registry(registry)
    return config


def create_campaign(keyword: str, location: str, activate: bool = True) -> dict[str, Any]:
    if backend.is_postgres_backend():
        campaign_id = build_campaign_id(keyword, location)
        try:
            config = backend.postgres_get_campaign(campaign_id)
        except Exception:
            config = _default_campaign_config(keyword, location, campaign_id=campaign_id)
            backend.postgres_save_campaign(config)
        config = ensure_campaign_copy_defaults(config)
        if activate:
            backend.postgres_set_active_campaign_id(campaign_id)
        return config
    ensure_campaign_system()
    campaign_id = build_campaign_id(keyword, location)
    campaign_dir = CAMPAIGNS_DIR / campaign_id
    campaign_dir.mkdir(parents=True, exist_ok=True)
    try:
        config = get_campaign(campaign_id)
    except Exception:
        config = _default_campaign_config(keyword, location, campaign_id=campaign_id)
        save_campaign_config(config)
    config = ensure_campaign_copy_defaults(config)

    if activate:
        set_active_campaign(campaign_id)
    return config


def update_campaign(campaign_id: str, updates: dict[str, Any], bump_version: bool = True) -> dict[str, Any]:
    config = get_campaign(campaign_id)
    meaningful_updates = {k: v for k, v in updates.items() if config.get(k) != v}
    if meaningful_updates:
        next_keyword = meaningful_updates.get("keyword", config.get("keyword", ""))
        next_location = meaningful_updates.get("location", config.get("location", ""))
        if ("keyword" in meaningful_updates or "location" in meaningful_updates) and "id_prefix" not in meaningful_updates:
            meaningful_updates["id_prefix"] = build_id_prefix(next_keyword, next_location)
        config.update(meaningful_updates)
        if bump_version and any(k not in LAST_RUN_FIELDS for k in meaningful_updates):
            config["config_version"] = int(config.get("config_version") or 0) + 1
            config["draft_config_version"] = int(config.get("draft_config_version") or 0) + 1
            if any(k in RESEARCH_RELEVANT_FIELDS for k in meaningful_updates):
                config["research_config_version"] = int(config.get("research_config_version") or 0) + 1
        save_campaign_config(config)
    return config


def _normalize_campaign_query(keyword: str, location: str) -> dict[str, str]:
    normalized_keyword = (keyword or "").strip()
    normalized_location = (location or "").strip()
    if not normalized_keyword or not normalized_location:
        raise ValueError("Both keyword and location are required.")
    return {"keyword": normalized_keyword, "location": normalized_location}


def _campaign_query_key(keyword: str, location: str) -> tuple[str, str]:
    return (slugify(keyword), slugify(location))


def list_campaign_extra_queries(campaign_id: str = "") -> list[dict[str, str]]:
    campaign = get_campaign(campaign_id) if campaign_id else get_active_campaign()
    extra_queries = campaign.get("extra_queries")
    if not isinstance(extra_queries, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in extra_queries:
        if not isinstance(item, dict):
            continue
        keyword = str(item.get("keyword") or "").strip()
        location = str(item.get("location") or "").strip()
        if keyword and location:
            normalized.append({"keyword": keyword, "location": location})
    return normalized


def add_campaign_extra_query(keyword: str, location: str, campaign_id: str = "") -> dict[str, Any]:
    campaign = get_campaign(campaign_id) if campaign_id else get_active_campaign()
    new_query = _normalize_campaign_query(keyword, location)
    primary_key = _campaign_query_key(campaign.get("keyword", ""), campaign.get("location", ""))
    new_key = _campaign_query_key(new_query["keyword"], new_query["location"])
    if new_key == primary_key:
        raise ValueError("That query already matches the campaign's primary keyword/location.")

    extra_queries = list_campaign_extra_queries(campaign.get("id", ""))
    if any(_campaign_query_key(item["keyword"], item["location"]) == new_key for item in extra_queries):
        raise ValueError("That extra query already exists on this campaign.")

    updated = extra_queries + [new_query]
    return update_campaign(campaign["id"], {"extra_queries": updated}, bump_version=False)


def remove_campaign_extra_query(keyword: str, location: str, campaign_id: str = "") -> dict[str, Any]:
    campaign = get_campaign(campaign_id) if campaign_id else get_active_campaign()
    target = _campaign_query_key(keyword, location)
    extra_queries = list_campaign_extra_queries(campaign.get("id", ""))
    updated = [
        item
        for item in extra_queries
        if _campaign_query_key(item["keyword"], item["location"]) != target
    ]
    if len(updated) == len(extra_queries):
        raise ValueError("That extra query was not found on this campaign.")
    return update_campaign(campaign["id"], {"extra_queries": updated}, bump_version=False)


def bump_campaign_version(campaign_id: str) -> dict[str, Any]:
    config = get_campaign(campaign_id)
    config["config_version"] = int(config.get("config_version") or 0) + 1
    config["draft_config_version"] = int(config.get("draft_config_version") or 0) + 1
    return save_campaign_config(config)


def mark_campaign_stage_run(campaign_id: str, stage: str) -> dict[str, Any]:
    field = f"last_{stage}_at"
    if field not in LAST_RUN_FIELDS:
        return get_campaign(campaign_id)
    return update_campaign(
        campaign_id,
        {field: datetime.now().strftime("%Y-%m-%d %H:%M")},
        bump_version=False,
    )


def format_rank_keyword(campaign: dict[str, Any], plz: str = "") -> str:
    template = (campaign.get("rank_keyword_template") or "{keyword} {plz}").strip()
    keyword = (campaign.get("keyword") or "").strip()
    location = (campaign.get("location") or "").strip()
    context = {
        "keyword": keyword,
        "plz": (plz or "").strip(),
        "location": location,
    }
    rank_keyword = template.format(**context).strip()
    if "{plz}" in template and not plz:
        rank_keyword = f"{keyword} {location}".strip()
    return re.sub(r"\s+", " ", rank_keyword).strip()


def get_portfolio_dir(campaign: dict[str, Any]) -> str:
    return ""


def get_flyer_path(campaign: dict[str, Any]) -> str:
    return ""


def get_hooks_library_path(campaign: dict[str, Any]) -> str:
    if backend.is_postgres_backend():
        return f"postgres://campaigns/{campaign.get('id', '')}/hooks"
    default_path = _campaign_layout_for_config(campaign)["hooks_library_path"]
    return str(resolve_path(campaign.get("hooks_library_path") or default_path))


def get_template_overrides_path(campaign: dict[str, Any]) -> str:
    if backend.is_postgres_backend():
        return f"postgres://campaigns/{campaign.get('id', '')}/template-overrides"
    default_path = _campaign_layout_for_config(campaign)["template_overrides_path"]
    return str(resolve_path(campaign.get("template_overrides_path") or default_path))


def ensure_campaign_copy_defaults(config: dict[str, Any], overwrite: bool = False) -> dict[str, Any]:
    from crm_templates import build_default_campaign_copy_payloads, invalidate_campaign_copy_cache

    hooks_payload, template_payload = build_default_campaign_copy_payloads(config)
    changed = False

    if backend.is_postgres_backend():
        hooks_json = config.get("hooks_library_json")
        template_json = config.get("template_overrides_json")
        if overwrite or not isinstance(hooks_json, dict) or not hooks_json:
            config["hooks_library_json"] = hooks_payload
            changed = True
        if overwrite or not isinstance(template_json, dict) or not template_json:
            config["template_overrides_json"] = template_payload
            changed = True
        if changed:
            config = save_campaign_config(config)
    else:
        hooks_path = Path(get_hooks_library_path(config))
        template_path = Path(get_template_overrides_path(config))
        if overwrite or not hooks_path.exists():
            _write_json(hooks_path, hooks_payload)
            changed = True
        if overwrite or not template_path.exists():
            _write_json(template_path, template_payload)
            changed = True

    if changed:
        invalidate_campaign_copy_cache(config)
    return config
