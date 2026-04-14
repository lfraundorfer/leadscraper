#!/usr/bin/env python3
"""
crm.py - CLI entry point for the multi-campaign CRM.

Commands:
  migrate              One-time: add CRM columns + assign IDs to existing CSV
  bootstrap-postgres   Import existing local campaigns/leads into hosted Postgres
  enrich [--id X]      Fill owner names from FirmenABC
  research [--id X]    Website + Google Reviews + rank + competitors
  analyze [--id X] [--no-review]  Generate AI messages with OpenAI
  refresh-drafts       Re-render saved drafts from current templates only
  daily [--limit N]    Show today's action list
  log <ID> <outcome>   Log a contact attempt
  send-email <ID>      Send the generated email via SMTP
  sync-mailbox         Sync mailbox replies and daemon notices via IMAP
  send-scheduled       Process queued email sends that are due
  stats                Show pipeline overview
"""

from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

# Load .env file before anything else
load_dotenv()


def cmd_migrate(_args) -> None:
    from campaign_service import get_active_campaign, mark_campaign_stage_run
    from crm_store import migrate
    count = migrate()
    if count:
        mark_campaign_stage_run(get_active_campaign()["id"], "migrated")


def cmd_enrich(args) -> None:
    from crm_enrich import main
    main(force=args.force, single_id=args.id or "")


def cmd_research(args) -> None:
    from crm_research import main
    main(force=args.force, single_id=args.id or "", from_id=args.from_id or "")


def cmd_analyze(args) -> None:
    from crm_analyze import main
    main(force=args.force, single_id=args.id or "", no_review=args.no_review, limit=args.limit, gpt_hooks=args.gpt_hooks)


def cmd_refresh_drafts(args) -> None:
    from campaign_service import get_active_campaign
    from crm_store import load_leads, save_leads
    from crm_templates import refresh_saved_drafts

    campaign = get_active_campaign()
    leads = load_leads(campaign=campaign)
    lead_ids = {args.id.strip()} if args.id else None
    count = refresh_saved_drafts(leads, campaign=campaign, lead_ids=lead_ids, pending_only=not args.all_with_drafts)
    if count:
        save_leads(leads, campaign=campaign)
    scope = f" in {campaign['id']}" if campaign.get("id") else ""
    print(f"Refreshed {count} draft(s){scope}.")


def cmd_daily(args) -> None:
    from crm_daily import show_daily
    show_daily(limit=args.limit)


def cmd_log(args) -> None:
    from crm_tracker import log_contact
    log_contact(args.id, args.outcome, notes=args.notes, channel=args.channel or "")


def cmd_send_email(args) -> None:
    from crm_mailer import send_email
    send_email(args.id, dry_run=args.dry_run)


def cmd_sync_mailbox(args) -> None:
    from crm_mail_sync import sync_mailbox

    summary = sync_mailbox(lookback_hours=args.lookback_hours)
    print(
        "Mailbox sync complete: "
        f"relevant={summary['inbox_relevant']} "
        f"matched={summary['matched']} "
        f"unmatched={summary['unmatched']} "
        f"unknown={summary['unknown_marked']}"
    )


def cmd_stats(_args) -> None:
    from crm_daily import show_stats
    show_stats()


def cmd_generate_hooks(args) -> None:
    from crm_templates import generate_hooks_library
    generate_hooks_library(force=args.force)


def cmd_campaigns(_args) -> None:
    from campaign_service import list_campaigns, load_registry

    registry = load_registry()
    active_id = registry.get("active_campaign_id", "")
    for campaign in list_campaigns():
        marker = "*" if campaign["id"] == active_id else " "
        print(f"{marker} {campaign['id']:<24} {campaign.get('label', '')}")


def cmd_campaign_create(args) -> None:
    from campaign_service import create_campaign

    campaign = create_campaign(args.keyword, args.location, activate=True)
    print(f"Campaign ready: {campaign['id']} -> {campaign.get('label', '')}")


def cmd_campaign_activate(args) -> None:
    from campaign_service import set_active_campaign

    campaign = set_active_campaign(args.campaign_id)
    print(f"Active campaign: {campaign['id']} -> {campaign.get('label', '')}")


def cmd_campaign_queries(args) -> None:
    from campaign_service import get_campaign, get_active_campaign, list_campaign_extra_queries

    campaign = get_campaign(args.campaign_id) if args.campaign_id else get_active_campaign()
    extra_queries = list_campaign_extra_queries(campaign["id"])
    print(f"Campaign: {campaign['id']} -> {campaign.get('label', '')}")
    print(f"Primary query: {campaign.get('keyword', '')} / {campaign.get('location', '')}")
    if not extra_queries:
        print("Extra queries: none")
        return
    print("Extra queries:")
    for item in extra_queries:
        print(f"- {item['keyword']} / {item['location']}")


def cmd_campaign_query_add(args) -> None:
    from campaign_service import add_campaign_extra_query

    campaign = add_campaign_extra_query(args.keyword, args.location, campaign_id=args.campaign_id or "")
    print(f"Added extra query to {campaign['id']}: {args.keyword.strip()} / {args.location.strip()}")


def cmd_campaign_query_remove(args) -> None:
    from campaign_service import remove_campaign_extra_query

    campaign = remove_campaign_extra_query(args.keyword, args.location, campaign_id=args.campaign_id or "")
    print(f"Removed extra query from {campaign['id']}: {args.keyword.strip()} / {args.location.strip()}")


def cmd_scrape(args) -> None:
    from campaign_service import create_campaign, get_active_campaign, mark_campaign_stage_run
    from crm_scrape import scrape_campaign

    if bool(args.keyword) != bool(args.location):
        print("Use both --keyword and --location together, or neither.")
        return

    if args.keyword and args.location:
        campaign = create_campaign(args.keyword, args.location, activate=True)
    else:
        campaign = get_active_campaign()

    result = scrape_campaign(
        campaign,
        pages=args.pages,
        page_pause=args.page_pause,
        search_pause=args.search_pause,
        no_search=args.no_search,
        visible=args.visible,
        dump_html=args.dump_html,
        verbose=args.verbose,
    )
    mark_campaign_stage_run(campaign["id"], "scraped")
    print(f"Scraped {result['new_entries']} new entries into {result['output']}")


def cmd_bootstrap_postgres(args) -> None:
    import crm_backend as backend

    result = backend.bootstrap_postgres_from_files(force=args.force)
    print(f"Bootstrapped Postgres: {result['campaigns']} campaign(s), {result['leads']} lead(s).")


def cmd_send_scheduled(args) -> None:
    from crm_scheduled import main

    main(limit=args.limit, dry_run=args.dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="crm",
        description="Campaign CRM - AI-powered outreach automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crm.py migrate
  python crm.py bootstrap-postgres
  python crm.py enrich --id LEAD-001
  python crm.py research --id LEAD-001
  python crm.py analyze --id LEAD-001
  python crm.py analyze --no-review        # auto-approve all drafts
  python crm.py refresh-drafts             # refresh pending drafts from templates only
  python crm.py daily
  python crm.py log LEAD-001 sent --channel email --notes "Sent intro email"
  python crm.py log LEAD-002 called --notes "Left voicemail"
  python crm.py send-email LEAD-001 --dry-run
  python crm.py sync-mailbox --lookback-hours 24
  python crm.py send-scheduled --dry-run
  python crm.py stats
  python crm.py campaigns
  python crm.py campaign-create Schluesseldienst Wien
  python crm.py campaign-activate schluesseldienst_wien
  python crm.py campaign-queries
  python crm.py campaign-query-add Schluesseldienst Graz
  python crm.py campaign-query-remove Schluesseldienst Graz
  python crm.py scrape
""",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # migrate
    subparsers.add_parser("migrate", help="Add CRM columns + assign IDs to current campaign leads")

    p = subparsers.add_parser("bootstrap-postgres", help="Import existing campaigns and leads into Postgres")
    p.add_argument("--force", action="store_true", help="Replace existing Postgres campaign data before importing")

    # campaigns
    subparsers.add_parser("campaigns", help="List saved campaigns")

    p = subparsers.add_parser("campaign-create", help="Create a saved campaign from keyword + location")
    p.add_argument("keyword", help="Business keyword, e.g. Schluesseldienst")
    p.add_argument("location", help="Location, e.g. Wien")

    p = subparsers.add_parser("campaign-activate", help="Switch the active campaign")
    p.add_argument("campaign_id", help="Campaign id, e.g. schluesseldienst_wien")

    p = subparsers.add_parser("campaign-queries", help="List extra scrape queries for a campaign")
    p.add_argument("--campaign-id", default="", help="Optional campaign id; defaults to the active campaign")

    p = subparsers.add_parser("campaign-query-add", help="Add an extra keyword/location pair to a campaign")
    p.add_argument("keyword", help="Business keyword, e.g. Werbeagentur")
    p.add_argument("location", help="Location, e.g. Berlin")
    p.add_argument("--campaign-id", default="", help="Optional campaign id; defaults to the active campaign")

    p = subparsers.add_parser("campaign-query-remove", help="Remove an extra keyword/location pair from a campaign")
    p.add_argument("keyword", help="Business keyword, e.g. Werbeagentur")
    p.add_argument("location", help="Location, e.g. Berlin")
    p.add_argument("--campaign-id", default="", help="Optional campaign id; defaults to the active campaign")

    p = subparsers.add_parser("scrape", help="Scrape Herold leads into the active campaign backend")
    p.add_argument("--keyword", default="", help="Optional keyword to create/activate before scraping")
    p.add_argument("--location", default="", help="Optional location to create/activate before scraping")
    p.add_argument("--pages", default="", help="Page range like 1-5")
    p.add_argument("--page-pause", type=float, default=4.0, help="Seconds between pages")
    p.add_argument("--search-pause", type=float, default=2.5, help="Seconds between searches")
    p.add_argument("--no-search", action="store_true", help="Skip website/firmenabc search fallback")
    p.add_argument("--visible", action="store_true", help="Show the browser window while scraping")
    p.add_argument("--dump-html", default="", help="Dump raw HTML to this directory")
    p.add_argument("--verbose", action="store_true", help="Verbose scraper logging")

    # enrich
    p = subparsers.add_parser("enrich", help="Fill owner names from FirmenABC")
    p.add_argument("--id", default="", help="Only enrich a single lead by ID")
    p.add_argument("--force", action="store_true", help="Re-enrich even if already done")

    # research
    p = subparsers.add_parser("research", help="Website + Google Reviews + rank + competitors")
    p.add_argument("--id", default="", help="Only research a single lead by ID")
    p.add_argument("--from", dest="from_id", default="", help="Resume from this ID (e.g. INSTWIEN-0581), skipping all leads before it")
    p.add_argument("--force", action="store_true", help="Re-research even if already done")

    # analyze
    p = subparsers.add_parser("analyze", help="Generate AI messages with OpenAI")
    p.add_argument("--id", default="", help="Only analyze a single lead by ID")
    p.add_argument("--force", action="store_true", help="Re-analyze even if already done")
    p.add_argument("--no-review", action="store_true", help="Auto-approve drafts (skip review step)")
    p.add_argument("--limit", type=int, default=0, help="Only analyze the first N leads (e.g. --limit 50)")
    p.add_argument("--gpt-hooks", action="store_true", help="Generate custom hook via GPT (default: use pre-written library)")

    # refresh-drafts
    p = subparsers.add_parser("refresh-drafts", help="Re-render saved drafts from current templates without refetching websites")
    p.add_argument("--id", default="", help="Only refresh a single lead by ID")
    p.add_argument("--all-with-drafts", action="store_true", help="Refresh every lead that already has drafts, including approved/contacted ones")

    # daily
    p = subparsers.add_parser("daily", help="Show today's action list")
    p.add_argument("--limit", type=int, default=10, help="Max leads to show (default: 10)")

    # log
    p = subparsers.add_parser("log", help="Log a contact attempt result")
    p.add_argument("id", help="Lead ID, e.g. LEAD-001")
    p.add_argument(
        "outcome",
        choices=["sent", "called", "voicemail", "no_answer", "replied", "meeting", "won", "done", "lost", "blacklist"],
        help="What happened",
    )
    p.add_argument("--notes", default="", help="Optional notes")
    p.add_argument("--channel", default="", choices=["email", "phone", "whatsapp", ""],
                   help="Override channel (auto-detected from outcome if omitted)")

    # send-email
    p = subparsers.add_parser("send-email", help="Send the generated email via SMTP")
    p.add_argument("id", help="Lead ID, e.g. LEAD-001")
    p.add_argument("--dry-run", action="store_true", help="Print email without sending")

    p = subparsers.add_parser("sync-mailbox", help="Sync mailbox replies and delivery notices via IMAP")
    p.add_argument("--lookback-hours", type=int, default=24, help="How far back to scan the mailbox (default: 24)")

    p = subparsers.add_parser("send-scheduled", help="Send all queued emails that are due right now")
    p.add_argument("--limit", type=int, default=100, help="Max queued emails to process (default: 100)")
    p.add_argument("--dry-run", action="store_true", help="Show what would send without sending anything")

    # stats
    subparsers.add_parser("stats", help="Show pipeline overview stats")

    # generate-hooks
    p = subparsers.add_parser("generate-hooks", help="Generate hook library via GPT (run once, saves hooks_library.json)")
    p.add_argument("--force", action="store_true", help="Regenerate even if hooks_library.json already exists")

    args = parser.parse_args()

    commands = {
        "migrate": cmd_migrate,
        "bootstrap-postgres": cmd_bootstrap_postgres,
        "campaigns": cmd_campaigns,
        "campaign-create": cmd_campaign_create,
        "campaign-activate": cmd_campaign_activate,
        "campaign-queries": cmd_campaign_queries,
        "campaign-query-add": cmd_campaign_query_add,
        "campaign-query-remove": cmd_campaign_query_remove,
        "scrape": cmd_scrape,
        "enrich": cmd_enrich,
        "research": cmd_research,
        "analyze": cmd_analyze,
        "refresh-drafts": cmd_refresh_drafts,
        "daily": cmd_daily,
        "log": cmd_log,
        "send-email": cmd_send_email,
        "sync-mailbox": cmd_sync_mailbox,
        "send-scheduled": cmd_send_scheduled,
        "stats": cmd_stats,
        "generate-hooks": cmd_generate_hooks,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
