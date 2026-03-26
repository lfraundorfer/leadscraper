#!/usr/bin/env python3
"""
crm.py – CLI entry point for the Installateur Wien CRM.

Commands:
  migrate              One-time: add CRM columns + assign IDs to existing CSV
  enrich [--id X]      Fill owner names from FirmenABC
  research [--id X]    Website + Google Reviews + rank + competitors
  analyze [--id X] [--no-review]  Generate AI messages with Claude
  daily [--limit N]    Show today's action list
  log <ID> <outcome>   Log a contact attempt
  send-email <ID>      Send the generated email via SMTP
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
    from crm_store import migrate
    migrate()


def cmd_enrich(args) -> None:
    from crm_enrich import main
    main(force=args.force, single_id=args.id or "")


def cmd_research(args) -> None:
    from crm_research import main
    main(force=args.force, single_id=args.id or "", from_id=args.from_id or "")


def cmd_analyze(args) -> None:
    from crm_analyze import main
    main(force=args.force, single_id=args.id or "", no_review=args.no_review, limit=args.limit, gpt_hooks=args.gpt_hooks)


def cmd_daily(args) -> None:
    from crm_daily import show_daily
    show_daily(limit=args.limit)


def cmd_log(args) -> None:
    from crm_tracker import log_contact
    log_contact(args.id, args.outcome, notes=args.notes, channel=args.channel or "")


def cmd_send_email(args) -> None:
    from crm_mailer import send_email
    send_email(args.id, dry_run=args.dry_run)


def cmd_stats(_args) -> None:
    from crm_daily import show_stats
    show_stats()


def cmd_generate_hooks(args) -> None:
    from crm_templates import generate_hooks_library
    generate_hooks_library(force=args.force)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="crm",
        description="Installateur Wien CRM – AI-powered outreach automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crm.py migrate
  python crm.py enrich --id LEAD-001
  python crm.py research --id LEAD-001
  python crm.py analyze --id LEAD-001
  python crm.py analyze --no-review        # auto-approve all drafts
  python crm.py daily
  python crm.py log LEAD-001 sent --channel email --notes "Sent intro email"
  python crm.py log LEAD-002 called --notes "Left voicemail"
  python crm.py send-email LEAD-001 --dry-run
  python crm.py stats
""",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # migrate
    subparsers.add_parser("migrate", help="Add CRM columns + assign IDs to CSV (run once)")

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
    p = subparsers.add_parser("analyze", help="Generate AI messages with Claude")
    p.add_argument("--id", default="", help="Only analyze a single lead by ID")
    p.add_argument("--force", action="store_true", help="Re-analyze even if already done")
    p.add_argument("--no-review", action="store_true", help="Auto-approve drafts (skip review step)")
    p.add_argument("--limit", type=int, default=0, help="Only analyze the first N leads (e.g. --limit 50)")
    p.add_argument("--gpt-hooks", action="store_true", help="Generate custom hook via GPT (default: use pre-written library)")

    # daily
    p = subparsers.add_parser("daily", help="Show today's action list")
    p.add_argument("--limit", type=int, default=10, help="Max leads to show (default: 10)")

    # log
    p = subparsers.add_parser("log", help="Log a contact attempt result")
    p.add_argument("id", help="Lead ID, e.g. LEAD-001")
    p.add_argument(
        "outcome",
        choices=["sent", "called", "voicemail", "no_answer", "replied", "meeting", "won", "lost", "blacklist"],
        help="What happened",
    )
    p.add_argument("--notes", default="", help="Optional notes")
    p.add_argument("--channel", default="", choices=["email", "phone", "whatsapp", ""],
                   help="Override channel (auto-detected from outcome if omitted)")

    # send-email
    p = subparsers.add_parser("send-email", help="Send the generated email via SMTP")
    p.add_argument("id", help="Lead ID, e.g. LEAD-001")
    p.add_argument("--dry-run", action="store_true", help="Print email without sending")

    # stats
    subparsers.add_parser("stats", help="Show pipeline overview stats")

    # generate-hooks
    p = subparsers.add_parser("generate-hooks", help="Generate hook library via GPT (run once, saves hooks_library.json)")
    p.add_argument("--force", action="store_true", help="Regenerate even if hooks_library.json already exists")

    args = parser.parse_args()

    commands = {
        "migrate": cmd_migrate,
        "enrich": cmd_enrich,
        "research": cmd_research,
        "analyze": cmd_analyze,
        "daily": cmd_daily,
        "log": cmd_log,
        "send-email": cmd_send_email,
        "stats": cmd_stats,
        "generate-hooks": cmd_generate_hooks,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
