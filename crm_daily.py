"""
crm_daily.py – CLI daily action list and pipeline stats.
"""

from __future__ import annotations

from collections import Counter
from datetime import date

from crm_store import load_leads, save_leads, VALID_STATUSES, TERMINAL_STATUSES
from crm_tracker import check_and_archive_stale

PRIORITY_LABELS = {1: "P1 🔴", 2: "P2 🟠", 3: "P3 🟡", 4: "P4 🟢", 5: "P5 ⚫"}
CHANNEL_EMOJI = {"email": "📧", "phone": "📞", "whatsapp": "💬", "none": "⛔"}


def show_daily(limit: int = 10) -> None:
    """Show today's action list — leads that need attention today."""
    leads = load_leads()
    if not leads:
        print("No leads. Run `python crm.py migrate` first.")
        return

    # Auto-archive stale leads
    leads, archived = check_and_archive_stale(leads)
    if archived:
        save_leads(leads)
        print(f"[Auto-archived {archived} lead(s) with no reply after 14 days → 'no_contact']\n")

    today = date.today().isoformat()
    actionable = []

    for lead in leads:
        status = lead.get("Status", "new")
        if status in TERMINAL_STATUSES or status == "no_contact":
            continue
        next_date = lead.get("Next_Action_Date", "")
        next_type = lead.get("Next_Action_Type", "")
        if not next_type or next_type == "none":
            continue
        if next_date and next_date > today:
            continue
        actionable.append(lead)

    # Sort by priority (asc), then next_action_date (asc)
    actionable.sort(key=lambda l: (int(l.get("Priority") or 5), l.get("Next_Action_Date") or ""))

    print(f"\n=== TODAY'S ACTIONS ({today}) ===\n")

    if not actionable:
        print("Nothing to do today! Check back tomorrow.")
        return

    # Header
    print(f"{'#':<3} {'ID':<10} {'Unternehmen':<30} {'Channel':<10} {'Pri':<7} {'Score':<7} {'★':<6} {'Rank'}")
    print("-" * 85)

    for i, lead in enumerate(actionable[:limit], 1):
        lid = lead.get("ID", "?")
        company = lead.get("Unternehmen", "")[:28]
        channel = lead.get("Next_Action_Type", "?")
        ch_emoji = CHANNEL_EMOJI.get(channel, "?")
        pri = lead.get("Priority", "?")
        pri_label = PRIORITY_LABELS.get(int(pri) if str(pri).isdigit() else 5, f"P{pri}")
        score = lead.get("Website_Score", "")
        score_str = f"{score}/10" if score else "no web"
        rating = lead.get("Google_Rating", "")
        rating_str = f"{rating}★" if rating else "-"
        rank = lead.get("Google_Rank_Position", "")
        rank_str = f"#{rank}" if rank and rank not in ("not_found", "error", "") else (rank or "-")

        print(f"{i:<3} {lid:<10} {company:<30} {ch_emoji} {channel:<8} {pri_label:<7} {score_str:<7} {rating_str:<6} {rank_str}")

    if len(actionable) > limit:
        print(f"\n  … and {len(actionable) - limit} more leads. Use --limit N to see more.")

    print("\n--- QUICK COMMANDS ---")
    if actionable:
        first = actionable[0]
        lid = first.get("ID", "LEAD-001")
        ch = first.get("Next_Action_Type", "email")
        if ch == "email":
            print(f"  python crm.py send-email {lid} [--dry-run]")
        else:
            print(f"  python crm.py log {lid} called --notes \"spoke to owner\"")
        print(f"  python crm.py log {lid} <sent|called|voicemail|no_answer|replied|meeting|won|lost>")


def show_stats() -> None:
    """Show pipeline overview stats."""
    leads = load_leads()
    if not leads:
        print("No leads.")
        return

    status_counts = Counter(l.get("Status", "new") for l in leads)
    priority_counts = Counter(l.get("Priority", "5") for l in leads)
    channel_counts = Counter(l.get("Channel_Used") or l.get("Next_Action_Type", "none") for l in leads)

    score_buckets = {"No website": 0, "1-3 (bad)": 0, "4-6 (avg)": 0, "7-10 (good)": 0, "Not analyzed": 0}
    for l in leads:
        s = l.get("Website_Score", "")
        cat = l.get("Website_Category", "")
        if cat == "none" or s == "0":
            score_buckets["No website"] += 1
        elif not s:
            score_buckets["Not analyzed"] += 1
        elif int(s) <= 3:
            score_buckets["1-3 (bad)"] += 1
        elif int(s) <= 6:
            score_buckets["4-6 (avg)"] += 1
        else:
            score_buckets["7-10 (good)"] += 1

    print(f"\n=== CRM PIPELINE STATS ===\n")
    print(f"Total leads: {len(leads)}\n")

    print("Pipeline:")
    for status in ["new", "draft_ready", "approved", "contacted", "replied", "meeting_scheduled", "won", "lost", "no_contact", "blacklist"]:
        n = status_counts.get(status, 0)
        bar = "█" * n
        print(f"  {status:<20} {n:>4}  {bar}")

    print("\nPriority:")
    for p in ["1", "2", "3", "4", "5"]:
        n = priority_counts.get(p, 0)
        label = PRIORITY_LABELS.get(int(p), f"P{p}")
        print(f"  {label:<12} {n:>4}")

    print("\nPrimary channel:")
    for ch, emoji in CHANNEL_EMOJI.items():
        n = channel_counts.get(ch, 0)
        print(f"  {emoji} {ch:<10} {n:>4}")

    print("\nWebsite scores:")
    for label, n in score_buckets.items():
        print(f"  {label:<20} {n:>4}")
