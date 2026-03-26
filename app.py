"""
app.py – Streamlit CRM frontend for Installateur Wien outreach.

Pages:
  Dashboard    – Pipeline funnel + today's quick actions
  Review Queue – Review & edit AI drafts before approving
  Outreach     – Act on approved leads (send email, log calls, copy WhatsApp)
  All Leads    – Full filterable table with expand/edit

Run: streamlit run app.py
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from crm_templates import get_subject_options, parse_email_draft, compose_email_draft
from crm_store import load_leads, save_leads, ALL_COLUMNS, TERMINAL_STATUSES, get_bezirk
from crm_tracker import log_contact, check_and_archive_stale

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Installateur Wien CRM",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PRIORITY_COLOR = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🟢", 5: "⚫"}
CHANNEL_EMOJI = {"email": "📧", "phone": "📞", "whatsapp": "💬", "none": "⛔", "": "⛔"}
STATUS_EMOJI = {
    "new": "🆕", "draft_ready": "✍️", "approved": "✅",
    "contacted": "📤", "replied": "💬", "meeting_scheduled": "📅",
    "won": "🏆", "lost": "❌", "no_contact": "👻", "blacklist": "🚫",
}


@st.cache_data(ttl=5)
def cached_leads():
    return load_leads()


def reload():
    st.cache_data.clear()
    st.rerun()


def _generate_drafts(lead_ids: list[str], container) -> int:
    """Generate AI message drafts for the given lead IDs. Shows progress in container."""
    import datetime
    import time
    from openai import OpenAI
    from crm_analyze import (
        analyze_website, select_channel_and_priority, NO_WEBSITE_ANALYSIS
    )
    from crm_templates import render_drafts, pick_template_key, choose_hook
    from crm_research import categorize_website, fetch_and_clean_html, RATE_LIMIT_SEC
    from herold_scraper import HeroldFetcher

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        container.error("OPENAI_API_KEY not set in .env")
        return 0

    client = OpenAI(api_key=api_key)
    all_leads = load_leads()
    target_set = set(lead_ids)
    targets = [l for l in all_leads if l.get("ID") in target_set]
    if not targets:
        return 0

    prog = container.progress(0.0)
    status_txt = container.empty()
    fetcher = HeroldFetcher(headless=True)
    done = 0

    try:
        for i, lead in enumerate(targets):
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")[:35]
            status_txt.caption(f"⏳ {lid} – {company}…")
            prog.progress(i / len(targets))

            website = lead.get("Website", "").strip()
            category = lead.get("Website_Category") or categorize_website(website)
            lead["Website_Category"] = category

            if category == "real":
                html = fetch_and_clean_html(website, fetcher)
                if html:
                    analysis = analyze_website(lead.get("Unternehmen", ""), website, html, client)
                else:
                    lead["Website_Category"] = "fetch_error"
                    analysis = NO_WEBSITE_ANALYSIS
                time.sleep(RATE_LIMIT_SEC)
            else:
                analysis = NO_WEBSITE_ANALYSIS

            lead["Website_Score"] = str(analysis.get("score", 0))
            lead["Pain_Points"] = " | ".join(analysis.get("pain_points", []))
            lead["Pain_Categories"] = " | ".join(analysis.get("pain_categories", []))

            primary, _, priority = select_channel_and_priority(lead, analysis)
            lead["Channel_Used"] = primary
            lead["Priority"] = str(priority)
            if not lead.get("Next_Action_Date") and primary != "none":
                lead["Next_Action_Date"] = date.today().isoformat()
                lead["Next_Action_Type"] = primary

            template_key = pick_template_key(analysis.get("pain_categories", []), lead)
            hook = choose_hook(template_key, lead)
            msgs = render_drafts(lead, hook, "", template_key=template_key)
            lead.update(msgs)
            lead["Analyzed_At"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            if lead.get("Status", "new") == "new":
                lead["Status"] = "draft_ready"

            save_leads(all_leads)
            done += 1
            time.sleep(1)
    finally:
        fetcher.close()

    prog.progress(1.0)
    status_txt.caption(f"✅ Done — {done} draft(s) generated")
    return done


def score_bar(score_str: str) -> str:
    try:
        s = int(score_str)
    except (ValueError, TypeError):
        return "–"
    filled = "█" * s
    empty = "░" * (10 - s)
    color = "🔴" if s <= 3 else "🟠" if s <= 5 else "🟡" if s <= 7 else "🟢"
    return f"{color} {filled}{empty} {s}/10"


def _lead_header(lead: dict) -> str:
    lid = lead.get("ID", "?")
    company = lead.get("Unternehmen", "")
    adresse = lead.get("Adresse", "")
    plz, bezirk = get_bezirk(adresse)
    pri = lead.get("Priority", "?")
    pri_icon = PRIORITY_COLOR.get(int(pri) if str(pri).isdigit() else 5, "⚫")
    status = lead.get("Status", "new")
    st_icon = STATUS_EMOJI.get(status, "")
    return f"{lid}  |  **{company}**  |  {plz} {bezirk}  |  {st_icon} {status}  |  {pri_icon} P{pri}"


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

st.sidebar.title("🔧 Installateur CRM")
page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Review Queue", "Outreach", "All Leads"],
    label_visibility="collapsed",
)

# Auto-archive stale leads once per session
if "archived_stale" not in st.session_state:
    leads_raw = load_leads()
    leads_raw, n = check_and_archive_stale(leads_raw)
    if n:
        save_leads(leads_raw)
    st.session_state["archived_stale"] = True

# ---------------------------------------------------------------------------
# PAGE: Dashboard
# ---------------------------------------------------------------------------

if page == "Dashboard":
    st.title("Dashboard")

    leads = cached_leads()
    if not leads:
        st.warning("No leads found. Run `python crm.py migrate` first.")
        st.stop()

    today = date.today().isoformat()
    status_counts = Counter(l.get("Status", "new") for l in leads)

    # KPI row
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Leads", len(leads))
    col2.metric("Draft Ready", status_counts.get("draft_ready", 0), help="AI drafts waiting for your review")
    col3.metric("Approved", status_counts.get("approved", 0), help="Ready to send")
    col4.metric("In Progress", status_counts.get("contacted", 0) + status_counts.get("replied", 0))
    col5.metric("Won", status_counts.get("won", 0))

    st.divider()

    # Generate Drafts section
    needs_gen = [
        l for l in leads
        if l.get("Website_Category") and not l.get("Analyzed_At") and l.get("Status") == "new"
    ]
    if needs_gen:
        st.subheader(f"🤖 Generate Drafts ({len(needs_gen)} leads ready)")
        st.caption("These leads have research data but no AI drafts yet.")
        gen_col, _ = st.columns([1, 3])
        gen_all_btn = gen_col.button(f"Generate All ({len(needs_gen)})", type="primary", key="gen_all")
        gen_container = st.container()
        if gen_all_btn:
            count = _generate_drafts([l["ID"] for l in needs_gen], gen_container)
            if count:
                reload()
        st.divider()

    # Pipeline funnel
    st.subheader("Pipeline")
    pipeline_order = ["new", "draft_ready", "approved", "contacted", "replied", "meeting_scheduled", "won", "lost", "no_contact"]
    funnel_data = {s: status_counts.get(s, 0) for s in pipeline_order}

    cols = st.columns(len(pipeline_order))
    for col, (status, count) in zip(cols, funnel_data.items()):
        icon = STATUS_EMOJI.get(status, "")
        col.metric(f"{icon} {status.replace('_', ' ').title()}", count)

    st.divider()

    # Today's actions
    st.subheader(f"Today's Actions ({today})")
    actionable = [
        l for l in leads
        if l.get("Status") not in TERMINAL_STATUSES
        and l.get("Status") != "no_contact"
        and l.get("Next_Action_Type", "none") not in ("none", "")
        and (not l.get("Next_Action_Date") or l.get("Next_Action_Date") <= today)
    ]
    actionable.sort(key=lambda l: (int(l.get("Priority") or 5), l.get("Next_Action_Date") or ""))

    if not actionable:
        st.success("Nothing to do today!")
    else:
        st.info(f"**{len(actionable)} leads** need attention today")
        for lead in actionable[:5]:
            lid = lead.get("ID", "?")
            company = lead.get("Unternehmen", "")
            ch = lead.get("Next_Action_Type", "?")
            pri = lead.get("Priority", "?")
            score = lead.get("Website_Score", "")
            rating = lead.get("Google_Rating", "")
            rank = lead.get("Google_Rank_Position", "")

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                c1.markdown(f"**{lid}** – {company}")
                c2.markdown(f"{CHANNEL_EMOJI.get(ch, '?')} {ch}")
                c3.markdown(f"{PRIORITY_COLOR.get(int(pri) if str(pri).isdigit() else 5, '⚫')} P{pri}")
                score_s = f"{score}/10" if score else "no web"
                rating_s = f"★{rating}" if rating else "–"
                rank_s = f"#{rank}" if rank and rank not in ("not_found", "error", "") else (rank or "–")
                c4.markdown(f"{score_s} | {rating_s} | {rank_s}")

        if len(actionable) > 5:
            st.caption(f"+ {len(actionable)-5} more — go to **Review Queue** tab")

    st.divider()

    # Priority breakdown
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Priority Breakdown")
        priority_counts = Counter(l.get("Priority", "5") for l in leads)
        for p in ["1", "2", "3", "4", "5"]:
            n = priority_counts.get(p, 0)
            icon = PRIORITY_COLOR.get(int(p), "⚫")
            st.progress(n / max(len(leads), 1), text=f"{icon} P{p}: {n} leads")

    with col_b:
        st.subheader("Website Scores")
        no_web = sum(1 for l in leads if l.get("Website_Category") == "none" or l.get("Website_Score") == "0")
        bad = sum(1 for l in leads if l.get("Website_Score") and int(l.get("Website_Score") or 0) in range(1, 4))
        avg = sum(1 for l in leads if l.get("Website_Score") and int(l.get("Website_Score") or 0) in range(4, 7))
        good = sum(1 for l in leads if l.get("Website_Score") and int(l.get("Website_Score") or 0) >= 7)
        not_analyzed = sum(1 for l in leads if not l.get("Website_Score") and l.get("Website_Category") != "none")

        for label, count in [("🔴 No website", no_web), ("🟠 Bad (1-3)", bad), ("🟡 Average (4-6)", avg), ("🟢 Good (7-10)", good), ("⚫ Not analyzed", not_analyzed)]:
            st.text(f"{label}: {count}")

# ---------------------------------------------------------------------------
# PAGE: Review Queue
# ---------------------------------------------------------------------------

elif page == "Review Queue":
    st.title("✍️ Review Queue")
    st.caption("Review AI-generated drafts before approving. Edit freely — changes are saved on Approve.")

    leads = cached_leads()
    queue = [l for l in leads if l.get("Status") == "draft_ready"]
    queue.sort(key=lambda l: (int(l.get("Priority") or 5), l.get("Analyzed_At") or ""))

    if not queue:
        st.success("Queue is empty! All drafts have been reviewed.")
        st.info("Go to **Dashboard** and click **Generate All** to create new drafts.")
        st.stop()

    st.info(f"**{len(queue)} draft(s)** waiting for review")

    # Lead selector
    lead_options = {f"{l['ID']} – {l['Unternehmen'][:40]}": l for l in queue}
    selected_key = st.selectbox("Select lead to review:", list(lead_options.keys()))
    lead = lead_options[selected_key]

    st.divider()

    lid = lead.get("ID", "?")
    company = lead.get("Unternehmen", "")
    website = lead.get("Website", "")
    adresse = lead.get("Adresse", "")
    plz, bezirk = get_bezirk(adresse)
    score = lead.get("Website_Score", "")
    category = lead.get("Website_Category", "")
    pain_pts = [p for p in lead.get("Pain_Points", "").split(" | ") if p]
    rating = lead.get("Google_Rating", "")
    rev_count = lead.get("Google_Review_Count", "")
    snippets = [s for s in lead.get("Google_Review_Snippets", "").split(" | ") if s]
    rank_kw = lead.get("Google_Rank_Keyword", "")
    rank_pos = lead.get("Google_Rank_Position", "")
    map_pack = lead.get("Google_Map_Pack", "")
    competitors = [c for c in lead.get("Google_Competitors", "").split(" | ") if c]
    channel = lead.get("Channel_Used") or lead.get("Next_Action_Type", "?")
    pri = lead.get("Priority", "?")

    # Header
    pri_icon = PRIORITY_COLOR.get(int(pri) if str(pri).isdigit() else 5, "⚫")
    st.markdown(f"### {lid} – {company}")
    col_h1, col_h2, col_h3 = st.columns(3)
    col_h1.markdown(f"📍 {plz} {bezirk}")
    col_h2.markdown(f"{CHANNEL_EMOJI.get(channel, '?')} Primary: **{channel}**")
    col_h3.markdown(f"{pri_icon} Priority: **P{pri}**")

    st.divider()

    # Two-column layout: research left, drafts right
    left, right = st.columns([2, 3])

    with left:
        st.subheader("📊 Research Summary")

        # Website
        with st.expander("🌐 Website", expanded=True):
            if category == "none":
                st.error("No website found")
            elif category == "directory":
                st.warning(f"Directory link only: {website}")
            elif category == "fetch_error":
                st.warning(f"Couldn't fetch: {website}")
            else:
                st.markdown(f"[{website}]({website})")
                if score:
                    st.markdown(score_bar(score))

            if pain_pts:
                st.markdown("**Pain points:**")
                for p in pain_pts:
                    st.markdown(f"- {p}")

            # Mobile screenshot
            if lead.get("Mobile_Screenshot") == "yes":
                ss_path = f"screenshots/{lid}_mobile.png"
                if os.path.exists(ss_path):
                    st.image(ss_path, caption="📱 Mobile view", width="stretch")

        # Google Reviews
        with st.expander("⭐ Google Reviews", expanded=True):
            if rating or rev_count:
                stars = float(rating) if rating else 0
                st.markdown(f"**{'★' * int(stars)}{'☆' * (5 - int(stars))} {rating}** ({rev_count} Bewertungen)")
                if int(rev_count or 0) < 5:
                    st.error("Very few reviews — strong pain point!")
                elif float(rating or 5) < 4.0:
                    st.warning("Below-average rating")
            else:
                st.info("No review data collected")
            if snippets:
                for s in snippets:
                    if s.startswith("[NEG]"):
                        st.error(f'🚨 "{s[5:].strip()}"')
                    else:
                        st.caption(f'"{s}"')

        # Google Rank
        with st.expander("🔍 Google Rank", expanded=True):
            if rank_kw:
                st.markdown(f"**Keyword:** `{rank_kw}`")
                if rank_pos == "not_found":
                    st.error("Not found in top 10!")
                elif rank_pos and rank_pos.isdigit():
                    pos = int(rank_pos)
                    if pos <= 3:
                        st.success(f"Position #{rank_pos} ✓")
                    elif pos <= 7:
                        st.warning(f"Position #{rank_pos}")
                    else:
                        st.error(f"Position #{rank_pos} (barely visible)")
                if map_pack == "yes":
                    st.success("✓ In Google Map Pack (3-pack)")
                elif map_pack == "no":
                    st.warning("Not in local 3-pack")
                if competitors:
                    st.markdown("**Competitors ranking ahead:**")
                    for c in competitors:
                        st.markdown(f"- {c}")
            else:
                st.info("No rank data collected")

        # Contact info
        with st.expander("📇 Contact Info"):
            st.text(f"Name:  {lead.get('Kontaktname', '–') or '–'}")
            st.text(f"Email: {lead.get('Email', '–') or '–'}")
            st.text(f"Tel:   {lead.get('TelNr', '–') or '–'}")
            if website and website not in ("X", ""):
                st.markdown(f"[Open website ↗]({website})")
            maps = lead.get("Google_Maps_Link", "")
            if maps:
                st.markdown(f"[Google Maps ↗]({maps})")

    with right:
        st.subheader("✏️ Message Drafts")
        st.caption("Pick your preferred channel, edit any draft, then approve.")

        # Portfolio thumbnails (reference for Linus)
        portfolio_imgs = sorted([
            f for f in os.listdir("portfolio") if f.lower().endswith(".png")
        ]) if os.path.isdir("portfolio") else []
        if portfolio_imgs:
            with st.expander("🖼️ My Portfolio (reference)", expanded=False):
                cols_p = st.columns(min(len(portfolio_imgs), 3))
                for i, img in enumerate(portfolio_imgs[:3]):
                    cols_p[i].image(f"portfolio/{img}", caption=img, width="stretch")

        from crm_store import is_mobile
        has_mobile = is_mobile(lead.get("TelNr", ""))
        has_email = bool(lead.get("Email", "").strip())
        has_phone = bool(lead.get("TelNr", "").strip())

        # Build available channel options
        channel_options = []
        if has_email:
            channel_options.append("📧 Email")
        if has_mobile:
            channel_options.append("💬 WhatsApp")
        if has_phone:
            channel_options.append("📞 Phone call")

        # Pre-select the AI-suggested channel
        ai_channel = lead.get("Channel_Used") or lead.get("Next_Action_Type", "")
        channel_map = {"email": "📧 Email", "whatsapp": "💬 WhatsApp", "phone": "📞 Phone call"}
        default_option = channel_map.get(ai_channel, channel_options[0] if channel_options else "📧 Email")
        default_idx = channel_options.index(default_option) if default_option in channel_options else 0

        if channel_options:
            selected_channel_label = st.radio(
                "Which channel do you want to use for first contact?",
                channel_options,
                index=default_idx,
                horizontal=True,
                key=f"ch_{lid}",
            )
            selected_channel = {"📧 Email": "email", "💬 WhatsApp": "whatsapp", "📞 Phone call": "phone"}[selected_channel_label]
        else:
            st.error("No contact channels available for this lead.")
            selected_channel = "none"

        st.divider()

        current_subject, current_email_body = parse_email_draft(lead.get("Email_Draft", ""))
        subject_options = get_subject_options(lead, current_subject=current_subject)
        subject_index = subject_options.index(current_subject) if current_subject in subject_options else 0

        with st.form(key=f"approve_{lid}"):
            selected_subject = st.selectbox(
                "📧 Subject",
                options=subject_options,
                index=subject_index,
                key=f"subject_{lid}",
                help="One of 20 personalized subject lines for this lead. You can switch before approving.",
            )

            email_body = st.text_area(
                "📧 Email Body",
                value=current_email_body,
                height=250,
                key=f"email_body_{lid}",
            )

            # WhatsApp
            if has_mobile:
                wa_draft = st.text_area(
                    "💬 WhatsApp Draft",
                    value=lead.get("WhatsApp_Draft", ""),
                    height=110,
                    key=f"wa_{lid}",
                )
            else:
                wa_draft = lead.get("WhatsApp_Draft", "")
                st.caption("💬 WhatsApp not available (no mobile number)")

            # Phone script
            phone_script = st.text_area(
                "📞 Phone Script",
                value=lead.get("Phone_Script", ""),
                height=230,
                key=f"phone_{lid}",
            )

            # Action buttons
            b1, b2, b3 = st.columns(3)
            approve = b1.form_submit_button("✅ Approve & Next", type="primary", width="stretch")
            skip = b2.form_submit_button("⏭ Skip", width="stretch")
            blacklist = b3.form_submit_button("🚫 Blacklist", width="stretch")

        if approve:
            email_draft = compose_email_draft(selected_subject, email_body)
            all_leads = load_leads()
            for l in all_leads:
                if l.get("ID") == lid:
                    l["Email_Draft"] = email_draft
                    l["WhatsApp_Draft"] = wa_draft
                    l["Phone_Script"] = phone_script
                    l["Channel_Used"] = selected_channel
                    l["Next_Action_Type"] = selected_channel
                    l["Status"] = "approved"
                    l["Drafts_Approved"] = "1"
                    break
            save_leads(all_leads)
            st.success(f"✅ {lid} approved via {selected_channel}!")
            reload()

        if skip:
            st.info(f"Skipped {lid} — will appear again next time.")
            reload()

        if blacklist:
            all_leads = load_leads()
            for l in all_leads:
                if l.get("ID") == lid:
                    l["Status"] = "blacklist"
                    l["Next_Action_Type"] = "none"
                    break
            save_leads(all_leads)
            st.warning(f"🚫 {lid} blacklisted.")
            reload()

# ---------------------------------------------------------------------------
# PAGE: Ready to Send (Phase 2)
# ---------------------------------------------------------------------------

elif page == "Outreach":
    from crm_mailer import send_email as _send_email, get_whatsapp_link
    from crm_tracker import log_contact as _log_contact

    st.title("📬 Outreach")

    leads = cached_leads()

    approved = [l for l in leads if l.get("Status") == "approved" and l.get("Drafts_Approved") == "1"]
    approved.sort(key=lambda l: (int(l.get("Priority") or 5), l.get("Unternehmen") or ""))

    if not approved:
        st.success("No approved leads yet — go to **Review Queue** to approve drafts.")
        st.stop()

    st.caption(f"{len(approved)} lead(s) ready to send")

    for lead in approved:
        lid = lead.get("ID", "?")
        company = lead.get("Unternehmen", "")
        channel = lead.get("Channel_Used") or lead.get("Next_Action_Type", "?")
        pri = lead.get("Priority", "?")
        pri_icon = PRIORITY_COLOR.get(int(pri) if str(pri).isdigit() else 5, "⚫")
        email_addr = lead.get("Email", "").strip()
        tel = lead.get("TelNr", "").strip()

        label = f"{lid} – {company[:40]}  |  {CHANNEL_EMOJI.get(channel, '?')} {channel}  |  {pri_icon} P{pri}"
        with st.expander(label):
            # Contact details header
            info_col, action_col = st.columns([3, 1])
            if channel == "email" and email_addr:
                info_col.caption(f"To: **{email_addr}**")
            elif channel in ("whatsapp", "phone") and tel:
                info_col.caption(f"Number: **{tel}**")

            # ── Email channel ──────────────────────────────────────────────
            if channel == "email":
                draft = lead.get("Email_Draft", "")
                if draft:
                    st.text_area("Email draft", value=draft, height=250, key=f"prev_email_{lid}")
                if email_addr:
                    send_col, log_col = st.columns(2)
                    if send_col.button("📧 Send Email", key=f"send_email_{lid}", type="primary"):
                        with st.spinner("Sending…"):
                            ok = _send_email(lid)
                        if ok:
                            st.success(f"Sent to {email_addr} ✓")
                            st.rerun()
                        else:
                            st.error("Send failed — check SMTP settings in .env")
                    if log_col.button("✅ Mark as Sent (manual)", key=f"log_email_{lid}"):
                        _log_contact(lid, "sent", channel="email")
                        st.success("Logged ✓")
                        st.rerun()
                else:
                    st.warning("No email address on file — cannot send.")
                    if st.button("✅ Mark as Sent (manual)", key=f"log_email_naddr_{lid}"):
                        _log_contact(lid, "sent", channel="email")
                        st.success("Logged ✓")
                        st.rerun()

            # ── WhatsApp channel ───────────────────────────────────────────
            elif channel == "whatsapp":
                draft = lead.get("WhatsApp_Draft", "")
                if draft:
                    st.text_area("WhatsApp message", value=draft, height=120, key=f"prev_wa_{lid}")
                if tel:
                    wa_link = get_whatsapp_link(lid)
                    wa_col, log_col = st.columns(2)
                    if wa_link:
                        wa_col.link_button("💬 Open WhatsApp", wa_link)
                    else:
                        wa_col.warning("Could not build WhatsApp link")
                    if log_col.button("✅ Mark as Sent", key=f"log_wa_{lid}"):
                        _log_contact(lid, "sent", channel="whatsapp")
                        st.success("Logged ✓")
                        st.rerun()
                else:
                    st.warning("No phone number — cannot open WhatsApp.")
                    if st.button("✅ Mark as Sent (manual)", key=f"log_wa_ntel_{lid}"):
                        _log_contact(lid, "sent", channel="whatsapp")
                        st.success("Logged ✓")
                        st.rerun()

            # ── Phone channel ──────────────────────────────────────────────
            elif channel == "phone":
                script = lead.get("Phone_Script", "")
                if script:
                    st.text_area("Phone script", value=script, height=280, key=f"prev_phone_{lid}")
                if tel:
                    st.markdown(f"📞 **[Call {tel}](tel:{tel})**")
                log_c1, log_c2, log_c3 = st.columns(3)
                if log_c1.button("✅ Called", key=f"log_called_{lid}"):
                    _log_contact(lid, "called", channel="phone")
                    st.success("Logged ✓"); st.rerun()
                if log_c2.button("📭 Voicemail", key=f"log_vm_{lid}"):
                    _log_contact(lid, "voicemail", channel="phone")
                    st.success("Logged ✓"); st.rerun()
                if log_c3.button("📵 No Answer", key=f"log_na_{lid}"):
                    _log_contact(lid, "no_answer", channel="phone")
                    st.success("Logged ✓"); st.rerun()

            # ── Other drafts (collapsed) ───────────────────────────────────
            other_drafts = []
            if channel != "email" and lead.get("Email_Draft"):
                other_drafts.append(("📧 Email draft", lead["Email_Draft"], 200))
            if channel != "whatsapp" and lead.get("WhatsApp_Draft"):
                other_drafts.append(("💬 WhatsApp draft", lead["WhatsApp_Draft"], 100))
            if channel != "phone" and lead.get("Phone_Script"):
                other_drafts.append(("📞 Phone script", lead["Phone_Script"], 200))

            if other_drafts:
                with st.expander("Other channel drafts"):
                    for lbl, content, h in other_drafts:
                        st.text_area(lbl, value=content, height=h, disabled=True, key=f"other_{lid}_{lbl[:8]}")

# ---------------------------------------------------------------------------
# PAGE: All Leads
# ---------------------------------------------------------------------------

elif page == "All Leads":
    st.title("📋 All Leads")

    leads = cached_leads()
    if not leads:
        st.warning("No leads. Run `python crm.py migrate` first.")
        st.stop()

    # Search
    search_q = st.text_input("🔎 Search", placeholder="Company name, address, contact, email…", label_visibility="collapsed")

    # Filters + Sort
    with st.expander("🔍 Filters & Sort", expanded=True):
        fc1, fc2, fc3, fc4, fc5 = st.columns(5)
        all_statuses = sorted({l.get("Status", "new") for l in leads})
        sel_status = fc1.multiselect("Status", all_statuses, default=[])

        all_priorities = sorted({l.get("Priority", "5") for l in leads if l.get("Priority")})
        sel_priority = fc2.multiselect("Priority", all_priorities, default=[])

        all_channels = sorted({l.get("Channel_Used") or l.get("Next_Action_Type", "none") for l in leads})
        sel_channel = fc3.multiselect("Channel", all_channels, default=[])

        bezirks = sorted({get_bezirk(l.get("Adresse", ""))[1] for l in leads if get_bezirk(l.get("Adresse", ""))[1]})
        sel_bezirk = fc4.multiselect("Bezirk", bezirks, default=[])

        sort_by = fc5.selectbox("Sort by", ["Priority", "Website Score", "Google Rating", "Company A-Z"])

    filtered = leads
    if search_q:
        q = search_q.lower()
        filtered = [
            l for l in filtered
            if q in (l.get("Unternehmen") or "").lower()
            or q in (l.get("Adresse") or "").lower()
            or q in (l.get("Kontaktname") or "").lower()
            or q in (l.get("Email") or "").lower()
        ]
    if sel_status:
        filtered = [l for l in filtered if l.get("Status", "new") in sel_status]
    if sel_priority:
        filtered = [l for l in filtered if l.get("Priority", "5") in sel_priority]
    if sel_channel:
        filtered = [l for l in filtered if (l.get("Channel_Used") or l.get("Next_Action_Type", "none")) in sel_channel]
    if sel_bezirk:
        filtered = [l for l in filtered if get_bezirk(l.get("Adresse", ""))[1] in sel_bezirk]

    if sort_by == "Website Score":
        filtered.sort(key=lambda l: -(int(l.get("Website_Score") or 0)))
    elif sort_by == "Google Rating":
        filtered.sort(key=lambda l: -(float(l.get("Google_Rating") or 0)))
    elif sort_by == "Company A-Z":
        filtered.sort(key=lambda l: l.get("Unternehmen") or "")
    else:
        filtered.sort(key=lambda l: (int(l.get("Priority") or 5), l.get("Unternehmen") or ""))

    st.caption(f"Showing {len(filtered)} of {len(leads)} leads")

    for lead in filtered:
        lid = lead.get("ID", "?")
        company = lead.get("Unternehmen", "")
        status = lead.get("Status", "new")
        pri = lead.get("Priority", "?")
        pri_icon = PRIORITY_COLOR.get(int(pri) if str(pri).isdigit() else 5, "⚫")
        st_icon = STATUS_EMOJI.get(status, "")
        channel = lead.get("Channel_Used") or lead.get("Next_Action_Type", "none")
        score = lead.get("Website_Score", "")

        label = f"{lid} | {company[:35]} | {st_icon} {status} | {pri_icon} P{pri} | {CHANNEL_EMOJI.get(channel, '?')} {channel}"
        with st.expander(label):
            c1, c2 = st.columns([2, 3])

            with c1:
                st.markdown("**Contact Info**")
                st.text(f"Name:    {lead.get('Kontaktname', '–') or '–'}")
                st.text(f"Email:   {lead.get('Email', '–') or '–'}")
                st.text(f"Tel:     {lead.get('TelNr', '–') or '–'}")
                st.text(f"Adresse: {lead.get('Adresse', '–') or '–'}")
                website = lead.get("Website", "")
                if website and website not in ("X", ""):
                    st.markdown(f"[Website ↗]({website})")
                maps = lead.get("Google_Maps_Link", "")
                if maps:
                    st.markdown(f"[Maps ↗]({maps})")
                firmenabc = lead.get("FirmenABC_Link", "")
                if firmenabc and "firmenabc.at" in firmenabc:
                    st.markdown(f"[FirmenABC ↗]({firmenabc})")

                st.divider()
                st.markdown("**Research**")
                st.text(f"Website Score:  {score}/10" if score else "Website Score: –")
                # Mobile screenshot
                if lead.get("Mobile_Screenshot") == "yes":
                    ss_path = f"screenshots/{lid}_mobile.png"
                    if os.path.exists(ss_path):
                        with st.expander("📱 Mobile view"):
                            st.image(ss_path, caption="Mobile screenshot (390px)")

                # Expandable Google Reviews
                rating = lead.get("Google_Rating", "") or ""
                rev_count = lead.get("Google_Review_Count", "") or ""
                snippets_raw = lead.get("Google_Review_Snippets", "") or ""
                snippets = [s.strip() for s in snippets_raw.split(" | ") if s.strip()]
                review_header = f"⭐ {rating}★ ({rev_count} reviews)" if rating else "⭐ Google Reviews (no data)"
                with st.expander(review_header):
                    if snippets:
                        for s in snippets:
                            if s.startswith("[NEG]"):
                                st.error(s[5:].strip())
                            else:
                                st.caption(f'"{s}"')
                    else:
                        st.caption("No review snippets collected yet.")

                rank_pos = lead.get("Google_Rank_Position", "")
                st.text(f"Google Rank:    #{rank_pos}" if rank_pos and rank_pos not in ("not_found", "error", "") else f"Google Rank:    {rank_pos or '–'}")

                pain_pts = [p for p in lead.get("Pain_Points", "").split(" | ") if p]
                if pain_pts:
                    st.markdown("**Pain Points:**")
                    for p in pain_pts:
                        st.caption(f"• {p}")

                st.divider()
                st.markdown("**CRM**")
                st.text(f"Status:       {status}")
                st.text(f"Contacts:     {lead.get('Contact_Count', '0')}")
                st.text(f"Last contact: {lead.get('Last_Contact_Date', '–') or '–'}")
                st.text(f"Next action:  {lead.get('Next_Action_Type', '–')} on {lead.get('Next_Action_Date', '–') or '–'}")

                price_default = os.getenv("PRICE_DEFAULT", "1490")
                price_edit = st.text_input("Price (€)", value=lead.get("Price") or price_default, key=f"price_{lid}")
                notes_edit = st.text_area("Notes", value=lead.get("Notes", ""), key=f"notes_{lid}", height=80)
                if st.button("Save", key=f"save_notes_{lid}"):
                    all_leads = load_leads()
                    for l in all_leads:
                        if l.get("ID") == lid:
                            l["Notes"] = notes_edit
                            l["Price"] = price_edit
                            break
                    save_leads(all_leads)
                    st.success("Saved!")
                    reload()

                if st.button("🚫 Blacklist", key=f"bl_{lid}"):
                    all_leads = load_leads()
                    for l in all_leads:
                        if l.get("ID") == lid:
                            l["Status"] = "blacklist"
                            l["Next_Action_Type"] = "none"
                            break
                    save_leads(all_leads)
                    st.warning(f"{lid} blacklisted")
                    reload()

            with c2:
                st.markdown("**Message Drafts**")
                email_draft = lead.get("Email_Draft", "")
                if email_draft:
                    st.text_area("Email", value=email_draft, height=200, disabled=True, key=f"all_email_{lid}")
                wa_draft = lead.get("WhatsApp_Draft", "")
                if wa_draft:
                    st.text_area("WhatsApp", value=wa_draft, height=100, disabled=True, key=f"all_wa_{lid}")
                phone_script = lead.get("Phone_Script", "")
                if phone_script:
                    st.text_area("Phone Script", value=phone_script, height=200, disabled=True, key=f"all_phone_{lid}")

                if not email_draft and not wa_draft and not phone_script:
                    if lead.get("Website_Category"):
                        if st.button("🤖 Generate Drafts", key=f"gen_{lid}"):
                            count = _generate_drafts([lid], st.container())
                            if count:
                                reload()
                    else:
                        st.info("Run research first (`python crm.py research`), then generate from Dashboard.")
