# Multi-Niche Campaign CRM

AI-powered outreach automation for local service businesses.
Create saved campaigns like `Installateur Wien` or `Schluesseldienst Wien`, scrape leads from Herold, research them, generate niche-specific drafts, and run outreach from one dashboard.

---

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium

cp .env.example .env
# -> fill in your API keys / SMTP settings

streamlit run app.py           # open dashboard
```

Then in the app:

1. Open **Campaigns**
2. Create a campaign from `keyword + location`
3. Run `Scrape -> Migrate -> Enrich -> Research -> Analyze`
4. Review drafts, approve them, and send outreach

---

## Setup: What You Need to Fill In

### 1. OpenAI API Key (required)

Used for website scoring and (optionally) custom hooks.

1. Go to [platform.openai.com](https://platform.openai.com) → API keys → Create new key
2. Add to `.env`:
   ```
   OPENAI_API_KEY=sk-proj-...
   ```

**Cost:** Website analysis ≈ $0.01–0.02 per lead (gpt-4o). Hooks use 0 tokens by default (pre-written library).

---

### 2. Google Places API Key (required for Google reviews + ratings)

Used to fetch star ratings, review count, and review snippets for each lead.

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a project (or use existing)
3. Enable **"Places API (New)"** — not the old Places API
4. Create an API key: APIs & Services → Credentials → Create credentials → API key
5. Add to `.env`:
   ```
   GOOGLE_PLACES_API_KEY=AIza...
   ```

**Cost:** Google gives $200/month free credit. 940 leads ≈ $30 one-time.

---

### 3. Email Sending via SMTP (required for Send Email button)

#### Option A: Gmail (recommended)

Gmail requires an **App Password** — your regular password won't work with SMTP.

1. Go to [myaccount.google.com](https://myaccount.google.com) → Security
2. Make sure **2-Step Verification** is enabled
3. Search for "App passwords" → Create one → name it "CRM"
4. Copy the 16-character password shown
5. Add to `.env`:
   ```
   SMTP_HOST=smtp.gmail.com
   SMTP_PORT=465
   SMTP_USER=your@gmail.com
   SMTP_PASS=xxxx xxxx xxxx xxxx
   SENDER_EMAIL=your@gmail.com
   ```

#### Option B: Strato / GMX / other provider

```
SMTP_HOST=smtp.strato.de        # or smtp.gmx.net, mail.your-domain.com, etc.
SMTP_PORT=465
SMTP_USER=your@domain.com
SMTP_PASS=your-password
SENDER_EMAIL=your@domain.com
```

> Port 465 uses SSL (default). If your provider uses STARTTLS, change to port 587.

---

### 4. Sender Identity

These values are used as defaults when creating a new campaign. You can override them per campaign in the dashboard.

```
SENDER_NAME=Linus
SENDER_EMAIL=linus@megaphonia.com
SENDER_PHONE=+43 XXX XXXXXXX
SENDER_COMPANY=Digitalagentur Megaphonia
SENDER_WEBSITE=https://www.megaphonia.com
```

---

### 5. WhatsApp Sending

**No API setup needed.** The "Open WhatsApp" button in the Outreach page generates a `wa.me/` deep link that opens WhatsApp Web (or the app) with the message pre-filled. You just hit Send.

Requirements:

- The lead must have a mobile phone number in `TelNr`
- Austrian mobile numbers are auto-detected (06xx or +436xx)

After opening WhatsApp and sending manually, click **"Mark as Sent"** in the dashboard to log it.

#### Optional: Automated WhatsApp via Twilio (no manual step)

If you want fully automated sending without opening WhatsApp manually:

1. Sign up at [twilio.com](https://www.twilio.com)
2. Enable the **WhatsApp sandbox** (free for testing) or buy a WhatsApp Business number
3. Add to `.env`:
   ```
   TWILIO_ACCOUNT_SID=AC...
   TWILIO_AUTH_TOKEN=...
   TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
   ```
4. `pip install twilio`

---

### 5. Campaign Config vs .env

`.env` is now only for infrastructure and secrets:

- OpenAI API key
- Google Places API key
- SMTP settings
- default sender identity used to seed new campaigns

Each saved campaign owns its own:

- keyword + location
- leads CSV
- flyer
- example / portfolio links
- portfolio images
- prices
- sender-facing copy
- outreach wording and rank keywords

---

## CLI Commands

```bash
python crm.py campaigns
python crm.py campaign-create Schluesseldienst Wien
python crm.py campaign-activate schluesseldienst_wien
python crm.py scrape                              # scrape active campaign from Herold
python crm.py migrate                             # add CRM columns + assign campaign IDs
python crm.py enrich [--id SCHLWIEN-0001]
python crm.py research [--id X] [--from X]
python crm.py analyze [--id X] [--limit 50]
python crm.py analyze --gpt-hooks
python crm.py generate-hooks                      # writes the active campaign hook library
python crm.py daily
python crm.py log SCHLWIEN-0001 sent
python crm.py send-email SCHLWIEN-0001 --dry-run
python crm.py stats
```

---

## Dashboard (Streamlit)

```bash
streamlit run app.py
```

| Page             | What it does                                                           |
| ---------------- | ---------------------------------------------------------------------- |
| **Campaigns**    | Create / activate campaigns, edit niche config, upload flyer/assets, run pipeline stages |
| **Dashboard**    | KPIs, pipeline funnel, today's actions, bulk draft generation          |
| **Review Queue** | Review AI drafts with research data → approve or edit before sending   |
| **Outreach**     | One-click Send Email / Open WhatsApp / phone script for approved leads |
| **All Leads**    | Search and filter active-campaign leads; inspect stale research/drafts |

---

## Pipeline Flow

```
new → draft_ready → approved → contacted → replied → meeting_scheduled → won
                                                                        → lost
                                         → (3x no reply, 14 days)     → no_contact
```

Campaign config changes mark old drafts / research as stale. Re-run the relevant step to refresh them.

---

## Token Usage (OpenAI)

| Operation                                      | Cost (gpt-4o)    |
| ---------------------------------------------- | ---------------- |
| Website analysis per lead                      | ~$0.015          |
| Custom hook per lead (`--gpt-hooks`)           | ~$0.005 extra    |
| Hook library generation (once, all categories) | ~$0.03 total     |
| **Default run (no `--gpt-hooks`)**             | **~$0.015/lead** |

940 leads × $0.015 ≈ **$14 total** for a full analysis run.

---

## File Structure

```
├── crm.py                 CLI entry point
├── campaign_service.py    Saved campaign registry + active campaign resolution
├── crm_store.py           CSV persistence for the active campaign
├── crm_research.py        Website fetch + Google Places API + rank checking
├── crm_enrich.py          Contact name enrichment from FirmenABC
├── crm_analyze.py         OpenAI website scoring + draft generation
├── crm_templates.py       Campaign-aware templates + hook library handling
├── crm_mailer.py          SMTP email sender + WhatsApp link generator
├── crm_tracker.py         Contact logging + follow-up scheduling
├── crm_daily.py           Daily action list + pipeline stats
├── app.py                 Streamlit dashboard
├── campaigns/             Saved campaign configs, assets, portfolio, and CSVs
├── .env                   Your secrets / infrastructure config
└── .env.example           Template for .env
```
