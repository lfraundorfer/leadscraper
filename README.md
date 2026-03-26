# Installateur Wien CRM

AI-powered outreach automation for Installateurbetriebe in Wien.
Scrapes leads → researches websites + Google → generates personalized messages → lets you send email / open WhatsApp in one click.

---

## Quick Start

Drop your flyer as flyer.png in the project folder, then add to .env:

FLYER_IMAGE=flyer.png
That's it — the code in crm_mailer.py already handles it. When send_email() runs, it:

Checks if flyer.png exists
Embeds it inline at the bottom of the HTML via Content-ID: <flyer>
No click needed — the image just shows up in the email body

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium

cp .env.example .env
# → fill in your keys (see setup below)

python crm.py migrate          # add CRM columns to CSV
python crm.py research         # fetch websites + Google reviews
python crm.py analyze          # generate message drafts
streamlit run app.py           # open dashboard
```

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

### 4. Sender Identity (shown in emails + messages)

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

### 6. Pricing Config

```
PRICE_DEFAULT=500       # one-time price shown in email/WhatsApp (€)
PRICE_MONTHLY=25        # monthly hosting fee shown in templates
```

Override per lead in the **All Leads** dashboard (Price field, editable inline).

---

### 7. Portfolio URLs

Shown as example links in email templates.

```
PORTFOLIO_URLS=https://installateur-wien.megaphonia.com
# https://installateur-muster.megaphonia.com
# https://muster-installateur.megaphonia.com
# https://instant-install.megaphonia.com
```

Uncomment the lines you want included. All active URLs appear in emails; only the first appears in WhatsApp.

Drop PNG screenshots into a `portfolio/` folder — they'll appear in the dashboard alongside drafts.

---

## CLI Commands

```bash
python crm.py migrate                             # one-time: add CRM columns + assign IDs
python crm.py enrich [--id INSTWIEN-0001]         # fill contact names from FirmenABC
python crm.py research [--id X] [--from X]        # website + Google reviews + rank
python crm.py analyze [--id X] [--limit 50]       # generate AI message drafts (1 GPT call/lead)
python crm.py analyze --gpt-hooks                 # use GPT for custom hooks (2 GPT calls/lead)
python crm.py generate-hooks                      # generate hook library via GPT (run once)
python crm.py daily                               # today's action list
python crm.py log INSTWIEN-0001 sent              # log a contact attempt
python crm.py log INSTWIEN-0001 called --notes "Left voicemail"
python crm.py send-email INSTWIEN-0001            # send email via SMTP
python crm.py send-email INSTWIEN-0001 --dry-run  # preview without sending
python crm.py stats                               # pipeline overview
```

---

## Dashboard (Streamlit)

```bash
streamlit run app.py
```

| Page             | What it does                                                           |
| ---------------- | ---------------------------------------------------------------------- |
| **Dashboard**    | KPIs, pipeline funnel, today's actions, bulk draft generation          |
| **Review Queue** | Review AI drafts with research data → approve or edit before sending   |
| **Outreach**     | One-click Send Email / Open WhatsApp / phone script for approved leads |
| **All Leads**    | Search, filter, sort all leads; edit price and notes inline            |

---

## Pipeline Flow

```
new → draft_ready → approved → contacted → replied → meeting_scheduled → won
                                                                        → lost
                                         → (3x no reply, 14 days)     → no_contact
```

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
├── crm.py              CLI entry point
├── crm_store.py        CSV persistence (atomic reads/writes)
├── crm_research.py     Website fetch + Google Places API + rank checking
├── crm_enrich.py       Contact name enrichment from FirmenABC
├── crm_analyze.py      OpenAI website scoring + pain point detection
├── crm_templates.py    Pre-written message templates + hook/subject library
├── crm_mailer.py       SMTP email sender + WhatsApp link generator
├── crm_tracker.py      Contact logging + follow-up scheduling
├── crm_daily.py        Daily action list + pipeline stats
├── app.py              Streamlit dashboard
├── hooks_library.json  GPT-generated hooks (created by generate-hooks, edit freely)
├── new_leads.csv       Lead database
├── screenshots/        Mobile screenshots (INSTWIEN-0001_mobile.png)
├── portfolio/          Your portfolio PNGs (shown in dashboard)
├── .env                Your secrets — never commit this
└── .env.example        Template — copy to .env and fill in
```
