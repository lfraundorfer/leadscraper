# Multi-Niche Campaign CRM

Hybrid outreach workflow for local service businesses:

- run `scrape`, `enrich`, `research`, and `analyze` locally
- store campaigns and leads either in local CSVs or in hosted Postgres
- review, approve, queue, and monitor outreach in Streamlit
- send emails through your existing SMTP account

The current v1 target is:

- `Streamlit Community Cloud` for the private dashboard
- `Supabase Postgres` for hosted data
- `GitHub Actions` for scheduled sends
- `Hostinger SMTP` for real email delivery

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

cp .env.example .env
# fill in OpenAI, Google Places, SMTP, and optionally DATABASE_URL

streamlit run app.py
```

## Backends

### Local-only mode

Keep `.env` like this:

```env
CRM_BACKEND=csv
```

Everything stays in local campaign folders and CSV files.

### Hybrid hosted mode

Use a hosted Postgres database and switch to:

```env
CRM_BACKEND=postgres
DATABASE_URL=postgresql://...
TZ=Europe/Vienna
```

Then import your current local data once:

```bash
python crm.py bootstrap-postgres
```

After that:

- local CLI commands write directly to Postgres
- Streamlit reads the same hosted data
- no CSV upload step is needed

## Daily Workflow

1. Run local pipeline commands:

```bash
python crm.py scrape
python crm.py enrich
python crm.py research
python crm.py analyze
```

2. Open the Streamlit app and review drafts.
3. Use one of:
   - `Approve`
   - `Send Today`
   - `Send Tomorrow`
4. Scheduled emails are sent later by `python crm.py send-scheduled`.

## CLI Commands

```bash
python crm.py campaigns
python crm.py campaign-create Schluesseldienst Wien
python crm.py campaign-activate schluesseldienst_wien

python crm.py scrape
python crm.py migrate
python crm.py enrich --id SCHLWIEN-0001
python crm.py research --id SCHLWIEN-0001
python crm.py analyze --limit 50
python crm.py refresh-drafts

python crm.py bootstrap-postgres
python crm.py bootstrap-postgres --force

python crm.py send-email SCHLWIEN-0001 --dry-run
python crm.py send-scheduled --dry-run
python crm.py send-scheduled

python crm.py daily
python crm.py stats
```

## Streamlit Pages

- `Campaigns`: create/activate campaigns, edit config, edit hooks/templates, run pipeline stages
- `Dashboard`: KPIs and batch draft generation
- `Review Queue`: review drafts, approve, queue for today/tomorrow
- `Outreach`: manual email/WhatsApp/phone actions, queued send overview
- `All Leads`: filter/search the whole active campaign

## Scheduled Sending

Queued send rules:

- `Approve only` keeps the draft approved without scheduling a send
- `Send Today` queues for today at `17:00 Europe/Vienna` if still before that time, otherwise next business day `09:00`
- `Send Tomorrow` queues for the next business day at `09:00 Europe/Vienna`

The sender only processes leads where:

- `Scheduled_Send_Status = queued`
- `Scheduled_Send_Channel = email`
- `Scheduled_Send_At <= now`

Run it manually:

```bash
python crm.py send-scheduled
```

Or schedule it with GitHub Actions using `.github/workflows/send-scheduled.yml`.

## Environment Variables

Required for the hybrid hosted setup:

```env
CRM_BACKEND=postgres
DATABASE_URL=postgresql://...
OPENAI_API_KEY=...
GOOGLE_PLACES_API_KEY=...
SMTP_HOST=smtp.hostinger.com
SMTP_PORT=465
SMTP_USER=you@your-domain.com
SMTP_PASS=...
SENDER_NAME=Linus
SENDER_EMAIL=you@your-domain.com
SENDER_PHONE=+43...
SENDER_COMPANY=...
SENDER_WEBSITE=https://...
TZ=Europe/Vienna
```

## What Changed In This Version

- added a shared `csv | postgres` backend switch
- moved hooks/template overrides into campaign JSON for Postgres mode
- removed flyer, screenshots, portfolio images, and portfolio URL copy from the v1 flow
- added scheduled email queue fields and processing
- kept WhatsApp and phone as manual actions
- kept heavy scraping/research/analyze work local

## Files To Know

```text
crm.py              CLI entry point
crm_scrape.py       Backend-aware scraping
crm_backend.py      Postgres schema and persistence helpers
campaign_service.py Campaign config + active campaign handling
crm_store.py        Lead load/save logic
crm_schedule.py     Vienna scheduling rules
crm_scheduled.py    Queued email sender
crm_mailer.py       SMTP sending
crm_tracker.py      Contact logging and follow-up logic
crm_templates.py    Hooks, templates, and draft rendering
app.py              Streamlit dashboard
```
