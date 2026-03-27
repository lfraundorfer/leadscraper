# Go Live Checklist

This is the recommended order for putting the hybrid CRM live:

- local machine runs `scrape`, `enrich`, `research`, and `analyze`
- Streamlit hosts the private review UI
- Supabase Postgres stores the live data
- GitHub Actions runs scheduled sending
- Hostinger SMTP sends the emails

## 1. Prepare Accounts

You need:

- a GitHub repo with this project
- a Supabase account
- a Streamlit Community Cloud account
- your Hostinger email inbox and SMTP password
- your OpenAI API key
- your Google Places API key

## 2. Create The Supabase Project

1. Create a new Supabase project.
2. Open the project settings.
3. Find the Postgres connection details.
4. Copy the connection string.
5. Prefer the session pooler connection string for this app.
6. Save it as `DATABASE_URL`.

## 3. Set Up Local `.env`

Put these values into your local `.env`:

```env
CRM_BACKEND=postgres
DATABASE_URL=postgresql://...
OPENAI_API_KEY=...
GOOGLE_PLACES_API_KEY=...
SMTP_HOST=smtp.hostinger.com
SMTP_PORT=465
SMTP_USER=you@your-domain.com
SMTP_PASS=your-mailbox-password
SENDER_NAME=Your Name
SENDER_EMAIL=you@your-domain.com
SENDER_PHONE=+43...
SENDER_COMPANY=Your Company
SENDER_WEBSITE=https://your-site.com
TZ=Europe/Vienna
```

## 4. Install Dependencies Locally

Run:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## 5. Import Existing Data Into Postgres

If your current campaigns and leads still live locally:

```bash
python crm.py bootstrap-postgres
```

This imports:

- campaign configs
- hooks library
- template overrides
- lead rows
- missing lead IDs

## 6. Verify Locally Against Postgres

Run:

```bash
streamlit run app.py
```

Check that:

1. campaigns load
2. the right lead counts show up
3. Review Queue works
4. Outreach works
5. Template Editor works
6. `python crm.py send-scheduled --dry-run` runs cleanly

## 7. Test One Full Lead Locally

Run the normal pipeline:

```bash
python crm.py scrape
python crm.py enrich
python crm.py research
python crm.py analyze
```

Then:

1. open the app
2. approve one draft
3. queue it for `Send Today` or `Send Tomorrow`
4. run:
   ```bash
   python crm.py send-scheduled --dry-run
   ```
5. confirm that the lead appears in the due queue when expected

If you want a real local delivery test, send one email to your own inbox:

```bash
python crm.py send-scheduled
```

## 8. Push To GitHub

Commit and push the repo once local verification looks right.

## 9. Deploy The Streamlit App

1. Open Streamlit Community Cloud.
2. Create a new app from your GitHub repo.
3. Select `app.py` as the entrypoint.
4. Deploy it.

## 10. Add Streamlit Secrets

In the Streamlit app settings, add secrets like this:

```toml
CRM_BACKEND="postgres"
DATABASE_URL="postgresql://..."
OPENAI_API_KEY="..."
GOOGLE_PLACES_API_KEY="..."
SMTP_HOST="smtp.hostinger.com"
SMTP_PORT="465"
SMTP_USER="you@your-domain.com"
SMTP_PASS="your-mailbox-password"
SENDER_NAME="Your Name"
SENDER_EMAIL="you@your-domain.com"
SENDER_PHONE="+43..."
SENDER_COMPANY="Your Company"
SENDER_WEBSITE="https://your-site.com"
TZ="Europe/Vienna"
```

## 11. Make The Streamlit App Private

In Streamlit settings:

1. open sharing/access
2. restrict the app so it is not public
3. confirm only you can open it

## 12. Add GitHub Actions Secrets

In your GitHub repo settings, add:

- `DATABASE_URL`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `TZ`

Recommended values:

- `SMTP_HOST=smtp.hostinger.com`
- `SMTP_PORT=465`
- `TZ=Europe/Vienna`

## 13. Test The Workflow Manually

In GitHub Actions:

1. open `Send Scheduled Emails`
2. run it manually
3. first use:
   - `dry_run=true`
   - `limit=10`
4. confirm the output matches the leads you expect

## 14. Test One Real Hosted Send

1. queue one test lead to your own inbox
2. run the GitHub workflow manually with:
   - `dry_run=false`
   - `limit=10`
3. confirm:
   - the email arrives
   - the lead is no longer queued
   - the contact log updates
   - `Sent_At` is filled

## 15. Start Real Operation

Once that works:

1. leave the hourly GitHub schedule enabled
2. use the live app normally

Normal daily flow:

1. run local scrape/research/analyze
2. open Streamlit
3. review drafts
4. queue today/tomorrow sends
5. let GitHub Actions send automatically

## 16. First Week Safety Rules

For the first few days:

1. keep send volume low
2. test one email to yourself each day
3. check GitHub Actions logs daily
4. check the app for queue errors
5. make sure Hostinger is not rate-limiting your mailbox

## 17. If Something Goes Wrong

If sending fails:

1. do not blindly rerun everything
2. check whether the lead is still `queued`
3. run:
   ```bash
   python crm.py send-scheduled --dry-run
   ```
4. fix the cause
5. retry a very small batch first
