# Hybrid CRM Implementation Plan

This repository now targets a hybrid setup:

- local machine runs `scrape`, `enrich`, `research`, and `analyze`
- hosted Streamlit is the private review and queue dashboard
- hosted Postgres is the shared source of truth
- Hostinger SMTP sends real emails
- GitHub Actions runs scheduled queued sends

## Operational Checklist

1. Set `.env` locally:
   - `CRM_BACKEND=postgres`
   - `DATABASE_URL=...`
   - `OPENAI_API_KEY=...`
   - `GOOGLE_PLACES_API_KEY=...`
   - `SMTP_HOST=smtp.hostinger.com`
   - `SMTP_PORT=465`
   - `SMTP_USER=...`
   - `SMTP_PASS=...`
   - `TZ=Europe/Vienna`
2. Import existing local campaigns:
   - `python crm.py bootstrap-postgres`
3. Run the local pipeline as usual:
   - `python crm.py scrape`
   - `python crm.py enrich`
   - `python crm.py research`
   - `python crm.py analyze`
4. Review and queue drafts in Streamlit.
5. Configure GitHub Actions secrets:
   - `DATABASE_URL`
   - `SMTP_HOST`
   - `SMTP_PORT`
   - `SMTP_USER`
   - `SMTP_PASS`
   - `TZ`
6. Enable `.github/workflows/send-scheduled.yml`.

## Acceptance Targets

- local scrape/research/analyze writes directly to Postgres
- Streamlit shows the same data without file uploads
- queued sends respect Vienna business-day scheduling
- Hostinger SMTP is used for actual delivery
- media-related portfolio/flyer workflow is out of v1
